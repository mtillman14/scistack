"""
Tests for the Variant wrapper — per-input branch_param pinning for for_each / load.

Variant pins a for_each input (or a .load()) to a specific branch_param variant.
It is an orthogonal, load-time filter, composable with Fixed, ColumnSelection,
and Merge (per-constituent), and order-agnostic with respect to Fixed.

Covers:
- Construction guards (Merge, EachOf, empty, nested-conflict)
- to_key() / __name__ canonical strings
- Pinning a plain input
- Variant + Fixed (both orders load identically)
- Variant + ColumnSelection
- Variant inside Merge (per-constituent)
- EachOf(Variant(...), Variant(...)) runs once per variant, concatenated
- Aggregation no longer smushes variants when pinned (the motivating case)
- Variant(Merge(...)) raises, Variant(EachOf(...)) raises, conflict raises
"""

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import (
    BaseVariable,
    EachOf,
    Fixed,
    Merge,
    Variant,
    branch_param,
    configure_database,
    for_each,
)


def test_branch_param_factory_builds_namespaced_dict():
    """branch_param(fn, **params) builds the namespaced filter without a dotted kwarg."""
    assert branch_param("bandpass", low_hz=30) == {"bandpass.low_hz": 30}
    assert branch_param("fn", a=1, b=2) == {"fn.a": 1, "fn.b": 2}


# ---------------------------------------------------------------------------
# Schema and fixtures
# ---------------------------------------------------------------------------

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    """Fresh database with subject/session schema for each test."""
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_variant.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


# ---------------------------------------------------------------------------
# Variable types
# ---------------------------------------------------------------------------


class RawSignal(BaseVariable):
    pass


class FilteredEMG(BaseVariable):
    pass


class Force(BaseVariable):
    pass


class Result(BaseVariable):
    pass


class Aggregated(BaseVariable):
    pass


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------


def bandpass(signal, low_hz):
    """Filter with a parameter that creates branch_param variants."""
    return signal * low_hz


def scale(x, factor=1.0):
    """Identity-ish op used to consume a pinned input."""
    if isinstance(x, np.ndarray):
        return x * factor
    return x * factor


def aggregate_sum(signal):
    """Sum numeric values from the aggregated input."""
    if isinstance(signal, pd.DataFrame):
        return float(signal.select_dtypes(include="number").values.sum())
    if isinstance(signal, np.ndarray):
        return float(np.asarray(signal, dtype=float).sum())
    return float(signal)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_two_variants(db, subjects=("S01",), sessions=("1",)):
    """Save RawSignal then create low_hz=20 and low_hz=50 FilteredEMG variants."""
    for subj in subjects:
        for sess in sessions:
            RawSignal.save(np.array([1.0, 2.0, 3.0]), subject=subj, session=sess)
    for low_hz in [20, 50]:
        for_each(
            bandpass,
            {"signal": RawSignal, "low_hz": low_hz},
            [FilteredEMG],
            subject=list(subjects),
            session=list(sessions),
        )


# ---------------------------------------------------------------------------
# 1. Construction guards
# ---------------------------------------------------------------------------


class TestVariantConstruction:
    def test_rejects_merge(self):
        with pytest.raises(TypeError, match="cannot wrap a Merge"):
            Variant(Merge(FilteredEMG, Force), low_hz=20)

    def test_rejects_each_of(self):
        with pytest.raises(TypeError, match="cannot wrap an EachOf"):
            Variant(EachOf(FilteredEMG, Force), low_hz=20)

    def test_requires_branch_params(self):
        with pytest.raises(ValueError, match="at least one branch_param"):
            Variant(FilteredEMG)

    def test_nested_variant_merges(self):
        v = Variant(Variant(FilteredEMG, low_hz=20), threshold=0.5)
        assert v.var_type is FilteredEMG
        assert v.branch_params == {"low_hz": 20, "threshold": 0.5}

    def test_nested_variant_conflict_raises(self):
        with pytest.raises(ValueError, match="Conflicting branch_param"):
            Variant(Variant(FilteredEMG, low_hz=20), low_hz=50)

    def test_fn_namespaces_branch_params(self):
        """fn= disambiguation namespaces params (no dotted-string kwarg needed)."""
        v = Variant(FilteredEMG, fn="bandpass", low_hz=20)
        assert v.branch_params == {"bandpass.low_hz": 20}

    def test_fn_equivalent_to_dotted_kwarg(self):
        a = Variant(FilteredEMG, fn="detect_spikes", threshold=0.5)
        b = Variant(FilteredEMG, **{"detect_spikes.threshold": 0.5})
        assert a.branch_params == b.branch_params == {"detect_spikes.threshold": 0.5}

    def test_fn_requires_a_param(self):
        with pytest.raises(ValueError, match="at least one branch_param"):
            Variant(FilteredEMG, fn="bandpass")


# ---------------------------------------------------------------------------
# 2. to_key / __name__
# ---------------------------------------------------------------------------


class TestVariantKeys:
    def test_to_key_plain(self):
        assert (
            Variant(FilteredEMG, low_hz=20).to_key()
            == "Variant(FilteredEMG, low_hz=20)"
        )

    def test_to_key_sorted(self):
        v = Variant(FilteredEMG, low_hz=20, threshold=0.5)
        assert v.to_key() == "Variant(FilteredEMG, low_hz=20, threshold=0.5)"

    def test_to_key_wraps_fixed(self):
        v = Variant(Fixed(FilteredEMG, session="BL"), low_hz=20)
        assert v.to_key() == "Variant(Fixed(FilteredEMG, session='BL'), low_hz=20)"

    def test_name_property(self):
        assert (
            Variant(FilteredEMG, low_hz=20).__name__
            == "Variant(FilteredEMG, low_hz=20)"
        )


# ---------------------------------------------------------------------------
# 3. Pinning a plain input
# ---------------------------------------------------------------------------


class TestVariantPinsPlainInput:
    def test_pins_single_variant(self, db):
        _make_two_variants(db)

        # Without pinning, two FilteredEMG variants exist at S01/1
        assert len(db.list_versions(FilteredEMG, subject="S01", session="1")) == 2

        # Pin to low_hz=20: scale runs over only that variant
        result = for_each(
            scale,
            {"x": Variant(FilteredEMG, low_hz=20)},
            [Result],
            subject=["S01"],
            session=["1"],
            save=False,
        )
        assert result is not None
        assert len(result) == 1
        val = result["Result"].iloc[0]
        val = val.sum() if isinstance(val, np.ndarray) else val
        # low_hz=20 variant is [20, 40, 60] → sum 120
        assert val == 120.0

    def test_pins_other_variant(self, db):
        _make_two_variants(db)
        result = for_each(
            scale,
            {"x": Variant(FilteredEMG, low_hz=50)},
            [Result],
            subject=["S01"],
            session=["1"],
            save=False,
        )
        val = result["Result"].iloc[0]
        val = val.sum() if isinstance(val, np.ndarray) else val
        # low_hz=50 variant is [50, 100, 150] → sum 300
        assert val == 300.0

    def test_load_with_branch_param_kwarg(self, db):
        """Sanity: plain .load() filters by namespaced branch param suffix."""
        _make_two_variants(db)
        f = FilteredEMG.load(subject="S01", session="1", low_hz=20)
        np.testing.assert_array_equal(f.data, np.array([20.0, 40.0, 60.0]))


# ---------------------------------------------------------------------------
# 4. Variant + Fixed (order-agnostic)
# ---------------------------------------------------------------------------


class TestVariantWithFixed:
    def test_both_orders_load_identical_data(self, db):
        """Fixed(Variant(...)) and Variant(Fixed(...)) load identically.

        Asserts at the loader layer (order-agnostic composition is the plan's
        concern) to avoid coupling to scifor.Fixed's per-combo broadcast.
        """
        from scidb.foreach import _load_input

        # Two sessions, baseline "BL" and current "EX"
        RawSignal.save(2.0, subject="S01", session="BL")
        RawSignal.save(5.0, subject="S01", session="EX")
        for low_hz in [20, 50]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [FilteredEMG],
                subject=["S01"],
                session=["BL", "EX"],
            )

        spec_a = Fixed(Variant(FilteredEMG, low_hz=20), session="BL")
        spec_b = Variant(Fixed(FilteredEMG, session="BL"), low_hz=20)

        loaded_a = _load_input(spec_a, db, None)
        loaded_b = _load_input(spec_b, db, None)

        # Both resolve to a scifor.Fixed wrapping the same pinned data.
        assert loaded_a.fixed_metadata == loaded_b.fixed_metadata == {"session": "BL"}
        df_a = loaded_a.data.reset_index(drop=True)
        df_b = loaded_b.data.reset_index(drop=True)
        pd.testing.assert_frame_equal(df_a, df_b)

        # Pinned to low_hz=20: values are RawSignal * 20, never * 50.
        vals = sorted(df_a["FilteredEMG"].tolist())
        assert vals == [40.0, 100.0]  # BL: 2*20=40, EX: 5*20=100 (no *50 rows)


# ---------------------------------------------------------------------------
# 5. Variant + ColumnSelection
# ---------------------------------------------------------------------------


class TestVariantWithColumnSelection:
    def test_variant_wraps_column_selection(self, db):
        # FilteredEMG holds a DataFrame with a "v" column
        def make_frame(signal, low_hz):
            return pd.DataFrame({"v": signal * low_hz})

        for subj in ["S01"]:
            RawSignal.save(np.array([1.0, 2.0, 3.0]), subject=subj, session="1")
        for low_hz in [20, 50]:
            for_each(
                make_frame,
                {"signal": RawSignal, "low_hz": low_hz},
                [FilteredEMG],
                subject=["S01"],
                session=["1"],
            )

        spec = Variant(FilteredEMG["v"], low_hz=20)
        result = for_each(
            scale, {"x": spec}, [Result], subject=["S01"], session=["1"], save=False
        )
        val = result["Result"].iloc[0]
        val = val.sum() if isinstance(val, np.ndarray) else val
        assert val == 120.0


# ---------------------------------------------------------------------------
# 6. Variant inside Merge (per-constituent)
# ---------------------------------------------------------------------------


class TestVariantInMerge:
    def test_per_constituent_pinning(self, db):
        """Variant pins one Merge constituent; the merged table has one row.

        Uses scalar data + as_table aggregation (the proven Merge test pattern):
        the function is called once with the full merged table, so we can assert
        directly on its contents.
        """
        captured = []

        def collect(merged):
            captured.append(merged.copy())
            return 0.0

        RawSignal.save(2.0, subject="S01", session="1")
        for low_hz in [20, 50]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [FilteredEMG],
                subject=["S01"],
                session=["1"],
            )
        # Force has a single variant
        Force.save(7.0, subject="S01", session="1")

        spec = Merge(Variant(FilteredEMG, low_hz=20), Force)
        for_each(collect, {"merged": spec}, [Result], as_table=True, save=False)

        assert len(captured) == 1
        df = captured[0]
        # Pinning excludes the low_hz=50 variant → exactly one merged row
        # (FilteredEMG=2*20=40 joined with Force=7). Without pinning there would
        # be two rows (one per FilteredEMG variant).
        assert len(df) == 1
        numeric_sum = float(df.select_dtypes(include="number").values.sum())
        assert numeric_sum == 40.0 + 7.0


# ---------------------------------------------------------------------------
# 7. EachOf(Variant, Variant) — once per variant, concatenated
# ---------------------------------------------------------------------------


class TestEachOfVariant:
    def test_runs_once_per_pinned_variant(self, db):
        _make_two_variants(db)
        result = for_each(
            scale,
            {
                "x": EachOf(
                    Variant(FilteredEMG, low_hz=20), Variant(FilteredEMG, low_hz=50)
                )
            },
            [Result],
            subject=["S01"],
            session=["1"],
            save=False,
        )
        assert result is not None
        # Two alternatives → two concatenated rows
        assert len(result) == 2
        vals = sorted(
            (v.sum() if isinstance(v, np.ndarray) else v) for v in result["Result"]
        )
        assert vals == [120.0, 300.0]


# ---------------------------------------------------------------------------
# 8. Aggregation no longer smushes variants when pinned (motivating case)
# ---------------------------------------------------------------------------


class TestVariantFixesAggregationSmushing:
    def test_pinned_aggregation_sees_one_variant(self, db):
        for sess in ["1", "2"]:
            RawSignal.save(1.0, subject="S01", session=sess)
        for low_hz in [20, 50]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [FilteredEMG],
                subject=["S01"],
                session=["1", "2"],
            )

        # Without pinning, full aggregation AUTO-SPLITS per variant group (D1):
        # low_hz=20 → 20+20 = 40, low_hz=50 → 50+50 = 100 — one call each.
        # (Pre-D1 it pooled all 4 records into a single 140.0 "smush".)
        unpinned = for_each(
            aggregate_sum, {"signal": FilteredEMG}, [Aggregated], save=False
        )
        assert len(unpinned) == 2
        assert sorted(float(v) for v in unpinned["Aggregated"]) == [40.0, 100.0]

        # Pinned to low_hz=20: aggregation only sees the 2 matching records → 40.
        pinned = for_each(
            aggregate_sum,
            {"signal": Variant(FilteredEMG, low_hz=20)},
            [Aggregated],
            save=False,
        )
        assert len(pinned) == 1
        assert pinned["Aggregated"].iloc[0] == 40.0


# ---------------------------------------------------------------------------
# 9. where= + Variant coexist
# ---------------------------------------------------------------------------


class TestVariantWithWhere:
    def test_where_and_branch_param_coexist_in_load(self, db):
        """where= and branch_params filtering coexist on the load() path.

        Exercises database._load_with_where followed by the branch_params
        post-step (the Variant mechanism at the leaf-load level).
        """
        from scidb import schema_key

        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session=sess)
        for low_hz in [20, 50]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [FilteredEMG],
                subject=["S01"],
                session=["1", "2"],
            )

        # where restricts to session 1 (2 variants there); branch param pins low_hz=20.
        results = FilteredEMG.load(where=schema_key("session") == "1", low_hz=20)
        # Exactly one record: session 1, low_hz=20
        if isinstance(results, list):
            assert len(results) == 1
            results = results[0]
        np.testing.assert_array_equal(results.data, np.array([20.0, 40.0, 60.0]))
        assert str(results.metadata["session"]) == "1"

    def test_where_and_variant_compose_in_for_each(self, db):
        from scidb import schema_key

        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session=sess)
        for low_hz in [20, 50]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [FilteredEMG],
                subject=["S01"],
                session=["1", "2"],
            )

        # where restricts to session 1; Variant pins low_hz=20.
        result = for_each(
            scale,
            {"x": Variant(FilteredEMG, low_hz=20)},
            [Result],
            where=schema_key("session") == "1",
            subject=["S01"],
            session=["1"],
            save=False,
        )
        assert len(result) == 1
        val = result["Result"].iloc[0]
        val = val.sum() if isinstance(val, np.ndarray) else val
        assert val == 120.0


# ---------------------------------------------------------------------------
# VariantAxes — the three dimensions, parsed once
# ---------------------------------------------------------------------------


class TestVariantAxes:
    """`branch_params` carries three axes in one dict, keyed by reserved
    prefixes (docs/claude/variant-space.md §2). Four places used to re-parse
    those prefixes: the loader's three-way split, the code filter, the run
    filter, and the plotting layer's column mapping."""

    def _axes(self):
        from scidb.variant import VariantAxes

        return VariantAxes.of(
            {
                "bandpass.low_hz": 20,
                "__save__.run": "A",
                "__code__": "latest",
                "__code__.bandpass": "v2",
                "__run__.loader": "distribute=true",
            }
        )

    def test_splits_the_three_dimensions(self):
        axes = self._axes()
        assert axes.constants == {"bandpass.low_hz": 20, "__save__.run": "A"}
        assert axes.code == {None: "latest", "bandpass": "v2"}
        assert axes.run == {"loader": "distribute=true"}

    def test_round_trips_to_the_one_dict_spelling(self):
        from scidb.variant import VariantAxes

        raw = {
            "bandpass.low_hz": 20,
            "__code__": "latest",
            "__run__.loader": "distribute=true",
        }
        assert VariantAxes.of(raw).to_dict() == raw

    def test_a_bare_save_kwarg_is_a_constant_not_a_pin(self):
        from scidb.variant import VariantAxes

        axes = VariantAxes.of({"__save__.side": "L"})
        assert axes.constants == {"__save__.side": "L"}
        assert not axes.pins_code_or_run

    def test_pins_code_or_run_is_what_uncollapsed_loading_turns_on(self):
        from scidb.variant import pin_loads_uncollapsed

        assert self._axes().pins_code_or_run
        assert pin_loads_uncollapsed({"__code__": "latest"})
        assert pin_loads_uncollapsed({"__run__.fn": "distribute=true"})
        assert not pin_loads_uncollapsed({"bandpass.low_hz": 20})
        assert not pin_loads_uncollapsed(None)

    def test_empty_is_falsey(self):
        from scidb.variant import VariantAxes

        assert not VariantAxes.of({})
        assert not VariantAxes.of(None)


class TestMatchBareName:
    """One suffix rule, so "which variant do I load" and "what does
    {low_hz} mean in this output path" cannot disagree."""

    def test_exact_wins_over_suffix(self):
        from scidb.variant import match_bare_name

        bp = {"low_hz": 1, "bandpass.low_hz": 2}
        assert match_bare_name(bp, "low_hz") == "low_hz"

    def test_bare_name_suffix_matches(self):
        from scidb.variant import match_bare_name

        assert match_bare_name({"bandpass.low_hz": 2}, "low_hz") == "bandpass.low_hz"
        assert match_bare_name({"__save__.side": "L"}, "side") == "__save__.side"

    def test_absent_is_none_not_an_error(self):
        from scidb.variant import match_bare_name

        assert match_bare_name({"bandpass.low_hz": 2}, "nope") is None

    def test_ambiguous_raises_naming_the_candidates(self):
        import pytest

        from scidb.exceptions import AmbiguousParamError
        from scidb.variant import match_bare_name

        with pytest.raises(AmbiguousParamError) as exc:
            match_bare_name({"a.hz": 1, "b.hz": 2}, "hz")
        assert "a.hz" in str(exc.value) and "b.hz" in str(exc.value)
