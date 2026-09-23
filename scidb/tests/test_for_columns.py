"""
Tests for for_columns — column-wise iteration + reassembly in for_each().

`MyVar.for_columns([...])` (or `.for_columns()` for all columns) runs the
function once per column of a wide-table variable and reassembles the
per-column results into a single output variable whose data, per schema combo,
is a one-row table with the same column names as the source.

Covers:
- all-columns resolution and explicit-subset selection
- output is reassembled into one variable (1 x N, same column names)
- two for_columns inputs zipped by name (baseline Fixed + value)
- mismatched column sets raise
- column drift (a requested column absent) is a hard error
- where= still applies under iteration
- caching: identical re-run is a hit; changing the column set is not
- dry_run returns None
"""

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, ColName, Fixed, PathOutput, configure_database, for_each

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_for_columns.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


# --- Variable types --------------------------------------------------------


class GaitData(BaseVariable):
    pass


class DeltaGait(BaseVariable):
    pass


class OtherData(BaseVariable):
    pass


# --- Pipeline functions ----------------------------------------------------


def col_mean(value):
    """Mean of a single column's values."""
    return float(np.mean(value))


def col_max(value):
    """Max of a single column's values (a different function for cache tests)."""
    return float(np.max(value))


def mean_change(baseline, value):
    """Change in column mean from a baseline."""
    return float(np.mean(value) - np.mean(baseline))


def col_stats(value):
    """Multiple named stats per column -> <col>__mean, <col>__max."""
    return {"mean": float(np.mean(value)), "max": float(np.max(value))}


def col_stats_varying(value):
    """Different number of stats depending on the column's magnitude."""
    if np.max(value) < 5:
        return {"mean": float(np.mean(value))}
    return {"mean": float(np.mean(value)), "max": float(np.max(value))}


# --- Seeding ---------------------------------------------------------------


def _seed_wide(db, session="A"):
    """Two subjects, each a wide table with StepLength + Cadence columns."""
    GaitData.save(
        pd.DataFrame({"StepLength": [1.0, 2.0, 3.0], "Cadence": [10.0, 20.0, 30.0]}),
        db=db,
        subject="1",
        session=session,
    )
    GaitData.save(
        pd.DataFrame({"StepLength": [4.0, 5.0, 6.0], "Cadence": [40.0, 50.0, 60.0]}),
        db=db,
        subject="2",
        session=session,
    )


# ---------------------------------------------------------------------------
# ColumnSelection / for_columns basics
# ---------------------------------------------------------------------------


class TestForColumnsConstruction:
    def test_for_columns_all_sets_iterate(self):
        cs = GaitData.for_columns()
        assert cs.iterate is True
        # Empty list [] is the all-columns sentinel (resolved at for_each time).
        assert cs.columns == []

    def test_for_columns_empty_list_equivalent_to_no_arg(self):
        assert GaitData.for_columns([]).columns == []

    def test_for_columns_none_alias_for_all(self):
        # None is accepted as a backward-compatible alias for the [] sentinel.
        assert GaitData.for_columns(None).columns == []

    def test_for_columns_subset(self):
        cs = GaitData.for_columns(["StepLength", "Cadence"])
        assert cs.iterate is True
        assert cs.columns == ["StepLength", "Cadence"]

    def test_for_columns_single_string(self):
        cs = GaitData.for_columns("StepLength")
        assert cs.columns == ["StepLength"]

    def test_bracket_selection_not_iterate(self):
        assert GaitData["StepLength"].iterate is False

    def test_to_key_includes_iterate_and_columns(self):
        k_iter = GaitData.for_columns(["StepLength"]).to_key()
        k_plain = GaitData["StepLength"].to_key()
        assert "iterate=True" in k_iter
        assert k_iter != k_plain


# ---------------------------------------------------------------------------
# Reassembly into a single output variable
# ---------------------------------------------------------------------------


class TestForColumnsReassembly:
    def test_all_columns_reassembled(self, db):
        _seed_wide(db)

        result = for_each(
            col_mean,
            inputs={"value": GaitData.for_columns()},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )

        # One row per subject, columns mirror the source table.
        assert "StepLength" in result.columns
        assert "Cadence" in result.columns

        # load() returns the object directly for a single match.
        d1 = DeltaGait.load(db=db, subject="1", session="A")
        assert list(d1.data.columns) == ["StepLength", "Cadence"]
        assert d1.data["StepLength"].iloc[0] == pytest.approx(2.0)
        assert d1.data["Cadence"].iloc[0] == pytest.approx(20.0)

        d2 = DeltaGait.load(db=db, subject="2", session="A")
        assert d2.data["StepLength"].iloc[0] == pytest.approx(5.0)
        assert d2.data["Cadence"].iloc[0] == pytest.approx(50.0)

    def test_subset_columns(self, db):
        _seed_wide(db)

        for_each(
            col_mean,
            inputs={"value": GaitData.for_columns(["StepLength"])},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )

        d1 = DeltaGait.load(db=db, subject="1", session="A")
        assert list(d1.data.columns) == ["StepLength"]
        assert d1.data["StepLength"].iloc[0] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# Deferred ColName() — resolves to the current for_columns column
# ---------------------------------------------------------------------------


def col_name_len(value, col_name):
    """Return the length of the current column's name (proves col_name is the
    source column being iterated, not a static single value)."""
    return {"name_len": len(col_name)}


def capture_filename(value, filename):
    """Return the per-column filename so a test can assert templating happened."""
    return {"fname": str(filename)}


class TestForColumnsDeferredColName:
    def test_deferred_colname_resolves_per_column(self, db):
        _seed_wide(db)

        for_each(
            col_name_len,
            inputs={"value": GaitData.for_columns(), "col_name": ColName()},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )

        # Per-column dict return -> "<col>__name_len"; the value is the length
        # of that source column's own name (StepLength=10, Cadence=7).
        d1 = DeltaGait.load(db=db, subject="1", session="A")
        assert d1.data["StepLength__name_len"].iloc[0] == 10
        assert d1.data["Cadence__name_len"].iloc[0] == 7

    def test_deferred_colname_without_iterate_raises(self, db):
        _seed_wide(db)
        with pytest.raises(ValueError, match="requires at least one iterate input"):
            for_each(
                col_name_len,
                inputs={"value": GaitData["StepLength"], "col_name": ColName()},
                outputs=[DeltaGait],
                db=db,
                subject=[],
                session=[],
            )


class TestForColumnsPathOutput:
    def test_pathoutput_resolves_metadata_and_column(self, db, tmp_path):
        """PathOutput substitutes combo metadata ({subject}) and the current
        column ({ColName}), reaching the function as a plain per-column Path."""
        _seed_wide(db)
        root = tmp_path / "out"

        for_each(
            capture_filename,
            inputs={
                "value": GaitData.for_columns(),
                "filename": PathOutput(root / "{subject}_{ColName}_anova2way.pdf"),
            },
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )

        d1 = DeltaGait.load(db=db, subject="1", session="A")
        assert d1.data["StepLength__fname"].iloc[0] == str(
            root / "1_StepLength_anova2way.pdf"
        )
        assert d1.data["Cadence__fname"].iloc[0] == str(
            root / "1_Cadence_anova2way.pdf"
        )

        d2 = DeltaGait.load(db=db, subject="2", session="A")
        assert d2.data["StepLength__fname"].iloc[0] == str(
            root / "2_StepLength_anova2way.pdf"
        )

    def test_pathoutput_colname_token_without_iterate_raises(self, db):
        """{ColName} resolves per-column, so it still requires an iterate input."""
        _seed_wide(db)
        with pytest.raises(ValueError, match="requires at least one iterate input"):
            for_each(
                capture_filename,
                inputs={
                    "value": GaitData["StepLength"],
                    "filename": PathOutput("{ColName}.pdf"),
                },
                outputs=[DeltaGait],
                db=db,
                subject=[],
                session=[],
            )


# ---------------------------------------------------------------------------
# Two for_columns inputs zipped by name (baseline + value)
# ---------------------------------------------------------------------------


class TestForColumnsZip:
    def test_baseline_and_value(self, db):
        # Baseline session BL, value session FV.
        GaitData.save(
            pd.DataFrame({"StepLength": [1.0, 1.0], "Cadence": [10.0, 10.0]}),
            db=db,
            subject="1",
            session="BL",
        )
        GaitData.save(
            pd.DataFrame({"StepLength": [3.0, 3.0], "Cadence": [40.0, 40.0]}),
            db=db,
            subject="1",
            session="FV",
        )

        for_each(
            mean_change,
            inputs={
                "baseline": Fixed(GaitData.for_columns(), session="BL"),
                "value": GaitData.for_columns(),
            },
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=["FV"],
        )

        d = DeltaGait.load(db=db, subject="1", session="FV")
        assert d.data["StepLength"].iloc[0] == pytest.approx(2.0)  # 3 - 1
        assert d.data["Cadence"].iloc[0] == pytest.approx(30.0)  # 40 - 10

    def test_mismatched_column_sets_raise(self, db):
        _seed_wide(db)

        with pytest.raises(ValueError, match="same columns"):
            for_each(
                mean_change,
                inputs={
                    "baseline": GaitData.for_columns(["StepLength"]),
                    "value": GaitData.for_columns(["Cadence"]),
                },
                outputs=[DeltaGait],
                db=db,
                subject=[],
                session=[],
            )


# ---------------------------------------------------------------------------
# Column drift is a hard error
# ---------------------------------------------------------------------------


class TestForColumnsDrift:
    def test_missing_column_raises(self, db):
        _seed_wide(db)

        with pytest.raises(ValueError, match="drift"):
            for_each(
                col_mean,
                inputs={"value": GaitData.for_columns(["StepLength", "DoesNotExist"])},
                outputs=[DeltaGait],
                db=db,
                subject=[],
                session=[],
            )


# ---------------------------------------------------------------------------
# where= under iteration
# ---------------------------------------------------------------------------


class TestForColumnsWhere:
    def test_where_coexists_with_iteration(self, db):
        # A where= clause must not break column iteration. (where semantics
        # themselves are covered by test_where.py; here we only assert that
        # passing where under for_columns still runs and produces the output.)
        _seed_wide(db)

        result = for_each(
            col_mean,
            inputs={"value": GaitData.for_columns(["StepLength"])},
            outputs=[DeltaGait],
            db=db,
            where=GaitData["StepLength"] > 0.0,  # keeps all rows
            subject=["1"],
            session=[],
        )

        assert "StepLength" in result.columns
        d = DeltaGait.load(db=db, subject="1", session="A")
        assert list(d.data.columns) == ["StepLength"]
        assert d.data["StepLength"].iloc[0] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


class TestForColumnsCaching:
    def test_identical_rerun_is_cached(self, db):
        _seed_wide(db)

        kwargs = {
            "inputs": {"value": GaitData.for_columns()},
            "outputs": [DeltaGait],
            "db": db,
            "subject": [],
            "session": [],
        }
        for_each(col_mean, **kwargs)
        for_each(col_mean, **kwargs)

        versions = DeltaGait.list_versions(db=db, subject="1", session="A")
        assert len(versions) == 1

    def test_empty_list_and_no_arg_resolve_identically(self, db):
        """for_columns([]) and for_columns() resolve to the same column set, so
        the second run is a cache hit (one version), not a distinct record."""
        _seed_wide(db)

        for_each(
            col_mean,
            inputs={"value": GaitData.for_columns()},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )
        for_each(
            col_mean,
            inputs={"value": GaitData.for_columns([])},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )

        versions = DeltaGait.list_versions(db=db, subject="1", session="A")
        assert len(versions) == 1

    def test_changing_function_creates_new_record(self, db):
        # Different function over the same columns -> distinct version key ->
        # a new record coexists (same physical output columns, so the table
        # schema is unchanged).
        _seed_wide(db)

        for_each(
            col_mean,
            inputs={"value": GaitData.for_columns(["StepLength"])},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )
        for_each(
            col_max,
            inputs={"value": GaitData.for_columns(["StepLength"])},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )

        versions = DeltaGait.list_versions(db=db, subject="1", session="A")
        assert len(versions) == 2


# ---------------------------------------------------------------------------
# dry_run
# ---------------------------------------------------------------------------


class TestForColumnsDryRun:
    def test_dry_run_returns_none(self, db):
        _seed_wide(db)

        result = for_each(
            col_mean,
            inputs={"value": GaitData.for_columns()},
            outputs=[DeltaGait],
            db=db,
            dry_run=True,
            subject=[],
            session=[],
        )
        assert result is None


# ---------------------------------------------------------------------------
# as_table interaction (schema columns available per-column call)
# ---------------------------------------------------------------------------


class TestForColumnsAsTable:
    def test_as_table_passes_dataframe_with_schema_cols(self, db):
        """as_table=True + for_columns feeds each per-column call a DataFrame
        with the schema key columns + the one current column (mirrors the
        non-iterate ColumnSelection as_table behavior), flowing through the
        scidb load + scifor delegation path."""
        _seed_wide(db)
        received = []

        def col_max_table(value):
            received.append(value)
            data_col = [c for c in value.columns if c not in SCHEMA][0]
            return float(value[data_col].max())

        for_each(
            col_max_table,
            inputs={"value": GaitData.for_columns()},
            outputs=[DeltaGait],
            db=db,
            as_table=True,
            subject=[],
            session=[],
        )

        # Each per-column call received a DataFrame: schema cols + 1 data column
        assert received, "function was never called"
        for r in received:
            assert isinstance(r, pd.DataFrame), f"expected DataFrame, got {type(r)}"
            assert "subject" in r.columns
            assert "session" in r.columns
            non_schema = [c for c in r.columns if c not in SCHEMA]
            assert len(non_schema) == 1, f"expected 1 data column, got {non_schema}"
            assert non_schema[0] in ("StepLength", "Cadence")

        # Reassembled output: per-column max per subject
        d1 = DeltaGait.load(db=db, subject="1", session="A")
        assert d1.data["StepLength"].iloc[0] == pytest.approx(3.0)
        assert d1.data["Cadence"].iloc[0] == pytest.approx(30.0)
        d2 = DeltaGait.load(db=db, subject="2", session="A")
        assert d2.data["StepLength"].iloc[0] == pytest.approx(6.0)
        assert d2.data["Cadence"].iloc[0] == pytest.approx(60.0)

    def test_as_table_false_passes_array(self, db):
        """Default (no as_table) still feeds each per-column call a bare array."""
        _seed_wide(db)
        received = []

        def col_max_arr(value):
            received.append(value)
            return float(np.max(value))

        for_each(
            col_max_arr,
            inputs={"value": GaitData.for_columns()},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )

        assert received, "function was never called"
        for r in received:
            assert not isinstance(r, pd.DataFrame), (
                "default should not pass a DataFrame"
            )
            assert isinstance(r, np.ndarray), f"expected ndarray, got {type(r)}"


# ---------------------------------------------------------------------------
# Multi-output-per-column reassembly (dict return -> <col>__<key> columns)
# ---------------------------------------------------------------------------


class TestForColumnsMultiOutput:
    def test_dict_return_round_trips_as_suffixed_columns(self, db):
        """A dict return per column reassembles into <col>__<key> columns,
        saved to one output variable and round-tripped through the DB."""
        _seed_wide(db)

        result = for_each(
            col_stats,
            inputs={"value": GaitData.for_columns()},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )
        assert "StepLength__mean" in result.columns
        assert "Cadence__max" in result.columns

        d1 = DeltaGait.load(db=db, subject="1", session="A")
        assert list(d1.data.columns) == [
            "StepLength__mean",
            "StepLength__max",
            "Cadence__mean",
            "Cadence__max",
        ]
        # subject 1: StepLength=[1,2,3], Cadence=[10,20,30]
        assert d1.data["StepLength__mean"].iloc[0] == pytest.approx(2.0)
        assert d1.data["StepLength__max"].iloc[0] == pytest.approx(3.0)
        assert d1.data["Cadence__mean"].iloc[0] == pytest.approx(20.0)
        assert d1.data["Cadence__max"].iloc[0] == pytest.approx(30.0)

        d2 = DeltaGait.load(db=db, subject="2", session="A")
        # subject 2: StepLength=[4,5,6], Cadence=[40,50,60]
        assert d2.data["StepLength__mean"].iloc[0] == pytest.approx(5.0)
        assert d2.data["Cadence__max"].iloc[0] == pytest.approx(60.0)

    def test_varying_output_counts_per_column(self, db):
        """Different source columns may emit different numbers of outputs.

        NOTE: scidb fixes a variable's physical columns on first write, so the
        per-column output set must be consistent *across combos*. Here the seed
        keeps StepLength < 5 (-> mean only) and Cadence >= 5 (-> mean + max) for
        every subject, so the column set is stable while still differing
        per-source-column. (Varying *across combos* is a documented limitation.)
        """
        GaitData.save(
            pd.DataFrame(
                {"StepLength": [1.0, 2.0, 3.0], "Cadence": [10.0, 20.0, 30.0]}
            ),
            db=db,
            subject="1",
            session="A",
        )
        GaitData.save(
            pd.DataFrame(
                {"StepLength": [2.0, 3.0, 4.0], "Cadence": [40.0, 50.0, 60.0]}
            ),
            db=db,
            subject="2",
            session="A",
        )

        for_each(
            col_stats_varying,
            inputs={"value": GaitData.for_columns()},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )
        d1 = DeltaGait.load(db=db, subject="1", session="A")
        # StepLength max=3 (<5) -> mean only; Cadence max=30 (>=5) -> mean + max
        assert list(d1.data.columns) == [
            "StepLength__mean",
            "Cadence__mean",
            "Cadence__max",
        ]
        assert d1.data["StepLength__mean"].iloc[0] == pytest.approx(2.0)
        assert d1.data["Cadence__mean"].iloc[0] == pytest.approx(20.0)
        assert d1.data["Cadence__max"].iloc[0] == pytest.approx(30.0)

        d2 = DeltaGait.load(db=db, subject="2", session="A")
        # StepLength max=4 (<5) -> mean only; Cadence max=60 (>=5) -> mean + max
        assert list(d2.data.columns) == [
            "StepLength__mean",
            "Cadence__mean",
            "Cadence__max",
        ]
        assert d2.data["StepLength__mean"].iloc[0] == pytest.approx(3.0)
        assert d2.data["Cadence__max"].iloc[0] == pytest.approx(60.0)

    def test_dict_return_identical_rerun_is_cached(self, db):
        """An identical multi-output re-run is a cache hit (one version)."""
        _seed_wide(db)

        for_each(
            col_stats,
            inputs={"value": GaitData.for_columns()},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )
        for_each(
            col_stats,
            inputs={"value": GaitData.for_columns()},
            outputs=[DeltaGait],
            db=db,
            subject=[],
            session=[],
        )

        versions = DeltaGait.list_versions(db=db, subject="1", session="A")
        assert len(versions) == 1


# ---------------------------------------------------------------------------
# "Every column" is read from storage metadata, not a load (cleanup-audit F28)
# ---------------------------------------------------------------------------


class TestEveryColumnFromMetadata:
    def test_metadata_names_the_same_columns_a_load_would(self, db):
        """Same set AND order: the resolved list becomes the selector, which
        is folded into invocation_id, so an order change would re-identify
        every existing all-columns call site."""
        from scidb.foreach import _load_var_type_as_spread, _resolve_all_columns

        _seed_wide(db)
        loaded = _load_var_type_as_spread(GaitData, db, None)
        from_load = [
            c for c in loaded.columns if c not in SCHEMA and not str(c).startswith("__")
        ]
        assert db.data_column_names(GaitData) == ["StepLength", "Cadence"]
        assert _resolve_all_columns(GaitData, db) == from_load == ["StepLength", "Cadence"]

    def test_resolving_every_column_loads_nothing(self, db, monkeypatch):
        """It used to load the whole variable just to read its column names,
        and prepare then loaded it again."""
        import scidb.foreach as fe

        _seed_wide(db)
        loads = []
        real = fe._load_var_type_as_spread

        def counting(var_type, *a, **k):
            loads.append(getattr(var_type, "__name__", var_type))
            return real(var_type, *a, **k)

        monkeypatch.setattr(fe, "_load_var_type_as_spread", counting)
        assert fe._resolve_all_columns(GaitData, db) == ["StepLength", "Cadence"]
        assert loads == []

    def test_an_unregistered_type_has_no_metadata_answer(self, db):
        assert db.data_column_names(OtherData) is None
