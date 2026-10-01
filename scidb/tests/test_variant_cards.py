"""Variant cards: one card per FULL-CHAIN variant of a variable.

``.claude/plan-variants-popup.md`` Stage 1; rules in
``docs/claude/variant-pins-and-deletion.md``. ``Inspector.variant_cards`` is
what the GUI Variants popup shows and what ``scidb variants X --cards`` prints.

What is pinned here:

1. **The one-hop gap is closed.** Two records that differ only in a setting two
   steps upstream are two cards (``Inspector.variants`` shows them as one row).
2. **A card's selection names its records.** ``records_for_variant`` with
   ``card.selection`` returns records of that card and of no other card, at
   every one of its locations. That is what Stage 2's pin relies on.
3. Code-version and run-option splits are cards too, each with only the
   differing axis in ``distinguishing``.
4. The upstream DAG and the runs are there, and the build is batched.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.exceptions import NotFoundError
from scidb.inspect.cli import main as cli_main
from scidb.inspect.variant_cards import (
    coordinate_selection,
    selection_matches,
    varying_axes,
)
from scidb.provenance_query import records_for_variant
from scidb.variant import CODE_PIN_PREFIX, RUN_PIN_PREFIX

SCHEMA = ["subject", "trial"]
SUBJECTS = ["01", "02"]


class VcRaw(BaseVariable):
    pass


class VcFiltered(BaseVariable):
    pass


class VcSteps(BaseVariable):
    pass


class VcScaled(BaseVariable):
    pass


class VcLoaded(BaseVariable):
    pass


def vc_bandpass(signal, low_hz):
    return np.asarray(signal) * float(low_hz)


def vc_detect(filtered):
    return float(np.sum(filtered))


def _configure(path):
    _scifor.set_schema([])
    return configure_database(path, SCHEMA)


def build_upstream_split(path, subjects=SUBJECTS):
    """``VcRaw → vc_bandpass(low_hz=10|20) → VcFiltered → vc_detect → VcSteps``.

    ``vc_detect`` has no constants, so the two ``VcSteps`` variants differ only
    in ``vc_bandpass.low_hz``, one step upstream of their producer: the shape
    ``Inspector.variants`` reports as a single row.
    """
    db = _configure(path)
    for subject in subjects:
        VcRaw.save(np.array([1.0, 2.0]), subject=subject, trial=1)
    for low_hz in (10, 20):
        for_each(
            vc_bandpass,
            {"signal": VcRaw, "low_hz": low_hz},
            [VcFiltered],
            subject=[],
            trial=[],
        )
    for_each(vc_detect, {"filtered": VcFiltered}, [VcSteps], subject=[], trial=[])
    return db


def build_two_code_versions(path):
    db = _configure(path)
    for subject in SUBJECTS:
        VcRaw.save(np.array([1.0, 2.0]), subject=subject, trial=1)

    def scale_v1(raw):
        return float(np.sum(raw))

    def scale_v2(raw):
        return float(np.sum(raw)) + 100.0

    for body in (scale_v1, scale_v2):
        body.__name__ = "vc_scale"
        for_each(body, inputs={"raw": VcRaw}, outputs=[VcScaled], subject=[], trial=[])
    return db


def _vc_rows():
    import pandas as pd

    return pd.DataFrame({"v": [10.0, 20.0, 30.0]})


def build_two_run_option_sets(path):
    db = _configure(path)
    for_each(_vc_rows, {}, [VcLoaded], subject=["01"], trial=[1, 2, 3])
    for_each(_vc_rows, {}, [VcLoaded], distribute=True, subject=["01"])
    return db


@pytest.fixture
def upstream_split(tmp_path):
    db = build_upstream_split(tmp_path / "vc_split.duckdb")
    yield db
    db.close()
    _scifor.set_schema([])


@pytest.fixture
def two_code_versions(tmp_path):
    db = build_two_code_versions(tmp_path / "vc_code.duckdb")
    yield db
    db.close()
    _scifor.set_schema([])


@pytest.fixture
def two_run_option_sets(tmp_path):
    db = build_two_run_option_sets(tmp_path / "vc_run.duckdb")
    yield db
    db.close()
    _scifor.set_schema([])


@pytest.fixture
def split_path(tmp_path):
    path = tmp_path / "vc_split_cli.duckdb"
    build_upstream_split(path).close()
    _scifor.set_schema([])
    return path


# ---------------------------------------------------------------------------
# 1. The one-hop gap
# ---------------------------------------------------------------------------


class TestUpstreamOnlyDifference:
    def test_two_cards_where_the_one_hop_view_has_one_row(self, upstream_split):
        db = upstream_split
        cards = db.inspect.variant_cards("VcSteps")
        one_hop = db.inspect.variants("VcSteps")

        assert len(cards.cards) == 2, [c.selection for c in cards.cards]
        # The diagnostic this card view exists for: the call-grouped view
        # cannot tell the two apart.
        assert len(one_hop) == 1, [v.constants for v in one_hop]

    def test_only_the_upstream_constant_distinguishes_them(self, upstream_split):
        cards = upstream_split.inspect.variant_cards("VcSteps")
        assert cards.varying_axes == ["vc_bandpass.low_hz"]
        assert sorted(c.distinguishing["vc_bandpass.low_hz"] for c in cards.cards) == [
            "10",
            "20",
        ]
        assert not cards.producer_varies

    def test_every_card_covers_every_location(self, upstream_split):
        for card in upstream_split.inspect.variant_cards("VcSteps").cards:
            assert card.location_keys == ["subject", "trial"]
            assert [loc["subject"] for loc in card.locations] == SUBJECTS
            assert card.verdict == "current", card.verdict_label

    def test_cards_are_oldest_first(self, upstream_split):
        cards = upstream_split.inspect.variant_cards("VcFiltered").cards
        assert [c.selection["vc_bandpass.low_hz"] for c in cards] == [10, 20]


# ---------------------------------------------------------------------------
# 2. A card's selection names its records
# ---------------------------------------------------------------------------


def _assert_selection_round_trips(db, variable):
    result = db.inspect.variant_cards(variable)
    assert result.cards, variable
    for card in result.cards:
        assert card.selection_exact, (card.card_id, card.overlaps_with)
        got = set(records_for_variant(db, variable, card.selection))
        others = {
            rid for other in result.cards if other is not card for rid in other.record_ids
        }
        assert got, card.selection
        assert got <= set(card.record_ids), (card.selection, got - set(card.record_ids))
        assert not got & others, card.selection
        # Every location of the card, not just some of them.
        got_sids = {
            r[0]
            for r in db._duck._fetchall(
                "SELECT schema_id FROM _record WHERE record_id IN ("
                + ", ".join("?" * len(got))
                + ")",
                list(got),
            )
        }
        assert len(got_sids) == len(card.locations), card.selection


class TestSelectionRoundTrip:
    def test_upstream_split(self, upstream_split):
        _assert_selection_round_trips(upstream_split, "VcSteps")
        _assert_selection_round_trips(upstream_split, "VcFiltered")

    def test_code_versions(self, two_code_versions):
        _assert_selection_round_trips(two_code_versions, "VcScaled")

    def test_run_options(self, two_run_option_sets):
        _assert_selection_round_trips(two_run_option_sets, "VcLoaded")


# ---------------------------------------------------------------------------
# 3. Code-version and run-option splits
# ---------------------------------------------------------------------------


class TestOtherAxes:
    def test_code_versions_are_two_cards(self, two_code_versions):
        result = two_code_versions.inspect.variant_cards("VcScaled")
        key = f"{CODE_PIN_PREFIX}.vc_scale"
        assert result.varying_axes == [key]
        assert [c.distinguishing[key] for c in result.cards] == ["v1", "v2"]

    def test_the_older_code_version_is_not_what_a_load_returns(self, two_code_versions):
        v1, v2 = two_code_versions.inspect.variant_cards("VcScaled").cards
        assert v1.verdict == "superseded", v1.verdict_label
        assert v1.current_location_count == 0
        assert v2.verdict == "current", v2.verdict_label

    def test_the_upstream_step_reports_its_code_version(self, two_code_versions):
        v1, v2 = two_code_versions.inspect.variant_cards("VcScaled").cards
        (step,) = v2.upstream.steps
        assert step.function_name == "vc_scale"
        assert [c.version for c in step.code] == ["v2"]
        assert step.uniform

    def test_run_option_sets_are_two_cards(self, two_run_option_sets):
        result = two_run_option_sets.inspect.variant_cards("VcLoaded")
        key = f"{RUN_PIN_PREFIX}._vc_rows"
        assert result.varying_axes == [key]
        assert sorted(c.distinguishing[key] for c in result.cards) == [
            "distribute=false",
            "distribute=true",
        ]


# ---------------------------------------------------------------------------
# 4. Upstream DAG, runs, raw saves, exclusions, errors
# ---------------------------------------------------------------------------


class TestUpstreamAndRuns:
    def test_the_whole_chain_is_drawn(self, upstream_split):
        card = upstream_split.inspect.variant_cards("VcSteps").cards[0]
        up = card.upstream
        assert up.variables == ["VcFiltered", "VcRaw", "VcSteps"]
        by_fn = {s.function_name: s for s in up.steps}
        assert set(by_fn) == {"vc_bandpass", "vc_detect"}
        assert by_fn["vc_bandpass"].constants == {"low_hz": {"10": 2}}
        # No declared name was recorded, so the Parameter is the argument.
        assert by_fn["vc_bandpass"].parameter_names == {"low_hz": "low_hz"}
        assert by_fn["vc_bandpass"].inputs == {"signal": "VcRaw"}
        assert by_fn["vc_detect"].inputs == {"filtered": "VcFiltered"}
        assert all(s.uniform for s in up.steps)
        edges = {(e.source, e.target, e.param) for e in up.edges}
        assert ("var:VcRaw", "fn:vc_bandpass->VcFiltered", "signal") in edges
        assert ("fn:vc_detect->VcSteps", "var:VcSteps", "") in edges

    def test_runs_are_listed_newest_first(self, upstream_split):
        card = upstream_split.inspect.variant_cards("VcSteps").cards[0]
        assert card.runs
        stamps = [r.timestamp for r in card.runs]
        assert stamps == sorted(stamps, reverse=True)

    def test_include_runs_false_skips_them(self, upstream_split):
        card = upstream_split.inspect.variant_cards("VcSteps", include_runs=False).cards[0]
        assert card.runs == []

    def test_a_raw_saved_variable_is_one_card_with_no_steps(self, upstream_split):
        result = upstream_split.inspect.variant_cards("VcRaw")
        (card,) = result.cards
        assert card.function_name is None
        assert card.upstream.steps == []
        assert card.distinguishing == {}
        assert result.varying_axes == []

    def test_excluded_records_are_counted_not_carded(self, upstream_split):
        db = upstream_split
        card = db.inspect.variant_cards("VcFiltered").cards[0]
        db.exclude_variant(card.record_ids[0])
        result = db.inspect.variant_cards("VcFiltered")
        assert result.excluded_record_count == 1
        assert card.record_ids[0] not in {r for c in result.cards for r in c.record_ids}

    def test_an_unknown_name_raises(self, upstream_split):
        with pytest.raises(NotFoundError):
            upstream_split.inspect.variant_cards("NoSuchVariable")


class TestBatching:
    def test_query_count_does_not_grow_with_records(self, tmp_path):
        """The N+1 rule: six subjects cost the same queries as two."""

        def count(path, subjects):
            db = build_upstream_split(path, subjects)
            duck = db._duck
            original = duck._fetchall
            seen: list = []

            def counting(sql, params=None):
                seen.append(sql)
                return original(sql, params)

            duck._fetchall = counting
            try:
                db.inspect.variant_cards("VcSteps")
            finally:
                duck._fetchall = original
                db.close()
                _scifor.set_schema([])
            return len(seen)

        small = count(tmp_path / "small.duckdb", ["01", "02"])
        large = count(tmp_path / "large.duckdb", ["01", "02", "03", "04", "05", "06"])
        assert small == large, (small, large)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_varying_axes_counts_a_missing_key_as_a_difference(self):
        assert varying_axes([{"a.x": 1, "b.y": 2}, {"a.x": 1}]) == ["b.y"]

    def test_varying_axes_orders_constants_code_run(self):
        sels = [
            {"__run__.f": "distribute=true", "__code__.f": "v1", "f.k": 1},
            {"__run__.f": "distribute=false", "__code__.f": "v2", "f.k": 2},
        ]
        assert varying_axes(sels) == ["f.k", "__code__.f", "__run__.f"]

    def test_one_selection_varies_in_nothing(self):
        assert varying_axes([{"a.x": 1}]) == []

    def test_selection_matches_needs_every_key(self):
        assert selection_matches({"a.x": 1}, {"a.x": 1, "b.y": 2})
        assert not selection_matches({"a.x": 1, "b.y": 2}, {"a.x": 1})
        assert not selection_matches({"a.x": 1}, {"a.x": 2})
        assert selection_matches({}, {"a.x": 1})

    def test_coordinate_selection_omits_functions_without_a_choice(self):
        chain = {"code": {"f": "h1", "g": "h9"}, "run": {"f": "distribute=false", "g": None}}
        ordinals = {"f": {"h1": "v1", "h2": "v2"}}  # g is single-version
        run_axes = {}  # nothing ran two ways
        assert coordinate_selection({"f.k": 3}, chain, ordinals, run_axes) == {
            "__code__.f": "v1",
            "f.k": 3,
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class TestCli:
    def test_json_matches_the_python_object(self, split_path, capsys):
        rc = cli_main(["--db", str(split_path), "variants", "VcSteps", "--cards", "--json"])
        out = capsys.readouterr().out
        assert rc == 0, out
        payload = json.loads(out)
        assert payload["variable"] == "VcSteps"
        assert payload["varying_axes"] == ["vc_bandpass.low_hz"]
        assert len(payload["cards"]) == 2
        card = payload["cards"][0]
        assert card["selection"] == {"vc_bandpass.low_hz": 10}
        assert {s["function_name"] for s in card["upstream"]["steps"]} == {
            "vc_bandpass",
            "vc_detect",
        }

    def test_human_render_names_the_differing_axis(self, split_path, capsys):
        rc = cli_main(["--db", str(split_path), "variants", "VcSteps", "--cards"])
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "they differ in vc_bandpass.low_hz" in out
        assert "[1] vc_bandpass.low_hz=10" in out
        assert "[2] vc_bandpass.low_hz=20" in out
        assert "step  vc_bandpass -> VcFiltered" in out
