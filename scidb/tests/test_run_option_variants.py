"""Run options (``distribute`` / ``as_table``) as a variant dimension.

``distribute`` and ``as_table`` are folded into ``invocation_id``, so re-running
unchanged code at unchanged constants under a different flag is a DIFFERENT
invocation that writes a second record to every schema location. Until
2026-09-14 nothing downstream told those two apart:

* ``_find_record``'s latest-collapse keyed on ``output_num``, and a distributed
  run's ``output_num`` is the slice index — so the ``distribute=false`` and the
  ``distribute=true`` record at one trial almost never shared a key and BOTH
  survived (``scidb show GAITRiteLoaded ...`` listed two "latest" records per
  trial);
* ``variant_identity_batch``'s ``is_latest`` compared code chains only, so
  both were "current" and Plot Studio drew them on top of each other.

These tests pin the fix at both layers plus the ``Variant(run_options=...)`` pin.
See ``.claude/plan-run-option-variants.md``.
"""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd
import pytest
import scifor as _scifor
from scidb import BaseVariable, Variant, configure_database, for_each
from scidb.exceptions import AmbiguousParamError
from scidb.provenance_query import (
    run_option_axes,
    run_options_batch,
    run_options_label,
    variant_identity_batch,
)
from scidb.variant import RUN_PIN_PREFIX

SCHEMA = ["subject", "trial"]
TRIALS = [1, 2, 3]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "run_options.duckdb", SCHEMA)
    yield database
    _scifor.set_schema([])
    database.close()


class Loaded(BaseVariable):
    """What a file loader writes: one row per trial when distributed, the whole
    file when not."""


class Consumed(BaseVariable):
    pass


def make_rows():
    # Three rows, one per trial. Values name the trial so a test can tell a
    # distributed piece (one row, v == 10 * trial) from the whole frame.
    return pd.DataFrame({"v": [10.0, 20.0, 30.0]})


def _records(db, type_name: str) -> int:
    return db._duck._fetchone(
        "SELECT COUNT(*) FROM _record WHERE type = ?", [type_name]
    )[0]


def _total(loaded) -> float:
    """Sum of the ``v`` column however for_each hands the record over.

    A DataFrame-valued record is stored ``multi_column`` and reassembled for
    the function as a DICT of arrays (``foreach._resolve_mapping_inputs``);
    a one-row one may also arrive as a frame, a Series, or scalar-unwrapped to
    the bare value (``df[var].iloc[0]`` — see test_as_table_full_iteration).
    The shape is not what these tests are about, and a body that only accepts
    one of them is skipped by for_each as a per-combo failure, which then shows
    up as "no Consumed records" instead of as the real assertion. (Observed:
    ``float(dict)`` skipped every combo, 2026-09-14.)
    """
    if isinstance(loaded, Mapping):
        return float(np.sum(np.asarray(loaded["v"], dtype=float)))
    if isinstance(loaded, pd.DataFrame):
        return float(loaded["v"].sum())
    if isinstance(loaded, pd.Series):
        return float(loaded.sum())
    return float(np.sum(np.asarray(loaded, dtype=float)))


def _consumed_values(db) -> list[float]:
    frame = db.load_all_as_df(Consumed)
    assert "data" in frame.columns, (
        "for_each wrote no Consumed records at all — every combo was skipped; "
        "check scidb.log for 'consume raised'"
    )
    return sorted(float(v) for v in frame["data"].tolist())


@pytest.fixture
def both_runs(db):
    """The exact 2026-09-14 sequence: a loader run at trial level WITHOUT
    distribute (every trial receives the whole file), then re-run one level up
    WITH distribute (each trial gets its own slice). Same code, same constants,
    two records per trial."""
    for_each(make_rows, {}, [Loaded], subject=["SS01"], trial=TRIALS)
    for_each(make_rows, {}, [Loaded], distribute=True, subject=["SS01"])
    return db


# --- the label -------------------------------------------------------------


class TestLabel:
    def test_default_options_read_as_distribute_false(self):
        assert run_options_label(False, None) == "distribute=false"
        assert run_options_label(False, []) == "distribute=false"

    def test_distribute_alone(self):
        assert run_options_label(True, None) == "distribute=true"

    def test_as_table_is_sorted_and_appended(self):
        assert (
            run_options_label(False, ["df", "cfg"]) == "distribute=false, as_table=[cfg, df]"
        )
        assert run_options_label(True, ["x"]) == "distribute=true, as_table=[x]"


# --- the scenario is real --------------------------------------------------


def test_both_runs_write_two_records_per_trial(both_runs):
    """Precondition: nothing was overwritten. distribute is identity-bearing,
    so the second run is a second invocation with its own records."""
    assert _records(both_runs, "Loaded") == 2 * len(TRIALS)


def test_run_options_are_read_off_the_chain(both_runs):
    rids = [
        r[0]
        for r in both_runs._duck._fetchall(
            "SELECT record_id FROM _record WHERE type = 'Loaded'"
        )
    ]
    runs = run_options_batch(both_runs._duck, rids)
    labels = {chain["make_rows"] for chain in runs.values()}
    assert labels == {"distribute=false", "distribute=true"}


def test_a_function_that_ran_both_ways_is_an_axis(both_runs):
    axes = run_option_axes(both_runs._duck, ["make_rows"])
    assert axes == {"make_rows": ["distribute=false", "distribute=true"]}


def test_a_function_that_ran_one_way_is_not_an_axis(db):
    for_each(make_rows, {}, [Loaded], distribute=True, subject=["SS01"])
    assert run_option_axes(db._duck, ["make_rows"]) == {}


# --- Stage 0: the load path supersedes the older option set ---------------


class TestLoadPathSupersession:
    def test_load_returns_one_record_per_trial(self, both_runs):
        """Before the fix this came back as a two-record list at every trial
        (the WARN "schema location(s) retain >1 record after collapse")."""
        for trial in TRIALS:
            loaded = Loaded.load(subject="SS01", trial=trial)
            assert not isinstance(loaded, list), (
                f"trial={trial} still holds both the distribute=false and the "
                f"distribute=true record after the latest-collapse"
            )

    def test_the_survivor_is_the_newer_distributed_run(self, both_runs):
        """The distributed piece is one row whose value names its trial; the
        superseded whole-file record has three."""
        for trial in TRIALS:
            data = Loaded.load(subject="SS01", trial=trial).data
            assert len(data) == 1, f"trial={trial}: got the whole file, not its slice"
            assert float(data["v"].iloc[0]) == 10.0 * trial

    def test_newest_wins_regardless_of_which_flag_it_carries(self, db):
        """The rule is recency, not "distribute beats not". Run the distributed
        loader FIRST, then the whole-file one, and the whole file survives."""
        for_each(make_rows, {}, [Loaded], distribute=True, subject=["SS01"])
        for_each(make_rows, {}, [Loaded], subject=["SS01"], trial=TRIALS)
        for trial in TRIALS:
            data = Loaded.load(subject="SS01", trial=trial).data
            assert len(data) == 3, f"trial={trial}: the newer whole-file run should win"

    def test_a_single_option_set_is_untouched(self, db):
        """Only the distributed run: every trial keeps its own piece — the
        supersession pass has nothing to do and must not merge slices."""
        for_each(make_rows, {}, [Loaded], distribute=True, subject=["SS01"])
        for trial in TRIALS:
            data = Loaded.load(subject="SS01", trial=trial).data
            assert len(data) == 1
            assert float(data["v"].iloc[0]) == 10.0 * trial

    def test_downstream_for_each_sees_one_input_per_trial(self, both_runs):
        """The consequence that matters: a step loading Loaded per trial must
        not fan out over both runs."""

        def consume(loaded):
            return _total(loaded)

        for_each(consume, inputs={"loaded": Loaded}, outputs=[Consumed], subject=[], trial=[])
        assert _records(both_runs, "Consumed") == len(TRIALS)
        values = _consumed_values(both_runs)
        assert values == [10.0, 20.0, 30.0], (
            f"expected the distributed slices, got {values} — the whole-file "
            f"records (sum 60) leaked through"
        )


# --- Stage 1: is_latest is run-option-aware --------------------------------


class TestIsLatest:
    def _ident(self, db):
        rids = [
            r[0]
            for r in db._duck._fetchall(
                "SELECT record_id FROM _record WHERE type = 'Loaded'"
            )
        ]
        return variant_identity_batch(db._duck, rids)

    def test_exactly_one_latest_per_location(self, both_runs):
        ident = self._ident(both_runs)
        latest = [rid for rid, info in ident.items() if info["is_latest"]]
        assert len(latest) == len(TRIALS), (
            "both option sets were 'latest' — the run options are not in the "
            "chain signature"
        )
        assert all(
            ident[rid]["run_chain"] == {"make_rows": "distribute=true"} for rid in latest
        )

    def test_run_chain_names_the_axis(self, both_runs):
        ident = self._ident(both_runs)
        labels = {info["run_chain"].get("make_rows") for info in ident.values()}
        assert labels == {"distribute=false", "distribute=true"}

    def test_run_chain_is_empty_for_a_function_that_ran_one_way(self, db):
        """Same presence rule as code_chain: a key here IS a real axis."""
        for_each(make_rows, {}, [Loaded], distribute=True, subject=["SS01"])
        ident = self._ident(db)
        assert all(info["run_chain"] == {} for info in ident.values())
        assert all(info["is_latest"] for info in ident.values())


# --- the Variant pin ------------------------------------------------------


class TestVariantPin:
    def test_construction_uses_the_reserved_namespace(self):
        pinned = Variant(Loaded, run_options="distribute=true")
        assert pinned.branch_params == {RUN_PIN_PREFIX: "distribute=true"}

    def test_fn_disambiguates(self):
        pinned = Variant(Loaded, fn="make_rows", run_options="distribute=false")
        assert pinned.branch_params == {f"{RUN_PIN_PREFIX}.make_rows": "distribute=false"}

    def test_pinning_the_superseded_run_still_reaches_it(self, both_runs):
        """The older option set is gone from a plain load (Stage 0) but a pin
        names it explicitly, so it loads uncollapsed and selects it."""

        def consume(loaded):
            return _total(loaded)

        for_each(
            consume,
            inputs={"loaded": Variant(Loaded, run_options="distribute=false")},
            outputs=[Consumed],
            subject=[],
            trial=[],
        )
        values = _consumed_values(both_runs)
        assert values and all(v == 60.0 for v in values), (
            f"expected the whole-file records (sum 60) at every trial, got {values}"
        )

    def test_pinning_the_current_run(self, both_runs):
        def consume(loaded):
            return _total(loaded)

        for_each(
            consume,
            inputs={"loaded": Variant(Loaded, fn="make_rows", run_options="distribute=true")},
            outputs=[Consumed],
            subject=[],
            trial=[],
        )
        assert _consumed_values(both_runs) == [10.0, 20.0, 30.0]

    def test_an_unknown_label_is_an_error_naming_what_exists(self, both_runs):
        def consume(loaded):
            return 0.0

        with pytest.raises(ValueError, match="distribute=true"):
            for_each(
                consume,
                inputs={"loaded": Variant(Loaded, run_options="distribute=maybe")},
                outputs=[Consumed],
                subject=[],
                trial=[],
            )

    def test_bare_pin_is_ambiguous_across_two_functions(self, db):
        """Two upstream functions each ran both ways: a bare pin cannot know
        which one it means — same rule as a bare code_version."""

        class Twice(BaseVariable):
            pass

        def make_more():
            return pd.DataFrame({"w": [1.0, 2.0, 3.0]})

        for_each(make_rows, {}, [Loaded], subject=["SS01"], trial=TRIALS)
        for_each(make_rows, {}, [Loaded], distribute=True, subject=["SS01"])
        for_each(make_more, {}, [Twice], subject=["SS01"], trial=TRIALS)
        for_each(make_more, {}, [Twice], distribute=True, subject=["SS01"])

        def merge(loaded, twice):
            return float(loaded["v"].sum() + twice["w"].sum())

        for_each(
            merge,
            inputs={"loaded": Loaded, "twice": Twice},
            outputs=[Consumed],
            subject=[],
            trial=[],
        )

        class Downstream(BaseVariable):
            pass

        def consume(consumed):
            return float(consumed)

        with pytest.raises(AmbiguousParamError, match="run_options"):
            for_each(
                consume,
                inputs={"consumed": Variant(Consumed, run_options="distribute=true")},
                outputs=[Downstream],
                subject=[],
                trial=[],
            )


class TestListValuedPin:
    """The Plot Studio location picker hands scidb the row's selection as it
    holds it — list values, ``{"__run__.fn": ["distribute=true"]}`` — through
    ``branch_params_for`` -> ``location_states``. Branch params already read a
    list as membership; the run pin stringified it and matched nothing
    (``run_options="['distribute=true']" matches nothing``, 2026-09-14)."""

    def _load(self, db, pin):
        return db.load_all_as_df(
            Loaded, version_id="all", branch_params_filter={"__run__.make_rows": pin}
        )

    def test_single_element_list_selects_that_run(self, both_runs):
        frame = self._load(both_runs, ["distribute=true"])
        assert len(frame) == len(TRIALS)
        assert len(self._load(both_runs, "distribute=true")) == len(TRIALS)

    def test_list_is_membership(self, both_runs):
        frame = self._load(both_runs, ["distribute=false", "distribute=true"])
        assert len(frame) == 2 * len(TRIALS)

    def test_a_list_of_only_unknown_labels_still_errors(self, both_runs):
        with pytest.raises(ValueError, match="matches nothing"):
            self._load(both_runs, ["distribute=maybe"])


class TestCurrencyIsPerFunctionNotPerLocation:
    """The 2026-09-14 follow-up. Discovery said four trials; the whole-file run
    wrote trials 1–4; the distributed re-run produced three slices because the
    file held three trials. Per-LOCATION "latest" — right for code versions,
    where a subject never re-run keeps its own newest record — called the
    whole-file record at trial 4 current (nothing newer THERE), and the figure
    mixed the two runs. Run options are judged per function, globally: the
    option set the function was most recently run under wins everywhere."""

    @pytest.fixture
    def stale_trial(self, db):
        for_each(make_rows, {}, [Loaded], subject=["SS01"], trial=[1, 2, 3, 4])
        for_each(make_rows, {}, [Loaded], distribute=True, subject=["SS01"])
        return db

    def _ident(self, db):
        rids = [
            r[0]
            for r in db._duck._fetchall(
                "SELECT record_id FROM _record WHERE type = 'Loaded'"
            )
        ]
        return variant_identity_batch(db._duck, rids)

    def test_current_run_options_is_the_newest_set(self, stale_trial):
        from scidb.provenance_query import current_run_options

        assert current_run_options(stale_trial._duck, ["make_rows"]) == {
            "make_rows": "distribute=true"
        }

    def test_the_orphaned_old_record_is_not_latest(self, stale_trial):
        """Seven records: four whole-file, three slices. Only the slices are
        current — the trial-4 whole-file record has no newer neighbour at its
        location and must still be stale."""
        ident = self._ident(stale_trial)
        assert len(ident) == 7
        latest = {rid for rid, info in ident.items() if info["is_latest"]}
        assert len(latest) == 3
        assert all(
            ident[rid]["run_chain"] == {"make_rows": "distribute=true"} for rid in latest
        )

    def test_load_path_drops_the_orphaned_record_too(self, stale_trial):
        frame = stale_trial.load_all_as_df(Loaded)
        assert len(frame) == 3, (
            "load() should see only the distributed slices; the trial-4 "
            "whole-file record is a location the current run never produced"
        )

    def test_the_older_run_is_still_reachable_by_pin(self, stale_trial):
        frame = stale_trial.load_all_as_df(
            Loaded,
            version_id="all",
            branch_params_filter={"__run__.make_rows": "distribute=false"},
        )
        assert len(frame) == 4
