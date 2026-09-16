"""Provenance of a pinned variant, down to the run.

``.claude/plan-variant-provenance-introspection.md``. Before this, answering
"which run produced the records I am looking at?" meant a dozen hand-written
``scidb sql`` joins: ``trace`` keyed on a record_id or on metadata (never on the
variant the plot UI pins), and ``TraceNode`` stopped above the run — it carried
``function_hash`` and ``run_count`` but no invocation, call_id, run_id or
run-options label.

Four things are pinned here, one per gap:

1. **Stage 1** — a pin resolves to the same records ``load()`` returns for the
   same ``Variant(...)``. The two must never disagree; if they can, every
   answer below is about different data than the figure shows.
2. **Stage 2** — a record reproduced by a second run lists *both* runs, a
   second producing invocation (if one is ever written) is not halved away,
   and the enrichments are batched (a bounded query count, not one per node).
3. **Stage 3** — ``scidb trace --variant … --runs --json`` round-trips, and
   ``scidb variants`` lists run-option sets.
4. The display spelling (``Code:fn``) and scidb's own (``__code__.fn``) are one
   selection, canonicalized in one place.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
import scifor as _scifor
from scidb import BaseVariable, Variant, configure_database, for_each
from scidb.exceptions import AmbiguousVersionError, NotFoundError
from scidb.inspect.api import Inspector
from scidb.inspect.cli import main as cli_main
from scidb.provenance_query import (
    invocation_call_ids_batch,
    producing_invocation_batch,
    producing_invocations_batch,
    records_for_variant,
    runs_for_invocations_batch,
)
from scidb.variant import CODE_PIN_PREFIX, RUN_PIN_PREFIX, normalize_selection

SCHEMA = ["subject", "trial"]
SUBJECTS = ["01", "02"]
TRIALS = [1, 2, 3]


class VpRaw(BaseVariable):
    pass


class VpScaled(BaseVariable):
    pass


class VpLoaded(BaseVariable):
    pass


# ---------------------------------------------------------------------------
# Scenario builders — each returns an OPEN database; the `*_path` fixtures
# build one and close it, because the CLI opens the file itself.
# ---------------------------------------------------------------------------


def _configure(path):
    _scifor.set_schema([])
    return configure_database(path, SCHEMA)


def build_two_code_versions(path):
    """``vp_scale`` at two bodies producing DIFFERENT values.

    Two records per location, distinguishable only by which code made them —
    the shape ``--variant Code:vp_scale=v1`` exists to select.
    """
    db = _configure(path)
    for subject in SUBJECTS:
        VpRaw.save(np.array([1.0, 2.0]), subject=subject, trial=1)

    def scale_v1(raw):
        return float(np.sum(raw))

    def scale_v2(raw):
        return float(np.sum(raw)) + 100.0

    for body in (scale_v1, scale_v2):
        body.__name__ = "vp_scale"
        for_each(body, inputs={"raw": VpRaw}, outputs=[VpScaled], subject=[], trial=[])
    return db


def build_reproduced_record(path):
    """One record produced by ONE invocation in TWO runs.

    The same body re-run over the same inputs reuses the invocation (it is
    content-addressed over hash + inputs + constants + run options) and the
    record (``generate_record_id`` hashes content PLUS the save metadata, which
    carries ``__fn_hash`` and the input rids) — and appends a second ``_run``
    row. That is the shape "which run produced this?" has a list answer for.

    What does NOT happen, measured 2026-09-16 (``tmp.py`` diagnostic): a
    *different* body computing the same value does not land on the existing
    record. Because the save metadata is part of the record_id, an edit or a
    run-option flip always writes a new record from a new invocation. So a
    record with several producing invocations is a shape the schema permits
    (``_invocation_output`` has no uniqueness on ``output_record_id``) but the
    save path never creates; :class:`TestAllProducingInvocations` pins the
    plural helper against a hand-written row for that reason.
    """
    db = _configure(path)
    for subject in SUBJECTS:
        VpRaw.save(np.array([1.0, 2.0]), subject=subject, trial=1)

    def vp_same(raw):
        return float(np.sum(raw))

    for _ in range(2):
        for_each(vp_same, inputs={"raw": VpRaw}, outputs=[VpScaled], subject=[], trial=[])
    return db


def _add_second_producer(db, record_id: str) -> str:
    """Hand-write a second producing invocation for ``record_id``.

    The save path never does this (see :func:`build_reproduced_record`), but
    the schema allows it and the plural helper must report it if it is ever
    there — a MATLAB-side writer or a future identity change would otherwise
    be silently halved back to one producer.
    """
    inv_id = "ffffffffffffffff"
    db._duck._execute(
        "INSERT INTO _invocation (invocation_id, function_name, function_hash, "
        "as_table, distribute) VALUES (?, 'vp_same', 'deadbeefdeadbeef', NULL, FALSE)",
        [inv_id],
    )
    db._duck._execute(
        "INSERT INTO _invocation_output (invocation_id, output_num, output_record_id) "
        "VALUES (?, 0, ?)",
        [inv_id, record_id],
    )
    return inv_id


def _vp_rows():
    """Three rows, one per trial — so a distributed run slices into trials and
    a non-distributed one writes the whole frame at every trial."""
    return pd.DataFrame({"v": [10.0, 20.0, 30.0]})


def build_two_run_option_sets(path):
    """``_vp_rows`` run at trial level WITHOUT distribute, then one level up
    WITH it: same code, same constants, two records per trial.

    ``distribute``/``as_table`` are folded into ``invocation_id``, so this is a
    second variant that nothing in constants or code distinguishes — exactly
    what ``scidb variants`` was not reporting
    (docs/claude/run-option-variants.md, "Not done / open").
    """
    db = _configure(path)
    for_each(_vp_rows, {}, [VpLoaded], subject=["01"], trial=TRIALS)
    for_each(_vp_rows, {}, [VpLoaded], distribute=True, subject=["01"])
    return db


@pytest.fixture
def two_code_versions(tmp_path):
    db = build_two_code_versions(tmp_path / "vp_code.duckdb")
    yield db
    db.close()
    _scifor.set_schema([])


@pytest.fixture
def reproduced_record(tmp_path):
    db = build_reproduced_record(tmp_path / "vp_repro.duckdb")
    yield db
    db.close()
    _scifor.set_schema([])


@pytest.fixture
def two_run_option_sets(tmp_path):
    db = build_two_run_option_sets(tmp_path / "vp_runopts.duckdb")
    yield db
    db.close()
    _scifor.set_schema([])


@pytest.fixture
def code_versions_path(tmp_path):
    """A closed database for the CLI, which opens the file read-only itself."""
    path = tmp_path / "vp_code_cli.duckdb"
    build_two_code_versions(path).close()
    _scifor.set_schema([])
    return path


@pytest.fixture
def run_options_path(tmp_path):
    path = tmp_path / "vp_runopts_cli.duckdb"
    build_two_run_option_sets(path).close()
    _scifor.set_schema([])
    return path


def _scaled(tree):
    return next(n for n in tree.nodes if n.variable == "VpScaled")


def _record_ids(db, type_name):
    return [
        r[0]
        for r in db._duck._fetchall(
            "SELECT record_id FROM _record WHERE type = ?", [type_name]
        )
    ]


# ---------------------------------------------------------------------------
# Stage 1 — a pin resolves to the records load() returns
# ---------------------------------------------------------------------------


class TestRecordsForVariant:
    def test_pin_matches_what_the_for_each_loader_returns(self, two_code_versions):
        """The invariant the whole feature rests on: introspecting a variant
        and feeding it to a function must name the same records.

        The comparison is the for_each input loader's exact call
        (`foreach._load_var_type_as_spread`): `branch_params_filter` plus the
        `pin_loads_uncollapsed` version rule, which both sides now read from
        one place."""
        from scidb.variant import pin_loads_uncollapsed

        db = two_code_versions
        pin = Variant(VpScaled, fn="vp_scale", code_version="v1").branch_params

        rids = records_for_variant(db, VpScaled, pin)
        loaded = db.load_all_as_df(
            VpScaled,
            include_rid=True,
            version_id="all" if pin_loads_uncollapsed(pin) else "latest",
            branch_params_filter=pin,
        )

        assert rids, "the pin must select something"
        assert sorted(rids) == sorted(loaded["__record_id"].tolist())

    def test_a_code_pin_survives_the_latest_collapse(self, two_code_versions):
        """`version_id="latest"` merges v1 into v2 before any filter runs, so
        a collapsed load can never see v1. The pin resolver must load
        uncollapsed — the same rule the for_each loader applies."""
        from scidb.variant import pin_loads_uncollapsed

        pin = {f"{CODE_PIN_PREFIX}.vp_scale": "v1"}
        assert pin_loads_uncollapsed(pin)
        assert not pin_loads_uncollapsed({"vp_scale.low_hz": 20})

        assert len(records_for_variant(two_code_versions, VpScaled, pin)) == len(SUBJECTS)

    def test_a_branch_param_pin_uses_the_collapsed_load(self, two_code_versions):
        """The v2 body is the newest at every location, so a plain (no
        code pin) lookup collapses to it — one record per location, and the
        v1 records must not leak back in."""
        db = two_code_versions
        rids = records_for_variant(db, VpScaled)
        v2 = records_for_variant(db, VpScaled, {f"{CODE_PIN_PREFIX}.vp_scale": "v2"})

        assert sorted(rids) == sorted(v2)

    def test_the_two_code_pins_select_disjoint_records(self, two_code_versions):
        db = two_code_versions
        v1 = records_for_variant(db, VpScaled, {f"{CODE_PIN_PREFIX}.vp_scale": "v1"})
        v2 = records_for_variant(db, VpScaled, {f"{CODE_PIN_PREFIX}.vp_scale": "v2"})

        assert len(v1) == len(SUBJECTS)
        assert len(v2) == len(SUBJECTS)
        assert not set(v1) & set(v2)

    def test_schema_keys_narrow_the_pin(self, two_code_versions):
        db = two_code_versions
        rids = records_for_variant(
            db, VpScaled, {f"{CODE_PIN_PREFIX}.vp_scale": "v1"}, subject="01"
        )

        assert len(rids) == 1

    def test_display_spelling_is_the_same_selection(self, two_code_versions):
        """`Code:vp_scale` is what the Plot Studio picker shows; pasting it in
        must not mean something else."""
        db = two_code_versions

        assert records_for_variant(db, VpScaled, {"Code:vp_scale": "v1"}) == (
            records_for_variant(db, VpScaled, {f"{CODE_PIN_PREFIX}.vp_scale": "v1"})
        )

    def test_a_list_value_means_membership(self, two_code_versions):
        """The picker sends `{"Code:f": ["v1"]}`; a list has always meant
        "any of these" and must keep doing so through this path."""
        db = two_code_versions
        both = records_for_variant(db, VpScaled, {"Code:vp_scale": ["v1", "v2"]})

        assert len(both) == 2 * len(SUBJECTS)

    def test_an_unknown_level_raises_rather_than_selecting_everything(
        self, two_code_versions
    ):
        with pytest.raises(ValueError, match="matches nothing"):
            records_for_variant(two_code_versions, VpScaled, {"Code:vp_scale": "v9"})


class TestNormalizeSelection:
    def test_display_keys_canonicalize(self):
        assert normalize_selection({"Code:f": "v1", "Run:g": "distribute=true"}) == {
            f"{CODE_PIN_PREFIX}.f": "v1",
            f"{RUN_PIN_PREFIX}.g": "distribute=true",
        }

    def test_already_canonical_is_unchanged(self):
        pin = {f"{CODE_PIN_PREFIX}.f": "v1", "bandpass.low_hz": 20}

        assert normalize_selection(pin) == pin

    def test_idempotent(self):
        once = normalize_selection({"Code:f": "v1"})

        assert normalize_selection(once) == once

    def test_latest_column_becomes_the_chain_wide_pin(self):
        assert normalize_selection({"CodeIsLatest": True}) == {
            CODE_PIN_PREFIX: "latest"
        }

    def test_not_latest_is_dropped_rather_than_inverted(self):
        assert normalize_selection({"CodeIsLatest": False}) == {}

    def test_the_plot_layer_delegates_here(self):
        """One owner. `scistackplotdb.branch_params_for` is the picker's entry
        point and must not grow a second copy of the mapping."""
        pytest.importorskip("scistackplotdb")
        from scistackplotdb.variants import branch_params_for

        selection = {"Code:f": "v1", "CodeIsLatest": True, "bandpass.low_hz": 20}

        assert branch_params_for(selection) == normalize_selection(selection)


# ---------------------------------------------------------------------------
# Stage 2 — down to the run
# ---------------------------------------------------------------------------


class TestAllProducingInvocations:
    def test_a_re_run_reuses_the_record_and_the_invocation(self, reproduced_record):
        """Precondition for everything below: the second run wrote no new
        record and no new invocation — only a second `_run` row."""
        db = reproduced_record
        rids = _record_ids(db, "VpScaled")

        assert len(rids) == len(SUBJECTS)
        assert db._duck._fetchone("SELECT COUNT(*) FROM _invocation")[0] == len(SUBJECTS)
        assert db._duck._fetchone("SELECT COUNT(*) FROM _run")[0] == 2

    def test_trace_node_lists_both_runs(self, reproduced_record):
        """Two for_each executions, two `_run` rows, both reachable from the
        node — the question that needed hand-written SQL."""
        node = _scaled(
            reproduced_record.inspect.provenance(VpScaled, subject="01", trial=1)
        )

        assert len(node.runs) == 2
        assert len({r.run_id for r in node.runs}) == 2
        assert {r.invocation_id for r in node.runs} == {node.invocation_id}
        assert node.invocation_ids == [node.invocation_id]
        assert all(r.timestamp for r in node.runs)

    def test_the_plural_reports_a_second_producer_when_one_exists(
        self, reproduced_record
    ):
        """The schema permits several producers per record even though the
        save path never writes them; the plural must not halve them away."""
        db = reproduced_record
        rid = _record_ids(db, "VpScaled")[0]
        extra = _add_second_producer(db, rid)

        many = producing_invocations_batch(db._duck, [rid])

        assert len(many[rid]) == 2
        assert extra in {inv for inv, _fn, _h in many[rid]}
        assert [inv for inv, _fn, _h in many[rid]] == sorted(
            inv for inv, _fn, _h in many[rid]
        )

    def test_the_singular_keeps_picking_the_lowest(self, reproduced_record):
        """The lossy one stays lossy on purpose — variant identity, the latest
        collapse and `function_hash` all need ONE value and must keep
        reporting the one they always did."""
        db = reproduced_record
        rid = _record_ids(db, "VpScaled")[0]
        _add_second_producer(db, rid)

        one = producing_invocation_batch(db._duck, [rid])
        many = producing_invocations_batch(db._duck, [rid])

        assert len(many[rid]) == 2, "the plural adds something"
        assert one[rid] == many[rid][0]
        assert one[rid][0] == min(p[0] for p in many[rid])

    def test_trace_node_carries_every_invocation(self, reproduced_record):
        db = reproduced_record
        rid = next(
            r
            for r in _record_ids(db, "VpScaled")
            if db._duck._fetchone(
                "SELECT s.subject FROM _record r JOIN _schema s ON s.schema_id = "
                "r.schema_id WHERE r.record_id = ?",
                [r],
            )[0]
            == "01"
        )
        extra = _add_second_producer(db, rid)

        node = _scaled(db.inspect.provenance(VpScaled, subject="01", trial=1))

        assert len(node.invocation_ids) == 2
        assert extra in node.invocation_ids
        assert node.invocation_id == node.invocation_ids[0] == min(node.invocation_ids)

    def test_runs_are_oldest_first(self, reproduced_record):
        node = _scaled(
            reproduced_record.inspect.provenance(VpScaled, subject="01", trial=1)
        )

        assert [r.timestamp for r in node.runs] == sorted(
            r.timestamp for r in node.runs
        )

    def test_runs_are_absent_without_include_runs(self, reproduced_record):
        """`trace` keeps its old cost: the run join is extra work most callers
        do not want."""
        tree = reproduced_record.inspect.trace(VpScaled, subject="01", trial=1)

        assert all(not n.runs for n in tree.nodes)
        # The identity fields are cheap and always populated.
        assert any(n.invocation_id for n in tree.nodes)


class TestNodeIdentityFields:
    def test_call_id_matches_the_pipeline_variant(self, two_code_versions):
        """One recipe, both directions: the call_id a trace node reports is the
        one `scidb variants` lists for that step."""
        db = two_code_versions
        node = _scaled(
            db.inspect.provenance(
                VpScaled, {"Code:vp_scale": "v1"}, subject="01", trial=1
            )
        )
        known = {v.call_id for v in db.inspect.variants("VpScaled")}

        assert node.call_id in known

    def test_code_version_ordinal_is_reported(self, two_code_versions):
        db = two_code_versions
        v1 = db.inspect.provenance(
            VpScaled, {"Code:vp_scale": "v1"}, subject="01", trial=1
        )
        v2 = db.inspect.provenance(
            VpScaled, {"Code:vp_scale": "v2"}, subject="01", trial=1
        )

        assert _scaled(v1).code_version == "v1"
        assert _scaled(v2).code_version == "v2"

    def test_run_options_label_is_reported(self, two_code_versions):
        node = _scaled(
            two_code_versions.inspect.provenance(
                VpScaled, {"Code:vp_scale": "v1"}, subject="01", trial=1
            )
        )

        assert node.run_options == "distribute=false"

    def test_a_raw_record_has_no_invocation(self, two_code_versions):
        tree = two_code_versions.inspect.provenance(
            VpScaled, {"Code:vp_scale": "v1"}, subject="01", trial=1
        )
        raw = next(n for n in tree.nodes if n.variable == "VpRaw")

        assert raw.invocation_id is None
        assert raw.invocation_ids == []
        assert raw.call_id is None
        assert raw.runs == []


class TestBatching:
    """The N+1 rule. These helpers are handed the WHOLE tree at once, so the
    query count must not track the number of nodes."""

    def test_runs_helper_issues_one_query(self, reproduced_record):
        db = reproduced_record
        inv_ids = [
            r[0] for r in db._duck._fetchall("SELECT invocation_id FROM _invocation")
        ]
        assert len(inv_ids) >= 2, "need several invocations for this to mean anything"

        calls = _count_fetchall(
            db, lambda: runs_for_invocations_batch(db._duck, inv_ids)
        )

        assert calls == 1

    def test_call_id_helper_is_bounded(self, reproduced_record):
        """A fixed handful of reads whatever the input size: invocations,
        input edges, glue sources, glue ids."""
        db = reproduced_record
        inv_ids = [
            r[0] for r in db._duck._fetchall("SELECT invocation_id FROM _invocation")
        ]

        calls = _count_fetchall(
            db, lambda: invocation_call_ids_batch(db._duck, inv_ids)
        )

        assert calls <= 4, f"{calls} queries for {len(inv_ids)} invocations"

    def test_producers_helper_issues_one_query(self, reproduced_record):
        db = reproduced_record
        rids = _record_ids(db, "VpScaled")

        assert (
            _count_fetchall(db, lambda: producing_invocations_batch(db._duck, rids))
            == 1
        )

    def test_empty_input_touches_the_database_not_at_all(self, reproduced_record):
        db = reproduced_record

        assert (
            _count_fetchall(db, lambda: runs_for_invocations_batch(db._duck, [])) == 0
        )
        assert (
            _count_fetchall(db, lambda: producing_invocations_batch(db._duck, [])) == 0
        )
        assert _count_fetchall(db, lambda: invocation_call_ids_batch(db._duck, [])) == 0


def _count_fetchall(db, body) -> int:
    """Run ``body`` and return how many ``_fetchall`` calls it made."""
    duck = db._duck
    original = duck._fetchall
    seen: list[str] = []

    def counting(sql, params=None):
        seen.append(sql)
        return original(sql, params)

    duck._fetchall = counting
    try:
        body()
    finally:
        duck._fetchall = original
    return len(seen)


# ---------------------------------------------------------------------------
# Stage 3 — Inspector.provenance and the CLI
# ---------------------------------------------------------------------------


class TestInspectorProvenance:
    def test_a_pin_roots_at_a_matching_record(self, two_code_versions):
        db = two_code_versions
        tree = db.inspect.provenance(VpScaled, {"Code:vp_scale": "v2"})

        assert tree.root_record_id in records_for_variant(
            db, VpScaled, {"Code:vp_scale": "v2"}
        )

    def test_the_canonical_pin_is_echoed_back(self, two_code_versions):
        tree = two_code_versions.inspect.provenance(VpScaled, {"Code:vp_scale": "v2"})

        assert tree.selection == {f"{CODE_PIN_PREFIX}.vp_scale": "v2"}

    def test_every_matched_record_is_reported(self, two_code_versions):
        """A pin spans schema locations by design; the ones not traced must not
        vanish silently."""
        tree = two_code_versions.inspect.provenance(VpScaled, {"Code:vp_scale": "v2"})

        assert len(tree.matched_record_ids) == len(SUBJECTS)
        assert tree.root_record_id in tree.matched_record_ids

    def test_under_specified_metadata_still_refuses(self, two_code_versions):
        """Without a pin, several matches is under-specification and keeps
        raising — the pin path must not turn every old error into a guess."""
        with pytest.raises(AmbiguousVersionError):
            two_code_versions.inspect.provenance(VpScaled)

    def test_a_pin_with_record_id_is_refused(self, two_code_versions):
        db = two_code_versions
        rid = records_for_variant(db, VpScaled, {"Code:vp_scale": "v1"})[0]

        with pytest.raises(ValueError, match="one or the other"):
            db.inspect.provenance(VpScaled, {"Code:vp_scale": "v1"}, record_id=rid)

    def test_an_unmatched_pin_says_so(self, two_code_versions):
        with pytest.raises((NotFoundError, ValueError)):
            two_code_versions.inspect.provenance(VpScaled, {"Code:vp_scale": "v7"})

    def test_trace_is_unchanged_for_its_old_callers(self, two_code_versions):
        """`trace(variable, **metadata)` keeps resolving and returning exactly
        what it did — this plan added to it, it did not redefine it."""
        tree = two_code_versions.inspect.trace(VpRaw, subject="01", trial=1)

        assert tree.selection == {}
        assert tree.root_record_id
        assert [n.variable for n in tree.nodes] == ["VpRaw"]


class TestVariantsRunOptions:
    def test_variants_reports_the_run_option_set(self, two_code_versions):
        for v in two_code_versions.inspect.variants("VpScaled"):
            assert v.run_options == "distribute=false"

    def test_two_option_sets_are_two_variants(self, two_run_option_sets):
        labels = {v.run_options for v in two_run_option_sets.inspect.variants("VpLoaded")}

        assert len(labels) == 2, f"expected two run-option sets, got {labels}"
        assert "distribute=false" in labels
        assert "distribute=true" in labels


class TestCli:
    def test_variant_and_json_round_trip(self, code_versions_path, capsys):
        rc = cli_main(
            [
                "--db",
                str(code_versions_path),
                "trace",
                "VpScaled",
                "subject=01",
                "trial=1",
                "--variant",
                "Code:vp_scale=v2",
                "--runs",
                "--json",
            ]
        )
        out = capsys.readouterr().out
        assert rc == 0, out
        payload = json.loads(out)

        assert payload["selection"] == {f"{CODE_PIN_PREFIX}.vp_scale": "v2"}
        scaled = next(n for n in payload["nodes"] if n["variable"] == "VpScaled")
        assert scaled["code_version"] == "v2"
        assert scaled["call_id"]
        assert scaled["invocation_id"] in scaled["invocation_ids"]
        assert scaled["runs"] and scaled["runs"][0]["run_id"]
        assert scaled["run_options"] == "distribute=false"

    def test_runs_appear_in_the_human_render(self, code_versions_path, capsys):
        rc = cli_main(
            [
                "--db",
                str(code_versions_path),
                "trace",
                "VpScaled",
                "subject=01",
                "trial=1",
                "--variant",
                "Code:vp_scale=v1",
                "--runs",
            ]
        )
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "variant: __code__.vp_scale=v1" in out
        assert "invocation " in out
        assert "run options: distribute=false" in out
        assert "code v1" in out

    def test_default_render_is_unchanged_without_runs(self, code_versions_path, capsys):
        rc = cli_main(
            [
                "--db",
                str(code_versions_path),
                "trace",
                "VpScaled",
                "subject=01",
                "trial=1",
                "--variant",
                "Code:vp_scale=v1",
            ]
        )
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "invocation " not in out
        assert "run options:" not in out

    def test_record_id_with_variant_is_a_usage_error(self, code_versions_path, capsys):
        with Inspector.open(code_versions_path) as insp:
            rid = records_for_variant(
                insp._db, VpScaled, {"Code:vp_scale": "v1"}
            )[0]

        rc = cli_main(
            [
                "--db",
                str(code_versions_path),
                "trace",
                "--record-id",
                rid,
                "--variant",
                "Code:vp_scale=v1",
            ]
        )

        assert rc == 1
        assert "one or the other" in capsys.readouterr().err

    def test_repeated_variant_keys_mean_any_of(self, code_versions_path, capsys):
        rc = cli_main(
            [
                "--db",
                str(code_versions_path),
                "trace",
                "VpScaled",
                "subject=01",
                "trial=1",
                "--variant",
                "Code:vp_scale=v1",
                "--variant",
                "Code:vp_scale=v2",
                "--json",
            ]
        )
        out = capsys.readouterr().out
        assert rc == 0, out

        assert len(json.loads(out)["matched_record_ids"]) == 2

    def test_malformed_variant_is_a_usage_error(self, code_versions_path, capsys):
        rc = cli_main(
            [
                "--db",
                str(code_versions_path),
                "trace",
                "VpScaled",
                "--variant",
                "Code:vp_scale",
            ]
        )

        assert rc == 1
        assert "KEY=VALUE" in capsys.readouterr().err

    def test_variants_command_reports_run_options(self, run_options_path, capsys):
        rc = cli_main(["--db", str(run_options_path), "variants", "VpLoaded", "--json"])
        out = capsys.readouterr().out
        assert rc == 0, out

        assert len({v["run_options"] for v in json.loads(out)}) == 2

    def test_variants_human_render_adds_the_column_when_it_distinguishes(
        self, run_options_path, capsys
    ):
        assert cli_main(["--db", str(run_options_path), "variants", "VpLoaded"]) == 0

        assert "run options" in capsys.readouterr().out

    def test_variants_human_render_omits_the_column_otherwise(
        self, code_versions_path, capsys
    ):
        """One option set everywhere is a column of noise, not information."""
        assert cli_main(["--db", str(code_versions_path), "variants", "VpScaled"]) == 0

        assert "run options" not in capsys.readouterr().out


class TestSharedApi:
    """The CLI's Inspector and the GUI's `db.inspect` are one object — the
    shared-API reading of "the CLI should power the GUI"."""

    def test_open_inspector_answers_the_same_as_db_inspect(self, tmp_path):
        path = tmp_path / "vp_shared.duckdb"
        db = build_two_code_versions(path)
        pin = {"Code:vp_scale": "v1"}
        from_db = db.inspect.provenance(VpScaled, pin, subject="01", trial=1)
        db.close()
        _scifor.set_schema([])

        with Inspector.open(path) as insp:
            standalone = insp.provenance("VpScaled", pin, subject="01", trial=1)

        assert standalone.root_record_id == from_db.root_record_id
        assert [n.record_id for n in standalone.nodes] == [
            n.record_id for n in from_db.nodes
        ]
        assert _scaled(standalone).invocation_ids == _scaled(from_db).invocation_ids
        assert _scaled(standalone).runs == _scaled(from_db).runs
