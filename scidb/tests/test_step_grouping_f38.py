"""Which call sites form one pipeline step — one rule, from recorded facts
(cleanup-audit F38, .claude/plan-f38-step-grouping.md).

``scidb graph`` used to group by its own key (function + variable inputs +
PathInput PARAMETER names, outputs ignored) while the canvas grouped by
``compute_wiring_id`` with the DECLARED PathInput name, which the database
did not record. Now a run records the declared PathInput name on the
PathInput edge, and ``database.call_site_wiring_ids`` is the grouping both
read.
"""

from pathlib import Path

import pytest

import scifor as _scifor
from scifor import EachOf, PathInput

from scidb import BaseVariable, configure_database, for_each
from scidb import provenance_query as pq
from scidb.database import aggregate_pipeline_variants, call_site_wiring_ids
from scidb.parameter import (
    Parameter,
    declared_input_names,
    stamp_path_input_name,
)

SUBJECTS = ["S01", "S02"]


class StepLoaded(BaseVariable):
    pass


class StepLoadedOther(BaseVariable):
    pass


def load_txt(filepath, k):
    return float(Path(filepath).read_text().strip()) * k


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "steps.duckdb", ["subject"])
    for folder in ("a", "b", "c"):
        for s in SUBJECTS:
            d = tmp_path / folder / s
            d.mkdir(parents=True)
            (d / "value.txt").write_text("2")
    yield database
    _scifor.set_schema([])
    database.close()


def _pi(tmp_path, folder: str, name: "str | None") -> PathInput:
    pi = PathInput("{subject}/value.txt", root_folder=str(tmp_path / folder))
    if name:
        stamp_path_input_name(pi, name)
    return pi


def _run(pi, k, output=StepLoaded):
    for_each(load_txt, {"filepath": pi, "k": k}, [output], subject=SUBJECTS)


def _steps(db) -> dict:
    """{wiring_id: sorted k values} — the partition into steps."""
    variants = pq.pipeline_variants(db._duck)
    agg = aggregate_pipeline_variants(variants)
    wids = call_site_wiring_ids(agg)
    out: dict = {}
    for v in variants:
        out.setdefault(wids[(v["function_name"], v["call_id"])], set()).add(
            v["constants"]["k"]
        )
    return {w: sorted(ks) for w, ks in out.items()}


class TestTheNameIsDeclaredOnce:
    def test_first_binding_wins_and_every_alternative_is_named(self, tmp_path):
        a, b = _pi(tmp_path, "a", None), _pi(tmp_path, "b", None)
        both = EachOf(a, b)
        stamp_path_input_name(both, "gait_files")
        stamp_path_input_name(both, "alias")  # `alias = gait_files`
        assert a.name == b.name == "gait_files"

    def test_owner_reads_parameters_and_path_inputs(self, tmp_path):
        p = Parameter(3)
        p.name = "gain"
        pi = _pi(tmp_path, "a", "gait_files")
        assert declared_input_names({"k": p, "filepath": pi, "x": 1}) == {
            "k": "gain",
            "filepath": "gait_files",
        }
        assert declared_input_names(
            {"filepath": pi}, {"filepath": "stated"}
        ) == {"filepath": "stated"}, "the caller's statement wins"

    def test_the_name_is_not_identity(self, tmp_path):
        assert _pi(tmp_path, "a", "x").to_key() == _pi(tmp_path, "a", None).to_key()


class TestRecorded:
    def test_the_run_records_the_declared_path_input(self, db, tmp_path):
        _run(_pi(tmp_path, "a", "gait_files"), 1.0)
        rows = db._duck._fetchall(
            "SELECT DISTINCT ii.param_name, ii.declared_name "
            "FROM _invocation_input ii JOIN _record r "
            "ON r.record_id = ii.input_record_id WHERE r.type = ?",
            [pq.PATHINPUT_TYPE],
        )
        assert rows == [("filepath", "gait_files")]
        [variant] = pq.pipeline_variants(db._duck)
        assert variant["path_input_names"] == {"filepath": "gait_files"}


class TestOneStepRule:
    def test_a_template_edit_under_one_name_stays_one_step(self, db, tmp_path):
        _run(_pi(tmp_path, "a", "gait_files"), 1.0)
        _run(_pi(tmp_path, "b", "gait_files"), 2.0)  # template edited
        assert sorted(_steps(db).values()) == [[1.0, 2.0]]

    def test_a_different_declared_path_input_is_a_different_step(self, db, tmp_path):
        _run(_pi(tmp_path, "a", "gait_files"), 1.0)
        _run(_pi(tmp_path, "b", "other_files"), 3.0)
        assert sorted(_steps(db).values()) == [[1.0], [3.0]]

    def test_different_outputs_are_different_steps(self, db, tmp_path):
        """The CLI used to merge these; the canvas never did."""
        _run(_pi(tmp_path, "a", "gait_files"), 1.0)
        _run(_pi(tmp_path, "a", "gait_files"), 5.0, output=StepLoadedOther)
        assert sorted(_steps(db).values()) == [[1.0], [5.0]]

    def test_an_unnamed_run_is_grouped_by_its_spec(self, db, tmp_path):
        """Template AND root folder: one template under two roots is two sets
        of files."""
        _run(_pi(tmp_path, "c", None), 4.0)
        _run(_pi(tmp_path, "c", None), 6.0)
        _run(_pi(tmp_path, "b", None), 7.0)
        assert sorted(_steps(db).values()) == [[4.0, 6.0], [7.0]]


def test_scidb_graph_uses_the_same_partition(db, tmp_path):
    from scidb.inspect.graph import build_pipeline_graph

    _run(_pi(tmp_path, "a", "gait_files"), 1.0)
    _run(_pi(tmp_path, "b", "gait_files"), 2.0)
    _run(_pi(tmp_path, "b", "other_files"), 3.0)
    _run(_pi(tmp_path, "a", "gait_files"), 5.0, output=StepLoadedOther)

    graph = build_pipeline_graph(db)
    by_step = sorted(
        sorted(float(v.constants["k"]) for v in fn.variants)
        for fn in graph.functions
        if fn.function_name == "load_txt"
    )
    assert by_step == sorted(_steps(db).values()) == [[1.0, 2.0], [3.0], [5.0]]
