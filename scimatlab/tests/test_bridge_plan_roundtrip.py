"""MATLAB's scifor.for_each plan crosses the bridge and back (runs without MATLAB).

P5 stage 3 (design review 05, M3): ``+scifor/for_each.m`` describes its
inputs, ``scifor.matlab_loop.plan`` decides the run, MATLAB slices and calls,
``assemble`` / ``close`` write the summary and the journal. These tests send
descriptions shaped exactly as ``describe_run`` in ``+scifor/for_each.m``
builds them (``jsonencode`` quirks included: a one-element list as a bare
value, "none" as ``[]``) and decode the answer as ``decode_plan`` does
(index matrix, 1-based row indices + offsets, limits).

Guards: the wire types (only ``str`` / ``bytes`` cross, so no datetime scalar
crosses bare); combos, ``[]`` resolution and labels from Python; MATLAB's
row-matching rule (numeric column vs text value); scidb's row selection and
``locations=`` keeping each combo's place; distribute target; for_columns
groups; share_limits from per-row extents; per-combo paths and their errors;
dry run; the summary, combine column list and journal from ``assemble`` /
``close``.
"""

import json

import numpy as np
import pytest
import scifor
from scifor import matlab_loop


@pytest.fixture(autouse=True)
def _schema():
    scifor.set_schema([])
    yield
    scifor.set_schema([])


class Described:
    """Builds a description the way +scifor/for_each.m does."""

    def __init__(self):
        self.numbers: list[float] = []

    def num(self, values) -> list[int]:
        ref = [len(self.numbers), len(values)]
        self.numbers.extend(float(v) for v in values)
        return ref

    def table(self, columns: dict, *, record_ids=None, extents=False) -> dict:
        names = list(columns)
        n = len(next(iter(columns.values()))) if columns else 0
        keys = {}
        for name, values in columns.items():
            if all(isinstance(v, (int, float)) for v in values):
                keys[name] = {"class": "double", "numbers": self.num(values)}
            else:
                keys[name] = {"class": "string", "texts": list(values)}
        t = {"columns": names, "n_rows": n, "keys": keys, "record_ids": [], "extents": []}
        if record_ids is not None:
            t["record_ids"] = record_ids
        if extents:
            ext = np.full((n, len(names), 2), np.nan)
            for c, values in enumerate(columns.values()):
                for r, v in enumerate(values):
                    if isinstance(v, (int, float)):
                        ext[r, c, :] = v
            t["extents"] = {"numbers": self.num(ext.ravel(order="F"))}
        return t

    def values(self, values) -> dict:
        if all(isinstance(v, (int, float)) for v in values):
            return {"numbers": self.num(values)}
        return {"texts": list(values)}

    def run(self, inputs, metadata=None, *, outputs=None, fn_nargin=-1, **options):
        desc = {
            "fn_name": "fn",
            "fn_nargin": fn_nargin,
            "fn_nargout": 1,
            "call_nargout": 1,
            "outputs": outputs or {"kind": "default"},
            "metadata": [
                {"key": k, "values": self.values(v)} for k, v in (metadata or {}).items()
            ],
            "schema_keys": [],
            "options": {"where": [], "locations": "", "record_id_column": "", **options},
            "inputs": inputs,
        }
        return json.dumps(desc), np.asarray(self.numbers, dtype="<f8").tobytes()


def _plan(desc_json, numbers, **kwargs):
    out = matlab_loop.plan(desc_json, numbers, **kwargs)
    for value in out.values():
        assert isinstance(value, (str, bytes)), type(value)
    plan = json.loads(out["json"])
    if plan.get("dry_run"):
        return plan, out
    n = plan["n_combos"]
    keys = plan["keys"]
    index = np.frombuffer(out["combo_index"], dtype="<i4").reshape((n, len(keys)), order="F")
    plan["combos"] = [
        {k: plan["pools"][ki]["v"][index[c, ki] - 1] for ki, k in enumerate(keys) if index[c, ki]}
        for c in range(n)
    ]
    plan["_row_index"] = np.frombuffer(out["row_index"], dtype="<i8")
    plan["_row_offsets"] = np.frombuffer(out["row_offsets"], dtype="<i8").reshape(
        (plan["n_slices"], n + 1), order="F"
    )
    plan["_limits"] = np.frombuffer(out["limits"], dtype="<f8").reshape(
        (n, len(plan["limits"]["names"]), 2), order="F"
    )
    return plan, out


def _rows(plan, slice_no, c):
    offs = plan["_row_offsets"]
    return list(plan["_row_index"][offs[slice_no - 1, c] : offs[slice_no - 1, c + 1]])


def _close(plan):
    matlab_loop.discard(plan["token"])


def test_cartesian_combos_labels_and_row_slices():
    scifor.set_schema(["subject", "session"])
    d = Described()
    tbl = d.table({"subject": [1, 1, 2, 2], "session": ["A", "B", "A", "B"], "value": [10, 20, 30, 40]})
    plan, _ = _plan(*d.run(
        [{"name": "x", "kind": "table", "table": tbl}],
        {"subject": [1, 2], "session": ["A", "B"]},
    ))
    assert plan["n_combos"] == 4
    assert plan["combos"][0] == {"subject": 1, "session": "A"}
    assert plan["labels"] == ["subject=1, session=A", "subject=1, session=B",
                              "subject=2, session=A", "subject=2, session=B"]
    (x,) = plan["inputs"]
    assert x["kind"] == "data" and x["per_combo"] and x["no_data_when_empty"]
    assert [_rows(plan, x["slice"], c) for c in range(4)] == [[1], [2], [3], [4]]
    assert plan["output_names"] == ["output"]
    _close(plan)


def test_empty_list_resolves_from_table_values():
    scifor.set_schema(["subject"])
    d = Described()
    tbl = d.table({"subject": [3, 1, 2, 1], "value": [1, 2, 3, 4]})
    plan, _ = _plan(*d.run([{"name": "x", "kind": "table", "table": tbl}], {"subject": []}))
    assert [c["subject"] for c in plan["combos"]] == [1, 2, 3]
    assert _rows(plan, 1, 0) == [2, 4]
    _close(plan)


def test_no_schema_filters_by_iterated_keys():
    """MATLAB's contract (TestSciforForEachNoSchema): with no schema the
    iterated keys filter the tables."""
    d = Described()
    tbl = d.table({"pass": [1, 2], "value": [10, 30]})
    plan, _ = _plan(*d.run([{"name": "x", "kind": "table", "table": tbl}], {"pass": [1, 2]}))
    assert plan["filter_keys"] == ["pass"]
    assert [_rows(plan, 1, c) for c in range(2)] == [[1], [2]]
    _close(plan)


def test_numeric_column_matches_text_value():
    """MATLAB compares a numeric key column numerically, parsing a text value."""
    scifor.set_schema(["subject"])
    d = Described()
    tbl = d.table({"subject": [1, 2], "value": [10, 20]})
    plan, _ = _plan(*d.run([{"name": "x", "kind": "table", "table": tbl}]),
                    prebuilt_combos=[{"subject": "2"}])
    assert _rows(plan, 1, 0) == [2]
    _close(plan)


def test_scidb_row_selection_and_locations_keep_each_combo_place():
    scifor.set_schema(["subject", "trial"])
    d = Described()
    tbl = d.table({"subject": [1, 1, 2], "trial": [1, 1, 1], "x__record_id": ["a", "b", "c"]},
                  record_ids=["a", "b", "c"])
    prebuilt = [{"subject": 1, "trial": 1}, {"subject": 2, "trial": 1}]
    selection = [{"x": ["b"]}, {"x": ["c"]}]
    paths = {"out": ["/p/s1", "/p/s2"]}
    plan, _ = _plan(
        *d.run(
            [{"name": "x", "kind": "table", "table": tbl},
             {"name": "out", "kind": "path_output", "template": "/p/{subject}"}],
            {"subject": [1, 2], "trial": [1]},
            record_id_column="x__record_id",
            locations=json.dumps({"include": [[["subject", 2]]]}),
        ),
        prebuilt_combos=prebuilt, row_selection=selection, resolved_path_outputs=paths,
    )
    assert plan["combos"] == [{"subject": 2, "trial": 1}]
    x, out = plan["inputs"]
    assert x["drop"] == ["x__record_id"]
    assert _rows(plan, x["slice"], 0) == [3]
    assert out["paths"] == ["/p/s2"] and out["value_class"] == "char"
    _close(plan)


def test_locations_narrow_rows_below_the_iterated_level():
    """Standalone MATLAB: a selection on a key the run does not iterate decides
    which rows each combo's slice holds, by the same rule as Python's loop."""
    scifor.set_schema(["subject", "minute"])
    d = Described()
    tbl = d.table({"subject": [1, 1, 2, 2], "minute": [5, 6, 5, 6], "value": [10, 20, 30, 40]})
    plan, _ = _plan(
        *d.run(
            [{"name": "x", "kind": "table", "table": tbl}],
            {"subject": [1, 2]},
            locations=json.dumps({"exclude_levels": {"minute": [5]}}),
        ),
    )
    x = plan["inputs"][0]
    assert _rows(plan, x["slice"], 0) == [2]
    assert _rows(plan, x["slice"], 1) == [4]
    _close(plan)


def test_record_id_selection_narrows_within_the_schema_filter():
    scifor.set_schema(["subject"])
    d = Described()
    tbl = d.table({"subject": [1, 1, 1], "x__record_id": ["a", "b", "c"]}, record_ids=["a", "b", "c"])
    plan, _ = _plan(*d.run([{"name": "x", "kind": "table", "table": tbl}], record_id_column="x__record_id"),
                    prebuilt_combos=[{"subject": 1}], row_selection=[{"x": ["a", "c"]}])
    assert _rows(plan, 1, 0) == [1, 3]
    _close(plan)


def test_distribute_target_and_top_fallback():
    scifor.set_schema(["subject", "trial"])
    d = Described()
    plan, _ = _plan(*d.run([], {"subject": [1]}, distribute=True))
    assert plan["distribute_key"] == "trial" and plan["distribute_top"] is False
    _close(plan)
    d = Described()
    plan, _ = _plan(*d.run([], {}, distribute=True))
    assert plan["distribute_key"] == "subject" and plan["distribute_top"] is True
    _close(plan)


def test_for_columns_groups_from_python():
    scifor.set_schema(["subject"])
    d = Described()
    tbl = d.table({"subject": [1], "a": [1], "b": [2]})
    plan, _ = _plan(*d.run(
        [{"name": "v", "kind": "column_selection", "table": tbl, "columns": ["a", "b"],
          "iterate": True, "groups": []}],
        {"subject": [1]},
    ))
    fc = plan["for_columns"]
    assert fc["mode"] == "combine" and fc["params"] == ["v"]
    assert fc["groups"] == [{"name": "a", "columns": ["a"]}, {"name": "b", "columns": ["b"]}]
    assert plan["inputs"][0]["shape"] == "iterate"
    _close(plan)


def test_share_limits_from_row_extents():
    scifor.set_schema(["subject", "trial"])
    d = Described()
    tbl = d.table({"subject": [1, 1, 2], "trial": [1, 2, 1], "y": [5, -3, 7]}, extents=True)
    plan, _ = _plan(*d.run(
        [{"name": "y", "kind": "table", "table": tbl}],
        {"subject": [1, 2], "trial": [1, 2]},
        share_limits={"y": "subject"},  # jsonencode writes a 1-element list bare
        fn_nargin=2,
    ))
    assert plan["limits"] == {"names": ["y"], "inject": True}
    by_subject = {c["subject"]: tuple(plan["_limits"][i, 0]) for i, c in enumerate(plan["combos"])}
    assert by_subject == {1: (-3.0, 5.0), 2: (7.0, 7.0)}
    _close(plan)


def test_pathinput_paths_and_missing_file_error(tmp_path):
    """A regex template raises on no match (a literal one returns the path
    as is); the error is that combo's, not the run's."""
    (tmp_path / "1").mkdir()
    (tmp_path / "2").mkdir()
    (tmp_path / "1" / "data.txt").write_text("x")
    pi = scifor.PathInput(r"{subject}/data\.txt", root_folder=str(tmp_path), regex=True, name="Raw")
    d = Described()
    plan, _ = _plan(*d.run([{"name": "p", "kind": "path_input", "text": "t"}], {"subject": [1, 2]}),
                    path_inputs={"p": pi})
    (p,) = plan["inputs"]
    assert p["value_class"] == "string"
    assert p["paths"][0].endswith("data.txt") and p["error_ids"][0] == ""
    assert p["paths"][1] == "" and p["error_ids"][1] == "scifor:PathInput:NoMatch"
    _close(plan)


def test_dry_run_returns_the_text():
    d = Described()
    out = matlab_loop.plan(*d.run([], {"subject": [1, 2]}, dry_run=True))
    plan = json.loads(out["json"])
    assert plan["dry_run"] is True
    assert "[dry-run] would process 2 iterations" in plan["dry_run_text"]


def test_output_count_names_every_table_output():
    d = Described()
    plan, _ = _plan(*d.run([], {"subject": [1]}, outputs={"kind": "count", "n": 2}))
    assert plan["output_names"] == ["output", "output"]
    _close(plan)


def test_categorical_labels_become_numbers_only_when_lossless():
    scifor.set_schema(["subject"])
    d = Described()
    tbl = {"columns": ["subject"], "n_rows": 2, "record_ids": [], "extents": [],
           "keys": {"subject": {"class": "categorical", "texts": ["2", "1"], "categories": ["1", "2"]}}}
    plan, _ = _plan(*d.run([{"name": "x", "kind": "table", "table": tbl}], {"subject": []}))
    assert [c["subject"] for c in plan["combos"]] == [1, 2]
    assert plan["column_types"]["subject"]["class"] == "categorical"
    _close(plan)
    tbl["keys"]["subject"]["texts"] = ["01", "02"]
    plan, _ = _plan(*Described().run([{"name": "x", "kind": "table", "table": tbl}], {"subject": []}))
    assert [c["subject"] for c in plan["combos"]] == ["01", "02"]
    _close(plan)


def _matlab_record(message, *, no_data=False):
    """A failure record as +scifor/failure_from_mexception.m writes it,
    after jsonencode (one frame as a bare object, no user frame as [])."""
    exc_type = "scifor:NoData" if no_data else "test:bad"
    return {
        "fn_name": "fn", "metadata": {}, "combo": "", "stage": "call",
        "exc_type": exc_type, "message": message, "reason": f"{exc_type}: {message}",
        "frames": {"file": "/u/fn.m", "line": 3, "function": "fn", "code": "", "origin": "user"},
        "user_frame": [] if no_data else {"file": "/u/fn.m", "line": 3, "function": "fn",
                                          "code": "", "origin": "user"},
        "no_data": no_data, "blame": "no_data" if no_data else "user_code",
        "probable": False, "remedy": [],
    }


def test_assemble_summary_and_close_journal(monkeypatch):
    scifor.set_schema(["subject"])
    d = Described()
    plan, _ = _plan(*d.run([], {"subject": [1, 2, 3]}))
    outcome = {
        "completed": 1, "skipped": 2, "elapsed": 0.5,
        "failures": [{"record": _matlab_record("bad"), "combos": 2},
                     {"record": _matlab_record("none", no_data=True), "combos": [3]}],
        "stop": [], "output_skips": [], "group_columns": [], "no_data_groups": [],
        "distribute_from_column": 0,
    }
    out = json.loads(matlab_loop.assemble(plan["token"], json.dumps(outcome)))
    s = out["summary"]
    assert (s["completed"], s["failed"], s["no_data"], s["total"]) == (1, 1, 1, 3)
    assert s["first_reason"] == "test:bad: bad"
    assert s["failures"][0]["combos"] == ["subject=2"]
    assert out["combine_columns"] is None

    events = []
    monkeypatch.setattr(matlab_loop.Log, "journal", classmethod(lambda cls, e: events.append(e) or ""))
    matlab_loop.close(plan["token"], "")
    (event,) = events
    assert event["language"] == "matlab" and event["failed"] == 1 and event["error"] is None
    assert plan["token"] not in matlab_loop.open_plans()
    matlab_loop.close(plan["token"], "")  # idempotent


def test_assemble_combine_columns_and_output_skip_lines(caplog):
    scifor.set_schema(["subject"])
    d = Described()
    tbl = d.table({"subject": [1], "a": [1], "b": [2]})
    plan, _ = _plan(*d.run(
        [{"name": "v", "kind": "column_selection", "table": tbl, "columns": ["a", "b"],
          "iterate": True, "groups": []}],
        {"subject": [1]},
    ))
    outcome = {"completed": 1, "skipped": 0, "elapsed": 0.1, "failures": [], "stop": [],
               "output_skips": [], "group_columns": [{"name": "a", "columns": "a"}],
               "no_data_groups": [{"name": "b", "combos": 1}], "distribute_from_column": 0}
    out = json.loads(matlab_loop.assemble(plan["token"], json.dumps(outcome)))
    assert out["combine_columns"] == ["a", "b"]
    _close(plan)


def test_failure_identifier_rule_is_pythons():
    from scifor.pathinput import matlab_error_id

    assert matlab_error_id("PathInput regex pattern 'x' matched no files in /d") == "scifor:PathInput:NoMatch"
    assert matlab_error_id("PathInput regex pattern 'x' matched 2 files in /d") == "scifor:PathInput:MultipleMatches"
    assert matlab_error_id("boom") == "scifor:PathInput:LoadError"


def test_malformed_description_is_a_wire_error():
    with pytest.raises(matlab_loop.MatlabWireError):
        matlab_loop.plan(json.dumps({"inputs": [{"name": "x", "kind": "nope"}]}), b"")


@pytest.mark.parametrize(
    "schema, metadata, options, error_id",
    [
        ([], {"subject": [1]}, {"distribute": True}, "scifor:for_each"),
        (["subject"], {"subject": []}, {}, "scifor:for_each"),
    ],
)
def test_refused_call_names_the_matlab_error(schema, metadata, options, error_id):
    """MATLAB raises a refused call with this identifier (tests and callers
    catch ``scifor:for_each``; scidb retags it ``scidb:``)."""
    scifor.set_schema(schema)
    out = matlab_loop.plan(*Described().run([], metadata, **options))
    refused = json.loads(out["json"])
    assert refused["error_id"] == error_id and refused["error_message"]


def test_colname_refusal_id():
    scifor.set_schema(["subject"])
    out = matlab_loop.plan(*Described().run([{"name": "c", "kind": "colname_deferred"}], {"subject": [1]}))
    assert json.loads(out["json"])["error_id"] == "scifor:ColName"


def test_record_id_selection_ignored_for_a_table_without_ids():
    """scidb strips the record-id column from Fixed / ColumnSelection / Merge
    inputs, but its row selection still names their record ids. Applying it
    to a table with no ids kept zero rows: every ColumnSelection and Fixed
    input of a scidb.for_each saved nothing (MATLAB run 2026-11-02, 34
    failing tests). The schema filter alone decides those rows."""
    scifor.set_schema(["subject"])
    d = Described()
    tbl = d.table({"subject": [1, 1, 2], "signal": [10, 20, 30]})
    plan, _ = _plan(
        *d.run([{"name": "v", "kind": "column_selection", "table": tbl,
                 "columns": ["signal"], "iterate": False, "groups": []}],
               record_id_column="x__record_id"),
        prebuilt_combos=[{"subject": 1}], row_selection=[{"v": ["rid-a"]}],
    )
    (v,) = plan["inputs"]
    assert v["drop"] == []
    assert _rows(plan, v["slice"], 0) == [1, 2]
    _close(plan)


def test_fatal_identifiers_end_the_run():
    """A key-type error from scidb's PathInput loader (and the structural
    for_columns errors) must end the run, as ``scifor_fatal`` errors do in
    Python's loop, not become one failure per combo: TestSchemaKeyTypes
    expects scidb.for_each to raise scidb:SchemaKeyTypeError."""
    d = Described()
    plan, _ = _plan(*d.run([], {"subject": [1]}))
    assert {"scidb:SchemaKeyTypeError", "scifor:for_each:forColumnsBadReturn"} <= set(plan["fatal_ids"])
    _close(plan)


def test_for_columns_missing_column_refuses_the_call():
    """for_columns naming a column the table lacks must raise, not run
    (TestForColumns/test_missing_column_raises). Python's per-combo drift
    check (``_check_column_drift``) runs once in the plan, since a MATLAB
    table is the same at every combo; stage 3 first dropped it as moot."""
    scifor.set_schema(["subject"])
    d = Described()
    tbl = d.table({"subject": [1], "StepLength": [1]})
    out = matlab_loop.plan(*d.run(
        [{"name": "value", "kind": "column_selection", "table": tbl,
          "columns": ["StepLength", "DoesNotExist"], "iterate": True, "groups": []}],
        {"subject": [1]},
    ))
    refused = json.loads(out["json"])
    assert refused["error_id"] == "scifor:for_each"
    assert "DoesNotExist" in refused["error_message"]
