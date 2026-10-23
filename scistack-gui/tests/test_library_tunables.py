"""Parameters, PathInputs and other tunable values of a placed library
pipeline: FAILING tests drafted 2026-10-23 (review round 3,
``.claude/plan-library-fork-chain.md``). GUI only: these are canvas and
sidebar features. The libraries come from Alice in
``library_fork_story.py`` (``emg_alice``: ``filter_emg(signal, cutoff=CUTOFF,
gain=1.0)``; ``load_alice``: PathInput ``EmgFile`` -> ``load_emg``).

Decisions these tests pin
-------------------------
D1. **Project always trumps library.** A Parameter / PathInput the project
    already declares under the same name is used, never overwritten; the
    placement check says so (``project_values``).
    (Variables keep today's error on conflicting definitions.)
D2. PathInput: the library carries templates, never a root folder. A run
    before the root is set says which PathInput needs one; after it is set,
    the run works.
E1. A placed library pipeline lists its tunables:
    ``GET /api/pipeline-uses/{use_id}/tunables`` ->
    ``{"parameters": [{"name", "value", "source"}],
       "path_inputs": [{"name", "template", "root_folder", "needs_root"}],
       "step_values": [{"key", "value", "default", "parameter"}],
       "key_map": {...}}``. ``source`` is ``"project"`` or
    ``"library default"``; a step value is a function input not wired to a
    Parameter (``key`` = ``"<fn label>.<input>"``).
E2. A project-level view: ``GET /api/tunables`` -> ``[{"use_id", "path",
    ...same fields}]``, one row per placement, nested ones included
    (``path``: pipeline names from the top). View only: edits go through
    the placement (one owner: the use's binding).
E3. ``PUT /api/pipeline-uses/{use_id}/tunables {key, value}`` writes that
    placement's binding; the next run uses it.
E4. Two placements of one library keep their own values.
E5. ``POST /api/pipeline-uses/{use_id}/tunables/promote {key, parameter}``
    makes a project Parameter holding the current value and points the
    placement at it; editing the Parameter then changes that placement.
E6. The sidebar's Parameter rows say which libraries use them
    (``used_by_libraries``).
E7. Placing declares what the project lacks by itself (no separate
    "declare" click): Parameters with the library default, PathInputs with
    the template and no root.
E8. The share report lists step values the author left un-wired
    (``step_values``), so intended knobs can become Parameters first.

    cd scistack-gui && pytest tests/test_library_tunables.py -v
"""

from __future__ import annotations

import json

import pytest

from tests.library_fork_story import (
    ALICE_FILTERED,
    CUTOFF,
    LIB_A,
    LOAD_A,
    RAW,
    SUBJECTS,
    Chain,
    StageFailed,
    write_project,
)

GAIN_KEY = f"{LIB_A}.filter_emg.gain"


@pytest.fixture(scope="module")
def chain(tmp_path_factory):
    return Chain("gui", tmp_path_factory.mktemp("tunables"))


def _scaled(factor: float) -> dict:
    return {s: [x * factor for x in v] for s, v in ALICE_FILTERED.items()}


def _person(chain: Chain, person: str, lib: str, before: "list[dict]" = ()) -> str:
    """A new project that installs *lib*; *before* runs first (the
    project's own declarations). Returns the library's pipeline id."""
    chain.alice_shared()
    write_project(chain.root(person), f"{person}_study", {}, "variables = []\n")
    out = chain.session(person, "add", [*before, chain.add_step(lib)], installed=[lib], init=True)
    row = next(r for r in out[-1]["body"]["libraries"] if r["library"] == lib)
    return row["pipelines"][0]["pipeline_id"]


def _place(parent: str, child: str) -> dict:
    return {"op": "http", "method": "POST", "path": f"/pipelines/{parent}/uses",
            "json": {"parent_pipeline_id": parent, "child_pipeline_id": child}}


def _get(path: str, params: "dict | None" = None) -> dict:
    return {"op": "http", "method": "GET", "path": path, "params": params}


def _save_raw() -> dict:
    return {"op": "save", "variable": "EmgRaw",
            "items": [[v, {"subject": s}] for s, v in RAW.items()]}


def _load(variable: str) -> dict:
    return {"op": "load", "variable": variable, "subjects": SUBJECTS}


def _param(rows: list, name: str) -> dict:
    return next((r for r in rows if r["name"] == name), {})


# ---------------------------------------------------------------------------
# E7 / E6: placing declares what it needs
# ---------------------------------------------------------------------------


class TestPlacingDeclaresWhatItNeeds:
    def test_placing_declares_missing_parameters(self, chain):
        """E7: no separate declare step."""
        pid = _person(chain, "tia", LIB_A)
        out = chain.session("tia", "place", [_place("main", pid), _get("/parameters")],
                            installed=[LIB_A])
        assert _param(out[1]["body"], "CUTOFF").get("values") == [CUTOFF], out[1]["body"]

    def test_the_parameter_row_names_the_library(self, chain):
        """E6"""
        pid = _person(chain, "tia6", LIB_A)
        out = chain.session("tia6", "place", [_place("main", pid), _get("/parameters")],
                            installed=[LIB_A])
        assert _param(out[1]["body"], "CUTOFF").get("used_by_libraries") == [LIB_A]

    def test_the_share_report_lists_unwired_step_values(self, chain):
        """E8"""
        assert chain.alice_shared()["emg"]["step_values"] == [GAIN_KEY]


# ---------------------------------------------------------------------------
# D1: project always trumps library
# ---------------------------------------------------------------------------


class TestProjectTrumpsLibrary:
    def test_the_project_s_parameter_value_is_used(self, chain):
        own = {"op": "http", "method": "POST", "path": "/parameters",
               "json": {"name": "CUTOFF", "values": [50]}}
        pid = _person(chain, "tuned", LIB_A, before=[own])
        out = chain.session("tuned", "place_and_run", [
            _get("/libraries/placement-check", {"pipeline_id": pid}),
            _place("main", pid),
            _get("/parameters"),
            _save_raw(),
            {"op": "run", "pipeline_id": pid},
            _load("EmgFiltered"),
        ], installed=[LIB_A])
        check = out[0]["body"]
        assert check["project_values"] == [
            {"name": "CUTOFF", "project": [50], "library": [CUTOFF]}], check
        assert check["missing_parameters"] == []
        assert _param(out[2]["body"], "CUTOFF")["values"] == [50], "the library default overwrote it"
        assert out[5] == {s: [x * 50 / 10 for x in v] for s, v in RAW.items()}

    def test_tunables_say_the_value_is_the_project_s(self, chain):
        own = {"op": "http", "method": "POST", "path": "/parameters",
               "json": {"name": "CUTOFF", "values": [50]}}
        pid = _person(chain, "tuned2", LIB_A, before=[own])
        out = chain.session("tuned2", "place", [_place("main", pid)], installed=[LIB_A])
        use_id = out[0]["body"]["use_id"]
        tun = chain.session("tuned2", "tunables", [_get(f"/pipeline-uses/{use_id}/tunables")],
                            installed=[LIB_A])[0]["body"]
        assert tun["parameters"] == [{"name": "CUTOFF", "value": [50], "source": "project"}]

    def test_the_project_s_path_input_is_used(self, chain):
        data = chain.root("pia_own") / "data"
        data.mkdir(parents=True)
        for s, v in RAW.items():
            (data / f"{s}_raw.csv").write_text(",".join(str(x) for x in v) + "\n")
        own = {"op": "http", "method": "POST", "path": "/path-inputs",
               "json": {"name": "EmgFile", "template": "{subject}_raw.csv",
                        "root_folder": str(data)}}
        pid = _person(chain, "pia_own", LOAD_A, before=[own])
        out = chain.session("pia_own", "run", [
            _place("main", pid), {"op": "run", "pipeline_id": pid}, _load("EmgRaw"),
            _get("/path-inputs"),
        ], installed=[LOAD_A])
        assert out[2] == RAW
        assert _param(out[3]["body"], "EmgFile")["template"] == "{subject}_raw.csv"


# ---------------------------------------------------------------------------
# D2: PathInputs need the recipient's root folder
# ---------------------------------------------------------------------------


class TestPathInputs:
    def _setup(self, chain, person: str) -> tuple[str, str]:
        pid = _person(chain, person, LOAD_A)
        out = chain.session(person, "place", [_place("main", pid)], installed=[LOAD_A])
        return pid, out[0]["body"]["use_id"]

    def test_placing_declares_the_template_and_asks_for_a_root(self, chain):
        pid, use_id = self._setup(chain, "pia")
        out = chain.session("pia", "look", [_get("/path-inputs"),
                                            _get(f"/pipeline-uses/{use_id}/tunables")],
                            installed=[LOAD_A])
        row = _param(out[0]["body"], "EmgFile")
        assert row.get("template") == "{subject}.csv" and not row.get("root_folder"), row
        assert out[1]["body"]["path_inputs"] == [
            {"name": "EmgFile", "template": "{subject}.csv", "root_folder": None,
             "needs_root": True}]

    def test_a_run_without_a_root_says_which_path_input_needs_one(self, chain):
        pid, _ = self._setup(chain, "pia2")
        try:
            out = chain.session("pia2", "run", [{"op": "run", "pipeline_id": pid}],
                                installed=[LOAD_A])
            message = json.dumps(out[0])
        except StageFailed as e:  # refused by raising is fine too, if it says why
            message = str(e)
        assert "EmgFile" in message and "root" in message.lower(), message[:3000]

    def test_after_setting_the_root_it_runs(self, chain):
        pid, _ = self._setup(chain, "pia3")
        data = chain.root("pia3") / "data"
        data.mkdir()
        for s, v in RAW.items():
            (data / f"{s}.csv").write_text(",".join(str(x) for x in v) + "\n")
        out = chain.session("pia3", "run", [
            {"op": "http", "method": "PUT", "path": "/path-inputs/EmgFile",
             "json": {"name": "EmgFile", "template": "{subject}.csv", "root_folder": str(data)}},
            {"op": "run", "pipeline_id": pid},
            _load("EmgRaw"),
        ], installed=[LOAD_A])
        assert out[2] == RAW


# ---------------------------------------------------------------------------
# E1-E5: the tunables form and the project-level view
# ---------------------------------------------------------------------------


def _set(use_id: str, key: str, value) -> dict:
    return {"op": "http", "method": "PUT", "path": f"/pipeline-uses/{use_id}/tunables",
            "json": {"key": key, "value": value}}


class TestTunables:
    def test_a_placement_lists_its_tunables(self, chain):
        """E1"""
        pid = _person(chain, "tun1", LIB_A)
        use_id = chain.session("tun1", "place", [_place("main", pid)],
                               installed=[LIB_A])[0]["body"]["use_id"]
        tun = chain.session("tun1", "look", [_get(f"/pipeline-uses/{use_id}/tunables")],
                            installed=[LIB_A])[0]["body"]
        assert tun["parameters"] == [{"name": "CUTOFF", "value": [CUTOFF],
                                      "source": "library default"}]
        assert tun["step_values"] == [{"key": GAIN_KEY, "value": 1.0, "default": 1.0,
                                       "parameter": None}]
        assert tun["path_inputs"] == [] and tun["key_map"] == {}

    def test_changing_a_step_value_is_used_by_the_next_run(self, chain):
        """E3"""
        pid = _person(chain, "tun2", LIB_A)
        use_id = chain.session("tun2", "place", [_place("main", pid)],
                               installed=[LIB_A])[0]["body"]["use_id"]
        out = chain.session("tun2", "set_and_run", [
            _set(use_id, GAIN_KEY, 2.0),
            _get(f"/pipeline-uses/{use_id}/tunables"),
            _save_raw(),
            {"op": "run", "pipeline_id": pid},
            _load("EmgFiltered"),
        ], installed=[LIB_A])
        assert out[1]["body"]["step_values"][0]["value"] == 2.0
        assert out[4] == _scaled(2.0)

    def test_two_placements_keep_their_own_values(self, chain):
        """E4"""
        pid = _person(chain, "tun3", LIB_A)
        out = chain.session("tun3", "place_twice", [_place("main", pid), _place("main", pid)],
                            installed=[LIB_A])
        first, second = out[0]["body"]["use_id"], out[1]["body"]["use_id"]
        out = chain.session("tun3", "set_one", [
            _set(second, GAIN_KEY, 3.0),
            _get(f"/pipeline-uses/{first}/tunables"),
            _get(f"/pipeline-uses/{second}/tunables"),
        ], installed=[LIB_A])
        assert out[1]["body"]["step_values"][0]["value"] == 1.0
        assert out[2]["body"]["step_values"][0]["value"] == 3.0

    def test_the_project_view_lists_nested_placements(self, chain):
        """E2: emg placed inside the project's own submodule 'analysis',
        itself placed on main."""
        pid = _person(chain, "tun4", LIB_A)
        out = chain.session("tun4", "nest", [
            {"op": "canvas", "pipeline": "analysis", "nodes": [], "edges": []},
        ], installed=[LIB_A])
        analysis = out[0]["pipeline_id"]
        out = chain.session("tun4", "place", [_place(analysis, pid), _place("main", analysis),
                                              _get("/tunables")], installed=[LIB_A])
        inner = out[0]["body"]["use_id"]
        rows = [r for r in out[2]["body"] if any(s["key"] == GAIN_KEY for s in r["step_values"])]
        assert [r["use_id"] for r in rows] == [inner], out[2]["body"]
        assert rows[0]["path"][-2:] == ["analysis", "emg"], rows[0]["path"]

    def test_make_this_a_parameter(self, chain):
        """E5"""
        pid = _person(chain, "tun5", LIB_A)
        use_id = chain.session("tun5", "place", [_place("main", pid)],
                               installed=[LIB_A])[0]["body"]["use_id"]
        out = chain.session("tun5", "promote", [
            {"op": "http", "method": "POST", "path": f"/pipeline-uses/{use_id}/tunables/promote",
             "json": {"key": GAIN_KEY, "parameter": "GAIN"}},
            _get("/parameters"),
            _get(f"/pipeline-uses/{use_id}/tunables"),
            {"op": "http", "method": "PUT", "path": "/parameters/GAIN",
             "json": {"name": "GAIN", "values": [3.0]}},
            _save_raw(),
            {"op": "run", "pipeline_id": pid},
            _load("EmgFiltered"),
        ], installed=[LIB_A])
        assert _param(out[1]["body"], "GAIN").get("values") == [1.0]
        assert out[2]["body"]["step_values"][0]["parameter"] == "GAIN"
        assert out[6] == _scaled(3.0)
