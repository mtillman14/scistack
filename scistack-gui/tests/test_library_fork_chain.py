"""Fork chain: share -> install + fork -> edit -> share MY copy -> someone else
installs (and forks) my copy -> it runs.

FAILING tests, drafted 2026-10-23 and rewritten after review rounds 1-3
(``.claude/plan-library-fork-chain.md``). Nothing here is implemented yet
except what ``TestSetup`` exercises. The story and people are in
``library_fork_story.py``; Parameters / PathInputs / tunables are in
``test_library_tunables.py``.

Every stage runs twice, once per front end (the ``chain`` fixture's param):
``gui`` drives the GUI's own HTTP routes (the calls the frontend makes);
``code`` drives the ``scistack library ...`` terminal commands and plain
``for_each`` scripts. A terminal user cannot edit a canvas, so on the
``code`` side Bob's edit is code only (no ``rectify`` step, no publish).

Why it fails today
------------------
Make my own copy puts the code in ``src/<pkg>/<lib>/`` but keeps the names
``<lib>.fn`` (``copied_libraries``): half project code, half library.
``share_as_library`` then reads those functions as another installed
library's (``scidb.names.library_of``), leaves their code out and lists the
old library as a dependency.

Decisions these tests pin
-------------------------
R1 -- a library you make is a library you author
  B1. ⇪ Share as library takes no destination: it writes
      ``<project>/libs/<name>/`` (the ``create_library`` layout) and switches
      the project to it. Same pipeline ids; functions become ``<name>.fn``.
  B2. ✎ Make my own copy takes a new name (``--as`` / ``new_name``) and
      writes ``<project>/libs/<new>/`` with the SAME layout, functions
      renamed, every placement switched, the old library unlisted.
  B3. The project makes its authored libraries importable itself: a GUI
      session sees ``emg_bob`` with nothing extra on ``PYTHONPATH``. The
      report gives terminal users the ``pip install -e`` command.
  B4. Libraries list rows say ``authored: true`` for these.
  B5. An authored library's pipelines are editable (the 🔒 is for libraries
      you install). Closes "a library's wiring can't be edited anywhere".
  B6. The project's canvas is the working copy of an authored library's
      pipeline: it is never re-synced from the files over the user's edits.
      ⇪ Share on it publishes: writes the canvas into the library and
      carries (and renames) any project code the canvas now uses.
  B7. Same shape on every machine: a library's files look the same however
      many times it was copied; nothing nests (no ``emg_bob/emg_alice/``).
  B8. No file names an older library or a project package; the forked
      library is not a dependency; third-party dependencies carry over.
  B9. MATLAB: ``+<new>/`` with every call and handle requalified.
  B10. Collisions refuse, naming both: two functions or two modules that
      would land on the same name / path in the library.
R2 -- results carry across a rename, computing nothing
  C1. When a rename (share or copy) changes a function's name but not its
      code hash, its existing results are re-recorded under the new name
      without calling it; the canvas stays green, ▶ Run adds nothing, the
      newest records name the new function, old records are kept.
  C2. Every renamed function is in the report's ``carried``
      (``{new label: record count}``) or ``not_carried``
      (``[{"function", "reason"}]``), never neither.
  C3. Code that really changed (Bob's edit) is not current and runs.
Accepted 2026-10-23:
  A8. ``[library] derived_from = ["emg_alice"]``: where it came from, never who.
  A9. ``from emg_bob import filter_emg`` works (top-level re-exports).

Edge cases -> tests
-------------------
* Alice's results carry into her library (C1)          TestResultsCarryOver
* Bob's results carry into his copy, no compute (C1)   TestResultsCarryOver
* rectify's results carry when Bob publishes (C1)      TestResultsCarryOver
* Carol's results carry into her copy (C1)             TestResultsCarryOver
* every renamed function accounted for (C2)            test_every_renamed_function_is_carried_or_explained
* Bob's edited step needs a run, then gives his result  test_bob_s_edited_step_is_not_current_and_runs
* library lands in libs/<name>/, project switches (B1)  TestAuthoredLibraries
* copy under a new name, old one unlisted (B2, B4)     TestAuthoredLibraries
* GUI imports its own library with no install (B3)     test_bob_s_session_imports_his_library_without_installing
* Alice can still edit her shared pipeline (B5, B6)    test_the_author_edits_and_republishes
* unpublished canvas edit survives a reopen (B6)       test_bob_s_canvas_edit_survives_reopening
* three generations, one shape, no nesting (B7)         TestSameShapeEverywhere
* Bob's edit ships, Alice's original does not          test_bob_s_edit_ships
* no leaks of old names (B8)                           test_no_file_mentions_an_older_library_or_a_project
* emg_alice not a dependency (B8)                      test_the_forked_library_is_not_a_dependency
* publish carries Bob's own rectify, renamed (B6)      test_publishing_carries_bob_s_own_step
* document names only the new library                  test_the_document_names_only_bob_s_library
* Variables + Parameter defaults carried               test_declarations_carry_over
* Carol without emg_alice runs it                      TestCarolInstallsBobsLibrary
* Dave runs code that came through two forks           test_dave_runs_code_that_came_through_two_forks
* MATLAB requalified through the fork (B9)             TestMatlabThroughTheFork
* function-name collision refuses (B10)                test_a_function_name_collision_refuses_naming_both
* module-path collision refuses (B10)                  test_a_module_path_collision_refuses_naming_both
* copy, edit and publish in one GUI session            test_copy_edit_and_publish_in_one_session
* Alice's later upgrade does not reach Bob's copy      test_alice_s_upgrade_does_not_reach_bob_s_copy
* a built wheel carries what Carol needs               test_the_built_wheel_carries_everything

The user runs these (Python is not available to Claude):

    cd scistack-gui && pytest tests/test_library_fork_chain.py -v

Each failure names the person, stage and step, with that session's log.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from tests.library_fork_story import (
    ALICE_ENTITIES,
    ALICE_FILES,
    ALICE_FILTERED,
    BOB_ENTITIES,
    BOB_FILES,
    BOB_FILTERED,
    BOB_RECTIFIED,
    CUTOFF,
    EMG_CANVAS,
    ENV_A,
    ENV_B,
    LIB_A,
    LIB_B,
    LIB_C,
    PROJECT_PACKAGES,
    RECTIFY_NODES,
    Chain,
    document_labels,
    entities,
    py_texts,
    run_subprocess,
    shape,
    write_project,
)


@pytest.fixture(scope="module", params=["gui", "code"])
def chain(request, tmp_path_factory):
    return Chain(request.param, tmp_path_factory.mktemp(f"fork_chain_{request.param}"))


def _gui_only(chain, why: str) -> None:
    if not chain.gui:
        pytest.skip(why)


def _renamed(report: dict) -> dict:
    return report["functions"]


# ---------------------------------------------------------------------------
# Setup: what works today (a failure here is a test-harness problem)
# ---------------------------------------------------------------------------


class TestSetup:
    def test_alice_s_project_runs(self, chain):
        built = chain.alice_built()
        assert built["ran"]["EmgFiltered"] == ALICE_FILTERED
        assert built["ran"]["function_names"] == ["filter_emg"]
        assert built["calls"] > 0, "helpers.scale never wrote to the call log"


# ---------------------------------------------------------------------------
# R1: libraries you make are libraries you author (B1-B6)
# ---------------------------------------------------------------------------


class TestAuthoredLibraries:
    def test_alice_s_share_lands_in_her_project(self, chain):
        """B1: no destination asked; the library is <project>/libs/<name>/."""
        report = chain.alice_shared()["emg"]
        folder = chain.root("alice") / "libs" / LIB_A
        assert Path(report["folder"]).resolve() == folder.resolve()
        assert (folder / "pyproject.toml").is_file()
        assert (folder / "src" / LIB_A / "pipelines" / "emg.json").is_file()
        assert _renamed(report) == {"filter_emg": f"{LIB_A}.filter_emg"}

    def test_alice_s_project_switches_to_her_library(self, chain):
        """B1/B4: one copy. The project lists emg_alice as authored."""
        rows = {r["library"]: r for r in chain.alice_shared()["libraries"]}
        assert rows.get(LIB_A, {}).get("authored") is True, rows

    def test_alice_s_canvas_names_her_library(self, chain):
        _gui_only(chain, "canvas")
        states = chain.alice_shared()["after"]["states"]
        assert set(states) == {f"{LIB_A}.filter_emg"}, states

    def test_bob_s_copy_lands_in_his_project_under_his_name(self, chain):
        """B2: a new name, the same layout, a terminal user is told how to
        install it."""
        report = chain.bob_forked()["emg"]
        folder = chain.root("bob") / "libs" / LIB_B
        assert Path(report["folder"]).resolve() == folder.resolve()
        assert _renamed(report) == {f"{LIB_A}.filter_emg": f"{LIB_B}.filter_emg"}
        assert str(folder) in report["install"] and "pip install -e" in report["install"]

    def test_bob_s_project_lists_his_copy_not_alice_s(self, chain):
        """B2/B4: the copy replaces the original in the project's list (it
        stays installed: SciStack never uninstalls)."""
        rows = {r["library"]: r for r in chain.bob_forked()["libraries"]}
        assert LIB_A not in rows and ENV_A not in rows, sorted(rows)
        assert rows[LIB_B].get("authored") is True and rows[ENV_B].get("authored") is True

    def test_bob_s_session_imports_his_library_without_installing(self, chain):
        """B3: Bob's GUI session has only Alice's libraries on PYTHONPATH and
        still runs emg_bob (the after-fork ▶ Run would fail otherwise)."""
        _gui_only(chain, "a terminal user installs it with the printed command")
        assert chain.bob_forked()["after"]["rerun_invocations"]["count"] > 0

    def test_bob_s_canvas_edit_survives_reopening(self, chain):
        """B5/B6: adding a step to his library's pipeline is allowed, and a
        new session (discovery runs again) does not re-sync emg_bob's files
        over the unpublished edit."""
        _gui_only(chain, "canvas")
        labels = chain.bob_edited()["graph"]["labels"]
        assert "rectify" in labels and "EmgRectified" in labels, labels

    def test_publishing_carries_bob_s_own_step(self, chain):
        """B6: ⇪ Share on emg_bob's pipeline carries the project code it now
        uses (bob_study.extra.rectify), renamed into emg_bob."""
        _gui_only(chain, "canvas")
        report = chain.bob_published()["report"]
        assert _renamed(report) == {"rectify": f"{LIB_B}.rectify"}
        assert report["kept_labels"] == []
        assert any("def rectify" in t for t in py_texts(chain.lib(LIB_B)).values())


def test_the_author_edits_and_republishes(chain):
    """B5/B6: after sharing, Alice can still change her pipeline; the
    library's files change only when she shares again."""
    _gui_only(chain, "canvas")
    chain.alice_shared()
    person = "alice_again"
    write_project(chain.root(person), "alice_study", ALICE_FILES, ALICE_ENTITIES)
    built = chain.session(person, "build", [EMG_CANVAS], init=True)
    pid = built[1]["pipeline_id"]
    chain.session(person, "share", [chain.share_step("emg", pid, "emg_again")])
    folder = chain.root(person) / "libs" / "emg_again"
    doc = folder / "src" / "emg_again" / "pipelines" / "emg.json"
    before = doc.read_text()
    edit = chain.session(person, "edit", [{"op": "add_nodes", "pipeline_id": pid,
                                           "nodes": [["n", "variableNode", "EmgEnvelope"]],
                                           "edges": []}])
    assert edit[0]["nodes"]["n"]["status"] == 200, edit[0]
    assert doc.read_text() == before, "an unpublished edit changed the library's files"
    chain.session(person, "republish", [chain.share_step("emg", pid, "emg_again")])
    assert "EmgEnvelope" in document_labels(folder, "emg_again", "emg")


# ---------------------------------------------------------------------------
# R2: results carry across a rename (C1-C3)
# ---------------------------------------------------------------------------


def _assert_carried(after: dict, old: str, new: str, calls_before: int, calls_after: int,
                    gui: bool) -> None:
    names = after["invocations"]["function_names"]
    assert new in names, f"no history under {new}: {names}"
    assert old in names, f"old history under {old} was removed: {names}"
    assert after["latest"]["functions"] == [new], after["latest"]
    assert calls_after == calls_before, (
        f"carrying results computed again ({calls_after - calls_before} call(s))")
    if gui:
        assert set(after["states"].values()) == {"green"}, after["states"]
        assert after["rerun_invocations"]["count"] == after["invocations"]["count"], (
            "▶ Run after the rename recorded new invocations: the results were not current")


class TestResultsCarryOver:
    def test_alice_s_results_carry_into_her_library(self, chain):
        built, shared = chain.alice_built(), chain.alice_shared()
        _assert_carried(shared["after"], "filter_emg", f"{LIB_A}.filter_emg",
                        built["calls"], shared["calls"], chain.gui)

    def test_bob_s_results_carry_into_his_copy(self, chain):
        used, forked = chain.bob_used(), chain.bob_forked()
        _assert_carried(forked["after"], f"{LIB_A}.filter_emg", f"{LIB_B}.filter_emg",
                        used["calls"], forked["calls"], chain.gui)

    def test_rectify_results_carry_when_bob_publishes(self, chain):
        _gui_only(chain, "publish is a canvas action")
        edited, published = chain.bob_edited(), chain.bob_published()
        _assert_carried(published["after"], "rectify", f"{LIB_B}.rectify",
                        edited["calls"], published["calls"], True)

    def test_carol_s_results_carry_into_her_copy(self, chain):
        ran, forked = chain.carol_ran(), chain.carol_forked()
        _assert_carried(forked["after"], f"{LIB_B}.filter_emg", f"{LIB_C}.filter_emg",
                        ran["calls"], forked["calls"], chain.gui)

    def test_every_renamed_function_is_carried_or_explained(self, chain):
        """C2, for every rename in the story (MATLAB included: its calls are
        requalified, so it may honestly be not_carried)."""
        reports = [chain.alice_shared()["emg"], chain.alice_shared()["envelope"],
                   chain.bob_forked()["emg"], chain.bob_forked()["envelope"],
                   chain.carol_forked()["report"]]
        if chain.gui:
            reports.append(chain.bob_published()["report"])
        for r in reports:
            new = set(_renamed(r).values())
            carried = set(r["carried"])
            not_carried = {x["function"] for x in r["not_carried"]}
            assert new == carried | not_carried and not (carried & not_carried), r
            assert all(x["reason"] for x in r["not_carried"]), r["not_carried"]

    def test_bob_s_edited_step_is_not_current_and_runs(self, chain):
        """C3"""
        edited = chain.bob_edited()
        assert edited["ran"]["EmgFiltered"] == BOB_FILTERED
        if chain.gui:
            states = edited["graph"]["states"]
            assert states[f"{LIB_B}.filter_emg"] != "green", states
            assert edited["ran"]["EmgRectified"] == BOB_RECTIFIED
            assert edited["latest"]["functions"] == [f"{LIB_B}.filter_emg"]


# ---------------------------------------------------------------------------
# B7: the same shape on every machine
# ---------------------------------------------------------------------------


class TestSameShapeEverywhere:
    def test_three_generations_have_one_shape(self, chain):
        chain.carol_forked()
        alice = shape(chain.lib(LIB_A), LIB_A)
        bob = shape(chain.lib(LIB_B), LIB_B)
        carol = shape(chain.lib(LIB_C), LIB_C)
        assert alice <= bob, f"files of emg_alice missing from emg_bob: {sorted(alice - bob)}"
        extra = {"src/<lib>/extra.py"} if chain.gui else set()  # Bob's own published step
        assert bob - alice == extra, sorted(bob - alice)
        assert carol == bob, sorted(carol ^ bob)

    def test_nothing_nests(self, chain):
        chain.carol_forked()
        names = {LIB_A, LIB_B, LIB_C, *PROJECT_PACKAGES}
        for lib in (LIB_B, LIB_C):
            pkg = chain.lib(lib) / "src" / lib
            nested = [str(p.relative_to(pkg)) for p in pkg.rglob("*")
                      if p.is_dir() and p.name in names]
            assert nested == [], f"{lib} nests another library or project: {nested}"


# ---------------------------------------------------------------------------
# What Bob's library holds
# ---------------------------------------------------------------------------


class TestBobsLibrary:
    def test_bob_s_edit_ships(self, chain):
        chain.bob_published()
        texts = py_texts(chain.lib(LIB_B))
        assert any("def filter_emg" in t and "cutoff / 5" in t for t in texts.values()), sorted(texts)
        assert not any("cutoff / 10" in t for t in texts.values()), "Alice's original shipped"

    def test_no_file_mentions_an_older_library_or_a_project(self, chain):
        """B8"""
        chain.carol_forked()
        for lib, banned in ((LIB_B, (LIB_A,)), (LIB_C, (LIB_A, LIB_B))):
            texts = py_texts(chain.lib(lib))
            leaks = {f: [w for w in (*banned, *PROJECT_PACKAGES) if w in t]
                     for f, t in texts.items()}
            assert {f: w for f, w in leaks.items() if w} == {}, lib

    def test_the_forked_library_is_not_a_dependency(self, chain):
        """B8"""
        chain.carol_forked()
        bob = (chain.lib(LIB_B) / "pyproject.toml").read_text()
        carol = (chain.lib(LIB_C) / "pyproject.toml").read_text()
        assert LIB_A not in bob and LIB_A not in carol and LIB_B not in carol
        assert "numpy" in bob and "numpy" in carol  # Alice's third-party dependency

    def test_the_document_names_only_bob_s_library(self, chain):
        chain.bob_published()
        labels = document_labels(chain.lib(LIB_B), LIB_B, "emg")
        expected = {f"{LIB_B}.filter_emg", "EmgRaw", "EmgFiltered", "CUTOFF"}
        if chain.gui:
            expected |= {f"{LIB_B}.rectify", "EmgRectified"}
        assert labels == expected

    def test_declarations_carry_over(self, chain):
        chain.bob_published()
        data = entities(chain.lib(LIB_B), LIB_B)
        needed = {"EmgRaw", "EmgFiltered"} | ({"EmgRectified"} if chain.gui else set())
        assert needed <= set(data["variables"])
        assert data["parameters"] == {"CUTOFF": CUTOFF}

    def test_the_derivative_records_its_origin(self, chain):
        """A8: where it came from, never who."""
        chain.carol_forked()
        assert entities(chain.lib(LIB_B), LIB_B)["library"].get("derived_from") == [LIB_A]
        assert entities(chain.lib(LIB_C), LIB_C)["library"].get("derived_from") == [LIB_B, LIB_A]

    def test_code_users_import_from_the_top_level(self, chain):
        """A9: ``from emg_bob import filter_emg``."""
        chain.bob_published()
        proc = run_subprocess(f"from {LIB_B} import filter_emg",
                              [str(chain.lib(LIB_B) / "src")], chain.base)
        assert proc.returncode == 0, proc.stderr


# ---------------------------------------------------------------------------
# Carol installs ONLY emg_bob and runs it; Dave installs ONLY emg_carol
# ---------------------------------------------------------------------------


class TestCarolInstallsBobsLibrary:
    def test_carol_gets_bob_s_results(self, chain):
        ran = chain.carol_ran()
        assert ran["EmgFiltered"] == BOB_FILTERED
        if chain.gui:
            assert ran["EmgRectified"] == BOB_RECTIFIED

    def test_carol_s_history_names_bob_s_library(self, chain):
        ran = chain.carol_ran()
        expected = [f"{LIB_B}.filter_emg"] + ([f"{LIB_B}.rectify"] if chain.gui else [])
        assert ran["function_names"] == expected

    def test_carol_s_canvas_shows_bob_s_pipeline(self, chain):
        _gui_only(chain, "canvas")
        labels = chain.carol_ran()["graph"]["labels"]
        assert f"{LIB_B}.filter_emg" in labels and f"{LIB_B}.rectify" in labels
        assert not [label for label in labels if label.startswith(f"{LIB_A}.")]

    def test_dave_runs_code_that_came_through_two_forks(self, chain):
        ran = chain.dave_ran()
        assert ran["EmgFiltered"] == BOB_FILTERED
        expected = [f"{LIB_C}.filter_emg"] + ([f"{LIB_C}.rectify"] if chain.gui else [])
        assert ran["function_names"] == expected


# ---------------------------------------------------------------------------
# B9: MATLAB through the fork (structure only: running needs MATLAB)
# ---------------------------------------------------------------------------


class TestMatlabThroughTheFork:
    def test_matlab_functions_are_copied_and_requalified(self, chain):
        report = chain.bob_forked()["envelope"]
        assert _renamed(report) == {f"{ENV_A}.envelope_emg": f"{ENV_B}.envelope_emg"}
        mdir = chain.lib(ENV_B) / "src" / ENV_B / "matlab" / f"+{ENV_B}"
        text = (mdir / "envelope_emg.m").read_text()
        assert f"{ENV_B}.smooth_emg(abs(x))" in text and f"@{ENV_B}.smooth_emg" in text
        assert ENV_A not in text
        assert (mdir / "smooth_emg.m").is_file()
        assert ENV_A not in (chain.lib(ENV_B) / "pyproject.toml").read_text()


# ---------------------------------------------------------------------------
# B10 and one-session flows (GUI, each on a fresh Bob of its own)
# ---------------------------------------------------------------------------


def _fresh_bob(chain, person: str, files: dict) -> str:
    """A new Bob who installs emg_alice and copies it as emg_bob; returns
    the emg pipeline id (unchanged by the copy, B2)."""
    chain.alice_shared()
    write_project(chain.root(person), "bob_study", files, BOB_ENTITIES, matlab={})
    chain.session(person, "add", [chain.add_step(LIB_A)], installed=[LIB_A], init=True)
    return chain.pipeline_id(person, "emg", [LIB_A])


def test_copy_edit_and_publish_in_one_session(chain):
    """The GUI user clicks ✎, adds a step and clicks ⇪ without reopening."""
    _gui_only(chain, "one GUI session")
    person = "bob_quick"
    pid = _fresh_bob(chain, person, BOB_FILES)
    out = chain.session(person, "all_at_once", [
        chain.copy_step(LIB_A, "emg_quick"),
        {"op": "add_nodes", "pipeline_id": pid, **RECTIFY_NODES},
        chain.share_step("emg", pid, "emg_quick"),
    ], installed=[LIB_A])
    report = chain.report_of(out[2])
    assert report["kept_labels"] == []
    assert _renamed(report) == {"rectify": "emg_quick.rectify"}
    assert "emg_quick.rectify" in document_labels(
        chain.root(person) / "libs" / "emg_quick", "emg_quick", "emg")


def test_a_function_name_collision_refuses_naming_both(chain):
    """B10: Bob's OWN filter_emg (bob_study/own.py) and emg_bob.filter_emg
    would both be emg_bob.filter_emg."""
    _gui_only(chain, "needs a canvas edit")
    person = "bob_clash"
    files = {**BOB_FILES, "own.py": """\
        def filter_emg(signal, cutoff):
            return signal
        """}
    pid = _fresh_bob(chain, person, files)
    lib = chain.root(person) / "libs" / LIB_B
    out = chain.session(person, "clash", [
        chain.copy_step(LIB_A, LIB_B),
        {"op": "add_nodes", "pipeline_id": pid,
         "nodes": [["raw", "variableNode", "EmgRaw"], ["own", "functionNode", "filter_emg"],
                   ["out", "variableNode", "EmgRectified"]],
         "edges": [["raw", "own", None, "in__signal"], ["own", "out", "out__EmgRectified", None]]},
        chain.share_step("emg", pid, LIB_B),
    ], installed=[LIB_A], allow=(2,))
    refused = out[2]
    assert refused["status"] == 400, refused
    detail = json.dumps(refused["body"])
    assert "filter_emg" in detail and "own" in detail and LIB_B in detail, detail
    assert "EmgRectified" not in document_labels(lib, LIB_B, "emg"), (
        "a refused publish still wrote the library")


def test_a_module_path_collision_refuses_naming_both(chain):
    """B10: Bob's own bob_study/steps.py would land on emg_bob/steps.py,
    which already holds filter_emg."""
    _gui_only(chain, "needs a canvas edit")
    person = "bob_modclash"
    files = {**BOB_FILES, "steps.py": """\
        def smooth(x):
            return x
        """}
    pid = _fresh_bob(chain, person, files)
    out = chain.session(person, "clash", [
        chain.copy_step(LIB_A, LIB_B),
        {"op": "add_nodes", "pipeline_id": pid,
         "nodes": [["f", "variableNode", "EmgFiltered"], ["s", "functionNode", "smooth"],
                   ["out", "variableNode", "EmgRectified"]],
         "edges": [["f", "s", None, "in__x"], ["s", "out", "out__EmgRectified", None]]},
        chain.share_step("emg", pid, LIB_B),
    ], installed=[LIB_A], allow=(2,))
    refused = out[2]
    assert refused["status"] == 400, refused
    detail = json.dumps(refused["body"])
    assert "steps.py" in detail and "bob_study" in detail, detail


def test_alice_s_upgrade_does_not_reach_bob_s_copy(chain):
    """After the copy, Alice changing emg_alice (still installed on Bob's
    computer, -e) reaches nothing of Bob's."""
    chain.bob_edited()
    hits = [p for p in chain.lib(LIB_A).rglob("*.py") if "def filter_emg" in p.read_text()]
    original = hits[0].read_text()
    hits[0].write_text(original.replace("cutoff / 10", "cutoff * 100"))
    try:
        rows = chain.session("bob", "after_alice_upgrade", [{"op": "pipelines"}],
                             installed=chain.BOB_INSTALLED, front_end="gui")[0]["pipelines"]
        emg = [r for r in rows if r["name"] == "emg"]
        assert len(emg) == 1 and emg[0]["library"] == LIB_B, rows
        texts = py_texts(chain.lib(LIB_B))
        assert any("cutoff / 5" in t for t in texts.values())
        assert not any("cutoff * 100" in t for t in texts.values())
    finally:
        hits[0].write_text(original)


# ---------------------------------------------------------------------------
# Packaging: the wheel Carol would install from carries what she needs
# ---------------------------------------------------------------------------


def test_the_built_wheel_carries_everything(chain, tmp_path):
    pytest.importorskip("hatchling")
    _gui_only(chain, "same wheel either way; built once")
    chain.bob_published()
    out = chain.session("bob", "build", [{"op": "cli", "argv": [
        "library", "--json", "build", str(chain.lib(LIB_B)), "--out", str(tmp_path / "dist")]}],
        installed=chain.BOB_INSTALLED, front_end="code")
    wheel = Path(out[0]["json"]["wheel"])
    with zipfile.ZipFile(wheel) as z:
        names = set(z.namelist())
        for rel in (f"{LIB_B}/pipelines/emg.json", f"{LIB_B}/scistack_entities.toml",
                    f"{LIB_B}/__init__.py"):
            assert rel in names, f"{rel} missing from {wheel.name}: {sorted(names)}"
        assert any(n.startswith(f"{LIB_B}/") and n.endswith(".py")
                   and b"def filter_emg" in z.read(n) for n in names), (
            f"no module of {wheel.name} defines filter_emg")
