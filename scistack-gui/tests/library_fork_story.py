"""The fork-chain story, shared by ``test_library_fork_chain.py`` and
``test_library_tunables.py``. Not a test module.

Four people, each on their own computer: a separate process per session
(``library_fork_actor.py``) with only the libraries THEY installed on
``PYTHONPATH``, so a library that still imports one its recipient does not
have fails exactly as it would on the recipient's machine.

* **Alice** (``alice_study``) builds a submodule ``emg``
  (``EmgRaw -> filter_emg(cutoff=CUTOFF, gain=1.0) -> EmgFiltered``),
  places it and runs it, then **⇪ Share**s it as ``emg_alice``. Two more
  submodules are shared the same way: ``envelope`` (MATLAB) as ``env_alice``
  and ``load`` (a PathInput) as ``load_alice``.
* **Bob** (``bob_study``) installs ``emg_alice``, places and runs it, then
  **✎ Make my own copy** as ``emg_bob``. He edits the code
  (``cutoff / 10`` -> ``cutoff / 5``), and in the GUI adds his own
  ``rectify`` step, runs, and ⇪ Shares again (publishes).
* **Carol** (``carol_study``) installs ONLY ``emg_bob``, runs it, and makes
  her own copy ``emg_carol``.
* **Dave** (``dave_study``) installs ONLY ``emg_carol`` and runs it.

Where libraries live (decision R1): a library a project AUTHORS -- made by
⇪ Share or ✎ Make my own copy -- is the folder ``<project>/libs/<name>/``,
with the one layout ``scidb.library.create_library`` writes. "Installing"
someone's library here means putting ``<their project>/libs/<name>/src`` on
``PYTHONPATH`` (what ``pip install -e`` does for imports).

A code (terminal) user's scripts also see their own project's ``src/`` and
``libs/*/src`` (the ``pip install -e`` the reports print). GUI sessions do
NOT get those: the project must make its own authored libraries importable
itself (B3).

Every Python call of ``helpers.scale`` appends a line to
``$EMG_CALL_LOG`` (one file per person), so a test can prove that carrying
results across a rename computed nothing.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

ACTOR = Path(__file__).with_name("library_fork_actor.py")

LIB_A, LIB_B, LIB_C = "emg_alice", "emg_bob", "emg_carol"
ENV_A, ENV_B = "env_alice", "env_bob"
LOAD_A = "load_alice"
#: Whose project authors each library (where its folder is).
LIB_HOME = {LIB_A: "alice", ENV_A: "alice", LOAD_A: "alice",
            LIB_B: "bob", ENV_B: "bob", LIB_C: "carol"}
PROJECT_PACKAGES = ("alice_study", "bob_study", "carol_study", "dave_study")

SUBJECTS = ["S01", "S02"]
RAW = {"S01": [1.0, -2.0, 3.0], "S02": [-4.0, 5.0, -6.0]}
CUTOFF = 20
#: Alice's filter_emg: scales by cutoff / 10 * gain (gain defaults to 1.0).
ALICE_FILTERED = {s: [x * CUTOFF / 10 for x in v] for s, v in RAW.items()}
#: Bob's edit: cutoff / 5.
BOB_FILTERED = {s: [x * CUTOFF / 5 for x in v] for s, v in RAW.items()}
BOB_RECTIFIED = {s: [abs(x) for x in v] for s, v in BOB_FILTERED.items()}


class StageFailed(AssertionError):
    pass


# ---------------------------------------------------------------------------
# Project files (setup, not under test)
# ---------------------------------------------------------------------------


def write_project(root: Path, pkg: str, files: dict[str, str], entities: str,
                  *, matlab: "dict[str, str] | None" = None) -> None:
    src = root / "src" / pkg
    src.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "{pkg}"\nversion = "0.1.0"\ndependencies = ["scidb", "numpy"]\n'
    )
    (src / "__init__.py").write_text("")
    for rel, text in files.items():
        (src / rel).write_text(textwrap.dedent(text))
    (src / "scistack_entities.toml").write_text(textwrap.dedent(entities))
    config = f'modules = ["."]\nentities_file = "src/{pkg}/scistack_entities.toml"\n'
    if matlab is not None:
        mdir = root / "matlab"
        mdir.mkdir()
        for name, text in matlab.items():
            (mdir / name).write_text(text)
        config += '\n[matlab]\nsources = ["matlab"]\n'
    (root / "scistack.toml").write_text(config)


ALICE_FILES = {
    "helpers.py": """\
        import os

        import numpy as np


        def scale(x, k):
            log = os.environ.get("EMG_CALL_LOG")
            if log:
                with open(log, "a") as f:
                    f.write("scale\\n")
            return np.asarray(x, dtype=float) * float(k)
        """,
    "steps.py": """\
        from alice_study.helpers import scale


        def filter_emg(signal, cutoff, gain=1.0):
            return scale(signal, cutoff / 10 * gain)
        """,
    "reading.py": """\
        import numpy as np


        def load_emg(path):
            return np.loadtxt(path, delimiter=",", ndmin=1)
        """,
}
ALICE_ENTITIES = """\
    variables = ["EmgRaw", "EmgFiltered", "EmgEnvelope"]

    [parameters]
    CUTOFF = 20

    [path_inputs]
    EmgFile = { template = "{subject}.csv" }
    """
ALICE_MATLAB = {
    "envelope_emg.m": "function y = envelope_emg(x)\ny = smooth_emg(abs(x));\nh = @smooth_emg;\nend\n",
    "smooth_emg.m": "function y = smooth_emg(x)\ny = x;\nend\n",
}
EMG_CANVAS = {
    "op": "canvas", "pipeline": "emg",
    "nodes": [["raw", "variableNode", "EmgRaw"], ["cut", "parameterNode", "CUTOFF"],
              ["fn", "functionNode", "filter_emg"], ["filt", "variableNode", "EmgFiltered"]],
    "edges": [["raw", "fn", None, "in__signal"], ["cut", "fn", None, "in__cutoff"],
              ["fn", "filt", "out__EmgFiltered", None]],
}
ENVELOPE_CANVAS = {
    "op": "canvas", "pipeline": "envelope",
    "nodes": [["filt", "variableNode", "EmgFiltered"], ["env", "functionNode", "envelope_emg"],
              ["out", "variableNode", "EmgEnvelope"]],
    "edges": [["filt", "env", None, "in__x"], ["env", "out", "out__EmgEnvelope", None]],
}
LOAD_CANVAS = {
    "op": "canvas", "pipeline": "load",
    "nodes": [["file", "pathInputNode", "EmgFile"], ["fn", "functionNode", "load_emg"],
              ["raw", "variableNode", "EmgRaw"]],
    "edges": [["file", "fn", None, "in__path"], ["fn", "raw", "out__EmgRaw", None]],
}

BOB_FILES = {
    # Bob's own project code: the step he adds to his copy of the pipeline.
    "extra.py": """\
        import numpy as np


        def rectify(x):
            return np.abs(np.asarray(x, dtype=float))
        """,
}
BOB_ENTITIES = """\
    variables = ["EmgRectified"]
    """
RECTIFY_NODES = {
    "nodes": [["filt2", "variableNode", "EmgFiltered"],
              ["rect", "functionNode", "rectify"],
              ["out", "variableNode", "EmgRectified"]],
    "edges": [["filt2", "rect", None, "in__x"], ["rect", "out", "out__EmgRectified", None]],
}


def runner_script(package: str, with_rectify: bool) -> str:
    """A terminal user's script: their own Variable declarations (identical
    to the library's, so they merge), the functions found in *package*
    wherever its layout put them, ``for_each`` over every subject. Prints
    ``RESULT {json}``."""
    return textwrap.dedent(f"""\
        import importlib, json, pkgutil

        import numpy as np

        import {package}
        from scidb import BaseVariable, configure_database, for_each


        def find(name):
            for m in pkgutil.walk_packages({package}.__path__, "{package}."):
                mod = importlib.import_module(m.name)  # an import of a missing library fails HERE
                fn = getattr(mod, name, None)
                if callable(fn) and getattr(fn, "__module__", "").startswith("{package}"):
                    return fn
            raise SystemExit("{package} has no function " + name)


        class EmgRaw(BaseVariable):
            pass


        class EmgFiltered(BaseVariable):
            pass


        class EmgRectified(BaseVariable):
            pass


        db = configure_database("project.duckdb", ["subject"])
        db.save_batch(EmgRaw, [(np.asarray(v, dtype=float), {{"subject": s}})
                               for s, v in {RAW!r}.items()])
        for_each(find("filter_emg"), {{"signal": EmgRaw, "cutoff": {CUTOFF}}}, [EmgFiltered], subject=[])
        if {with_rectify!r}:
            for_each(find("rectify"), {{"x": EmgFiltered}}, [EmgRectified], subject=[])
        out = {{"EmgFiltered": {{s: [float(x) for x in EmgFiltered.load(subject=s).data]
                                for s in {SUBJECTS!r}}}}}
        if {with_rectify!r}:
            out["EmgRectified"] = {{s: [float(x) for x in EmgRectified.load(subject=s).data]
                                    for s in {SUBJECTS!r}}}
        rows = db._duck._fetchall("SELECT DISTINCT function_name FROM _invocation")
        out["function_names"] = sorted(r[0] for r in rows)
        print("RESULT " + json.dumps(out))
        """)


def script_result(step_result: dict) -> dict:
    line = next(ln for ln in reversed(step_result["stdout"].splitlines())
                if ln.startswith("RESULT "))
    return json.loads(line[len("RESULT "):])


# ---------------------------------------------------------------------------
# The chain: each stage runs once per front end, on first use
# ---------------------------------------------------------------------------


class Chain:
    def __init__(self, front_end: str, base: Path):
        self.front_end = front_end
        self.base = base
        self.gui = front_end == "gui"
        self._done: dict[str, object] = {}
        self.last_log: "Path | None" = None

    # -- plumbing ----------------------------------------------------------

    def root(self, person: str) -> Path:
        return self.base / person

    def db(self, person: str) -> Path:
        return self.root(person) / "project.duckdb"

    def lib(self, name: str) -> Path:
        """The folder of library *name*, in the project that authors it."""
        return self.root(LIB_HOME[name]) / "libs" / name

    def calls(self, person: str) -> int:
        """How many times *person*'s Python steps actually computed."""
        log = self.base / f"calls_{person}.log"
        return len(log.read_text().splitlines()) if log.is_file() else 0

    def session(self, person: str, stage: str, steps: list[dict], *,
                installed: "list[str] | tuple" = (), init: bool = False,
                front_end: "str | None" = None, allow: tuple = ()) -> list:
        """Run one session of *person* with only *installed* libraries
        importable. Raises StageFailed naming the stage, step and output.
        Indexes in *allow* may fail (an expected refusal)."""
        front = front_end or self.front_end
        spec = {"project": str(self.root(person)), "db": str(self.db(person)),
                "front_end": front,
                "steps": ([{"op": "init", "schema_keys": ["subject"]}] if init else []) + steps}
        spec_path = self.base / f"session_{person}_{stage}.json"
        spec_path.write_text(json.dumps(spec))
        paths = [str(self.lib(name) / "src") for name in installed]
        if front == "code":  # what `pip install -e` gave the terminal user
            paths += [str(self.root(person) / "src")]
            paths += [str(p / "src") for p in sorted((self.root(person) / "libs").glob("*"))]
        env = {**os.environ,
               "EMG_CALL_LOG": str(self.base / f"calls_{person}.log"),
               "PYTHONPATH": os.pathsep.join(
                   paths + [p for p in sys.path if p and "/libs/" not in p])}
        proc = subprocess.run([sys.executable, str(ACTOR), str(spec_path)],
                              cwd=self.root(person), capture_output=True, text=True,
                              timeout=900, env=env)
        # Kept for every session, passed or not: a failing assertion on its
        # results (a red node, a re-run) is diagnosed from this log.
        log_path = self.base / f"session_{person}_{stage}.log"
        log_path.write_text(proc.stderr)
        self.last_log = log_path
        line = next((ln for ln in reversed(proc.stdout.splitlines()) if ln.startswith("RESULT ")), None)
        if line is None:
            raise StageFailed(f"[{self.front_end}] {person}/{stage}: no result\n"
                              f"{proc.stdout[-3000:]}\n{proc.stderr[-6000:]}")
        results = json.loads(line[len("RESULT "):])
        for i, r in enumerate(results):
            bad = isinstance(r, dict) and (
                "error" in r
                or (r.get("status", 200) >= 400 and i not in allow)
                or (r.get("code") not in (None, 0) and i not in allow)
            )
            if bad:
                raise StageFailed(
                    f"[{self.front_end}] {person}/{stage}, step {i} "
                    f"({spec['steps'][i]['op']}) failed:\n{json.dumps(r, indent=2)[:6000]}\n"
                    f"--- session log ---\n{proc.stderr[-6000:]}")
        return results

    def once(self, key: str, fn):
        if key not in self._done:
            try:
                self._done[key] = fn()
            except Exception as e:  # later stages report the first failure
                self._done[key] = e
        value = self._done[key]
        if isinstance(value, Exception):
            raise value
        return value

    # -- the user's actions, per front end ----------------------------------

    def share_step(self, pipeline: str, pipeline_id: "str | None", lib: str) -> dict:
        """⇪ Share as library (B1): no destination; the library is made in
        ``<project>/libs/<lib>/`` and the project switches to it."""
        if self.gui:
            return {"op": "http", "method": "POST", "path": "/libraries/share",
                    "json": {"pipeline_id": pipeline_id, "name": lib}}
        return {"op": "cli", "argv": ["library", "--json", "create", "--from-submodule", pipeline,
                                      "--name", lib]}

    def copy_step(self, lib: str, new_name: str) -> dict:
        """✎ Make my own copy, under a new name (B2)."""
        if self.gui:
            return {"op": "http", "method": "POST", "path": "/libraries/copy",
                    "json": {"name": lib, "new_name": new_name}}
        return {"op": "cli", "argv": ["library", "--json", "copy", lib, "--as", new_name]}

    def add_step(self, lib: str) -> dict:
        if self.gui:
            return {"op": "http", "method": "POST", "path": "/libraries", "json": {"name": lib}}
        return {"op": "cli", "argv": ["library", "--json", "add", lib]}

    def list_step(self) -> dict:
        if self.gui:
            return {"op": "http", "method": "GET", "path": "/libraries"}
        return {"op": "cli", "argv": ["library", "--json", "list"]}

    def report_of(self, result: dict) -> dict:
        """A share/copy report from either front end."""
        if self.gui:
            body = result["body"]
            return body.get("report", body)
        return result["json"]["report"]

    def library_rows(self, result: dict) -> list[dict]:
        body = result["body"] if self.gui else result["json"]
        return body["libraries"] if isinstance(body, dict) else body

    def pipeline_id(self, person: str, name: str, installed: "list[str] | tuple" = ()) -> str:
        rows = self.session(person, f"find_{name}", [{"op": "pipelines"}],
                            installed=installed, front_end="gui")[0]["pipelines"]
        matches = [r for r in rows if r["name"] == name]
        assert len(matches) == 1, f"{person}: expected one pipeline {name!r}, got {rows}"
        return matches[0]["pipeline_id"]

    def place_and_run(self, pipeline_id: str, *, with_rectify: bool = False) -> list[dict]:
        """GUI steps: place on main, add raw data, ▶ Run, read back."""
        return [
            {"op": "http", "method": "POST", "path": "/pipelines/main/uses",
             "json": {"parent_pipeline_id": "main", "child_pipeline_id": pipeline_id}},
            {"op": "save", "variable": "EmgRaw",
             "items": [[v, {"subject": s}] for s, v in RAW.items()]},
            {"op": "run", "pipeline_id": pipeline_id},
            {"op": "load", "variable": "EmgFiltered", "subjects": SUBJECTS},
            *([{"op": "load", "variable": "EmgRectified", "subjects": SUBJECTS}]
              if with_rectify else []),
            {"op": "invocations"},
        ]

    def after_rename_steps(self, pipeline_id: "str | None", variable: str = "EmgFiltered") -> list[dict]:
        """What the user sees right after a rename, then a ▶ Run click (GUI).
        Indexes: 0 states, 1 invocations, 2 latest, 3 run, 4 invocations."""
        if not self.gui:
            return [{"op": "invocations"}, {"op": "latest", "variable": variable}]
        return [
            {"op": "graph", "pipeline_id": pipeline_id, "run_states": True},
            {"op": "invocations"},
            {"op": "latest", "variable": variable},
            {"op": "run", "pipeline_id": pipeline_id},
            {"op": "invocations"},
        ]

    def after_rename(self, out: list[dict]) -> dict:
        if not self.gui:
            return {"invocations": out[0], "latest": out[1]}
        return {"states": out[0]["states"], "invocations": out[1], "latest": out[2],
                "rerun_invocations": out[4], "log": str(self.last_log)}

    # -- Alice ---------------------------------------------------------------

    def alice_built(self) -> dict:
        def run():
            write_project(self.root("alice"), "alice_study", ALICE_FILES, ALICE_ENTITIES,
                          matlab=ALICE_MATLAB)
            built = self.session("alice", "build", [EMG_CANVAS, ENVELOPE_CANVAS, LOAD_CANVAS],
                                 init=True, front_end="gui")
            ids = {"emg": built[1]["pipeline_id"], "envelope": built[2]["pipeline_id"],
                   "load": built[3]["pipeline_id"]}
            if self.gui:
                out = self.session("alice", "first_run", self.place_and_run(ids["emg"]))
                ran = {"EmgFiltered": out[3], "function_names": out[4]["function_names"]}
            else:
                out = self.session("alice", "first_run", [
                    {"op": "script", "name": "run_emg.py",
                     "source": runner_script("alice_study", with_rectify=False)}])
                ran = script_result(out[0])
            return {"ids": ids, "ran": ran, "calls": self.calls("alice")}
        return self.once("alice_built", run)

    def alice_shared(self) -> dict:
        def run():
            ids = self.alice_built()["ids"]
            out = self.session("alice", "share", [
                self.share_step("emg", ids["emg"], LIB_A),
                self.share_step("envelope", ids["envelope"], ENV_A),
                self.share_step("load", ids["load"], LOAD_A),
            ])
            after = self.session("alice", "after_share", [
                *self.after_rename_steps(ids["emg"]), self.list_step()])
            return {"emg": self.report_of(out[0]), "envelope": self.report_of(out[1]),
                    "load": self.report_of(out[2]), "after": self.after_rename(after),
                    "libraries": self.library_rows(after[-1]), "calls": self.calls("alice")}
        return self.once("alice_shared", run)

    # -- Bob -----------------------------------------------------------------

    BOB_INSTALLED = [LIB_A, ENV_A]

    def bob_used(self) -> dict:
        """Bob installs Alice's libraries, places emg and runs it."""
        def run():
            self.alice_shared()
            write_project(self.root("bob"), "bob_study", BOB_FILES, BOB_ENTITIES, matlab={})
            self.session("bob", "init", [], init=True, front_end="gui")
            added = self.session("bob", "add", [self.add_step(LIB_A), self.add_step(ENV_A)],
                                 installed=self.BOB_INSTALLED)
            if self.gui:
                rows = added[0]["body"]["libraries"]
                emg = next(p["pipeline_id"] for r in rows if r["library"] == LIB_A
                           for p in r["pipelines"] if p["name"] == "emg")
                out = self.session("bob", "first_run", self.place_and_run(emg),
                                   installed=self.BOB_INSTALLED)
                ran = {"EmgFiltered": out[3], "function_names": out[4]["function_names"]}
            else:
                emg = None
                out = self.session("bob", "first_run", [
                    {"op": "script", "name": "run_emg.py",
                     "source": runner_script(LIB_A, with_rectify=False)}],
                    installed=self.BOB_INSTALLED)
                ran = script_result(out[0])
            return {"emg": emg, "ran": ran, "calls": self.calls("bob")}
        return self.once("bob_used", run)

    def bob_forked(self) -> dict:
        def run():
            emg = self.bob_used()["emg"]
            out = self.session("bob", "fork", [self.copy_step(LIB_A, LIB_B),
                                               self.copy_step(ENV_A, ENV_B)],
                               installed=self.BOB_INSTALLED)
            after = self.session("bob", "after_fork", [
                *self.after_rename_steps(emg), self.list_step()], installed=self.BOB_INSTALLED)
            return {"emg": self.report_of(out[0]), "envelope": self.report_of(out[1]),
                    "after": self.after_rename(after), "libraries": self.library_rows(after[-1]),
                    "calls": self.calls("bob")}
        return self.once("bob_forked", run)

    def bob_edited(self) -> dict:
        """Bob edits his copy's code (both front ends) and, in the GUI, adds
        his own rectify step to the pipeline; then runs it."""
        def run():
            self.bob_forked()
            hits = [p for p in self.lib(LIB_B).rglob("*.py") if "def filter_emg" in p.read_text()]
            assert len(hits) == 1, f"expected one file defining filter_emg in {self.lib(LIB_B)}: {hits}"
            text = hits[0].read_text()
            assert "cutoff / 10" in text, text
            hits[0].write_text(text.replace("cutoff / 10", "cutoff / 5"))  # Bob's code edit
            if not self.gui:
                out = self.session("bob", "after_edit", [
                    {"op": "script", "name": "run_emg.py",
                     "source": runner_script(LIB_B, with_rectify=False)}],
                    installed=self.BOB_INSTALLED)
                return {"ran": script_result(out[0])}
            emg = self.bob_used()["emg"]
            self.session("bob", "edit_canvas", [{"op": "add_nodes", "pipeline_id": emg,
                                                 **RECTIFY_NODES}],
                         installed=self.BOB_INSTALLED)
            out = self.session("bob", "after_edit", [
                {"op": "graph", "pipeline_id": emg, "run_states": True},
                {"op": "run", "pipeline_id": emg},
                {"op": "load", "variable": "EmgFiltered", "subjects": SUBJECTS},
                {"op": "load", "variable": "EmgRectified", "subjects": SUBJECTS},
                {"op": "latest", "variable": "EmgFiltered"},
            ], installed=self.BOB_INSTALLED)
            return {"graph": out[0], "ran": {"EmgFiltered": out[2], "EmgRectified": out[3]},
                    "latest": out[4], "calls": self.calls("bob")}
        return self.once("bob_edited", run)

    def bob_published(self) -> dict:
        """GUI: ⇪ Share on his library's pipeline writes the canvas edit and
        his own rectify into emg_bob (B6). Terminal: nothing to publish (a
        terminal user changed code only, already in the library folder)."""
        def run():
            self.bob_edited()
            if not self.gui:
                return {}
            emg = self.bob_used()["emg"]
            out = self.session("bob", "publish", [self.share_step("emg", emg, LIB_B)],
                               installed=self.BOB_INSTALLED)
            after = self.session("bob", "after_publish",
                                 self.after_rename_steps(emg, variable="EmgRectified"),
                                 installed=self.BOB_INSTALLED)
            return {"report": self.report_of(out[0]), "after": self.after_rename(after),
                    "calls": self.calls("bob")}
        return self.once("bob_published", run)

    # -- Carol / Dave: install only the latest library and run it -------------

    def ran(self, person: str, lib: str) -> dict:
        """*person* installs ONLY *lib*, places its pipeline (which declares
        what it needs by itself: E7), adds data and runs it."""
        write_project(self.root(person), f"{person}_study", {}, "variables = []\n")
        if not self.gui:
            out = self.session(person, "run", [
                self.add_step(lib),
                {"op": "script", "name": "run_emg.py",
                 "source": runner_script(lib, with_rectify=False)},
            ], installed=[lib], init=True)
            return {**script_result(out[2]), "calls": self.calls(person)}  # [0] is init
        added = self.session(person, "add", [self.add_step(lib)],
                             installed=[lib], init=True)[1]["body"]
        row = next(r for r in added["libraries"] if r["library"] == lib)
        assert row["errors"] == [], f"{lib} listed with errors on {person}'s computer: {row}"
        assert [p["name"] for p in row["pipelines"]] == ["emg"], row
        pid = row["pipelines"][0]["pipeline_id"]
        out = self.session(person, "run", [
            *self.place_and_run(pid, with_rectify=True),
            {"op": "graph", "pipeline_id": pid},
        ], installed=[lib])
        return {"pipeline_id": pid, "EmgFiltered": out[3], "EmgRectified": out[4],
                "function_names": out[5]["function_names"],
                "invocation_count": out[5]["count"], "graph": out[6],
                "calls": self.calls(person)}

    def carol_ran(self) -> dict:
        def run():
            self.bob_published()
            return self.ran("carol", LIB_B)
        return self.once("carol_ran", run)

    def carol_forked(self) -> dict:
        def run():
            pid = self.carol_ran().get("pipeline_id")
            out = self.session("carol", "fork", [self.copy_step(LIB_B, LIB_C)], installed=[LIB_B])
            after = self.session("carol", "after_fork", self.after_rename_steps(pid),
                                 installed=[LIB_B])
            return {"report": self.report_of(out[0]), "after": self.after_rename(after),
                    "calls": self.calls("carol")}
        return self.once("carol_forked", run)

    def dave_ran(self) -> dict:
        def run():
            self.carol_forked()
            return self.ran("dave", LIB_C)
        return self.once("dave_ran", run)


# ---------------------------------------------------------------------------
# Reading a library folder
# ---------------------------------------------------------------------------


def py_texts(folder: Path) -> dict[str, str]:
    return {str(p.relative_to(folder)): p.read_text() for p in folder.rglob("*.py")}


def entities(lib_folder: Path, lib: str) -> dict:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib
    return tomllib.loads((lib_folder / "src" / lib / "scistack_entities.toml").read_text())


def document_labels(lib_folder: Path, lib: str, pipeline: str) -> set[str]:
    doc = json.loads((lib_folder / "src" / lib / "pipelines" / f"{pipeline}.json").read_text())
    return {n["label"] for n in doc["canvas"]["nodes"]}


_NOISE = ("__pycache__", ".egg-info", "/dist/", ".pyc")


def shape(lib_folder: Path, lib: str) -> set[str]:
    """The library's files, relative, with its own name written ``<lib>``:
    what must look the same on every machine."""
    out = set()
    for p in lib_folder.rglob("*"):
        rel = "/" + str(p.relative_to(lib_folder)).replace(os.sep, "/")
        if p.is_file() and not any(n in rel for n in _NOISE):
            out.add(rel.replace(lib, "<lib>").lstrip("/"))
    return out


def run_subprocess(code: str, paths: list[str], cwd: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(paths + [p for p in sys.path if p])}
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                          env=env, cwd=cwd)
