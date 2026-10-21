"""Every execution starts from the project root.

A GUI MATLAB run used to start in the OS temp dir (``run()`` switches to
the generated script's folder), a sidecar run in the server's launch dir,
and a Python run in whatever directory the last run's user code left
behind. A user function opening a relative path then found it, or not,
depending on the route. The MATLAB half is the generated script's
``cd``; the Python half is ``scifor.enter_project_root`` at the top of
each run thread.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = "/studies/gait"


def _script(**kwargs):
    from scistack_gui.api.matlab_command import generate_matlab_command

    return generate_matlab_command(
        function_name="bandpass_filter",
        db_path="/data/experiment.duckdb",
        schema_keys=["subject", "session"],
        run_id="run-xyz",
        **kwargs,
    )


def _pipeline_script(**kwargs):
    from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

    return generate_matlab_pipeline_command(
        pipeline_id="main",
        steps=[
            {
                "function_name": "bandpass_filter",
                "variants": [
                    {
                        "input_types": {"signal": "RawSignal"},
                        "output_type": "FilteredSignal",
                        "constants": {"low_hz": 20},
                        "record_count": 4,
                    }
                ],
            }
        ],
        db_path="/data/experiment.duckdb",
        schema_keys=["subject"],
        run_id="run-pipe",
        **kwargs,
    )


_VARIANT = {
    "input_types": {"signal": "RawSignal"},
    "output_type": "FilteredSignal",
    "constants": {"low_hz": 20},
    "record_count": 4,
}


@pytest.mark.parametrize(
    "make",
    [
        lambda **kw: _script(**kw),  # first-run branch (no variants)
        lambda **kw: _script(variants=[_VARIANT], **kw),
        lambda **kw: _pipeline_script(**kw),
    ],
    ids=["first_run", "variants", "pipeline"],
)
def test_the_script_cds_to_the_project_root_before_anything_else(make):
    """First, so not even the run marker or the pyenv preamble runs from
    the temp dir."""
    cmd = make(project_root=ROOT, python_executable="/usr/bin/python3")
    cd = cmd.index(f"cd('{ROOT}');")
    assert cd < cmd.index("scidb.run_marker('begin'")
    assert cd < cmd.index("pyenv")
    assert "fprintf('[SciStack] working directory: %s\\n', pwd);" in cmd


def test_a_missing_root_warns_instead_of_throwing():
    """A throw here would precede the .started marker and read as "MATLAB
    never launched the script"."""
    cmd = _script(project_root=ROOT)
    assert f"if isfolder('{ROOT}')" in cmd
    assert "warning('SciStack:projectRoot'" in cmd


def test_a_quote_in_the_root_is_escaped():
    cmd = _script(project_root="/studies/o'brien")
    assert "cd('/studies/o''brien');" in cmd


def test_no_project_root_emits_no_cd():
    cmd = _script(project_root=None)
    assert "cd(" not in cmd


def test_every_python_run_thread_enters_the_project_root():
    """Structural guard: each in-process Python run thread calls
    ``enter_project_root`` — per run, because user code may chdir."""
    src = Path(__file__).resolve().parents[1] / "scistack_gui" / "api" / "run.py"
    tree = ast.parse(src.read_text(encoding="utf-8"))
    wanted = {"_run_in_thread", "_run_pipeline_in_thread"}
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            found[node.name] = any(
                isinstance(n, ast.Call)
                and isinstance(n.func, ast.Name)
                and n.func.id == "enter_project_root"
                for n in ast.walk(node)
            )
    assert found == {name: True for name in wanted}
