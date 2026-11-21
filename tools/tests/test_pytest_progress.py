"""tools/pytest-progress: the progress file the Claude Code status line reads."""

import json
import os
import subprocess
import sys
import textwrap

import pytest

pytest.importorskip("scistack_pytest_progress")


def _run(tmp_path, env_dir):
    (tmp_path / "test_sample.py").write_text(textwrap.dedent("""
        import pytest
        def test_ok(): pass
        def test_bad(): assert False
        @pytest.mark.skip
        def test_skip(): pass
        @pytest.fixture
        def broken(): raise RuntimeError
        def test_error(broken): pass
    """))
    env = {k: v for k, v in os.environ.items() if k != "SCISTACK_PYTEST_PROGRESS_DIR"}
    if env_dir is not None:
        env["SCISTACK_PYTEST_PROGRESS_DIR"] = str(env_dir)
    subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(tmp_path)],
        cwd=tmp_path, env=env, capture_output=True, text=True,
    )


def test_a_finished_run_is_counted(tmp_path):
    out = tmp_path / "progress"
    _run(tmp_path, out)
    (f,) = list(out.glob("*.json"))
    d = json.loads(f.read_text())
    assert d["state"] == "done"
    assert (d["total"], d["done"]) == (4, 4)
    assert (d["passed"], d["failed"], d["errors"], d["skipped"]) == (1, 1, 1, 1)
    assert [n.split("::")[-1] for n in d["failures"]] == ["test_bad", "test_error"]
    assert d["exitstatus"] == 1


def test_without_the_variable_nothing_is_written(tmp_path):
    out = tmp_path / "progress"
    _run(tmp_path, None)
    assert not out.exists()
