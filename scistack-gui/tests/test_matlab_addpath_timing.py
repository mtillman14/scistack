"""The generated MATLAB preamble times addpath PER DIRECTORY (cleanup-audit F29).

addpath was ~4.4 s of a ~4.7 s preamble on every MATLAB run, and a single
total could not say whether that was one slow network folder, the number of
folders, or re-adding folders already on the path.
"""

from scistack_gui.api.matlab_command import generate_matlab_command

DIRS = ["//server/share/lib", "/home/user/shared"]


def _cmd(dirs=DIRS):
    return generate_matlab_command(
        function_name="f",
        db_path="/data/experiment.duckdb",
        schema_keys=["subject"],
        addpath_dirs=dirs,
    )


def test_each_directory_is_added_once_in_order_and_timed():
    cmd = _cmd()
    first, second = (cmd.index(f"addpath('{d}');") for d in DIRS)
    assert first < second  # original order: path precedence unchanged
    for d in DIRS:
        assert cmd.count(f"addpath('{d}');") == 1
    assert "scistack_addpath_each__(1) = toc(scistack_t__);" in cmd
    assert "scistack_addpath_each__(2) = toc(scistack_t__);" in cmd


def test_the_breakdown_is_logged_and_its_temporaries_cleared():
    cmd = _cmd()
    assert "[timing] matlab_addpath: TOTAL=" in cmd
    assert "already_on_path=%d" in cmd and "slowest=" in cmd
    assert cmd.index("[timing] matlab_preamble") < cmd.index("[timing] matlab_addpath")
    clear_line = next(line for line in cmd.splitlines() if line.startswith("clear "))
    for temp in ("scistack_addpath_each__", "scistack_addpath_dirs__", "scistack_t__"):
        assert temp in clear_line


def test_no_directories_no_addpath_instrumentation():
    cmd = _cmd(dirs=[])
    assert "scistack_addpath" not in cmd
    assert "matlab_addpath" not in cmd
