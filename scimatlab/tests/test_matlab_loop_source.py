"""MATLAB's for_each keeps only the per-combo call (P5 stage 3, slice d).

Source-text guard, runs without MATLAB:

* ``+scifor/for_each.m`` plans, assembles and closes through
  ``py.scifor.matlab_loop`` and every entry it calls exists in Python;
* none of the local functions whose decision moved to Python is back
  (combos, ``[]`` resolution, distribute target, column groups,
  share_limits grouping, locations, summary text, journal);
* ``+scidb/for_each.m`` hands scidb's combos, row selection and PathOutput
  paths to the plan as Python objects instead of converting them per combo;
* ``PathInput.m`` names its load errors through Python's rule.
"""

import re
from pathlib import Path

import pytest
from scifor import matlab_loop, pathinput

_MATLAB = Path(__file__).parent.parent / "src" / "scimatlab" / "matlab"
_SCIFOR_M = _MATLAB / "+scifor" / "for_each.m"
_SCIDB_M = _MATLAB / "+scidb" / "for_each.m"

#: Decided by Python now; a MATLAB definition would be a second owner.
MOVED = [
    "distinct_values_from_inputs", "decategorize_schema_column", "capture_schema_column_types",
    "find_pathinput", "key_has_source", "project_combos", "canonical_numeric_values",
    "exact_string_key_loader", "location_kept_indices", "resolve_column_groups",
    "describe_for_columns_plan", "warn_overlapping_groups", "log_group_skips",
    "combine_output_columns", "compute_shared_limits", "numeric_extent", "build_limit_args",
    "warn_on_arity_mismatch", "filter_table_for_combo", "select_rows_by_id", "is_per_combo_table",
    "format_inputs", "print_dry_run_iteration", "format_value_preview",
    "count_distinct_combo_values", "failure_groups", "write_journal", "is_data_input",
]


def _defined(text: str) -> set[str]:
    return set(re.findall(r"^function\s+(?:\[[^\]]*\]\s*=\s*|\w+\s*=\s*)?(\w+)\s*\(", text, re.M))


def _code(path: Path) -> str:
    return "\n".join(line.split("%", 1)[0] for line in path.read_text().splitlines())


@pytest.mark.parametrize("name", MOVED)
def test_moved_function_is_not_defined_in_matlab(name):
    assert name not in _defined(_SCIFOR_M.read_text())


def test_scifor_loop_calls_the_plan_entries():
    code = _code(_SCIFOR_M)
    for entry in ("plan", "assemble", "close", "discard", "log_progress"):
        assert f"py.scifor.matlab_loop.{entry}(" in code, entry


def test_every_called_entry_exists():
    for path in _MATLAB.rglob("*.m"):
        for name in re.findall(r"py\.scifor\.matlab_loop\.(\w+)", _code(path)):
            assert callable(getattr(matlab_loop, name, None)), f"{path.name}: {name}"
        for name in re.findall(r"py\.scifor\.pathinput\.(\w+)\(", _code(path)):
            assert callable(getattr(pathinput, name, None)), f"{path.name}: {name}"


def test_scifor_loop_makes_no_per_combo_python_call_but_progress():
    """Inside run_combos the only py. call is the time-gated progress line."""
    text = _SCIFOR_M.read_text()
    body = text[text.index("function [state, stop_err] = run_combos"):]
    body = body[: body.index("\nfunction ", 1)]
    assert re.findall(r"py\.[\w.]+", body) == []


def test_scidb_loop_passes_python_objects_through():
    code = _code(_SCIDB_M)
    assert "scifor_opts{end+1} = py_full_combos;" in code
    assert "scifor_opts{end+1} = py_row_sel;" in code
    assert "scifor_opts{end+1} = py_resolved_paths;" in code
    assert "all_combos{ci}" not in code
    assert "row_selection{si}" not in code


def test_pathinput_error_ids_come_from_python():
    code = _code(_MATLAB / "+scifor" / "PathInput.m")
    assert "py.scifor.pathinput.matlab_error_id(" in code
    assert "'scifor:PathInput:NoMatch'" not in code


@pytest.mark.parametrize("path", [_SCIFOR_M, _SCIDB_M], ids=["scifor", "scidb"])
def test_no_code_between_functions(path):
    """After a function's closing ``end`` (column 0) only a blank line, a
    comment or the next ``function`` may follow. A scripted edit once cut a
    ``function ... = describe_input(`` header in half (2026-11-02), leaving
    its argument list between two functions: MATLAB refused to parse the file
    (``MATLAB:m_improper_grouping``) and all 432 tests of the suite failed."""
    lines = path.read_text().splitlines()
    stray = []
    after_end = False
    for number, line in enumerate(lines, start=1):
        if line == "end":
            after_end = True
            continue
        if after_end and line.strip() and not line.lstrip().startswith("%"):
            if not line.startswith("function "):
                stray.append(f"{path.name}:{number}: {line[:80]}")
            after_end = False
    assert stray == []
