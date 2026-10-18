"""MATLAB ``scifor.for_each`` and ``scidb.for_each`` are drop-in replacements.

Source-text guard (runs without MATLAB): parses the option names each .m
``split_options`` recognises, and MATLAB scifor's ``db_only_kwargs`` list, and
holds them to the same contract as Python
(``scidb/tests/test_foreach_signature_parity.py``):

* every MATLAB scifor option is accepted by MATLAB scidb;
* MATLAB scidb's extra options are exactly the ones MATLAB scifor discards;
* that discard list is Python's ``scifor.DB_ONLY_KWARGS`` plus MATLAB scidb's
  legacy ``preload`` / ``parallel``;
* MATLAB scifor takes every public Python scifor kwarg.

Also covers ``bridge.sync_output_column_name``, which carries a MATLAB class's
``column_name`` onto its Python surrogate.
"""

import inspect
import re
import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import pytest
import scifor
from scidb.variable import BaseVariable
from scimatlab.bridge import register_matlab_variable, sync_output_column_name

_MATLAB = _root / "scimatlab" / "src" / "scimatlab" / "matlab"
_SCIFOR_M = _MATLAB / "+scifor" / "for_each.m"
_SCIDB_M = _MATLAB / "+scidb" / "for_each.m"

# MATLAB-only options with no Python counterpart (MATLAB tables only).
MATLAB_ONLY_SCIFOR = {"categorical"}
MATLAB_LEGACY_DB_ONLY = {"preload", "parallel"}


def _function_body(text: str, header_regex: str) -> str:
    match = re.search(header_regex, text, flags=re.MULTILINE)
    assert match, f"no function matching {header_regex!r}"
    rest = text[match.end():]
    nxt = re.search(r"^function ", rest, flags=re.MULTILINE)
    return rest[: nxt.start()] if nxt else rest


def _option_cases(path: Path) -> set[str]:
    body = _function_body(path.read_text(), r"^function \[meta_args, opts\] = split_options")
    return set(re.findall(r'^\s*case "([A-Za-z_]+)"', body, flags=re.MULTILINE))


def _public(names: set[str]) -> set[str]:
    return {n for n in names if not n.startswith("_")}


def _matlab_db_only() -> set[str]:
    body = _function_body(_SCIFOR_M.read_text(), r"^function names = db_only_kwargs")
    return set(re.findall(r'"([A-Za-z_]+)"', body))


def test_matlab_db_only_list_is_python_set_plus_legacy():
    assert _matlab_db_only() == set(scifor.DB_ONLY_KWARGS) | MATLAB_LEGACY_DB_ONLY


def test_every_matlab_scifor_option_is_accepted_by_matlab_scidb():
    # scidb takes OUTPUTS positionally and rewrites outputs={...} into that
    # slot before split_options runs, so it has no case for it.
    missing = (
        _public(_option_cases(_SCIFOR_M)) - _public(_option_cases(_SCIDB_M)) - {"outputs"}
    )
    assert not missing, f"+scidb/for_each.m lacks scifor options: {sorted(missing)}"
    assert 'strcmpi(string(outputs), "outputs")' in _SCIDB_M.read_text()


def test_matlab_scidb_extras_are_exactly_what_scifor_discards():
    extras = _public(_option_cases(_SCIDB_M)) - _public(_option_cases(_SCIFOR_M))
    assert extras == _matlab_db_only()


def test_matlab_scifor_takes_every_python_scifor_kwarg():
    py_public = {
        name
        for name, p in inspect.signature(scifor.for_each).parameters.items()
        if not name.startswith("_")
        and p.kind is not inspect.Parameter.VAR_KEYWORD
        and name not in {"fn", "inputs"}
    }
    m_public = _public(_option_cases(_SCIFOR_M))
    assert py_public <= m_public, f"missing in MATLAB: {sorted(py_public - m_public)}"
    assert m_public - py_public == MATLAB_ONLY_SCIFOR


def test_matlab_scifor_never_matches_a_db_only_name_as_a_case():
    # Discarded names are filtered before the switch; a case for one would
    # mean it is half-implemented in scifor.
    assert not (_option_cases(_SCIFOR_M) & _matlab_db_only())


# --- sync_output_column_name ---------------------------------------------------


def test_sync_sets_and_clears_column_name_on_a_surrogate():
    register_matlab_variable("ColSync_A")
    assert sync_output_column_name("ColSync_A", "step_len") == "step_len"
    assert sync_output_column_name("ColSync_A", None) == "ColSync_A"
    assert sync_output_column_name("ColSync_A", "") == "ColSync_A"


def test_sync_does_not_clear_a_python_defined_class():
    class ColSyncPython(BaseVariable):
        column_name = "kept"

    assert sync_output_column_name("ColSyncPython", None) == "kept"
    assert ColSyncPython.column_name == "kept"


@pytest.mark.parametrize("name", ["ColSync_B"])
def test_surrogates_are_marked(name):
    assert getattr(register_matlab_variable(name), "_matlab_surrogate", False)
