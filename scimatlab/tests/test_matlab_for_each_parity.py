"""MATLAB ``scifor.for_each`` and ``scidb.for_each`` are drop-in replacements.

Which name-value pairs of a MATLAB call are options is decided by Python
(``scifor.options.classify_for_each_args``, through ``+scifor/option_pairs.m``),
from ``scifor.FOR_EACH_OPTIONS`` plus ``MATLAB_ONLY_OPTIONS``. Each MATLAB
``split_options`` keeps one ``case`` per option only to turn its value into a
MATLAB type. Source-text guard (runs without MATLAB):

* each MATLAB switch has a case for exactly the public options Python says
  that layer acts on (a missing case would raise "has no MATLAB handling";
  an extra case would be dead, since Python never routes that name to it);
* MATLAB scifor's discards are exactly the scidb options it cannot act on,
  and none of them has a case in its switch;
* MATLAB scifor takes every public Python scifor kwarg;
* neither ``split_options`` keeps a hand list of option names any more.

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
from scidb.model.variable import BaseVariable
from scifor.options import (
    DB_ONLY_KWARGS,
    MATLAB_ONLY_OPTIONS,
    classify_for_each_args,
    matlab_accepted_options,
    matlab_discarded_options,
)
from scimatlab.bridge import register_matlab_variable, sync_output_column_name

_MATLAB = _root / "scimatlab" / "src" / "scimatlab" / "matlab"
_SCIFOR_M = _MATLAB / "+scifor" / "for_each.m"
_SCIDB_M = _MATLAB / "+scidb" / "for_each.m"


def _function_body(text: str, header_regex: str) -> str:
    match = re.search(header_regex, text, flags=re.MULTILINE)
    assert match, f"no function matching {header_regex!r}"
    rest = text[match.end():]
    nxt = re.search(r"^function ", rest, flags=re.MULTILINE)
    return rest[: nxt.start()] if nxt else rest


def _split_options(path: Path) -> str:
    return _function_body(path.read_text(), r"^function \[meta_args, opts\] = split_options")


def _option_cases(path: Path) -> set[str]:
    return set(re.findall(r'^\s*case "([A-Za-z_]+)"', _split_options(path), flags=re.MULTILINE))


def _public(names: set[str]) -> set[str]:
    return {n for n in names if not n.startswith("_")}


@pytest.mark.parametrize("layer, path", [("scidb", _SCIDB_M), ("scifor", _SCIFOR_M)])
def test_matlab_switch_covers_exactly_the_python_options(layer, path):
    assert _public(_option_cases(path)) == set(matlab_accepted_options(layer))


@pytest.mark.parametrize("layer, path", [("scidb", _SCIDB_M), ("scifor", _SCIFOR_M)])
def test_split_options_asks_python(layer, path):
    body = _split_options(path)
    assert f"scifor.option_pairs(varargin, '{layer}')" in body
    assert "reserved_opts" not in body
    # The removed local function, not the `discard_db_only_kwargs` log tag
    # that deliberately matches Python scifor.outputs.
    assert not re.search(r"\bdb_only_kwargs\b", path.read_text())


def test_matlab_scifor_discards_what_scidb_takes_and_scifor_cannot():
    discarded = set(matlab_discarded_options("scifor"))
    assert discarded == set(DB_ONLY_KWARGS) | {
        n for n, layers in MATLAB_ONLY_OPTIONS.items() if layers == ("scidb",)
    }
    assert not (_option_cases(_SCIFOR_M) & discarded)


def test_every_matlab_scifor_option_is_accepted_by_matlab_scidb():
    # scidb takes OUTPUTS positionally and rewrites outputs={...} into that
    # slot before split_options runs, so it has no case for it.
    missing = set(matlab_accepted_options("scifor")) - set(matlab_accepted_options("scidb"))
    assert missing == {"outputs"}
    assert 'strcmpi(string(outputs), "outputs")' in _SCIDB_M.read_text()


def test_matlab_scifor_takes_every_python_scifor_kwarg():
    py_public = {
        name
        for name, p in inspect.signature(scifor.for_each).parameters.items()
        if not name.startswith("_")
        and p.kind is not inspect.Parameter.VAR_KEYWORD
        and name not in {"fn", "inputs", "hooks"}
    }
    m_public = set(matlab_accepted_options("scifor"))
    assert py_public <= m_public, f"missing in MATLAB: {sorted(py_public - m_public)}"
    assert m_public - py_public == {"categorical"}


# --- classify_for_each_args ---------------------------------------------------


def test_classify_roles_per_layer():
    names = ["save", "Dry_Run", "_all_combos", "subject", "categorical"]
    assert classify_for_each_args(names, "scidb") == [
        "option", "option", "internal", "axis", "option",
    ]
    assert classify_for_each_args(names, "scifor") == [
        "discard", "option", "internal", "axis", "option",
    ]


def test_classify_warns_about_a_misspelled_option(caplog):
    with caplog.at_level("WARNING"):
        roles = classify_for_each_args(["skipComputed"], "scidb")
    assert roles == ["axis"]
    assert "skip_computed" in caplog.text


def test_classify_rejects_an_unknown_layer():
    with pytest.raises(ValueError):
        classify_for_each_args(["save"], "python")


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
