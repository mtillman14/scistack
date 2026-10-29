"""The signature's output names (``scimatlab.source_hash.matlab_output_names``),
the one parser both a MATLAB run (via the bridge sentinel) and the GUI's
``matlab_parser`` use. ``.claude/plan-output-order-owner.md``."""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import pytest
from scimatlab.bridge import _make_matlab_fn_sentinel
from scimatlab.source_hash import matlab_output_names


@pytest.mark.parametrize(
    "text, expected",
    [
        ("function [time, force] = load_trial(path)\n", ["time", "force"]),
        ("function [time force] = load_trial(path)\n", ["time", "force"]),  # MATLAB allows spaces
        ("function out = f(x)\n", ["out"]),
        ("function f(x)\n", []),
        ("function out = f\n", ["out"]),
        ("% function [a, b] = commented(x)\nfunction c = real(x)\n", ["c"]),
        ("function [a, ... \n b] = f(x)\n", ["a", "b"]),  # line continuation
        ("", []),
    ],
)
def test_output_names(text, expected):
    assert matlab_output_names(text) == expected


def test_named_function_in_a_file_with_local_functions():
    text = "function y = main(x)\ny = helper(x);\nend\nfunction [p, q] = helper(x)\nend\n"
    assert matlab_output_names(text) == ["y"]
    assert matlab_output_names(text, "helper") == ["p", "q"]
    assert matlab_output_names(text, "pkg.helper") == ["p", "q"]


def test_the_bridge_sentinel_carries_them():
    s = _make_matlab_fn_sentinel("load_trial", None, "function [time, force] = load_trial(p)\n")
    assert s.output_params == ["time", "force"]
    assert not hasattr(_make_matlab_fn_sentinel("f", None, None), "output_params")
