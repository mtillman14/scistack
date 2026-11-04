"""One ``for_each`` option list feeds Python, MATLAB and GUI command generation.

``scifor.FOR_EACH_OPTIONS`` is the declaration (P5 stage 2). This guards that
every declared option is:

* accepted as an option by MATLAB scidb's ``split_options``
  (``scifor.options.classify_for_each_args``), not mistaken for an
  iteration axis;
* either emitted by the GUI's MATLAB command generator or skipped with a
  stated reason (``MATLAB_OPTION_EMISSION``);
* a keyword of ``scidb.for_each``.

Before the declaration, the GUI generator kept a hand list of four run
options; ``distribute`` once went missing from it and a MATLAB run saved 560
undistributed records without an error.
"""

import inspect

import pytest
from scidb import for_each as scidb_for_each
from scifor import FOR_EACH_OPTIONS
from scifor.options import classify_for_each_args

from scistack_gui.services.matlab_command_format import (
    MATLAB_OPTION_EMISSION,
    RUN_OPTION,
    _format_run_option_pairs,
)

_NAMES = [o.name for o in FOR_EACH_OPTIONS]


def test_every_option_is_placed_in_the_emission_table():
    assert set(MATLAB_OPTION_EMISSION) == set(_NAMES)


@pytest.mark.parametrize("name", _NAMES)
def test_matlab_scidb_accepts_every_option(name):
    assert classify_for_each_args([name], "scidb") == ["option"]


@pytest.mark.parametrize("name", _NAMES)
def test_every_option_is_a_scidb_for_each_keyword(name):
    assert name in inspect.signature(scidb_for_each).parameters


def _non_default(spec):
    if spec.name == "on_error":
        return "stop"
    if spec.kind == "names":
        return ["x"]
    if spec.kind == "bool":
        return not spec.default
    raise AssertionError(f"no non-default value for {spec.name} ({spec.kind})")


@pytest.mark.parametrize(
    "spec",
    [o for o in FOR_EACH_OPTIONS if MATLAB_OPTION_EMISSION[o.name] == RUN_OPTION],
    ids=lambda o: o.name,
)
def test_every_run_option_is_emitted_when_not_default(spec):
    text = _format_run_option_pairs({spec.name: _non_default(spec)}, "f")
    assert text.startswith(f"'{spec.name}', ")


@pytest.mark.parametrize(
    "spec",
    [o for o in FOR_EACH_OPTIONS if MATLAB_OPTION_EMISSION[o.name] == RUN_OPTION],
    ids=lambda o: o.name,
)
def test_a_default_run_option_is_omitted(spec):
    assert _format_run_option_pairs({spec.name: spec.default}, "f") == ""


def test_an_unknown_run_option_is_logged(caplog):
    with caplog.at_level("WARNING"):
        assert _format_run_option_pairs({"bogus": True}, "f") == ""
    assert "bogus" in caplog.text
