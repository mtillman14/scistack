"""The @scistack marker — replacement for @lineage_fcn.

@scistack tags a PLAIN function (no wrapping, raw return value) so discover.py
finds it and for_each can read its ``generates_file`` option.
"""

import types

from scidb.project.discover import discover_module
from scidb.pipeline import (
    GENERATES_FILE_ATTR,
    SCISTACK_FLAG,
    is_scistack_function,
)

from scidb import scistack


def test_bare_marker_tags_and_returns_plain_function():
    @scistack
    def f(x):
        return x + 1

    assert is_scistack_function(f)
    assert getattr(f, SCISTACK_FLAG) is True
    # Not wrapped — calling returns the raw value, not a result object.
    assert f(1) == 2


def test_called_marker_carries_generates_file():
    @scistack(generates_file="{subject}/out.csv")
    def g(x):
        return x, x * 2

    assert is_scistack_function(g)
    assert getattr(g, GENERATES_FILE_ATTR) == "{subject}/out.csv"
    assert g(3) == (3, 6)  # raw tuple, not wrapped


def test_plain_function_is_not_a_scistack_function():
    def h(x):
        return x

    assert not is_scistack_function(h)


def test_discover_module_finds_scistack_functions():
    mod = types.ModuleType("fake_pipeline_mod")

    @scistack
    def step(x):
        return x

    # Attribute it to this module (discover filters by __module__).
    step.__module__ = "fake_pipeline_mod"
    mod.step = step

    def helper(x):  # plain, untagged → not discovered
        return x

    helper.__module__ = "fake_pipeline_mod"
    mod.helper = helper

    exports = discover_module(mod)
    assert step in exports.functions
    assert helper not in exports.functions
