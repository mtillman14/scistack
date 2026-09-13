"""Tests for scistack_gui.library_functions — resolving library function
references (numpy/pandas/stdlib) by explicit import rather than by storing
them in the function registry.

The behaviour under test is why the module exists: a reference must stay
resolvable no matter what has happened to ``registry._functions``.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scistack_gui import library_functions as lf
from scistack_gui import registry


class TestCanonicalReference:
    def test_pd_expands_to_pandas(self):
        assert lf.canonical_reference("pd.read_csv") == "pandas.read_csv"

    def test_np_expands_to_numpy(self):
        assert lf.canonical_reference("np.mean") == "numpy.mean"

    def test_nested_attribute_keeps_its_tail(self):
        assert lf.canonical_reference("np.linalg.norm") == "numpy.linalg.norm"

    def test_real_name_is_unchanged(self):
        assert lf.canonical_reference("pandas.read_csv") == "pandas.read_csv"

    def test_bare_alias_has_no_attribute_to_expand(self):
        # No dot -> nothing to canonicalize; rejected later on its own
        # merits (there is no attribute being referenced).
        assert lf.canonical_reference("pd") == "pd"

    def test_alias_only_applies_to_the_root_segment(self):
        """`pd` in a non-root position is somebody else's module name."""
        assert lf.canonical_reference("mypkg.pd.thing") == "mypkg.pd.thing"

    def test_whitespace_stripped(self):
        assert lf.canonical_reference("  pd.read_csv  ") == "pandas.read_csv"

    def test_empty_is_empty(self):
        assert lf.canonical_reference("") == ""


class TestSplitReference:
    def test_dotted(self):
        assert lf.split_reference("pandas.read_csv") == ("pandas", "read_csv")

    def test_nested(self):
        assert lf.split_reference("numpy.linalg.norm") == ("numpy.linalg", "norm")

    def test_bare_name_is_a_builtin(self):
        assert lf.split_reference("len") == ("builtins", "len")


def unwrap(fn):
    """The real callable behind a name-qualified wrapper.

    ``resolve`` hands out a wrapper whose only difference is ``__name__``
    (see :class:`TestQualifiedName`), so identity is asserted through
    ``__wrapped__``. A reference whose name already matches is returned
    unwrapped, hence the getattr default.
    """
    return getattr(fn, "__wrapped__", fn)


class TestResolve:
    def test_stdlib_builtin(self):
        assert unwrap(lf.resolve("len")) is len

    def test_stdlib_module_function(self):
        import math

        assert unwrap(lf.resolve("math.sqrt")) is math.sqrt

    def test_numpy(self):
        import numpy

        assert unwrap(lf.resolve("numpy.mean")) is numpy.mean

    def test_pandas(self):
        import pandas

        assert unwrap(lf.resolve("pandas.read_csv")) is pandas.read_csv

    def test_alias(self):
        import pandas

        assert unwrap(lf.resolve("pd.read_csv")) is pandas.read_csv

    def test_nested_submodule(self):
        import numpy.linalg

        assert unwrap(lf.resolve("numpy.linalg.norm")) is numpy.linalg.norm

    def test_disallowed_but_installed_package_is_not_resolved(self):
        """duckdb is importable here (scidb depends on it) but is neither
        stdlib nor numpy/pandas — resolve() must enforce the same
        allow-list as validate(), or it becomes an import backdoor that
        bypasses the validated creation path."""
        assert lf.resolve("duckdb.connect") is None


class TestStdlibClassification:
    """Direct tests for the stdlib rule behind both resolve() and validate().

    These exist because the environment-dependent version of this bug was
    invisible to every test that went through ``resolve``/``validate``. The
    old rule asked "is the module's file under sysconfig's stdlib path?",
    which is true for site-packages on any interpreter where site-packages
    is a *subdirectory* of the stdlib dir — a plain (non-venv) interpreter,
    as in CI. Locally, inside a venv, site-packages sits elsewhere and the
    same code answered correctly. So the suite passed on a developer machine
    and every installed third-party package was resolvable in CI.

    Passing the origin in explicitly removes the interpreter layout from the
    question, so these fail on the old logic wherever they run.
    """

    def test_site_packages_path_is_not_stdlib(self):
        assert (
            lf._is_stdlib_root("json", "/usr/lib/python3.12/site-packages/json/__init__.py")
            is False
        )

    def test_dist_packages_path_is_not_stdlib(self):
        assert (
            lf._is_stdlib_root("json", "/usr/lib/python3/dist-packages/json/__init__.py")
            is False
        )

    def test_real_stdlib_path_is_stdlib(self):
        assert lf._is_stdlib_root("json", "/usr/lib/python3.12/json/__init__.py") is True

    def test_builtin_and_frozen_are_stdlib(self):
        assert lf._is_stdlib_root("sys", "built-in") is True
        assert lf._is_stdlib_root("itertools", None) is True

    def test_non_stdlib_name_is_never_stdlib(self):
        """The name check comes first, so a third-party package cannot pass
        by sitting at a stdlib-looking path."""
        assert lf._is_stdlib_root("duckdb", "/usr/lib/python3.12/duckdb/__init__.py") is False

    def test_stdlib_classification_does_not_depend_on_venv_layout(self):
        """The regression, stated as the invariant it broke: an installed
        package's verdict must be identical whether or not site-packages
        happens to live under the stdlib directory."""
        import duckdb

        in_venv = "/usr/lib/python3.12/site-packages/duckdb/__init__.py"
        under_stdlib = "/usr/lib/python3.12/duckdb/__init__.py"
        assert lf._is_stdlib_root("duckdb", in_venv) is False
        assert lf._is_stdlib_root("duckdb", under_stdlib) is False
        assert lf._is_stdlib_module(duckdb) is False

    def test_uninstalled_package(self):
        assert lf.resolve("totally_not_a_real_package_xyz.foo") is None

    def test_missing_attribute(self):
        assert lf.resolve("math.not_a_real_function") is None

    def test_non_callable(self):
        assert lf.resolve("math.pi") is None

    def test_junk(self):
        assert lf.resolve("not a valid ref!") is None

    def test_empty(self):
        assert lf.resolve("") is None
        assert lf.resolve(None) is None

    def test_a_lookup_miss_never_executes_the_module(self, tmp_path, monkeypatch):
        """resolve() runs on every registry miss, including names that are
        neither library refs nor registered functions. Classifying a root by
        importing it would execute user code as a side effect of a failed
        lookup — _root_allowed uses find_spec instead."""
        import sys

        (tmp_path / "sentinel_side_effect_pkg.py").write_text(
            "raise AssertionError('module must not be executed by a lookup miss')\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        sys.modules.pop("sentinel_side_effect_pkg", None)

        assert lf.resolve("sentinel_side_effect_pkg.anything") is None
        assert "sentinel_side_effect_pkg" not in sys.modules


class TestQualifiedName:
    """The resolved callable must NAME itself by its canonical reference.

    Regression: scifor/scidb record a run under ``getattr(fn, "__name__")``,
    and ``pandas.read_csv.__name__`` is the bare ``"read_csv"``. Handing the
    raw callable to ``for_each`` recorded the run as ``read_csv`` while the
    canvas node was ``pandas.read_csv``; the next graph build could not
    graduate the manual node onto its DB counterpart (``merge_manual_nodes``
    matches on ``(type, label)``) and rendered a SECOND function node.
    """

    def test_name_is_the_canonical_reference(self):
        assert lf.resolve("pandas.read_csv").__name__ == "pandas.read_csv"

    def test_alias_resolves_to_the_canonical_name_not_the_bare_one(self):
        """`pd.read_csv` must record as `pandas.read_csv` — the alias never
        reaches persistence, and neither does the bare attribute name."""
        assert lf.resolve("pd.read_csv").__name__ == "pandas.read_csv"

    def test_nested_submodule_keeps_its_full_path(self):
        assert lf.resolve("np.linalg.norm").__name__ == "numpy.linalg.norm"

    def test_a_name_that_already_matches_is_not_wrapped(self):
        """`len.__name__` is already "len" — no wrapper, no indirection."""
        assert lf.resolve("len") is len

    def test_signature_still_reads_through_to_the_real_function(self):
        """The settings panel builds its parameter handles from the
        signature; wrapping in *args/**kwargs without __wrapped__ would
        collapse read_csv's parameters to nothing."""
        import inspect

        params = inspect.signature(lf.resolve("pandas.read_csv")).parameters
        assert "filepath_or_buffer" in params
        assert "sep" in params

    def test_docstring_still_reads_through(self):
        import inspect

        doc = inspect.getdoc(lf.resolve("pandas.read_csv"))
        assert doc and "csv" in doc.lower()

    def test_source_location_still_points_at_the_library(self):
        """"Go to source" must land in pandas, not in library_functions.py.

        Not automatic, unlike signature/docstring: ``inspect`` is
        inconsistent about wrappers. ``getsourcelines()`` unwraps
        internally, but ``getsourcefile()`` reads ``__code__.co_filename``
        and does not — so the naive pairing returns the WRAPPER's file with
        the WRAPPED function's line number. ``get_function_source`` unwraps
        explicitly; this test is what caught the mismatch.
        """
        from scistack_gui.services import pipeline_service

        result = pipeline_service.get_function_source("pandas.read_csv")
        assert result["ok"], result.get("error")
        assert "pandas" in result["file"]
        assert "library_functions" not in result["file"]

    def test_source_line_belongs_to_the_reported_file(self):
        """The file/line pair must be internally consistent — the specific
        way the unwrap mismatch fails is a valid line number for a file it
        does not belong to."""
        from scistack_gui.services import pipeline_service

        result = pipeline_service.get_function_source("pandas.read_csv")
        source_lines = open(result["file"], encoding="utf-8").read().splitlines()
        assert result["line"] <= len(source_lines)
        assert "read_csv" in "\n".join(
            source_lines[result["line"] - 1 : result["line"] + 40]
        )

    def test_the_wrapper_actually_calls_through(self):
        assert lf.resolve("math.sqrt")(9) == 3.0

    def test_repeat_resolution_returns_one_stable_identity(self):
        """resolve() runs on every registry miss; a fresh wrapper object per
        call would churn identity for anything keyed on the callable."""
        assert lf.resolve("numpy.mean") is lf.resolve("numpy.mean")

    def test_validate_hands_out_the_same_qualified_callable(self):
        _err, name, fn = lf.validate("pd.read_csv")
        assert name == "pandas.read_csv"
        assert fn.__name__ == "pandas.read_csv"
        assert fn is lf.resolve("pandas.read_csv")


class TestIsLibraryReference:
    def test_bare_name(self):
        assert lf.is_library_reference("len") is True

    def test_dotted_name(self):
        assert lf.is_library_reference("pandas.read_csv") is True

    def test_user_function_name_is_still_shaped_like_one(self):
        """A bare name can't be told apart from a user function by shape
        alone — that's fine, resolve() is the authority and returns None."""
        assert lf.is_library_reference("compute_rolling_vo2") is True
        assert lf.resolve("compute_rolling_vo2") is None

    def test_junk_rejected(self):
        assert lf.is_library_reference("not a valid ref!") is False

    def test_empty_rejected(self):
        assert lf.is_library_reference("") is False


class TestRegistryLookup:
    """registry.lookup_function is the choke point every consumer uses."""

    def test_discovered_function_wins_over_import(self):
        """A user function named `len` must not be shadowed by the
        builtin — the registry is consulted first."""

        def user_len():
            return "mine"

        registry._functions["len"] = user_len
        try:
            assert registry.lookup_function("len") is user_len
        finally:
            registry._functions.pop("len", None)

    def test_library_reference_resolves_with_an_empty_registry(self):
        registry._functions.pop("pandas.read_csv", None)
        assert callable(registry.lookup_function("pandas.read_csv"))

    def test_lookup_hands_the_run_path_a_qualified_name(self):
        """api/run passes registry.get_function(name) straight to for_each,
        so the name scidb records is decided right here."""
        registry._functions.pop("pandas.read_csv", None)
        assert registry.get_function("pandas.read_csv").__name__ == "pandas.read_csv"

    def test_unknown_name_returns_none(self):
        assert registry.lookup_function("no_such_function_anywhere") is None

    def test_get_function_raises_for_unknown(self):
        import pytest

        with pytest.raises(KeyError, match="not found in registry"):
            registry.get_function("no_such_function_anywhere")

    def test_get_function_does_not_cache_into_the_registry(self):
        """Resolution must stay import-on-demand; caching would re-create
        the eviction bug the moment a refresh cleared the dict."""
        registry._functions.pop("numpy.mean", None)
        registry.get_function("numpy.mean")
        assert "numpy.mean" not in registry._functions
