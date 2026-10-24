"""scifor.signature: the one reader of a function's arguments."""

import functools
import inspect

import pandas as pd

from scifor.foreach import _accepted_param_names
from scifor.signature import Argument, function_arguments


def _f(a, b=1, *args, c, d="x", _private=None, **kwargs):
    return a


class TestFunctionArguments:
    def test_signature_order_and_defaults(self):
        args = function_arguments(_f)
        assert [a.name for a in args] == ["a", "b", "args", "c", "d", "_private", "kwargs"]
        by = {a.name: a for a in args}
        assert not by["a"].has_default and not by["a"].optional
        assert by["b"].has_default and by["b"].default == 1
        assert not by["c"].has_default  # keyword-only, required
        assert by["d"].default_repr() == "'x'"
        assert by["_private"].private
        assert by["args"].variadic and by["args"].optional and not by["args"].named
        assert by["kwargs"].default_repr() == "**kwargs"

    def test_follows_wraps(self):
        @functools.wraps(_f)
        def wrapper(*a, **k):
            return _f(*a, **k)

        assert [a.name for a in function_arguments(wrapper)][:2] == ["a", "b"]

    def test_no_signature_falls_back_to_scidb_params(self):
        class Proxy:
            # A non-Signature __signature__ makes inspect.signature raise
            # TypeError, the way an uninspectable proxy fails.
            __signature__ = "not a signature"

            def __call__(self, *a, **k):
                return None

        p = Proxy()
        p.__scidb_params__ = ("x", "y")
        args = function_arguments(p)
        assert [a.name for a in args] == ["x", "y"]
        assert all(not a.has_default for a in args)

    def test_nothing_known_is_none(self):
        class Opaque:
            __signature__ = "not a signature"

            def __call__(self):
                return None

        assert function_arguments(Opaque()) is None

    def test_read_csv_sep_has_a_default(self):
        by = {a.name: a for a in function_arguments(pd.read_csv)}
        assert not by["filepath_or_buffer"].has_default
        assert by["sep"].has_default
        assert sum(a.optional for a in by.values()) > 20

    def test_long_default_is_truncated(self):
        a = Argument("x", inspect.Parameter.KEYWORD_ONLY, "y" * 100)
        assert len(a.default_repr(limit=10)) == 10


class TestAcceptedParamNamesUsesTheOwner:
    """Behaviour unchanged by the move onto scifor.signature."""

    def test_kwargs_accepts_anything(self):
        assert _accepted_param_names(_f) is None

    def test_named_kinds_only(self):
        def g(a, /, b, *args, c=1):
            return a

        assert _accepted_param_names(g) == {"b", "c"}

    def test_scidb_params_fallback(self):
        class Proxy:
            __signature__ = "not a signature"

            def __call__(self):
                return None

        p = Proxy()
        p.__scidb_params__ = ["x"]
        assert _accepted_param_names(p) == {"x"}

    def test_unknown_is_empty(self):
        class Opaque:
            __signature__ = "not a signature"

            def __call__(self):
                return None

        assert _accepted_param_names(Opaque()) == set()
