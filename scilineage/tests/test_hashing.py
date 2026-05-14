"""Tests for scilineage hashing utilities."""

import os
import subprocess
import sys
import tempfile
import textwrap

import pytest

from scilineage import canonical_hash
from scilineage.hashing import compute_function_hash


class TestCanonicalHash:
    """Test canonical_hash function."""

    def test_primitives(self):
        """Primitives should produce deterministic hashes."""
        assert canonical_hash(42) == canonical_hash(42)
        assert canonical_hash("hello") == canonical_hash("hello")
        assert canonical_hash(3.14) == canonical_hash(3.14)
        assert canonical_hash(True) == canonical_hash(True)
        assert canonical_hash(None) == canonical_hash(None)

    def test_different_values_different_hashes(self):
        """Different values should produce different hashes."""
        assert canonical_hash(1) != canonical_hash(2)
        assert canonical_hash("a") != canonical_hash("b")
        assert canonical_hash(True) != canonical_hash(False)

    def test_lists(self):
        """Lists should hash deterministically."""
        assert canonical_hash([1, 2, 3]) == canonical_hash([1, 2, 3])
        assert canonical_hash([1, 2, 3]) != canonical_hash([1, 2, 4])
        assert canonical_hash([1, 2, 3]) != canonical_hash([3, 2, 1])  # Order matters

    def test_tuples(self):
        """Tuples should hash deterministically."""
        assert canonical_hash((1, 2, 3)) == canonical_hash((1, 2, 3))
        assert canonical_hash((1, 2, 3)) != canonical_hash([1, 2, 3])  # Type matters

    def test_dicts(self):
        """Dicts should hash deterministically regardless of key order."""
        d1 = {"a": 1, "b": 2}
        d2 = {"b": 2, "a": 1}
        assert canonical_hash(d1) == canonical_hash(d2)

        d3 = {"a": 1, "b": 3}
        assert canonical_hash(d1) != canonical_hash(d3)

    def test_nested_structures(self):
        """Nested structures should hash correctly."""
        nested = {"list": [1, 2, 3], "dict": {"x": 1}, "tuple": (4, 5)}
        assert canonical_hash(nested) == canonical_hash(nested.copy())

    def test_hash_length(self):
        """Hash should be 16 characters (64 bits)."""
        h = canonical_hash(42)
        assert len(h) == 16
        assert all(c in "0123456789abcdef" for c in h)


class TestCanonicalHashWithNumpy:
    """Test canonical_hash with numpy arrays."""

    @pytest.fixture
    def np(self):
        numpy = pytest.importorskip("numpy")
        return numpy

    def test_numpy_array(self, np):
        """Numpy arrays should hash based on content."""
        arr1 = np.array([1.0, 2.0, 3.0])
        arr2 = np.array([1.0, 2.0, 3.0])
        assert canonical_hash(arr1) == canonical_hash(arr2)

    def test_numpy_array_different_values(self, np):
        """Different array values should produce different hashes."""
        arr1 = np.array([1.0, 2.0, 3.0])
        arr2 = np.array([1.0, 2.0, 4.0])
        assert canonical_hash(arr1) != canonical_hash(arr2)

    def test_numpy_array_different_dtype(self, np):
        """Different dtypes should produce different hashes."""
        arr1 = np.array([1, 2, 3], dtype=np.int32)
        arr2 = np.array([1, 2, 3], dtype=np.int64)
        assert canonical_hash(arr1) != canonical_hash(arr2)

    def test_numpy_array_different_shape(self, np):
        """Different shapes should produce different hashes."""
        arr1 = np.array([[1, 2], [3, 4]])
        arr2 = np.array([1, 2, 3, 4])
        assert canonical_hash(arr1) != canonical_hash(arr2)


class TestCanonicalHashWithPandas:
    """Test canonical_hash with pandas objects."""

    @pytest.fixture
    def pd(self):
        pandas = pytest.importorskip("pandas")
        return pandas

    def test_dataframe(self, pd):
        """DataFrames should hash based on content."""
        df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df2 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        assert canonical_hash(df1) == canonical_hash(df2)

    def test_dataframe_different_values(self, pd):
        """Different DataFrame values should produce different hashes."""
        df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df2 = pd.DataFrame({"a": [1, 2], "b": [3, 5]})
        assert canonical_hash(df1) != canonical_hash(df2)

    def test_series(self, pd):
        """Series should hash based on content."""
        s1 = pd.Series([1, 2, 3], name="x")
        s2 = pd.Series([1, 2, 3], name="x")
        assert canonical_hash(s1) == canonical_hash(s2)


def _hash_in_temp_module(source: str, target: str = "target") -> str:
    """Write ``source`` to a temp .py file, import it, return compute_function_hash(<target>)."""
    import importlib.util

    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(textwrap.dedent(source))
        path = f.name
    try:
        spec = importlib.util.spec_from_file_location("_scilineage_test_mod", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return compute_function_hash(getattr(mod, target), truncate=64)
    finally:
        os.unlink(path)


class TestRecursiveFunctionHash:
    """AST-based recursive hashing through user-defined callees."""

    def test_callee_mutation_propagates(self):
        """Changing a module-level callee's body changes its caller's hash."""
        h1 = _hash_in_temp_module(
            """
            def bar(a):
                return a

            def target(a):
                return bar(a)
            """
        )
        h2 = _hash_in_temp_module(
            """
            def bar(a):
                return a + 1  # mutated

            def target(a):
                return bar(a)
            """
        )
        assert h1 != h2

    def test_callee_unchanged_keeps_hash(self):
        """If neither caller nor callee changes, the hash is stable across imports."""
        src = """
            def bar(a):
                return a * 2

            def target(a):
                return bar(a)
            """
        assert _hash_in_temp_module(src) == _hash_in_temp_module(src)

    def test_cosmetic_changes_ignored(self):
        """Whitespace, comments, and docstrings don't change the hash."""
        h1 = _hash_in_temp_module(
            """
            def helper(a):
                return a + 1

            def target(x):
                return helper(x) * 2
            """
        )
        h2 = _hash_in_temp_module(
            """
            def helper(a):
                # this comment is new
                '''and a docstring'''
                return a + 1


            def target(x):
                '''target docstring'''
                # call helper
                return helper(x) * 2
            """
        )
        assert h1 == h2

    def test_external_callee_not_recursed(self):
        """Stdlib / site-packages callees are treated as opaque."""
        from scilineage.hashing import _is_user_defined

        assert _is_user_defined(os.path.join) is False
        assert _is_user_defined(self.test_external_callee_not_recursed) is True

    def test_external_callee_hash_stable_under_stdlib(self):
        """A function calling os.path.join hashes successfully and stably."""

        def uses_stdlib(a, b):
            return os.path.join(a, b)

        h = compute_function_hash(uses_stdlib)
        assert len(h) == 16
        assert compute_function_hash(uses_stdlib) == h

    def test_mutual_recursion_terminates(self):
        """foo↔bar mutual recursion doesn't infinite-loop."""
        h = _hash_in_temp_module(
            """
            def bar(n):
                if n <= 0:
                    return 0
                return target(n - 1)

            def target(n):
                if n <= 0:
                    return 1
                return bar(n - 1)
            """
        )
        assert len(h) == 64

    def test_self_recursion_terminates(self):
        """Self-recursive function hashes without infinite recursion."""

        def fact(n):
            return 1 if n == 0 else n * fact(n - 1)

        h = compute_function_hash(fact)
        assert len(h) == 16

    def test_class_constructor_recursion(self):
        """`MyClass(...)` recurses into __init__; mutating __init__ changes the caller's hash."""
        h1 = _hash_in_temp_module(
            """
            class MyClass:
                def __init__(self, x):
                    self.x = x
                def method(self):
                    return self.x

            def target():
                return MyClass(1)
            """
        )
        h2 = _hash_in_temp_module(
            """
            class MyClass:
                def __init__(self, x):
                    self.x = x + 1  # mutated body
                def method(self):
                    return self.x

            def target():
                return MyClass(1)
            """
        )
        assert h1 != h2

    def test_unrelated_method_change_ignored(self):
        """Mutating an unused method on a class doesn't change the caller's hash."""
        h1 = _hash_in_temp_module(
            """
            class MyClass:
                def __init__(self, x):
                    self.x = x
                def unused(self):
                    return 1

            def target():
                return MyClass(1)
            """
        )
        h2 = _hash_in_temp_module(
            """
            class MyClass:
                def __init__(self, x):
                    self.x = x
                def unused(self):
                    return 999  # changed, but never called

            def target():
                return MyClass(1)
            """
        )
        assert h1 == h2

    def test_decorator_transparency(self):
        """Mutating a @lru_cache-decorated callee changes the caller's hash."""
        h1 = _hash_in_temp_module(
            """
            from functools import lru_cache

            @lru_cache
            def helper(x):
                return x * 2

            def target(x):
                return helper(x)
            """
        )
        h2 = _hash_in_temp_module(
            """
            from functools import lru_cache

            @lru_cache
            def helper(x):
                return x * 3  # mutated

            def target(x):
                return helper(x)
            """
        )
        assert h1 != h2

    def test_cross_process_stability(self):
        """Same source hashes to the same digest in separate Python processes."""
        src = textwrap.dedent(
            """
            def helper(x):
                return x + 1
            def target(x):
                inner = lambda y: y * 2
                return inner(helper(x))
            """
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(src)
            modpath = f.name
        try:
            runner = textwrap.dedent(
                f"""
                import sys, importlib.util
                sys.path.insert(0, {repr(os.path.join(os.path.dirname(__file__), '..', 'src'))})
                sys.path.insert(0, {repr(os.path.join(os.path.dirname(__file__), '..', '..', 'canonical-hash', 'src'))})
                from scilineage.hashing import compute_function_hash
                spec = importlib.util.spec_from_file_location('m', {modpath!r})
                m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
                print(compute_function_hash(m.target, truncate=64))
                """
            )
            outs = []
            for _ in range(2):
                result = subprocess.run(
                    [sys.executable, "-c", runner], capture_output=True, text=True, check=True
                )
                outs.append(result.stdout.strip())
            assert outs[0] == outs[1]
            assert len(outs[0]) == 64
        finally:
            os.unlink(modpath)

    def test_source_unavailable_fallback(self):
        """Functions defined via exec() (no source) fall back to bytecode hashing."""
        ns: dict = {}
        exec("def dynamic_fn(x): return x + 1", ns)  # noqa: S102
        h = compute_function_hash(ns["dynamic_fn"])
        assert len(h) == 16

    def test_non_recursive_flag(self):
        """recursive=False uses bytecode-only path and ignores callee changes."""
        import importlib.util

        def hash_with(source: str, recursive: bool) -> str:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write(textwrap.dedent(source))
                path = f.name
            try:
                spec = importlib.util.spec_from_file_location("_scilineage_test_nr", path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return compute_function_hash(mod.target, truncate=64, recursive=recursive)
            finally:
                os.unlink(path)

        v1 = """
            def bar(a):
                return a
            def target(a):
                return bar(a)
            """
        v2 = """
            def bar(a):
                return a + 1
            def target(a):
                return bar(a)
            """
        # bytecode-only path looks at target's bytecode only; bar's body doesn't matter
        assert hash_with(v1, recursive=False) == hash_with(v2, recursive=False)
        # recursive path picks up bar's change
        assert hash_with(v1, recursive=True) != hash_with(v2, recursive=True)
