"""Unit tests for the ``Parameter`` primitive."""

from __future__ import annotations

import pytest

from scidb import Parameter


# ---------------------------------------------------------------------------
# Scalar transparency
# ---------------------------------------------------------------------------
class TestScalarTransparency:
    def test_int_addition(self):
        sampling_rate = Parameter(1000, description="Hz")
        assert sampling_rate + 1 == 1001
        assert 1 + sampling_rate == 1001

    def test_int_arithmetic(self):
        x = Parameter(10)
        assert x - 3 == 7
        assert 20 - x == 10
        assert x * 4 == 40
        assert 2 * x == 20
        assert x / 2 == 5
        assert 20 / x == 2
        assert x // 3 == 3
        assert x % 3 == 1
        assert x**2 == 100
        assert -x == -10
        assert abs(Parameter(-5)) == 5

    def test_float_arithmetic(self):
        weight = Parameter(0.25, description="reg weight")
        assert weight + 0.25 == 0.5
        assert weight * 4 == 1.0
        assert float(weight) == 0.25

    def test_comparisons(self):
        x = Parameter(10)
        assert x == 10
        assert x != 11
        assert x < 11
        assert x <= 10
        assert x > 9
        assert x >= 10
        # Reverse comparisons
        assert 10 == x
        assert 9 < x

    def test_comparison_between_constants(self):
        a = Parameter(5)
        b = Parameter(10)
        assert a < b
        assert a != b
        assert a == Parameter(5)

    def test_bool_coercion(self):
        assert bool(Parameter(1)) is True
        assert bool(Parameter(0)) is False
        assert bool(Parameter("hello")) is True
        assert bool(Parameter("")) is False

    def test_hash(self):
        x = Parameter(42)
        d = {x: "answer"}
        assert d[42] == "answer"
        assert hash(x) == hash(42)

    def test_int_index(self):
        # Allows use as an index / slice argument.
        x = Parameter(3)
        assert list(range(x)) == [0, 1, 2]
        assert [10, 20, 30, 40][x] == 40

    def test_str_transparency(self):
        name = Parameter("session_a", description="default session")
        assert name + "_v2" == "session_a_v2"
        assert "session" in name
        assert len(name) == len("session_a")
        assert name.upper() == "SESSION_A"

    def test_repr_contains_value_and_description(self):
        x = Parameter(1000, description="sampling rate")
        r = repr(x)
        assert "1000" in r
        assert "sampling rate" in r

    def test_fstring_formatting(self):
        x = Parameter(0.12345)
        assert f"{x:.2f}" == "0.12"
        y = Parameter(255)
        assert f"{y:04d}" == "0255"
        z = Parameter("name")
        assert f"{z:>8}" == "    name"


# ---------------------------------------------------------------------------
# Container transparency
# ---------------------------------------------------------------------------
class TestContainerTransparency:
    def test_tuple_indexing(self):
        bandpass = Parameter((1.0, 40.0), description="bandpass Hz")
        assert bandpass[0] == 1.0
        assert bandpass[1] == 40.0
        low, high = bandpass
        assert (low, high) == (1.0, 40.0)
        assert len(bandpass) == 2

    def test_list_iteration(self):
        channels = Parameter([0, 1, 2, 3])
        assert list(channels) == [0, 1, 2, 3]
        assert list(reversed(channels)) == [3, 2, 1, 0]
        assert 2 in channels
        assert 99 not in channels
        assert channels[1:3] == [1, 2]

    def test_dict_access(self):
        config = Parameter({"fs": 1000, "n_channels": 32})
        assert config["fs"] == 1000
        assert "fs" in config
        assert set(config) == {"fs", "n_channels"}
        assert config.get("n_channels") == 32

    def test_nested_container(self):
        cfg = Parameter({"band": (1.0, 40.0), "notch": 60})
        assert cfg["band"][0] == 1.0
        assert cfg["notch"] + 1 == 61


# ---------------------------------------------------------------------------
# isinstance detection (critical for discovery scanner)
# ---------------------------------------------------------------------------
class TestIsInstance:
    def test_scalar_constant_is_parameter(self):
        x = Parameter(1000)
        assert isinstance(x, Parameter)

    def test_tuple_constant_is_parameter(self):
        x = Parameter((1.0, 40.0))
        assert isinstance(x, Parameter)

    def test_dict_constant_is_parameter(self):
        x = Parameter({"a": 1})
        assert isinstance(x, Parameter)

    def test_raw_value_is_not_constant(self):
        assert not isinstance(1000, Parameter)
        assert not isinstance((1.0, 40.0), Parameter)
        assert not isinstance({"a": 1}, Parameter)


# ---------------------------------------------------------------------------
# Description and source location capture
# ---------------------------------------------------------------------------
class TestMetadataCapture:
    def test_description_captured(self):
        x = Parameter(1000, description="Sampling rate in Hz")
        assert x.description == "Sampling rate in Hz"

    def test_description_defaults_to_empty(self):
        x = Parameter(1000)
        assert x.description == ""

    def test_source_file_captured(self):
        x = Parameter(1000, description="Hz")
        # Should point at this test file.
        assert x.source_file.endswith("test_parameter.py")

    def test_source_line_captured(self):
        import inspect as _inspect

        expected_line = _inspect.currentframe().f_lineno + 1
        x = Parameter(1000, description="Hz")
        assert x.source_line == expected_line

    def test_different_call_sites_get_different_locations(self):
        a = Parameter(1, description="a")
        b = Parameter(2, description="b")
        # Same file, different lines.
        assert a.source_file == b.source_file
        assert a.source_line != b.source_line
        assert b.source_line == a.source_line + 1


# ---------------------------------------------------------------------------
# Attribute passthrough (__getattr__)
# ---------------------------------------------------------------------------
class TestAttributePassthrough:
    def test_string_methods(self):
        s = Parameter("hello")
        assert s.upper() == "HELLO"
        assert s.startswith("he")

    def test_list_methods(self):
        lst = Parameter([3, 1, 2])
        # Non-mutating method access through passthrough.
        assert lst.count(1) == 1
        assert lst.index(2) == 2

    def test_metadata_attrs_take_precedence_over_value_attrs(self):
        # Even if the wrapped value had a ``description`` attribute, our
        # slot should win. Use an object that has one.
        class Holder:
            description = "value's own description"

        x = Parameter(Holder(), description="constant description")
        assert x.description == "constant description"


# ---------------------------------------------------------------------------
# Direct constructor (bypassing the factory)
# ---------------------------------------------------------------------------
class TestDirectConstructor:
    def test_construction_captures_its_own_call_site(self):
        """There is no factory-vs-constructor split any more: Parameter is a
        real class, so EVERY construction captures the caller's location
        (the old constant() factory was the only thing that did)."""
        x = Parameter(42, description="answer")
        assert x.description == "answer"
        assert x.source_file.endswith("test_parameter.py")
        assert x.source_line > 0


# ---------------------------------------------------------------------------
# Bitwise operations
# ---------------------------------------------------------------------------
class TestBitwiseOperations:
    def test_bitwise_and(self):
        x = Parameter(0b1100)
        assert (x & 0b1010) == 0b1000
        assert (0b1010 & x) == 0b1000

    def test_bitwise_or(self):
        x = Parameter(0b1100)
        assert (x | 0b0011) == 0b1111
        assert (0b0011 | x) == 0b1111

    def test_bitwise_xor(self):
        x = Parameter(0b1100)
        assert (x ^ 0b1010) == 0b0110
        assert (0b1010 ^ x) == 0b0110

    def test_left_shift(self):
        x = Parameter(1)
        assert (x << 4) == 16
        assert (1 << Parameter(4)) == 16

    def test_right_shift(self):
        x = Parameter(16)
        assert (x >> 2) == 4
        assert (16 >> Parameter(2)) == 4

    def test_invert(self):
        x = Parameter(0)
        assert ~x == ~0


# ---------------------------------------------------------------------------
# Unary operations
# ---------------------------------------------------------------------------
class TestUnaryOperations:
    def test_positive(self):
        x = Parameter(5)
        assert +x == 5

    def test_positive_negative(self):
        x = Parameter(-3)
        assert +x == -3

    def test_round_no_digits(self):
        x = Parameter(3.7)
        assert round(x) == 4

    def test_round_with_digits(self):
        x = Parameter(3.14159)
        assert round(x, 2) == 3.14

    def test_abs_positive(self):
        x = Parameter(5)
        assert abs(x) == 5

    def test_abs_negative(self):
        x = Parameter(-5)
        assert abs(x) == 5


# ---------------------------------------------------------------------------
# Type conversions
# ---------------------------------------------------------------------------
class TestTypeConversions:
    def test_complex_conversion(self):
        x = Parameter(3)
        assert complex(x) == 3 + 0j

    def test_int_from_float_constant(self):
        x = Parameter(3.9)
        assert int(x) == 3

    def test_str_conversion(self):
        x = Parameter(42)
        assert str(x) == "42"


# ---------------------------------------------------------------------------
# Constant-to-Constant arithmetic
# ---------------------------------------------------------------------------
class TestConstantToConstantArithmetic:
    def test_add_two_constants(self):
        a = Parameter(10)
        b = Parameter(20)
        assert a + b == 30

    def test_sub_two_constants(self):
        a = Parameter(20)
        b = Parameter(7)
        assert a - b == 13

    def test_mul_two_constants(self):
        a = Parameter(3)
        b = Parameter(4)
        assert a * b == 12

    def test_div_two_constants(self):
        a = Parameter(10)
        b = Parameter(4)
        assert a / b == 2.5

    def test_floordiv_two_constants(self):
        a = Parameter(10)
        b = Parameter(3)
        assert a // b == 3

    def test_mod_two_constants(self):
        a = Parameter(10)
        b = Parameter(3)
        assert a % b == 1

    def test_pow_two_constants(self):
        a = Parameter(2)
        b = Parameter(10)
        assert a**b == 1024

    def test_comparison_two_constants(self):
        a = Parameter(5)
        b = Parameter(10)
        assert a < b
        assert b > a
        assert a <= b
        assert b >= a
        assert a != b
        assert Parameter(5) == Parameter(5)


# ---------------------------------------------------------------------------
# value property
# ---------------------------------------------------------------------------
class TestValueProperty:
    def test_value_returns_wrapped(self):
        x = Parameter(42)
        assert x.value == 42
        assert type(x.value) is int

    def test_value_returns_original_container(self):
        lst = [1, 2, 3]
        x = Parameter(lst)
        assert x.value is lst

    def test_value_returns_dict(self):
        d = {"a": 1, "b": 2}
        x = Parameter(d)
        assert x.value is d


# ---------------------------------------------------------------------------
# No values at all — declared, not yet valued
# ---------------------------------------------------------------------------
class TestNoValues:
    """A Parameter may hold zero values. That is what the GUI's "New
    parameter" form produces (it collects a name and nothing else), and it
    used to be papered over with a placeholder 0 written into source, which
    is indistinguishable from a real value once written."""

    def test_constructs_with_no_values(self):
        p = Parameter()
        assert p.values == []
        assert p.alternatives == []

    def test_description_still_captured(self):
        p = Parameter(description="filled in later")
        assert p.description == "filled in later"

    def test_repr_is_valid_syntax(self):
        # Not "Parameter(, description='')" -- the empty case used to fall
        # out of an f-string with a hardcoded comma.
        assert repr(Parameter()) == "Parameter(description='')"

    def test_value_says_there_is_none_yet(self):
        with pytest.raises(TypeError, match="has none yet"):
            Parameter().value

    @pytest.mark.parametrize("op", [int, float, bool, abs])
    def test_conversions_raise(self, op):
        with pytest.raises(TypeError, match="has none yet"):
            op(Parameter())

    def test_hasattr_is_false_and_does_not_raise_typeerror(self):
        """scidb.input_spec.is_loadable probes with hasattr(spec, "load"),
        which swallows AttributeError and NOTHING else -- a TypeError here
        would take down every for_each carrying an unvalued Parameter."""
        assert hasattr(Parameter(), "load") is False

    def test_is_not_mistaken_for_a_path_input(self):
        """is_path_input asks whether EVERY alternative is a PathInput, and
        all([]) is True -- so without an explicit non-empty check an unvalued
        Parameter registers as a PathInput."""
        from scidb.discover import is_parameter, is_path_input

        assert is_parameter(Parameter()) is True
        assert is_path_input(Parameter()) is False

    def test_expanding_one_raises_naming_the_input(self):
        from scifor import require_alternatives

        with pytest.raises(ValueError, match="no alternatives") as excinfo:
            require_alternatives(Parameter(), kind="input", param="window_seconds")
        # The class name comes from type(), so the message says Parameter
        # without scifor knowing that class exists.
        assert "Parameter" in str(excinfo.value)
        assert "'window_seconds'" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Slots: reject arbitrary attributes, allow slot attributes
# ---------------------------------------------------------------------------
class TestAttributeAssignment:
    """Parameter is NOT __slots__-constrained (the old Constant was).
    EachOf, its base, carries a __dict__, so slots on the subclass would
    not reject anything anyway -- asserting rejection would pin an
    implementation detail that no longer exists."""

    def test_metadata_is_writable(self):
        x = Parameter(1, description="original")
        x.description = "changed"
        assert x.description == "changed"


# ---------------------------------------------------------------------------
# Set / dict membership
# ---------------------------------------------------------------------------
class TestSetDictMembership:
    def test_constant_in_set(self):
        x = Parameter(42)
        s = {42, 43, 44}
        assert x in s

    def test_constant_as_dict_key(self):
        x = Parameter("key")
        d = {x: "value"}
        assert d["key"] == "value"

    def test_two_constants_same_value_equal_hash(self):
        a = Parameter(42, description="first")
        b = Parameter(42, description="second")
        assert hash(a) == hash(b)
        assert a == b


# ---------------------------------------------------------------------------
# Computation identity — a Constant is a naming wrapper, never part of the
# identity of the computation it configures.
# ---------------------------------------------------------------------------
class TestVersionKeyIdentity:
    """A single-valued Parameter and the bare value are the SAME input.

    Load-bearing twice over: for MATLAB parity (the same pipeline must not
    fork in history depending on which language ran it), and for the merge
    itself -- adding a second value must be *only* adding an argument, which
    requires the one-value case to be indistinguishable from a bare scalar.
    """

    def test_one_value_parameter_is_an_eachof_of_one(self):
        """The expansion contract the whole design rests on: EachOf has no
        special case for a single alternative, so a one-value Parameter
        expands to exactly one call carrying the concrete value."""
        from scifor import EachOf

        p = Parameter(30)
        assert isinstance(p, EachOf)
        assert p.alternatives == [30]

    def test_multi_value_parameter_keeps_every_alternative(self):
        assert Parameter(10, 20, 30).alternatives == [10, 20, 30]

    def test_values_can_be_built_programmatically(self):
        """Plain varargs, so a Sweep's generated-list ergonomics survive."""
        assert Parameter(*range(10, 60, 10)).values == [10, 20, 30, 40, 50]
        assert Parameter(*[2**k for k in range(4)]).values == [1, 2, 4, 8]

    def test_multi_value_attribute_probe_raises_attribute_error(self):
        """hasattr() only swallows AttributeError -- a TypeError here would
        crash input_spec.is_loadable's hasattr(var_spec, "load") probe for
        every multi-valued Parameter."""
        p = Parameter(10, 20)
        assert not hasattr(p, "load")
        assert not hasattr(p, "anything_at_all")

    def test_single_value_attribute_probe_still_proxies(self):
        assert hasattr(Parameter("abc"), "upper")
        assert Parameter("abc").upper() == "ABC"

    def _config(self, **inputs):
        from scidb.foreach_config import ForEachConfig

        def fn(**kwargs):
            return None

        return ForEachConfig(fn=fn, inputs=inputs)

    def test_wrapped_and_bare_value_give_the_same_constants(self):
        wrapped = self._config(window=Parameter(30)).to_version_keys()
        bare = self._config(window=30).to_version_keys()
        assert wrapped["__constants"] == bare["__constants"] == {"window": 30}

    def test_wrapped_and_bare_value_give_the_same_call_id(self):
        assert (
            self._config(window=Parameter(30)).to_call_id()
            == self._config(window=30).to_call_id()
        )

    def test_description_does_not_affect_identity(self):
        """Editing only a constant's description must not fork history."""
        assert (
            self._config(window=Parameter(30, description="a")).to_call_id()
            == self._config(window=Parameter(30, description="b")).to_call_id()
        )

    def test_differing_values_still_differ(self):
        assert (
            self._config(window=Parameter(30)).to_call_id()
            != self._config(window=Parameter(45)).to_call_id()
        )

    def test_wrapped_constant_is_hashable_by_canonical_hash(self):
        """Regression: an un-unwrapped Parameter reached canonical_hash as an
        unknown type and raised 'Unserializable data type', so passing a
        declared Parameter into for_each failed outright."""
        from scicanonicalhash.hashing import canonical_hash

        keys = self._config(window=Parameter(30), label=Parameter("a")).to_version_keys()
        assert canonical_hash(keys["__constants"]) == canonical_hash(
            {"window": 30, "label": "a"}
        )
