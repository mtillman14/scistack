"""``ndarray_to_buffer`` must move a numpy array in ONE bridge crossing.

MATLAB's ``from_python`` converted every numpy array via ``ndarray.tolist()``
followed by ``cell(py_list)`` — one Python/MATLAB boundary crossing *per
element*. A 419-record EMG variable spread over 10 array columns is 1.74e8
samples, so that is 1.74e8 crossings plus a multi-GB transient MATLAB cell
array, which presents as a hung MATLAB rather than a slow one.

``ndarray_to_buffer`` hands over the raw bytes instead. These tests pin the
contract MATLAB relies on: correct dtype mapping, **Fortran (column-major)**
byte order so MATLAB can ``reshape`` without a permute, an exact element
count, and a clean decline (never an exception) for every dtype that has no
fixed-width buffer.

Runs entirely in Python without MATLAB — same pattern as
test_bridge_mapping_inputs.py.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np
import pytest
from scimatlab.bridge import flatten_sequences, ndarray_to_buffer


def _roundtrip(desc, shape=None):
    """Reproduce what +scidb/+internal/from_python.m does with the buffer.

    MATLAB: uint8(bytes) -> typecast(raw, matlab_class) -> reshape(vec, shape).
    numpy stand-in: frombuffer with the matching dtype, reshaped Fortran-order
    because MATLAB is column-major.
    """
    dtype = {
        "double": np.float64,
        "single": np.float32,
        "int8": np.int8,
        "int16": np.int16,
        "int32": np.int32,
        "int64": np.int64,
        "uint8": np.uint8,
        "uint16": np.uint16,
        "uint32": np.uint32,
        "uint64": np.uint64,
        "logical": np.bool_,
    }[desc["matlab_class"]]
    vec = np.frombuffer(desc["buffer"], dtype=dtype)
    if shape is None:
        return vec
    return vec.reshape(shape, order="F")


# --- Values survive the buffer round trip -------------------------------


def test_float64_1d_roundtrip():
    arr = np.array([1.5, -2.25, 3.0, 1e300], dtype=np.float64)
    desc = ndarray_to_buffer(arr)
    assert desc["ok"]
    assert desc["matlab_class"] == "double"
    assert desc["count"] == 4
    assert desc["itemsize"] == 8
    np.testing.assert_array_equal(_roundtrip(desc), arr)


def test_float32_maps_to_single():
    arr = np.array([1.5, -2.5, 3.5], dtype=np.float32)
    desc = ndarray_to_buffer(arr)
    assert desc["ok"]
    assert desc["matlab_class"] == "single"
    np.testing.assert_array_equal(_roundtrip(desc), arr)


@pytest.mark.parametrize(
    "dtype,matlab_class",
    [
        (np.int8, "int8"),
        (np.int16, "int16"),
        (np.int32, "int32"),
        (np.int64, "int64"),
        (np.uint8, "uint8"),
        (np.uint16, "uint16"),
        (np.uint32, "uint32"),
        (np.uint64, "uint64"),
    ],
)
def test_integer_dtypes_roundtrip(dtype, matlab_class):
    arr = np.array([0, 1, 2, 3], dtype=dtype)
    desc = ndarray_to_buffer(arr)
    assert desc["ok"]
    assert desc["matlab_class"] == matlab_class
    np.testing.assert_array_equal(_roundtrip(desc), arr)


def test_bool_maps_to_logical():
    arr = np.array([True, False, True, True])
    desc = ndarray_to_buffer(arr)
    assert desc["ok"]
    assert desc["matlab_class"] == "logical"
    assert desc["itemsize"] == 1
    np.testing.assert_array_equal(_roundtrip(desc), arr)


def test_nan_and_inf_survive():
    arr = np.array([np.nan, np.inf, -np.inf, 0.0])
    desc = ndarray_to_buffer(arr)
    out = _roundtrip(desc)
    assert np.isnan(out[0])
    assert out[1] == np.inf
    assert out[2] == -np.inf
    assert out[3] == 0.0


# --- Shape: the buffer is Fortran-order so MATLAB reshapes directly ------


def test_2d_buffer_is_fortran_order():
    # C-contiguous input: tobytes(order="F") must still transpose-copy, or
    # MATLAB's reshape would silently transpose the data.
    arr = np.arange(6, dtype=np.float64).reshape(2, 3)
    assert arr.flags["C_CONTIGUOUS"]
    desc = ndarray_to_buffer(arr)
    assert desc["ok"]
    assert desc["count"] == 6
    np.testing.assert_array_equal(_roundtrip(desc, shape=(2, 3)), arr)
    # The raw byte order is column-major: first value down column 0.
    flat = np.frombuffer(desc["buffer"], dtype=np.float64)
    np.testing.assert_array_equal(flat, np.array([0.0, 3.0, 1.0, 4.0, 2.0, 5.0]))


def test_3d_roundtrip():
    arr = np.arange(24, dtype=np.float64).reshape(2, 3, 4)
    desc = ndarray_to_buffer(arr)
    assert desc["ok"]
    np.testing.assert_array_equal(_roundtrip(desc, shape=(2, 3, 4)), arr)


def test_already_fortran_input_roundtrips():
    arr = np.asfortranarray(np.arange(6, dtype=np.float64).reshape(3, 2))
    desc = ndarray_to_buffer(arr)
    np.testing.assert_array_equal(_roundtrip(desc, shape=(3, 2)), arr)


def test_non_contiguous_view_roundtrips():
    # A strided view (every other column) has no usable memory layout of its
    # own; tobytes must materialize it rather than hand over neighbours.
    base = np.arange(12, dtype=np.float64).reshape(3, 4)
    view = base[:, ::2]
    desc = ndarray_to_buffer(view)
    assert desc["ok"]
    assert desc["count"] == 6
    np.testing.assert_array_equal(_roundtrip(desc, shape=(3, 2)), view)


# --- Empty and scalar edge cases -----------------------------------------


def test_empty_array_is_ok_with_zero_count():
    desc = ndarray_to_buffer(np.array([], dtype=np.float64))
    assert desc["ok"]
    assert desc["count"] == 0
    assert desc["buffer"] == b""


def test_zero_dimensional_array():
    desc = ndarray_to_buffer(np.float64(7.5))
    assert desc["ok"]
    assert desc["count"] == 1
    np.testing.assert_array_equal(_roundtrip(desc), np.array([7.5]))


# --- Declining is a contract, not a failure -------------------------------


def test_object_dtype_declines():
    arr = np.array([{"a": 1}, {"b": 2}], dtype=object)
    desc = ndarray_to_buffer(arr)
    assert not desc["ok"]
    assert "raw-buffer" in desc["reason"]


def test_string_dtype_declines():
    desc = ndarray_to_buffer(np.array(["alpha", "beta"]))
    assert not desc["ok"]
    assert desc["reason"]


def test_datetime_dtype_declines():
    desc = ndarray_to_buffer(np.array(["2026-09-14"], dtype="datetime64[D]"))
    assert not desc["ok"]


def test_complex_dtype_declines():
    desc = ndarray_to_buffer(np.array([1 + 2j], dtype=np.complex128))
    assert not desc["ok"]


def test_float16_declines_because_typecast_has_no_target():
    desc = ndarray_to_buffer(np.array([1.0, 2.0], dtype=np.float16))
    assert not desc["ok"]


def test_decline_keeps_the_full_result_shape():
    # MATLAB indexes every key unconditionally; a decline must still carry
    # them all or the MATLAB-side field access errors instead of falling back.
    desc = ndarray_to_buffer(np.array(["x"]))
    assert set(desc) == {
        "ok",
        "buffer",
        "matlab_class",
        "count",
        "itemsize",
        "reason",
    }


def test_non_array_input_declines_without_raising():
    desc = ndarray_to_buffer(object())
    assert not desc["ok"]
    assert desc["reason"]


# --- The path that actually hung: ragged array column ---------------------


def test_ragged_column_flattens_then_buffers():
    """The RawEMG shape: an object column of variable-length signals.

    from_python routes this through flatten_sequences (one flat array +
    lengths) and then through the ndarray branch — so the buffer path is
    what carries the samples. Reassembling must return the originals.
    """
    signals = [
        np.array([1.0, 2.0, 3.0]),
        np.array([4.0, 5.0]),
        np.array([6.0, 7.0, 8.0, 9.0]),
    ]
    flat, lengths = flatten_sequences(signals)
    assert flat is not None

    desc = ndarray_to_buffer(flat)
    assert desc["ok"]
    assert desc["count"] == 9

    vec = _roundtrip(desc)
    pos = 0
    for original, n in zip(signals, lengths):
        np.testing.assert_array_equal(vec[pos : pos + int(n)], original)
        pos += int(n)
    assert pos == len(vec)


def test_large_column_is_one_buffer_not_per_element():
    """Size sanity: the buffer is exactly itemsize * count, no per-element
    framing. This is the property that makes the crossing O(1)."""
    arr = np.arange(200_000, dtype=np.float64)
    desc = ndarray_to_buffer(arr)
    assert desc["ok"]
    assert len(desc["buffer"]) == 8 * 200_000
    np.testing.assert_array_equal(_roundtrip(desc), arr)


# ---------------------------------------------------------------------------
# flatten_sequences must decline anything that is not a flat sequence
# ---------------------------------------------------------------------------


def test_flatten_sequences_declines_2d_elements():
    """A list holding a MATRIX is not a list of sequences, and must decline.

    ``lengths`` is what MATLAB splits the concatenated buffer back apart with,
    and it only describes a 1-D element: for a 4x3 matrix ``len(arr)`` is 4, its
    ROW count, so MATLAB would slice the first 4 values out of a 12-value buffer
    and hand back a 4x1 column where a 4x3 matrix belongs — silently, and then
    save it (TestEndToEnd/test_matrix_through_pipeline, 2026-09-14).
    """
    flat, lengths = flatten_sequences([np.arange(12, dtype=np.float64).reshape(4, 3)])
    assert flat is None and lengths is None


def test_flatten_sequences_declines_mixed_1d_and_2d():
    """One non-flat element is enough to disqualify the whole list — the split
    is positional, so a single wrong length corrupts every element after it."""
    flat, lengths = flatten_sequences(
        [
            np.array([1.0, 2.0, 3.0]),
            np.arange(6, dtype=np.float64).reshape(2, 3),
        ]
    )
    assert flat is None and lengths is None


def test_flatten_sequences_still_accepts_ragged_1d():
    """The path this exists for is unaffected: ragged 1-D numeric columns (the
    DuckDB ``DOUBLE[]`` shape) still take the one-crossing route."""
    signals = [np.array([1.0, 2.0, 3.0]), np.array([4.0, 5.0]), np.array([6.0])]
    flat, lengths = flatten_sequences(signals)
    assert flat is not None
    np.testing.assert_array_equal(lengths, [3, 2, 1])
    np.testing.assert_array_equal(flat, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
