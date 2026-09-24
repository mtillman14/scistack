"""Deterministic hashing for arbitrary Python objects.

This module provides utilities for creating stable, deterministic hashes
of Python objects, which is essential for cache key computation, data
versioning, and reproducibility.
"""

import hashlib
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def canonical_hash(obj: Any) -> str:
    """
    Generate a deterministic hash for arbitrary Python objects.

    Strategy:
    1. For JSON-serializable primitives (None, bool, int, float, str): use JSON
    2. For numpy ndarrays: shape + dtype + raw bytes; ``object``-dtype arrays are
       hashed BY VALUE (their ``tobytes()`` would be non-deterministic pointers)
    3. For pandas DataFrames: per-column, columns SORTED by name, index IGNORED
       (column order and the index are non-semantic for stored content)
    4. For pandas Series: name + values (index ignored)
    5. For dicts: sort keys, recursively serialize
    6. For lists/tuples: preserve order, recursively serialize
    7. For other objects: raise ValueError

    Determinism note: the hash is invariant to DataFrame column order, the pandas
    index, and object-array memory layout, so logically-identical data always
    hashes the same — even across processes / MATLAB-bridge round-trips.

    Args:
        obj: Any Python object to hash

    Returns:
        16-character hex string (first 64 bits of SHA-256)

    Raises:
        ValueError: If an unserializable object is provided

    Example:
        >>> h = canonical_hash(42)
        >>> len(h) == 16 and all(c in '0123456789abcdef' for c in h)
        True
        >>> canonical_hash(42) == canonical_hash(42)  # Deterministic
        True
        >>> canonical_hash([1, 2, 3]) != canonical_hash([1, 2, 4])  # Content-sensitive
        True
    """
    serialized = _serialize_for_hash(obj)
    return hashlib.sha256(serialized).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Speed without changing a byte (cleanup-audit F30,
# .claude/plan-f30-fast-canonical-hash.md). The serialized bytes feed
# record_id, so every fast path below is pinned by
# tests/test_hash_byte_identity.py to a FROZEN copy of the serializer from
# before this work. Measured cost before: ~0.75 ms per one-row, 54-column
# DataFrame, two thirds of it pandas building a Series per column, the rest
# one json.dumps call per scalar.
# ---------------------------------------------------------------------------

_INF = float("inf")


def _float_bytes(x: float) -> bytes:
    """json.dumps(x) for an exact float: float.__repr__, with the three
    non-finite spellings json uses (allow_nan=True, its default)."""
    if x != x:
        return b"NaN"
    if x == _INF:
        return b"Infinity"
    if x == -_INF:
        return b"-Infinity"
    return float.__repr__(x).encode()


# EXACT types only: a subclass (IntEnum, a float subclass, numpy's float64)
# falls through to the json.dumps branch it always took. str keeps json.dumps
# — its escaping is where a hand-written encoder would go wrong.
_SCALAR_BYTES = {
    float: _float_bytes,
    int: lambda x: int.__repr__(x).encode(),
    bool: lambda x: b"true" if x else b"false",
    type(None): lambda x: b"null",
    str: lambda x: json.dumps(x).encode("utf-8"),
}

_DTYPE_BYTES: dict = {}  # dtype -> str(dtype).encode() (str(dtype) is slow)


def _dtype_bytes(dtype) -> bytes:
    # Keyed by dtype EQUALITY, so only plain dtypes are cached: structured
    # ones can compare equal yet print differently (field titles, padding).
    if dtype.fields is not None:
        return str(dtype).encode()
    b = _DTYPE_BYTES.get(dtype)
    if b is None:
        b = _DTYPE_BYTES[dtype] = str(dtype).encode()
    return b


# DataFrame fast column path: read each column's values straight from the
# frame's block manager instead of building a Series for it. pandas-internal
# (`_mgr.iget_values`), so it is never trusted blind: the FIRST frame of each
# (columns, dtypes) signature is checked against the public
# `df[col].to_numpy()`, and a signature that disagrees is served by the
# public path from then on (logged WARN). A bug costs speed, never identity.
_FRAME_VERDICTS: dict = {}  # signature -> True (fast path verified) / False
_COLUMN_ORDER: dict = {}  # columns tuple -> [(position, name bytes)] sorted
_FRAME_COUNTS = {"fast": 0, "reference": 0}
_CACHE_LIMIT = 4096


def frame_path_counts() -> dict:
    """How many DataFrames each path serialized in this process —
    ``{"fast": n, "reference": m}``. For timing lines and tests."""
    return dict(_FRAME_COUNTS)


def _reset_frame_caches() -> None:
    """Forget verdicts, column orders and counts (tests)."""
    _FRAME_VERDICTS.clear()
    _COLUMN_ORDER.clear()
    _FRAME_COUNTS["fast"] = _FRAME_COUNTS["reference"] = 0


def _column_as_numpy(values):
    """What ``Series.to_numpy()`` returns for a column's stored values."""
    import numpy as np

    return values if isinstance(values, np.ndarray) else values.to_numpy()


def _dataframe_bytes_reference(obj) -> bytes:
    """The DataFrame format, through the public API — the spec the fast path
    must equal, and its fallback."""
    parts = [b"dataframe:"]
    for col in sorted(obj.columns, key=str):
        parts.append(_serialize_for_hash(str(col)))
        parts.append(_serialize_for_hash(obj[col].to_numpy()))
    return b"|".join(parts)


def _fast_column_values(obj):
    """``(columns tuple, [values per position])`` or None when the fast path
    cannot address this frame (duplicate or MultiIndex column labels, or a
    pandas without the internal accessor)."""
    cols = obj.columns
    if cols.nlevels != 1 or not cols.is_unique:
        return None
    try:
        mgr = obj._mgr
        values = [mgr.iget_values(i) for i in range(len(cols))]
    except Exception:  # pandas internals moved: the public path still works
        return None
    return tuple(cols), values


def _dataframe_bytes(obj) -> bytes:
    fast = _fast_column_values(obj)
    if fast is not None:
        key_cols, values = fast
        arrays = [_column_as_numpy(v) for v in values]
        signature = (key_cols, tuple(a.dtype for a in arrays))
        verdict = _FRAME_VERDICTS.get(signature)
        if verdict is None:
            verdict = _verify_fast_columns(obj, key_cols, arrays)
            if len(_FRAME_VERDICTS) >= _CACHE_LIMIT:
                _FRAME_VERDICTS.clear()
            _FRAME_VERDICTS[signature] = verdict
        if verdict:
            order = _COLUMN_ORDER.get(key_cols)
            if order is None:
                positions = sorted(range(len(key_cols)), key=lambda i: str(key_cols[i]))
                order = [(i, _serialize_for_hash(str(key_cols[i]))) for i in positions]
                if len(_COLUMN_ORDER) >= _CACHE_LIMIT:
                    _COLUMN_ORDER.clear()
                _COLUMN_ORDER[key_cols] = order
            parts = [b"dataframe:"]
            for i, name_bytes in order:
                parts.append(name_bytes)
                parts.append(_serialize_for_hash(arrays[i]))
            _FRAME_COUNTS["fast"] += 1
            return b"|".join(parts)
    _FRAME_COUNTS["reference"] += 1
    return _dataframe_bytes_reference(obj)


def _verify_fast_columns(obj, key_cols, arrays) -> bool:
    """Once per signature: every fast column serializes exactly like the
    public ``df[col].to_numpy()``."""
    for col, arr in zip(key_cols, arrays):
        if _serialize_for_hash(arr) != _serialize_for_hash(obj[col].to_numpy()):
            logger.warning(
                "canonical_hash: fast column path disagrees with the public "
                "API for column %r (dtype %s); frames with this column/dtype "
                "signature use the public path (same hash, slower)",
                col,
                arr.dtype,
            )
            return False
    logger.debug(
        "canonical_hash: fast column path verified for %d column(s)", len(key_cols)
    )
    return True


def _serialize_for_hash(obj: Any) -> bytes:
    """Convert object to bytes for hashing."""

    # Exact-type scalars: the same bytes json.dumps writes, without calling it
    # once per value (F30). Subclasses take the json branch below.
    fast = _SCALAR_BYTES.get(type(obj))
    if fast is not None:
        return fast(obj)

    # Primitives - use JSON for stability
    if isinstance(obj, (type(None), bool, int, float, str)):
        return json.dumps(obj).encode("utf-8")

    # Dicts - sort keys for determinism
    if isinstance(obj, dict):
        sorted_items = sorted(obj.items(), key=lambda x: str(x[0]))
        parts = []
        for k, v in sorted_items:
            parts.append(_serialize_for_hash(k))
            parts.append(_serialize_for_hash(v))
        return b"dict:" + b"|".join(parts)

    # Lists/tuples - preserve order
    if isinstance(obj, (list, tuple)):
        type_prefix = b"list:" if isinstance(obj, list) else b"tuple:"
        parts = [_serialize_for_hash(item) for item in obj]
        return type_prefix + b"|".join(parts)

    # Numpy arrays - use shape, dtype, and raw bytes.
    if hasattr(obj, "tobytes") and hasattr(obj, "dtype") and hasattr(obj, "shape"):
        # CRITICAL: an `object`-dtype array stores Python references, so
        # `tobytes()` returns the bytes of POINTER values — non-deterministic
        # across runs/processes. Hash such arrays BY VALUE instead (recurse over
        # the nested Python lists), so e.g. a mixed-type DataFrame column of
        # strings hashes the strings, not their memory addresses.
        if obj.dtype == object:
            return (
                b"ndarray-object:"
                + str(obj.shape).encode()
                + b":"
                + _serialize_for_hash(obj.tolist())
            )
        return (
            b"ndarray:"
            + str(obj.shape).encode()
            + b":"
            + _dtype_bytes(obj.dtype)
            + b":"
            + obj.tobytes()
        )

    # Pandas DataFrame — content-canonical hash:
    #  - column ORDER is non-semantic for stored content (storage is keyed by
    #    column NAME), so sort columns → permuting columns can't change the hash;
    #  - the pandas INDEX is not part of the persisted content, so it must NOT
    #    affect the hash (a volatile index from row-splitting otherwise yields
    #    different hashes for identical data);
    #  - serialize PER COLUMN (each column keeps its own dtype) rather than via a
    #    whole-frame `to_numpy()`, which would collapse a mixed-dtype frame to an
    #    object array and hash pointer bytes (see above).
    if hasattr(obj, "to_numpy") and hasattr(obj, "columns"):
        return _dataframe_bytes(obj)

    # Pandas Series — hash name + values by value (index intentionally ignored,
    # consistent with the DataFrame path; object dtype handled above).
    if (
        hasattr(obj, "to_numpy")
        and hasattr(obj, "name")
        and not hasattr(obj, "columns")
    ):
        return (
            b"series:"
            + _serialize_for_hash(obj.name)
            + b":"
            + _serialize_for_hash(obj.to_numpy())
        )

    # Python array.array (MATLAB bridge can produce these)
    import array as _array_mod

    if isinstance(obj, _array_mod.array):
        import numpy as np

        return _serialize_for_hash(np.array(obj))

    # Unsupported type
    raise ValueError(f"Unserializable data type: {type(obj)}")


def generate_record_id(
    class_name: str,
    schema_version: int,
    content_hash: str,
    metadata: dict,
) -> str:
    """
    Generate a unique record ID from components.

    The record_id uniquely identifies a record by its type, schema, content,
    and metadata. Useful for addressing/querying versioned data.

    Args:
        class_name: The record type (e.g., "RotationMatrix")
        schema_version: Integer version of the serialization schema
        content_hash: Pre-computed hash of the data content
        metadata: The addressing metadata (subject, trial, etc.)

    Returns:
        16-character hex string

    Example:
        >>> rid = generate_record_id("MyData", 1, "abc123", {"subject": 1})
        >>> len(rid) == 16 and all(c in '0123456789abcdef' for c in rid)
        True
    """
    components = [
        f"class:{class_name}",
        f"schema:{schema_version}",
        f"content:{content_hash}",
        f"meta:{canonical_hash(metadata)}",
    ]
    combined = "|".join(components).encode("utf-8")
    return hashlib.sha256(combined).hexdigest()[:16]
