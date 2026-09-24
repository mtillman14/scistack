"""FROZEN reference serializer — the byte format record identity depends on.

A verbatim copy of ``scicanonicalhash.hashing._serialize_for_hash`` as of
commit dad361a4 (2026-09-24), taken BEFORE the F30 speed work
(.claude/plan-f30-fast-canonical-hash.md). Test-only: it is the oracle every
optimized path must match byte for byte, because the content hash feeds
``record_id`` and a single changed byte makes every existing record look new.

Never edit this file to make a test pass. If the production serializer must
change format on purpose, that is a migration decision, not a test fix.
"""

import json
from typing import Any


def reference_serialize(obj: Any) -> bytes:
    """Convert object to bytes for hashing."""

    # Primitives - use JSON for stability
    if isinstance(obj, (type(None), bool, int, float, str)):
        return json.dumps(obj).encode("utf-8")

    # Dicts - sort keys for determinism
    if isinstance(obj, dict):
        sorted_items = sorted(obj.items(), key=lambda x: str(x[0]))
        parts = []
        for k, v in sorted_items:
            parts.append(reference_serialize(k))
            parts.append(reference_serialize(v))
        return b"dict:" + b"|".join(parts)

    # Lists/tuples - preserve order
    if isinstance(obj, (list, tuple)):
        type_prefix = b"list:" if isinstance(obj, list) else b"tuple:"
        parts = [reference_serialize(item) for item in obj]
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
                + reference_serialize(obj.tolist())
            )
        return (
            b"ndarray:"
            + str(obj.shape).encode()
            + b":"
            + str(obj.dtype).encode()
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
        parts = [b"dataframe:"]
        for col in sorted(obj.columns, key=str):
            parts.append(reference_serialize(str(col)))
            parts.append(reference_serialize(obj[col].to_numpy()))
        return b"|".join(parts)

    # Pandas Series — hash name + values by value (index intentionally ignored,
    # consistent with the DataFrame path; object dtype handled above).
    if (
        hasattr(obj, "to_numpy")
        and hasattr(obj, "name")
        and not hasattr(obj, "columns")
    ):
        return (
            b"series:"
            + reference_serialize(obj.name)
            + b":"
            + reference_serialize(obj.to_numpy())
        )

    # Python array.array (MATLAB bridge can produce these)
    import array as _array_mod

    if isinstance(obj, _array_mod.array):
        import numpy as np

        return reference_serialize(np.array(obj))

    # Unsupported type
    raise ValueError(f"Unserializable data type: {type(obj)}")
