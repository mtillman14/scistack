"""How a schema-key value is spelled in the database, and read back.

Schema keys (``subject``, ``session``, ``trial``, ...) are stored as VARCHAR
in DuckDB. Two rules live here and NOWHERE else:

* :func:`schema_str` — the ONE spelling a value gets on the way IN.
  ``str(1.0)`` is ``"1.0"`` but ``str(1)`` is ``"1"``, and MATLAB sends every
  number as a double, so without one rule a MATLAB ``for_each`` and a Python
  one would write ``"1.0"`` and ``"1"`` for the same trial and never find
  each other's records. ``filters`` carried a copy of this rule
  (``_to_schema_str``, "mirrors database._schema_str") until 2026-09-20.

* :func:`from_schema_str` — the ONE spelling a value gets on the way OUT,
  when the KEY is declared numeric. Per key, never per value: applied per
  value, ``"01"``..``"09"`` stayed strings while ``"10"`` became ``10`` and
  one column held both (integration suite, 2026-09-19). Call
  :meth:`DatabaseManager.restore_schema_value`, which knows the declaration.

* :func:`canonical_numeric_value` — every spelling of a number a
  ``numeric``-declared key may receive, collapsed to one identity BEFORE
  ``schema_str`` writes it.

A leaf: ``database`` (which wraps DuckDB), ``foreach``, ``state``,
``locations``, ``filters`` and ``provenance_query`` all need the rule, and
``provenance_query`` importing it from ``database`` was the reason
``database`` imported ``provenance_query`` inside twenty functions.
"""

from __future__ import annotations

from typing import Any

from .exceptions import SchemaKeyTypeError

#: The declarable schema-key types (``[schema_key_types]`` in the project
#: config).
VALID_SCHEMA_KEY_TYPES = ("numeric", "string")


def schema_str(value: Any) -> str:
    """Stringify a schema key value, converting whole-number floats to int.

    Schema keys are stored as VARCHAR in DuckDB.  str(1.0) → "1.0" but
    str(1) → "1".  MATLAB sends all numbers as float, so without this
    conversion, queries and cache lookups fail because "1.0" ≠ "1".
    """
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def canonical_numeric_value(key: str, value: Any) -> Any:
    """Canonical value for a schema key declared ``"numeric"``.

    Collapses every spelling of the same number to one identity: ints stay
    ints, integral floats become ints (MATLAB doubles arrive as ``1.0``),
    digit strings lose leading zeros (``"001"`` → 1), and float-like strings
    normalize through ``float`` (``"1.50"`` → 1.5).  Values that cannot be
    read as a number violate the declaration and raise SchemaKeyTypeError —
    declared types are enforced, never guessed around.
    """
    if isinstance(value, bool):
        raise SchemaKeyTypeError(
            f"Schema key '{key}' is declared numeric but got a bool: {value!r}"
        )
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            return int(s)
        try:
            f = float(s)
            return int(f) if f.is_integer() else f
        except ValueError:
            pass
    raise SchemaKeyTypeError(
        f"Schema key '{key}' is declared numeric but got a non-numeric value: {value!r}"
    )


def from_schema_str(value: Any) -> Any:
    """Convert ONE schema VARCHAR value back to a numeric type if possible.

    Schema keys are stored as VARCHAR, so loaded values are always strings.
    This restores the original type (int or float) so that user-facing
    metadata has the same type as what was originally saved.

    Only converts when the round-trip preserves the original string exactly.
    This keeps zero-padded identifiers like "01" as strings (since str(1) ==
    "1" ≠ "01"), which is critical for subject/trial IDs that must match
    what the user passed into for_each.

    **Never call this on a loaded value directly** — use
    :meth:`DatabaseManager.restore_schema_value`, which decides per KEY. Per
    value, cycles "01".."09" stayed strings while "10" became the int 10, so
    one column held both (integration suite, 2026-09-19). Whether a key is
    numeric is a property of the key, not of each of its values.
    """
    if not isinstance(value, str):
        return value
    try:
        as_int = int(value)
        if str(as_int) == value:
            return as_int
    except (ValueError, TypeError):
        pass
    try:
        as_float = float(value)
        if str(as_float) == value:
            return as_float
    except (ValueError, TypeError):
        pass
    return value
