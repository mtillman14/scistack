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
  ``schema_str`` writes it. The rule itself is scifor's
  (``scifor.schema_types``); this raises scidb's error type.

A leaf: ``database`` (which wraps DuckDB), ``foreach``, ``state``,
``locations``, ``filters`` and ``provenance_query`` all need the rule, and
``provenance_query`` importing it from ``database`` was the reason
``database`` imported ``provenance_query`` inside twenty functions.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from scifor.schema_types import VALID_SCHEMA_KEY_TYPES  # noqa: F401 - re-export
from scifor.schema_types import SchemaKeyTypeError as _SciforSchemaKeyTypeError
from scifor.schema_types import canonical_numeric_value as _scifor_canonical_numeric_value

from .exceptions import SchemaKeyTypeError


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

    The rule is scifor's (``scifor.canonical_numeric_value``, the one owner
    since 2026-10-21: standalone scifor and scidb must read ``"001"`` the
    same way). This only re-raises its error as scidb's
    :class:`SchemaKeyTypeError`, which subclasses scifor's.
    """
    with scidb_key_type_errors():
        return _scifor_canonical_numeric_value(key, value)


@contextmanager
def scidb_key_type_errors():
    """Re-raise scifor's ``SchemaKeyTypeError`` as scidb's (its subclass), for
    callers of the shared ``scifor.schema_types`` helpers: scidb's callers
    catch, and MATLAB reports, the scidb type."""
    try:
        yield
    except _SciforSchemaKeyTypeError as exc:
        if isinstance(exc, SchemaKeyTypeError):
            raise
        raise SchemaKeyTypeError(str(exc)) from None


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
