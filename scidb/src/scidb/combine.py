"""``on_duplicate``: what a table save does with two rows at one location.

- ``"error"`` (default): nothing is combined; ``save_batch`` refuses two
  different records of one variant at one location (``DuplicateLocationError``,
  docs/claude/duplicate-location-vs-stale.md).
- ``"combine"``: before the table becomes save items, rows that share a
  location AND variant are combined into one (the rule:
  ``scifor.combine_duplicate_rows``); columns that needed a list are stored as
  sciduckdb ``row_list`` columns.

This module owns the vocabulary (``validate_on_duplicate``) and which
columns tell rows apart (``combine_key_columns``). Every save path that
combines calls ``combine_for_save``. Plan:
``.claude/plan-combine-duplicate-locations.md``.
"""

from __future__ import annotations

from scifor import CombineMixedTypesError, combine_duplicate_rows

from .log import Log

ON_DUPLICATE_VALUES = ("error", "combine")


def validate_on_duplicate(on_duplicate: str) -> str:
    """The one check of the ``on_duplicate`` option (every layer calls it)."""
    if on_duplicate not in ON_DUPLICATE_VALUES:
        raise ValueError(
            f"on_duplicate must be one of {ON_DUPLICATE_VALUES}, got {on_duplicate!r}"
        )
    return on_duplicate


def combine_key_columns(columns, value_columns) -> list:
    """The columns that keep rows apart: EVERY column that is not a value
    column — the schema keys, and every metadata / bookkeeping column
    (``__rid_*``, ``__vsig_*``, version keys, constants). Defined as the
    complement so two variants of one subject are never merged, the same way
    ``database._colliding_locations`` only collides items that agree on every
    metadata key."""
    values = set(value_columns)
    return [c for c in columns if c not in values]


def combine_table(df, value_columns, *, type_name: str):
    """``(combined_df, scifor.CombineReport)`` for one table about to be
    saved as ``type_name``: rows combined per location and variant, keys
    from :func:`combine_key_columns`.

    Raises ``scifor.CombineMixedTypesError`` (naming ``type_name``) when a
    column that needs a list holds more than one kind of value.
    """
    values = [c for c in value_columns if c in df.columns]
    keys = combine_key_columns(list(df.columns), values)
    try:
        out, report = combine_duplicate_rows(df, keys, values)
    except CombineMixedTypesError as exc:
        raise CombineMixedTypesError(exc.problems, variable=type_name) from None
    Log.debug(
        f"on_duplicate=combine: {type_name}: {report.n_rows_in} row(s) -> "
        f"{report.n_rows_out}, row_list column(s) {report.list_columns}, "
        f"{len(report.uncombinable)} location(s) left for the duplicate check"
    )
    return out, report


def combine_for_save(df, value_columns, *, type_name: str, on_duplicate: str):
    """``(df, row_list_columns)``: ``df`` with its rows combined per location
    when ``on_duplicate == "combine"`` (:func:`combine_table`), else
    unchanged with no row-list columns. ``row_list_columns`` are the value
    columns where some location needed a list; pass them to ``save_batch``.
    """
    if on_duplicate != "combine" or df is None or len(df) < 2:
        return df, []
    out, report = combine_table(df, value_columns, type_name=type_name)
    return out, list(report.list_columns)
