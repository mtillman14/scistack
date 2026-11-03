"""What ``for_each(outputs=...)`` means, and which kwargs scifor sets aside.

Owns two rules shared by ``scifor.for_each`` and ``scidb.for_each`` so the two
stay drop-in replacements for one another:

* :func:`discard_db_only_kwargs` — ``scifor.for_each`` accepts the public
  ``scidb.for_each`` kwargs that only mean something with a database behind
  the loop (:data:`scifor.options.DB_ONLY_KWARGS`, derived from the one
  option declaration) and discards them with one INFO line, instead of
  mistaking them for metadata iterables.
* :func:`output_column_name` — the result-table column an output lands in.
  scidb names its result columns (and matches them back to the variable types
  it saves) through this same function, so the two layers cannot name one
  output differently.
* :func:`is_empty_table` — a returned table with no rows is "nothing to save
  here" (a ``NoOutput`` slot / a no-data combination), for Python's loop,
  the MATLAB loop (which applies it with :data:`EMPTY_TABLE_DETAIL`) and
  ``save_batch``.
"""

from typing import Any

from scistacklog import Log

from .options import DB_ONLY_KWARGS

# Class attribute that overrides an output class's result-column name.
COLUMN_NAME_ATTR = "column_name"


def discard_db_only_kwargs(metadata_iterables: dict) -> dict:
    """Return ``metadata_iterables`` without the :data:`DB_ONLY_KWARGS`.

    Logs one INFO line naming what was dropped. The input is not mutated.
    """
    dropped = sorted(k for k in metadata_iterables if k in DB_ONLY_KWARGS)
    if not dropped:
        return metadata_iterables
    shown = ", ".join(f"{k}={_short(metadata_iterables[k])}" for k in dropped)
    Log.info(
        "discard_db_only_kwargs: ignored %s (scifor has no database; these "
        "only take effect in scidb.for_each)",
        shown,
        layer="scifor",
    )
    return {k: v for k, v in metadata_iterables.items() if k not in DB_ONLY_KWARGS}


def output_column_name(output: Any) -> str:
    """The result column name for one entry of ``outputs=``.

    First hit wins:

    1. a ``str`` is the name itself;
    2. a ``column_name`` class attribute set to a non-empty ``str``;
    3. ``view_name()`` if the object has one (scidb variable types);
    4. the class's ``__name__`` (or, for an instance, its type's).

    The name only applies when the output lands in ONE cell of the result
    table. When every output is a DataFrame and its rows spread, the
    DataFrame's own columns are used instead.
    """
    if isinstance(output, str):
        return output
    if hasattr(output, "view_name"):
        default_name = output.view_name()
    elif isinstance(output, type):
        default_name = output.__name__
    else:
        default_name = getattr(output, "__name__", type(output).__name__)
    return output_column_name_from(
        default_name, getattr(output, COLUMN_NAME_ATTR, None)
    )


def output_column_name_from(default_name: Any, column_name: Any) -> str:
    """The column-name rule on plain text: a non-empty ``column_name`` wins,
    otherwise ``default_name`` (a class name or a variable's view name).

    MATLAB (``+scifor/output_column_name.m``) calls this directly with the
    class name and its ``column_name`` Constant property, because Python
    cannot look inside a MATLAB object. :func:`output_column_name` calls it
    after extracting the same two values from a Python object.
    """
    if isinstance(column_name, str) and column_name:
        return column_name
    return str(default_name)


def _short(value: Any, limit: int = 40) -> str:
    text = repr(value) if isinstance(value, (bool, int, float, str, type(None))) else (
        f"<{type(value).__name__}>"
    )
    return text if len(text) <= limit else text[: limit - 3] + "..."


#: Reasons a :class:`NoOutput` slot can carry. ``"skipped"``: the producer
#: already reported why (the MATLAB loop leaves a skipped combination out of
#: that output's table and logs the reason; the bridge only sees the gap).
NO_OUTPUT_REASONS = ("no_data", "failed", "skipped")


class NoOutput:
    """INTERNAL marker: this output has nothing to save at this combination,
    while the call's other outputs may.

    A slot of a multi-output result holds one of these instead of a value.
    scifor sets it (for_columns split mode: a group with no data, or one the
    function raised on), keeps it in the result table for scidb, and
    replaces it with ``None`` in a standalone result. scidb's save loop
    writes no record for it. A call whose EVERY slot is a ``NoOutput`` is
    counted as one no-data or failed combination, never as completed.

    Not exported and never returned by user code: absence comes from the
    input, so SciStack decides it (``.claude/plan-for-columns-groups.md``).

    ``reason`` is one of :data:`NO_OUTPUT_REASONS`; ``detail`` is the one-line
    reason shown in the run summary (``"ValueError: all blank"``).
    """

    __slots__ = ("reason", "detail")

    def __init__(self, reason: str, detail: str = ""):
        if reason not in NO_OUTPUT_REASONS:
            raise ValueError(
                f"NoOutput reason must be one of {NO_OUTPUT_REASONS}, got {reason!r}"
            )
        self.reason = reason
        self.detail = detail

    def __repr__(self) -> str:
        detail = f", {self.detail!r}" if self.detail else ""
        return f"NoOutput({self.reason!r}{detail})"


def is_no_output(value: Any) -> bool:
    """Is *value* a :class:`NoOutput` slot? The one test every consumer uses."""
    return isinstance(value, NoOutput)


#: ``NoOutput.detail`` of a slot whose function returned a table with no
#: rows. The one spelling: the summary lines, the MATLAB loop's wire report
#: and the tests all read it from here.
EMPTY_TABLE_DETAIL = "returned an empty table"


def is_empty_table(value: Any) -> bool:
    """Did a function return an EMPTY table for this output?

    The one owner of "empty return" for both loops: a pandas DataFrame with
    no rows, whatever its columns (``pd.DataFrame()`` and a 0-row frame
    alike). A MATLAB ``table()`` reaches Python as such a frame and the
    MATLAB loop applies the same test (``height(t) == 0``) before it does.

    An empty table means "nothing to save at this combination": the slot
    becomes ``NoOutput("no_data", EMPTY_TABLE_DETAIL)``, so no record is
    written and the run's summary WARNs naming the combinations. Before
    2026-11-03 it reached the save as a record value: a phantom record with
    no data rows, or (when it happened to be the batch's first item) a
    zero-column reference schema that failed or dropped the batch.
    """
    try:
        import pandas as pd
    except ImportError:  # pragma: no cover - pandas is a scifor dependency
        return False
    return isinstance(value, pd.DataFrame) and len(value) == 0


def empty_tables_to_no_output(result: Any) -> "tuple[Any, list[int]]":
    """Replace every empty-table slot of one call's ``result`` with
    ``NoOutput("no_data", EMPTY_TABLE_DETAIL)``.

    ``result`` is a call's return value: a tuple (one slot per output) or a
    single value. The shape is kept. Returns ``(result, empty slot indices)``.
    """
    slots = result if isinstance(result, tuple) else (result,)
    empty = [i for i, v in enumerate(slots) if is_empty_table(v)]
    if not empty:
        return result, []
    replaced = tuple(
        NoOutput("no_data", EMPTY_TABLE_DETAIL) if i in empty else v
        for i, v in enumerate(slots)
    )
    return (replaced if isinstance(result, tuple) else replaced[0]), empty
