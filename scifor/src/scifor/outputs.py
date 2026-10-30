"""What ``for_each(outputs=...)`` means, and which kwargs scifor sets aside.

Owns two rules shared by ``scifor.for_each`` and ``scidb.for_each`` so the two
stay drop-in replacements for one another:

* :data:`DB_ONLY_KWARGS` — the public ``scidb.for_each`` kwargs that only mean
  something with a database behind the loop. ``scifor.for_each`` accepts them
  and discards them (with one INFO line) instead of mistaking them for
  metadata iterables. ``scidb/tests/test_foreach_signature_parity.py`` holds
  scidb's signature to exactly this set.
* :func:`output_column_name` — the result-table column an output lands in.
  scidb names its result columns (and matches them back to the variable types
  it saves) through this same function, so the two layers cannot name one
  output differently.
"""

from typing import Any

from scistacklog import Log

DB_ONLY_KWARGS: frozenset[str] = frozenset(
    {
        "save",
        "db",
        "introspect",
        "skip_computed",
        "schema_filter",
        "finalized",
        "glue",
        "parameter_names",
        "step",
        "pipeline",
        # scidb.combine: two result rows at one location saved as one record.
        "on_duplicate",
    }
)

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
    custom = getattr(output, COLUMN_NAME_ATTR, None)
    if isinstance(custom, str) and custom:
        return custom
    if hasattr(output, "view_name"):
        return output.view_name()
    if isinstance(output, type):
        return output.__name__
    return getattr(output, "__name__", type(output).__name__)


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
