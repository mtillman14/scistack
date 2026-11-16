"""A ``where=`` check: Yes/No factors on the rows of an ordinary figure.

A node's ``where=`` filters decide which locations its function runs on.
Seeing why a location is in or out is a figure question: *this* variable, at
*these* locations, passes or fails *this* comparison. So a check is not a
second screen. It is an ordinary figure whose table carries extra factor
columns, drawn by the same renderers, coloured and faceted by the same roles:

* :data:`MATCHING_FACTOR` -- Yes when every ENABLED filter passes, No when one
  fails, "No value" when none failed but one could not be answered;
* one factor per filter (``Filter 1``, ``Filter 2``, by position) when
  there are two or more, with the levels Yes / No / "No value".

**Who owns what.** scidb owns the verdict (``scidb.model.filters.
filter_report``); the source (``scistackplotdb.source``) joins it onto the
rows; this module owns only the NAMES and LEVELS those columns carry and the
spec field that asks for them. Nothing downstream reads a flag: the columns
are ordinary categorical factors by the time ``validate`` sees them
(synthetic-factors invariant 1).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: The factor holding the verdict of all enabled filters together.
MATCHING_FACTOR = "Matching"

LEVEL_YES = "Yes"
LEVEL_NO = "No"
LEVEL_NO_VALUE = "No value"

#: Declared level order of every check factor.
LEVEL_ORDER: tuple[str, ...] = (LEVEL_YES, LEVEL_NO, LEVEL_NO_VALUE)

#: The opening colours of a check (level -> colour), the same on every check
#: so the colours mean one thing. Green / red / grey.
CHECK_COLORS: dict[str, str] = {
    LEVEL_YES: "#2e9e5b",
    LEVEL_NO: "#d6453d",
    LEVEL_NO_VALUE: "#9aa0a6",
}


@dataclass(frozen=True)
class WhereCheckFilter:
    """One filter row of a check: the node's dict form plus a tick.

    ``enabled`` False keeps the row on screen (and as a factor) but leaves it
    out of :data:`MATCHING_FACTOR`: the tick box explores a filter's effect
    without deleting it.
    """

    variable: str
    column: str | None = None
    op: str = "=="
    value: str = ""
    enabled: bool = True

    def to_dict(self) -> dict:
        return {
            "variable": self.variable,
            "column": self.column,
            "op": self.op,
            "value": self.value,
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "WhereCheckFilter":
        value = raw.get("value")
        return cls(
            variable=str(raw.get("variable") or ""),
            column=(str(raw["column"]) if raw.get("column") else None),
            op=str(raw.get("op") or "=="),
            value="" if value is None else str(value),
            enabled=bool(raw.get("enabled", True)),
        )

    def filter_dict(self) -> dict:
        """The scidb filter dict (``filter_from_dict``) without the tick."""
        return {
            "variable": self.variable,
            "column": self.column,
            "op": self.op,
            "value": self.value,
        }


@dataclass(frozen=True)
class WhereCheck:
    """``PlotSpec.where_check``: the filters a figure is checking, and the
    function node they belong to (where Apply writes them back).

    Frozen and hashable: ``BaseSource.get_table`` memoizes on it.
    """

    node_id: str | None = None
    filters: tuple[WhereCheckFilter, ...] = ()
    #: The function's name, for the banner only; nothing reads it back.
    function_name: str | None = None

    def __post_init__(self) -> None:
        # The restore walk keeps a tuple-typed field as stored (a list of
        # dicts); the class decides, like FactorVariable.variant.
        rows = tuple(
            f if isinstance(f, WhereCheckFilter) else WhereCheckFilter.from_dict(f)
            for f in self.filters
        )
        object.__setattr__(self, "filters", rows)

    def __bool__(self) -> bool:
        return bool(self.filters)

    @property
    def enabled(self) -> tuple[WhereCheckFilter, ...]:
        return tuple(f for f in self.filters if f.enabled)

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "function_name": self.function_name,
            "filters": [f.to_dict() for f in self.filters],
        }

    @classmethod
    def from_dict(cls, raw: "dict | None") -> "WhereCheck | None":
        if not raw or not raw.get("filters"):
            return None
        return cls(
            node_id=raw.get("node_id"),
            function_name=raw.get("function_name"),
            filters=tuple(WhereCheckFilter.from_dict(f) for f in raw["filters"]),
        )


def filter_factor_name(index: int) -> str:
    """The factor of the *index*-th (0-based) filter. By POSITION, not by the
    filter's text: editing a comparison in the check must not rename the
    factor, or every role and colour naming it would go stale. The text is the
    section row's label."""
    return f"Filter {index + 1}"


def matching_verdict(verdicts: "list[Any]") -> str:
    """The Matching level for one location from its enabled filters' verdicts
    (``"pass"`` / ``"fail"`` / ``"no value"``). One rule, one place."""
    if any(v == "fail" for v in verdicts):
        return LEVEL_NO
    if any(v == "no value" for v in verdicts):
        return LEVEL_NO_VALUE
    return LEVEL_YES


def verdict_level(verdict: str) -> str:
    """The factor level of one filter's verdict."""
    return {"pass": LEVEL_YES, "fail": LEVEL_NO}.get(verdict, LEVEL_NO_VALUE)
