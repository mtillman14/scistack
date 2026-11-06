"""What a schema selection narrowed below the iterated level: a variant axis.

A selection on a key the run iterates only decides which calls happen. A
selection on a key it does NOT iterate decides what each call READS, so two such
selections are two computations at one output location, and they must be told
apart (docs/claude/selection-variant-axis.md).

The axis is read off the rows, before and after narrowing, never off how the
selection was spelled: ``include`` of minute 6 and ``exclude_levels`` of every
other minute read the same rows and are one variant.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd

from scidb.store.schema_values import schema_str

#: The input edge a narrowed call records, and the name of its coordinate
#: (``{fn}.__selection``). Dunder so it cannot be a function's argument.
SELECTION_PARAM = "__selection"

#: Values a label spells out for one key before it abbreviates.
_MAX_SPELLED = 6
#: Characters that would let two different value lists read as one label.
_AMBIGUOUS = set("/,=;#[]")

_Location = frozenset  # of (key, text | None)


def _cell_text(value: Any) -> "str | None":
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and value != value:
        return None
    return schema_str(value)


def _sort_key(text: "str | None") -> tuple:
    """Numbers in numeric order, text after them, a null last."""
    if text is None:
        return (2, 0.0, "")
    try:
        return (0, float(text), text)
    except ValueError:
        return (1, 0.0, text)


@dataclass(frozen=True)
class SelectionAxis:
    """Which rows a call read, of those it could have.

    ``keep`` is the per-key rule ``{key: values}``, naming only keys that were
    narrowed. ``rows`` is set when no per-key rule reproduces the rows (minute 5
    of session A with minute 6 of session B): it is then every kept location.
    """

    keep: tuple[tuple[str, tuple["str | None", ...]], ...] = ()
    rows: tuple[tuple[tuple[str, "str | None"], ...], ...] = ()

    def to_json(self) -> str:
        """THE stored form: canonical, so one meaning is one record."""
        payload: dict = {"keep": {key: list(values) for key, values in self.keep}}
        if self.rows:
            payload["rows"] = [[list(step) for step in row] for row in self.rows]
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str) -> "SelectionAxis":
        payload = json.loads(text)
        return cls(
            keep=tuple(
                (key, tuple(values)) for key, values in sorted(payload.get("keep", {}).items())
            ),
            rows=tuple(
                tuple((key, value) for key, value in row) for row in payload.get("rows", ())
            ),
        )

    @property
    def label(self) -> str:
        """THE coordinate value: what a card shows and a pin names.

        Readable, and still one label per meaning: anything it abbreviates or
        could misread gains a digest of the full form.
        """
        parts, exact = [], not self.rows
        for key, values in self.keep:
            texts = ["(none)" if v is None else str(v) for v in values]
            if any(set(text) & _AMBIGUOUS for text in texts):
                exact = False
            if len(texts) > _MAX_SPELLED:
                exact = False
                texts = [*texts[:3], f"…({len(values)})"]
            parts.append(f"{key}={'/'.join(texts)}")
        text = ", ".join(parts) or "selected rows"
        if self.rows:
            text += " (per location)"
        if exact:
            return text
        return f"{text} #{hashlib.sha1(self.to_json().encode()).hexdigest()[:8]}"

    def admits(self, location: Mapping[str, Any], columns: Iterable[str]) -> bool:
        """Would this location's row have been kept? *location* holds text and
        may omit a key the record has no value at; *columns* are the keys its
        table has, because a key with no column constrains nothing."""
        present = set(columns)
        if self.rows:
            return any(
                all(location.get(key) == value for key, value in row if key in present)
                for row in self.rows
            )
        return all(
            location.get(key) in values for key, values in self.keep if key in present
        )


@dataclass(frozen=True)
class SelectionAxes:
    """One run's axes: the same for every call, or decided per call."""

    uniform: "SelectionAxis | None" = None
    #: ``{iterated values as text: axis}``, when no one rule fits every call.
    per_call: "Mapping[tuple, SelectionAxis] | None" = None
    level: tuple[str, ...] = ()

    def for_combo(self, combo: Mapping[str, Any]) -> "SelectionAxis | None":
        if self.uniform is not None:
            return self.uniform
        if not self.per_call:
            return None
        return self.per_call.get(tuple(_cell_text(combo.get(key)) for key in self.level))


def _frame_pairs(before: Any, after: Any) -> Iterable[tuple[Any, Any]]:
    """The (before, after) tables that narrowing changed. A table it left alone
    (a ``Fixed`` pin, a coarser input without the key) has nothing to say."""
    from scifor import ColumnSelection, Merge

    if after is before:
        return
    if isinstance(after, pd.DataFrame):
        yield before, after
    elif isinstance(after, ColumnSelection):
        yield before.data, after.data
    elif isinstance(after, Merge):
        for old, new in zip(before.tables, after.tables, strict=True):
            yield from _frame_pairs(old, new)


def _locations_by_call(frame: Any, level: tuple, below: list) -> dict:
    """``{iterated values: {location below them}}``; a location is a frozenset
    because two tables may carry different keys."""
    columns = [key for key in level if key in frame.columns]
    columns += [key for key in below if key in frame.columns]
    out: dict = {}
    for values in frame[columns].drop_duplicates().itertuples(index=False, name=None):
        cells = dict(zip(columns, values, strict=True))
        call = tuple(_cell_text(cells[key]) if key in cells else None for key in level)
        location = frozenset(
            (key, _cell_text(cells[key])) for key in below if key in cells
        )
        out.setdefault(call, set()).add(location)
    return out


def _rule(calls: Iterable[tuple[set, set]]) -> "tuple[dict, set] | None":
    """The per-key rule these ``(available, kept)`` pairs follow and the keys it
    narrowed, or None when no per-key rule reproduces the kept rows."""
    pairs = [(a, k) for a, k in calls if k]
    allowed: dict = {}
    for _available, kept in pairs:
        for location in kept:
            for key, value in location:
                allowed.setdefault(key, set()).add(value)

    def passes(location: _Location) -> bool:
        return all(value in allowed.get(key, ()) for key, value in location)

    narrowed: set = set()
    for available, kept in pairs:
        if {location for location in available if passes(location)} != kept:
            return None
        for location in available:
            narrowed.update(k for k, v in location if v not in allowed.get(k, ()))
    return allowed, narrowed


def _axis_from_rule(allowed: dict, narrowed: set) -> SelectionAxis:
    return SelectionAxis(
        keep=tuple(
            (key, tuple(sorted(allowed.get(key, ()), key=_sort_key))) for key in sorted(narrowed)
        )
    )


def _axis_of_one_call(available: set, kept: set) -> "SelectionAxis | None":
    if available == kept:
        return None
    rule = _rule([(available, kept)])
    if rule is not None:
        return _axis_from_rule(*rule)
    # What each key still holds, for the label; `rows` carries the meaning.
    summary: dict = {}
    for location in kept:
        for key, value in location:
            summary.setdefault(key, set()).add(value)
    varied = {k for location in available for k, v in location if v not in summary.get(k, ())}
    varied |= {k for k, values in summary.items() if len(values) > 1}
    return SelectionAxis(
        keep=_axis_from_rule(summary, varied).keep,
        rows=tuple(sorted(
            (tuple(sorted(location, key=lambda s: s[0])) for location in kept),
            key=lambda row: [(k, _sort_key(v)) for k, v in row],
        )),
    )


def selection_axes_of(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    level: Iterable[str],
    schema_keys: Iterable[str],
    *,
    context: str = "",
) -> SelectionAxes:
    """The axes of a run, from its inputs before and after ``narrow_inputs``.

    Only calls that kept a row count, so running one subject of a selection
    cannot make its other sessions look narrowed.

    A single rule is preferred: a subject that only ever had minute 6 still reads
    ``minute=6`` beside the subjects it was narrowed from, or one selection would
    scatter across cards and downstream groups. When no single rule fits (all of
    subject 1, minute 6 of subject 2) each call answers for itself.
    """
    from scidb.model.log import Log

    level = tuple(level)
    below = [key for key in schema_keys if key not in level]
    tables = [
        (_locations_by_call(old, level, below), _locations_by_call(new, level, below))
        for name, value in after.items()
        for old, new in _frame_pairs(before[name], value)
    ]
    if not tables or not below:
        return SelectionAxes(level=level)
    calls = [
        (available.get(call, set()), kept[call])
        for available, kept in tables
        for call in kept
    ]
    rule = _rule(calls)
    if rule is not None:
        allowed, narrowed = rule
        if not narrowed:
            Log.debug(f"[selection] {context}: nothing narrowed within a call; no axis")
            return SelectionAxes(level=level)
        axis = _axis_from_rule(allowed, narrowed)
        Log.info(f"[selection] {context}: every call reads {axis.label}")
        return SelectionAxes(uniform=axis, level=level)

    per_call: dict = {}
    merged: dict = {}
    for available, kept in tables:
        for call, locations in kept.items():
            slot = merged.setdefault(call, (set(), set()))
            slot[0].update(available.get(call, set()))
            slot[1].update(locations)
    for call, (available, kept) in merged.items():
        axis = _axis_of_one_call(available, kept)
        if axis is not None:
            per_call[call] = axis
    Log.info(
        f"[selection] {context}: no single rule fits this selection, so each call "
        f"answers for itself: {len(per_call)} of {len(merged)} narrowed, "
        f"{len({a.label for a in per_call.values()})} distinct"
    )
    return SelectionAxes(per_call=per_call, level=level)
