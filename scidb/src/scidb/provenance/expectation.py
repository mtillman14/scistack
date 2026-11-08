"""Expected locations: for one variable, which schema-key combinations should
exist, and does each one match that expectation?

Every combination gets an **expectation** (generated from the per-key
settings below, overridden by an Expected / Not expected mark) and a
**fact** (the variable has a record there or not). The two give four
statuses, plus an analysis exclusion that sits outside them:

==================  =================  ======================
                    present            absent
==================  =================  ======================
**expected**        ``present``        ``missing_unexpected``
**not expected**    ``present_unexpected``  ``missing_expected``
==================  =================  ======================

``excluded``: an analysis exclusion (``scidb.store.exclusions``) governs the
combination, or every record there is marked excluded. It wins over the rest.

The first look is two colours (:data:`TONE`): ``ok`` when the fact matches
the expectation, ``problem`` when it does not. The exact status and a
one-sentence reason (:attr:`ExpectationRow.why`) are always available: this
module owns those words, and the CLI and the GUI only display them.

**The variable's keys.** The schema keys its records are saved at: the key
set most of its locations use (a tie goes to the deeper set). Keys below
that are never crossed. A record saved at a different key set is
``present_unexpected`` unless a mark accepts it.

**The universe** (what is expected), built key by key in schema order:

* a ``shared`` key (every parent has the same values) contributes its
  declared values, or else every value it has anywhere in the database
  (``DatabaseManager.distinct_schema_values``, in declared level order);
* a ``per_parent`` key contributes only the values found under each parent
  anywhere in the database. A parent with none gets one row with every value
  of the rest (``None``, shown as ``(all)``): nothing at all exists below it.

Automatic sharing (a key with no stated setting): the top key is shared, a
key to the left of the last key stated ``shared`` is shared, and every other
key is per parent. Declared values are allowed only on a shared key.

**Marks** (``_expectation_mark``) name a variable and some schema keys (the
rest are wildcards). The rule for which mark governs a combination is
:func:`scidb.store.exclusions.most_specific_match`, shared with exclusions.
A variable's own marks beat inherited ones; marks are inherited from every
variable upstream of it (``provenance.query.related_variable_types``), and
only apply where the variable has every key the mark names.

Storage: ``_schema_key_expectation`` and ``_expectation_mark``, both
append-only (the newest row is current); never who
(feedback_no_user_identity). Design: ``docs/claude/expected-locations.md``.
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from scidb.model.log import Log
from scidb.store.schema_values import schema_str
from scidb.store.tables import EXPECTATION_MARK, SCHEMA_KEY_EXPECTATION

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

SETTINGS_TABLE = SCHEMA_KEY_EXPECTATION.name
MARK_TABLE = EXPECTATION_MARK.name

SHARED = "shared"
PER_PARENT = "per_parent"
SHARING = (SHARED, PER_PARENT)

#: Why a key has the sharing it has (:attr:`KeySetting.rule`).
RULE_STATED = "stated"
RULE_TOP_KEY = "top_key"
RULE_LEFT_OF_SHARED = "left_of_shared"
RULE_DEFAULT = "default"

PRESENT = "present"
MISSING_UNEXPECTED = "missing_unexpected"
MISSING_EXPECTED = "missing_expected"
PRESENT_UNEXPECTED = "present_unexpected"
EXCLUDED = "excluded"
STATUSES = (PRESENT, MISSING_UNEXPECTED, PRESENT_UNEXPECTED, MISSING_EXPECTED, EXCLUDED)

#: The one wording of each status.
STATUS_LABELS = {
    PRESENT: "Present, as expected",
    MISSING_UNEXPECTED: "Missing unexpectedly",
    PRESENT_UNEXPECTED: "Unexpectedly present",
    MISSING_EXPECTED: "Missing as expected",
    EXCLUDED: "Excluded from analysis",
}

OK = "ok"
PROBLEM = "problem"
#: The two-level first look: does the fact match the expectation?
TONE = {
    PRESENT: OK,
    MISSING_EXPECTED: OK,
    MISSING_UNEXPECTED: PROBLEM,
    PRESENT_UNEXPECTED: PROBLEM,
    EXCLUDED: EXCLUDED,
}

#: Where a row's expectation came from (:attr:`ExpectationRow.source`).
SOURCE_RULE = "rule"
SOURCE_DECLARED_VALUES = "declared_values"
SOURCE_LEVEL = "level"
SOURCE_MARK = "mark"
SOURCE_EXCLUSION = "exclusion"
SOURCE_RECORD_EXCLUDED = "record_excluded"

#: How a wildcard cell (every value of that key) is shown.
ALL_LEVELS = "(all)"

#: Above this many combinations the universe is refused before it is built.
MAX_COMBINATIONS = 1_000_000

_PREVIEW = 6


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


@dataclass
class KeySetting:
    """How one schema key's values are expected, after automatic rules."""

    key: str
    sharing: str
    #: Declared values (shared keys only), or None: every value in the database.
    values: list[str] | None = None
    rule: str = RULE_DEFAULT
    #: What the user stated (None = automatic).
    stated_sharing: str | None = None
    declared_at: str | None = None

    def describe(self) -> str:
        if self.sharing == PER_PARENT:
            text = f"{self.key}: varies per parent"
        elif self.values is not None:
            text = f"{self.key}: shared, declared {_preview(self.values)}"
        else:
            text = f"{self.key}: shared (every value in the database)"
        auto = {
            RULE_TOP_KEY: "automatic: top key",
            RULE_LEFT_OF_SHARED: "automatic: left of a shared key",
            RULE_DEFAULT: "automatic",
        }.get(self.rule)
        return f"{text} ({auto})" if auto else text

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "sharing": self.sharing,
            "values": None if self.values is None else list(self.values),
            "rule": self.rule,
            "stated_sharing": self.stated_sharing,
            "declared_at": self.declared_at,
            "description": self.describe(),
        }


@dataclass(frozen=True)
class Mark:
    """One current Expected / Not expected mark."""

    variable: str
    keys: dict[str, str]
    expected: bool
    reason: str
    changed_at: Any

    def describe(self, inherited: bool = False) -> str:
        word = "expected" if self.expected else "not expected"
        where = f" in {self.variable}" if inherited else ""
        when = _date(self.changed_at)
        return f"marked {word}{where}{' on ' + when if when else ''}: {self.reason}"

    def to_dict(self) -> dict:
        return {
            "variable": self.variable,
            "keys": dict(self.keys),
            "expected": self.expected,
            "reason": self.reason,
            "changed_at": _iso(self.changed_at),
        }


@dataclass
class ExpectationRow:
    """One combination: its keys (None = every value), status and why."""

    values: dict[str, str | None]
    status: str
    expected: bool
    present: bool
    why: str
    source: str
    mark: Mark | None = None
    #: The upstream variable the governing mark belongs to, or None (own).
    inherited_from: str | None = None
    exclusion_reason: str | None = None
    #: Why the last run left this location empty (scidb.provenance.outcomes),
    #: for a missing row only.
    last_run: Any = None

    @property
    def label(self) -> str:
        return STATUS_LABELS[self.status]

    @property
    def tone(self) -> str:
        return TONE[self.status]

    def to_dict(self) -> dict:
        return {
            "values": dict(self.values),
            "status": self.status,
            "label": self.label,
            "tone": self.tone,
            "expected": self.expected,
            "present": self.present,
            "why": self.why,
            "source": self.source,
            "mark": None if self.mark is None else self.mark.to_dict(),
            "inherited_from": self.inherited_from,
            "exclusion_reason": self.exclusion_reason,
            "last_run": None if self.last_run is None else self.last_run.to_dict(),
        }


def _zero_counts() -> dict[str, int]:
    return {s: 0 for s in STATUSES}


@dataclass
class Expectation:
    """Every expected or present combination of one variable."""

    variable: str
    #: The variable's schema keys (schema order); the universe's columns.
    keys: list[str]
    #: Why those keys (a sentence).
    keys_rule: str
    settings: list[KeySetting] = field(default_factory=list)
    rows: list[ExpectationRow] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=_zero_counts)
    notes: list[str] = field(default_factory=list)
    universe_size: int = 0
    #: Every schema key any row names, in schema order (rows saved at another
    #: key set add columns).
    columns: list[str] = field(default_factory=list)

    @property
    def problems(self) -> list[ExpectationRow]:
        return [r for r in self.rows if r.tone == PROBLEM]

    def tone_counts(self) -> dict[str, int]:
        out = {OK: 0, PROBLEM: 0, EXCLUDED: 0}
        for status, n in self.counts.items():
            out[TONE[status]] += n
        return out

    def by(self, key: str) -> list[tuple[str, dict[str, int]]]:
        """Counts per value of *key* (``(all)`` for wildcard rows), in row order."""
        if key not in self.columns:
            raise ValueError(f"{key!r} is not one of this variable's keys: {self.columns}")
        out: dict[str, dict[str, int]] = {}
        for r in self.rows:
            v = r.values.get(key)
            label = ALL_LEVELS if v is None else v
            out.setdefault(label, _zero_counts())[r.status] += 1
        return list(out.items())

    def table(self, problems_only: bool = False, statuses=None) -> tuple[list[str], list[list]]:
        """``(header, rows)`` for display and CSV: key columns, then status
        label and why. The one owner of the table's layout."""
        rows = self.rows
        if problems_only:
            rows = [r for r in rows if r.tone == PROBLEM]
        if statuses:
            rows = [r for r in rows if r.status in statuses]
        header = [*self.columns, "status", "why"]
        body = [
            [_cell(r.values, k) for k in self.columns] + [r.label, r.why]
            for r in rows
        ]
        return header, body

    def to_dict(
        self, problems_only: bool = False, statuses=None, limit: int | None = None
    ) -> dict:
        """JSON shape shared by ``scidb expected --json`` and the GUI."""
        rows = self.rows
        if problems_only:
            rows = [r for r in rows if r.tone == PROBLEM]
        if statuses:
            rows = [r for r in rows if r.status in statuses]
        total = len(rows)
        if limit is not None:
            rows = rows[:limit]
        return {
            "variable": self.variable,
            "keys": list(self.keys),
            "keys_rule": self.keys_rule,
            "columns": list(self.columns),
            "settings": [s.to_dict() for s in self.settings],
            "rows": [r.to_dict() for r in rows],
            "total_rows": total,
            "truncated": len(rows) < total,
            "counts": dict(self.counts),
            "tone_counts": self.tone_counts(),
            "notes": list(self.notes),
            "universe_size": self.universe_size,
            "labels": dict(STATUS_LABELS),
            "tones": dict(TONE),
            "all_marker": ALL_LEVELS,
        }


# ---------------------------------------------------------------------------
# Per-key settings: storage
# ---------------------------------------------------------------------------


def read_key_settings(duck) -> dict[str, dict]:
    """The stated setting per key: ``{key: {sharing, values, declared_at,
    source}}``, newest row per key. A key whose newest row states nothing
    (both NULL) is left out. ``{}`` before the table exists."""
    if not duck.table_exists(SETTINGS_TABLE):
        return {}
    rows = duck.fetchall(f"""
        SELECT key, sharing, declared_values, declared_at, source FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY key ORDER BY declared_at DESC, rowid DESC
            ) AS rn
            FROM {SETTINGS_TABLE}
        ) WHERE rn = 1
    """)
    out = {}
    for key, sharing, values, declared_at, source in rows:
        if sharing is None and values is None:
            continue
        out[key] = {
            "sharing": sharing,
            "values": None if values is None else [str(v) for v in values],
            "declared_at": declared_at,
            "source": source,
        }
    return out


def key_setting_history(duck, key: str | None = None) -> list[dict]:
    """Every setting ever written, oldest first (optionally one key)."""
    if not duck.table_exists(SETTINGS_TABLE):
        return []
    where, params = ("WHERE key = ?", [key]) if key is not None else ("", None)
    rows = duck.fetchall(
        f"SELECT key, sharing, declared_values, declared_at, source "
        f"FROM {SETTINGS_TABLE} {where} ORDER BY declared_at, rowid",
        params,
    )
    return [
        {
            "key": k,
            "sharing": s,
            "values": None if v is None else [str(x) for x in v],
            "declared_at": at,
            "source": src,
        }
        for k, s, v, at, src in rows
    ]


def set_key_setting(
    db: DatabaseManager,
    key: str,
    sharing: str | None = None,
    values: list | None = None,
    *,
    source: str = "api",
) -> dict:
    """State how *key*'s values are expected. The whole setting is written:
    *sharing* ``"shared"`` / ``"per_parent"`` / None (automatic), *values*
    a list (shared keys only) or None (every value in the database).

    Giving *values* with *sharing* None states ``shared``. Writing the
    setting already stored is a no-op. Returns the stored setting.
    """
    _check_writable(db, "change schema key expectations")
    schema_keys = list(db.dataset_schema_keys)
    if key not in schema_keys:
        raise ValueError(f"{key!r} is not a schema key. Schema keys: {schema_keys}")
    if sharing is not None and sharing not in SHARING:
        raise ValueError(f"sharing must be one of {SHARING} or None, got {sharing!r}")
    if values is not None:
        values = _dedupe([schema_str(v) for v in values])
        if not values:
            raise ValueError(
                f"Declared values for {key!r} are empty. Pass None to use every "
                f"value in the database."
            )
        if sharing is None:
            sharing = SHARED
            Log.debug(f"expectation: declared values for {key!r} imply sharing={SHARED!r}")
        if sharing == PER_PARENT:
            raise ValueError(
                f"{key!r} varies per parent, so it has no single list of values. "
                f"Declare values only on a shared key."
            )
    current = read_key_settings(db._duck).get(key)
    if current is not None and current["sharing"] == sharing and current["values"] == values:
        Log.debug(f"expectation: {key!r} already set to {sharing!r} {values}; nothing written")
        return current
    if current is None and sharing is None and values is None:
        Log.debug(f"expectation: {key!r} is already automatic; nothing written")
        return {"sharing": None, "values": None, "declared_at": None, "source": None}
    declared_at = datetime.now(timezone.utc).isoformat()
    db._duck.execute(
        f"INSERT INTO {SETTINGS_TABLE} (key, sharing, declared_values, declared_at, source) "
        f"VALUES (?, ?, ?, ?, ?)",
        [key, sharing, values, declared_at, source],
    )
    Log.info(
        f"expectation: key {key!r} sharing {(current or {}).get('sharing')!r} -> {sharing!r}, "
        f"values {_preview((current or {}).get('values'))} -> {_preview(values)} (source={source})"
    )
    return {"sharing": sharing, "values": values, "declared_at": declared_at, "source": source}


def parse_values_text(text: str) -> list[str]:
    """Declared values as typed by a person: comma-separated values and
    ``a..b`` integer ranges. A range starting with a zero keeps its width
    (``01..12`` -> ``01``, ``02``, ... ``12``). The one parser, for the CLI
    and the GUI alike. Raises ValueError on an empty result or a bad range.
    """
    out: list[str] = []
    for part in (p.strip() for p in text.split(",")):
        if not part:
            continue
        if ".." in part:
            lo, hi = (s.strip() for s in part.split("..", 1))
            if not (lo.isdigit() and hi.isdigit()) or int(hi) < int(lo):
                raise ValueError(f"{part!r} is not a range like 1..5 or 01..12.")
            width = len(lo) if lo.startswith("0") and len(lo) > 1 else 0
            out.extend(str(i).zfill(width) for i in range(int(lo), int(hi) + 1))
        else:
            out.append(part)
    out = _dedupe(out)
    if not out:
        raise ValueError("No values given.")
    return out


def resolve_key_settings(
    schema_keys: list[str], stated: dict[str, dict]
) -> dict[str, KeySetting]:
    """Every key's effective setting: THE automatic rule.

    A stated key keeps what was stated. Otherwise the top key is shared, a
    key to the left of the last key stated shared is shared, and every other
    key varies per parent.
    """
    last_shared = max(
        (i for i, k in enumerate(schema_keys) if (stated.get(k) or {}).get("sharing") == SHARED),
        default=-1,
    )
    out: dict[str, KeySetting] = {}
    for i, key in enumerate(schema_keys):
        s = stated.get(key) or {}
        if s.get("sharing") is not None:
            sharing, rule = s["sharing"], RULE_STATED
        elif i == 0:
            sharing, rule = SHARED, RULE_TOP_KEY
        elif i < last_shared:
            sharing, rule = SHARED, RULE_LEFT_OF_SHARED
        else:
            sharing, rule = PER_PARENT, RULE_DEFAULT
        out[key] = KeySetting(
            key=key,
            sharing=sharing,
            values=s.get("values") if sharing == SHARED else None,
            rule=rule,
            stated_sharing=s.get("sharing"),
            declared_at=s.get("declared_at"),
        )
    return out


def key_settings(db: DatabaseManager) -> list[KeySetting]:
    """Every schema key's effective setting, in schema order."""
    keys = list(db.dataset_schema_keys)
    resolved = resolve_key_settings(keys, read_key_settings(db._duck))
    return [resolved[k] for k in keys]


# ---------------------------------------------------------------------------
# Marks: storage
# ---------------------------------------------------------------------------


def _mark_rows(duck, schema_keys: list[str], variables: list[str] | None, history: bool):
    if not duck.table_exists(MARK_TABLE):
        return []
    key_cols = ", ".join(f'"{k}"' for k in schema_keys)
    where, params = "", None
    if variables is not None:
        if not variables:
            return []
        where = "WHERE variable_name IN (" + ", ".join("?" for _ in variables) + ")"
        params = list(variables)
    if history:
        sql = (
            f"SELECT variable_name, {key_cols}, expected, active, reason, changed_at "
            f"FROM {MARK_TABLE} {where} ORDER BY changed_at, rowid"
        )
    else:
        sql = f"""
            SELECT variable_name, {key_cols}, expected, active, reason, changed_at FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY variable_name, {key_cols}
                    ORDER BY changed_at DESC, rowid DESC
                ) AS rn
                FROM {MARK_TABLE} {where}
            ) WHERE rn = 1 AND active
        """
    n = len(schema_keys)
    out = []
    for row in duck.fetchall(sql, params):
        keys = {k: str(v) for k, v in zip(schema_keys, row[1:1 + n]) if v is not None}
        expected, active, reason, changed_at = row[1 + n:]
        out.append((row[0], keys, bool(expected), bool(active), reason, changed_at))
    return out


def marks(db: DatabaseManager, variables: list[str] | None = None) -> list[Mark]:
    """The current (active) marks, optionally of some variables only."""
    keys = list(db.dataset_schema_keys)
    return [
        Mark(variable=v, keys=k, expected=e, reason=r, changed_at=at)
        for v, k, e, _active, r, at in _mark_rows(db._duck, keys, variables, history=False)
    ]


def mark_history(db: DatabaseManager, variable: str | None = None) -> list[dict]:
    """Every mark and unmark ever written, oldest first."""
    keys = list(db.dataset_schema_keys)
    rows = _mark_rows(db._duck, keys, None if variable is None else [_name(variable)], history=True)
    return [
        {
            "variable": v,
            "keys": k,
            "expected": e,
            "active": a,
            "reason": r,
            "changed_at": _iso(at),
        }
        for v, k, e, a, r, at in rows
    ]


def mark_expectation(
    db: DatabaseManager, variable, expected: bool, reason: str, **schema_keys
) -> Mark:
    """Mark *variable* Expected (``expected=True``) or Not expected at the
    combination *schema_keys* (keys left out are every value). A reason is
    required. Marking what is already marked the same way raises."""
    _check_writable(db, "mark expected locations")
    name = _name(variable)
    keys = _mark_keys(db, schema_keys)
    reason = (reason or "").strip()
    if not reason:
        raise ValueError("A mark needs a reason (why is this expected or not?).")
    current = _exact_mark(db, name, keys)
    if current is not None and current.expected == bool(expected):
        raise ValueError(
            f"{name} at {_where(keys)} is already marked "
            f"{'expected' if expected else 'not expected'} ({current.reason!r})."
        )
    at = _insert_mark(db, name, keys, bool(expected), True, reason)
    Log.info(
        f"expectation: marked {name} at {_where(keys)} "
        f"{'expected' if expected else 'not expected'} (was "
        f"{'unmarked' if current is None else ('expected' if current.expected else 'not expected')}): {reason}"
    )
    return Mark(variable=name, keys=keys, expected=bool(expected), reason=reason, changed_at=at)


def unmark_expectation(db: DatabaseManager, variable, reason: str = "", **schema_keys) -> None:
    """Take back the mark on exactly this keyset (nothing is deleted: an
    inactive row is appended). Raises when there is no such mark."""
    _check_writable(db, "unmark expected locations")
    name = _name(variable)
    keys = _mark_keys(db, schema_keys)
    current = _exact_mark(db, name, keys)
    if current is None:
        raise ValueError(f"{name} has no mark at exactly {_where(keys)}.")
    _insert_mark(db, name, keys, current.expected, False, (reason or "").strip() or "unmarked")
    Log.info(f"expectation: unmarked {name} at {_where(keys)} (was {current.describe()})")


def _mark_keys(db: DatabaseManager, schema_keys: dict) -> dict[str, str]:
    if not schema_keys:
        raise ValueError("Name at least one schema key to mark.")
    valid = list(db.dataset_schema_keys)
    unknown = sorted(set(schema_keys) - set(valid))
    if unknown:
        raise ValueError(f"Unknown schema key(s): {unknown}. Schema keys: {valid}")
    return {k: schema_str(schema_keys[k]) for k in valid if k in schema_keys}


def _exact_mark(db: DatabaseManager, name: str, keys: dict) -> Mark | None:
    for m in marks(db, [name]):
        if m.keys == keys:
            return m
    return None


def _insert_mark(db, name, keys, expected, active, reason) -> datetime:
    schema_keys = list(db.dataset_schema_keys)
    cols = ["variable_name", *schema_keys, "expected", "active", "reason", "changed_at"]
    at = datetime.now(timezone.utc).replace(tzinfo=None)
    values = [name, *[keys.get(k) for k in schema_keys], expected, active, reason, at]
    db._duck.execute(
        f"INSERT INTO {MARK_TABLE} ({', '.join(_quoted(c) for c in cols)}) "
        f"VALUES ({', '.join('?' for _ in cols)})",
        values,
    )
    return at


# ---------------------------------------------------------------------------
# The expectation of one variable
# ---------------------------------------------------------------------------


def expectation(
    db: DatabaseManager, variable, *, max_combinations: int = MAX_COMBINATIONS
) -> Expectation:
    """Every expected or present combination of *variable*, with its status
    and why. Read only.

    Raises ValueError when the variable has no records and is not
    registered, or when the universe would exceed *max_combinations*.
    """
    from scidb.provenance.query import related_variable_types
    from scidb.store.exclusions import governing_overrides, most_specific_match

    t0 = time.perf_counter()
    timings: dict[str, float] = {}

    def lap(step: str, since: float) -> float:
        now = time.perf_counter()
        timings[step] = (now - since) * 1000
        return now

    name = _name(variable)
    duck = db._duck
    schema_keys = list(db.dataset_schema_keys)

    locations, keyless = _variable_locations(duck, name, schema_keys)
    t = lap("variable_locations", t0)

    keys, keys_rule = _variable_keys(duck, name, schema_keys, locations, keyless)
    settings_all = resolve_key_settings(schema_keys, read_key_settings(duck))
    settings = [settings_all[k] for k in keys]
    for s in settings:
        Log.debug(f"expectation {name}: {s.describe()}")
    t = lap("resolve_keys", t)

    result = Expectation(variable=name, keys=keys, keys_rule=keys_rule, settings=settings)
    if not keys:
        result.notes.append(
            f"{name} is saved without schema keys, so no location is expected."
        )
        result.columns = []
        Log.info(f"expectation {name}: no schema keys ({keyless} keyless record(s))")
        return result

    order = {k: {v: i for i, v in enumerate(str(x) for x in db.distinct_schema_values(k))} for k in keys}
    universe = _universe(db, keys, settings_all, order, max_combinations)
    result.universe_size = len(universe)
    t = lap("build_universe", t)

    upstream = related_variable_types(duck, name, "upstream")
    all_marks = marks(db, [name, *upstream])
    own = [m for m in all_marks if m.variable == name]
    inherited = [m for m in all_marks if m.variable != name]
    Log.debug(
        f"expectation {name}: {len(own)} own mark(s), {len(inherited)} inherited from "
        f"{sorted({m.variable for m in inherited})} (upstream: {sorted(upstream)})"
    )
    t = lap("read_marks", t)

    at_keys = {loc: flag for (ks, loc), flag in locations.items() if ks == tuple(keys)}
    off_level = {(ks, loc): flag for (ks, loc), flag in locations.items() if ks != tuple(keys)}
    in_universe = {row for row in universe if None not in row}

    # Rows: the universe, then present combinations outside it, then
    # combinations an Expected mark adds.
    rows: list[tuple[tuple[str, ...], tuple]] = [(tuple(keys), row) for row in universe]
    extra_present = sorted(
        (loc for loc in at_keys if loc not in in_universe),
        key=lambda loc: _sort_key(keys, loc, order),
    )
    rows += [(tuple(keys), loc) for loc in extra_present]
    covered = set(in_universe) | set(extra_present)
    for m in own + inherited:
        if not m.expected or not set(m.keys) <= set(keys):
            continue
        if any(_covers(keys, row, m.keys) for row in universe) or any(
            _covers(keys, loc, m.keys) for loc in covered
        ):
            continue
        row = tuple(m.keys.get(k) for k in keys)
        rows.append((tuple(keys), row))
        covered.add(row)
    rows += [
        (ks, loc)
        for ks, loc in sorted(off_level, key=lambda x: (len(x[0]), x[1]))
    ]

    db_combos = {tuple(str(v) for v in c) for c in db.distinct_schema_combinations(keys)}
    combos = [
        {k: v for k, v in zip(ks, row) if v is not None} for ks, row in rows
    ]
    overrides = governing_overrides(combos, schema_keys, db)
    t = lap("match_rows", t)

    rare = _rare_values(duck, name, keys, settings_all, at_keys)
    t = lap("rare_values", t)

    from scidb.provenance.outcomes import latest_outcomes, outcome_for

    outcomes = latest_outcomes(duck, schema_keys, name)
    t = lap("read_outcomes", t)

    for (ks, row), combo, override in zip(rows, combos, overrides):
        mark = most_specific_match(own, combo)
        inherited_from = None
        if mark is None:
            mark = most_specific_match(inherited, combo)
            if mark is not None:
                inherited_from = mark.variable
        on_level = ks == tuple(keys)
        concrete = None not in row
        present = concrete and ((row in at_keys) if on_level else True)
        records_excluded = (
            at_keys.get(row, False) if on_level else off_level.get((ks, row), False)
        )
        generated, gen_source = _generated(on_level, row, in_universe, keys, settings_all)
        expected = mark.expected if mark is not None else generated
        source = SOURCE_MARK if mark is not None else gen_source

        if override is not None and not override.included:
            status, source = EXCLUDED, SOURCE_EXCLUSION
        elif present and records_excluded:
            status, source = EXCLUDED, SOURCE_RECORD_EXCLUDED
        elif expected:
            status = PRESENT if present else MISSING_UNEXPECTED
        else:
            status = PRESENT_UNEXPECTED if present else MISSING_EXPECTED

        values = {k: v for k, v in zip(ks, row)}
        why = _why(
            status, source, values, keys, ks, mark, inherited_from, override,
            settings_all, in_db=(row in db_combos) if on_level and concrete else None,
        )
        hint = _rare_hint(values, rare) if status == MISSING_UNEXPECTED else ""
        last_run = (
            outcome_for(outcomes, combo)
            if outcomes and status in (MISSING_UNEXPECTED, MISSING_EXPECTED)
            else None
        )
        if last_run is not None:
            hint += f" Last run: {last_run.describe()}."
        result.rows.append(
            ExpectationRow(
                values=values,
                status=status,
                expected=expected,
                present=present,
                why=why + hint,
                source=source,
                mark=mark,
                inherited_from=inherited_from,
                exclusion_reason=override.reason if override is not None and not override.included else None,
                last_run=last_run,
            )
        )
        result.counts[status] += 1

    result.columns = [k for k in schema_keys if any(k in r.values for r in result.rows) or k in keys]
    for (key, value), (types, total) in rare.items():
        result.notes.append(
            f"{key} {value} appears in only {_preview(types)} ({len(types)} of {total} "
            f"variables with a {key}); a misspelling, or data still to be loaded?"
        )
    lap("statuses", t)

    Log.info(
        f"expectation {name}: keys={keys} universe={result.universe_size} rows={len(result.rows)} "
        + " ".join(f"{s}={n}" for s, n in result.counts.items())
        + f" in {(time.perf_counter() - t0) * 1000:.1f} ms ("
        + ", ".join(f"{s} {ms:.1f}" for s, ms in timings.items())
        + ")"
    )
    return result


def _variable_locations(duck, name: str, schema_keys: list[str]):
    """``{(keyset, values): all_records_excluded}`` for every location the
    variable has a record at, and how many records have no location."""
    cols = ", ".join(f's."{k}"' for k in schema_keys)
    rows = duck.fetchall(
        f"SELECT {cols}, bool_and(coalesce(r.excluded, FALSE)) "
        f"FROM _record r JOIN _schema s ON s.schema_id = r.schema_id "
        f"WHERE r.type = ? GROUP BY {cols}",
        [name],
    )
    locations = {}
    for row in rows:
        pairs = [(k, str(v)) for k, v in zip(schema_keys, row[:-1]) if v is not None]
        ks = tuple(k for k, _ in pairs)
        loc = tuple(v for _, v in pairs)
        locations[(ks, loc)] = bool(row[-1])
    keyless = duck.fetchone(
        "SELECT count(*) FROM _record r WHERE r.type = ? AND NOT EXISTS "
        "(SELECT 1 FROM _schema s WHERE s.schema_id = r.schema_id)",
        [name],
    )[0]
    return locations, int(keyless)


def _variable_keys(duck, name, schema_keys, locations, keyless) -> tuple[list[str], str]:
    """The key set most of the variable's locations use (ties: deeper)."""
    if locations:
        counts = Counter(ks for ks, _ in locations)
        best = max(counts, key=lambda ks: (counts[ks], len(ks)))
        total = sum(counts.values())
        where = "/".join(best)
        if len(counts) == 1:
            rule = f"every location of {name} is at {where}"
        else:
            rule = f"{counts[best]} of {total} locations of {name} are at {where}"
        return list(best), rule
    if keyless:
        return [], f"{name} has {keyless} record(s), none at a schema location"
    row = duck.fetchone("SELECT schema_level FROM _variables WHERE variable_name = ?", [name])
    if row is None:
        raise ValueError(f"No variable named {name!r} (no records, not registered).")
    level = row[0]
    if level not in schema_keys:
        raise ValueError(
            f"{name} has no records and is registered at {level!r}, which is not a "
            f"schema key ({schema_keys})."
        )
    keys = schema_keys[: schema_keys.index(level) + 1]
    return keys, f"{name} has no records yet; registered at {level}"


def _universe(db, keys, settings, order, max_combinations) -> list[tuple]:
    """The expected combinations, in product order; None = every value."""
    partials: list[tuple] = [()]
    for i, key in enumerate(keys):
        s = settings[key]
        nxt: list[tuple] = []
        if s.sharing == SHARED:
            values = s.values if s.values is not None else list(order[key])
            for p in partials:
                if None in p:
                    nxt.append((*p, None))
                else:
                    nxt.extend((*p, v) for v in values)
        else:
            children: dict[tuple, list[str]] = {}
            for combo in db.distinct_schema_combinations(keys[: i + 1]):
                combo = tuple(str(v) for v in combo)
                children.setdefault(combo[:i], []).append(combo[i])
            rank = order[key]
            for p in partials:
                kids = None if None in p else children.get(p)
                if kids:
                    nxt.extend((*p, v) for v in sorted(kids, key=lambda v: rank.get(v, len(rank))))
                else:
                    nxt.append((*p, None))
        if len(nxt) > max_combinations:
            msg = (
                f"The expected locations of {keys[: i + 1]} would number {len(nxt):,}, above "
                f"the limit of {max_combinations:,}. Make a key vary per parent, or "
                f"declare fewer values."
            )
            Log.warn(f"expectation: refused - {msg}")
            raise ValueError(msg)
        Log.debug(
            f"expectation: universe after {key} ({s.sharing}"
            f"{', declared' if s.values is not None else ''}): {len(nxt)} combination(s)"
        )
        partials = nxt
    return partials


def _generated(on_level, row, in_universe, keys, settings) -> tuple[bool, str]:
    """The expectation before marks, and where it came from."""
    if not on_level:
        return False, SOURCE_LEVEL
    if None in row or row in in_universe:
        return True, SOURCE_RULE
    # Present outside the universe: only a declared value can put it there.
    return False, SOURCE_DECLARED_VALUES


def _rare_values(duck, name, keys, settings, at_keys) -> dict:
    """``{(key, value): (variables having it, variables with the key)}`` for
    each undeclared shared key's values this variable has no location with,
    found in fewer than half of the variables that use the key."""
    from scidb.provenance.graph import INTERNAL_RECORD_TYPES

    own_values = {k: {loc[i] for loc in at_keys} for i, k in enumerate(keys)}
    out = {}
    placeholders = ", ".join("?" for _ in INTERNAL_RECORD_TYPES)
    for key in keys:
        s = settings[key]
        if s.sharing != SHARED or s.values is not None:
            continue
        rows = duck.fetchall(
            f'SELECT s."{key}", list(DISTINCT r.type) '
            f"FROM _record r JOIN _schema s ON s.schema_id = r.schema_id "
            f'WHERE s."{key}" IS NOT NULL AND r.type NOT IN ({placeholders}) '
            f'GROUP BY s."{key}"',
            list(INTERNAL_RECORD_TYPES),
        )
        total = len({t for _, types in rows for t in types})
        for value, types in rows:
            value, types = str(value), sorted(types)
            if value in own_values[key] or name in types:
                continue
            if len(types) * 2 < total:
                out[(key, value)] = (list(types), total)
    if out:
        Log.debug(f"expectation {name}: rare values {sorted(out)}")
    return out


def _rare_hint(values: dict, rare: dict) -> str:
    for (key, value), (types, _total) in rare.items():
        if values.get(key) == value:
            return f" ({key} {value} appears only in {_preview(types)}.)"
    return ""


def _why(status, source, values, keys, ks, mark, inherited_from, override, settings, in_db) -> str:
    """The one sentence that says which quadrant a row is in and why."""
    label = STATUS_LABELS[status]
    if source == SOURCE_EXCLUSION:
        return f"{label}: {override.reason}" if override.reason else f"{label}."
    if source == SOURCE_RECORD_EXCLUDED:
        return f"{label}: every record here is marked excluded."
    if source == SOURCE_MARK:
        said = mark.describe(inherited=inherited_from is not None)
        if status == MISSING_UNEXPECTED:
            return f"{label}: {said}, but nothing has been saved here."
        if status == PRESENT_UNEXPECTED:
            return f"{label}: {said}, but data exists here."
        return f"{label}: {said}."
    if source == SOURCE_LEVEL:
        return (
            f"{label}: saved at {'/'.join(ks)}, while this variable is saved at "
            f"{'/'.join(keys)}."
        )
    if source == SOURCE_DECLARED_VALUES:
        outside = [
            (k, v) for k, v in values.items()
            if settings[k].values is not None and v not in settings[k].values
        ]
        if outside:
            k, v = outside[0]
            return (
                f"{label}: {k} {v} is not one of {k}'s declared values "
                f"({_preview(settings[k].values)})."
            )
        return f"{label}: outside the expected combinations."
    # SOURCE_RULE
    if status == PRESENT:
        return f"{label}."
    wild = [k for k, v in values.items() if v is None]
    if wild:
        parent = _where({k: v for k, v in values.items() if v is not None})
        return (
            f"{label}: nothing in the database has any {wild[0]} under {parent or 'this location'}."
        )
    if in_db:
        return f"{label}: other variables have data here; this one has none."
    shared = [k for k in keys if settings[k].sharing == SHARED]
    declared = [k for k in shared if settings[k].values is not None]
    why = (
        f"every {' x '.join(shared)} combination is expected"
        + (f" ({', '.join(k + ' declared' for k in declared)})" if declared else "")
    )
    return f"{label}: {why}, but nothing has been saved at this combination."


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _covers(keys, row, mark_keys) -> bool:
    """Does a row (None = every value) cover a mark's keys?"""
    for k, v in mark_keys.items():
        cell = row[keys.index(k)]
        if cell is not None and cell != v:
            return False
    return True


def _sort_key(keys, loc, order):
    return tuple(order[k].get(v, len(order[k])) for k, v in zip(keys, loc))


def _cell(values: dict, key: str) -> str:
    if key not in values:
        return ""
    v = values[key]
    return ALL_LEVELS if v is None else v


def _where(keys: dict) -> str:
    return ", ".join(f"{k}={v}" for k, v in keys.items())


def _preview(values) -> str:
    if values is None:
        return "(every value)"
    values = list(values)
    shown = ", ".join(str(v) for v in values[:_PREVIEW])
    return shown + (f", ... ({len(values)})" if len(values) > _PREVIEW else "")


def _dedupe(values: list[str]) -> list[str]:
    seen, out = set(), []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _name(variable) -> str:
    return getattr(variable, "__name__", variable)


def _date(at) -> str:
    return at.date().isoformat() if hasattr(at, "date") else ""


def _iso(at) -> str | None:
    return at.isoformat() if hasattr(at, "isoformat") else (None if at is None else str(at))


def _check_writable(db, what: str) -> None:
    if getattr(db, "read_only", False):
        raise ValueError(f"Cannot {what}: the database is open read-only.")


def _quoted(column: str) -> str:
    return '"' + column.replace('"', '""') + '"'
