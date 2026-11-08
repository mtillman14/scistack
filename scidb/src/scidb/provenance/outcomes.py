"""Why a location got no output in a run (``_run_outcome``).

The provenance graph records what a run produced. It records nothing for a
location where the function failed or found no input data, and a run that
saved nothing writes no ``_run`` row at all. This module keeps those
outcomes, one row per location, so "missing" can later say why:

    FilteredEMG failed here when run on 2026-11-03 14:02: Error in your
    function filter_emg: IndexError: index 3 is out of bounds

**Writer:** ``scidb.run.foreach`` collects scifor's per-location
``combo_skip`` events (metadata + the small failure form, which carries the
blame category) and calls :func:`record_outcomes` once per run, after the
loop, even when the run stopped or was cancelled. A failure to write is
logged and never fails the run.

**Readers:** ``scidb.provenance.expectation`` appends :meth:`Outcome.describe`
to a missing row's why; ``coverage.follow`` therefore shows it too.

**Rules:** the newest row per (function, location) is current; a location
that has data is never explained by an outcome (readers only ask for missing
rows). An outcome explains a row when every key it names equals the row's
(``exclusions.most_specific_match``, the shared rule): a function run per
subject that failed at S02 explains every S02 trial it should have written.
Blame wording is ``scistacklog.journal.blame_headline``'s.

Not covered yet: MATLAB runs (their loop runs in MATLAB; the failures reach
the GUI through the ``.done`` marker, not through this writer), and
locations excluded at run time.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from scidb.model.log import Log
from scidb.store.schema_values import schema_str
from scidb.store.tables import RUN_OUTCOME

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

TABLE = RUN_OUTCOME.name

FAILED = "failed"
NO_DATA = "no_data"
OUTCOMES = (FAILED, NO_DATA)

#: Rows per INSERT statement.
_CHUNK = 500


@dataclass(frozen=True)
class Outcome:
    """One location where a run produced nothing, and why."""

    function_name: str
    output_types: tuple[str, ...]
    keys: dict[str, str]
    outcome: str
    blame: str | None
    probable: bool
    reason: str | None
    recorded_at: Any

    @property
    def changed_at(self):
        """For ``exclusions.most_specific_match``."""
        return self.recorded_at

    def describe(self) -> str:
        """The one sentence: who ran, when, and what happened."""
        when = (
            self.recorded_at.strftime("%Y-%m-%d %H:%M")
            if hasattr(self.recorded_at, "strftime") else str(self.recorded_at)
        )
        if self.outcome == NO_DATA:
            return f"{self.function_name} found no input data here when run on {when}"
        from scistacklog.journal import blame_headline

        headline = blame_headline(self.blame, self.probable, self.function_name)
        return f"{self.function_name} failed here when run on {when}: {headline}: {self.reason}"

    def to_dict(self) -> dict:
        return {
            "function_name": self.function_name,
            "output_types": list(self.output_types),
            "keys": dict(self.keys),
            "outcome": self.outcome,
            "blame": self.blame,
            "probable": self.probable,
            "reason": self.reason,
            "recorded_at": (
                self.recorded_at.isoformat()
                if hasattr(self.recorded_at, "isoformat") else self.recorded_at
            ),
            "description": self.describe(),
        }


@dataclass
class OutcomeCollector:
    """Gathers one run's ``combo_skip`` events (``scidb.run.foreach``)."""

    events: list[dict] = field(default_factory=list)

    def observe(self, info: dict) -> None:
        if info.get("event") == "combo_skip" and info.get("failure"):
            self.events.append(
                {"metadata": dict(info.get("metadata") or {}), "failure": dict(info["failure"])}
            )


def record_outcomes(
    db: DatabaseManager, function_name: str, output_types: list[str], events: list[dict]
) -> int:
    """Store one run's failed / no-data locations. Returns the rows written.

    *events*: ``{metadata, failure}`` per location, as ``combo_skip`` carries
    them. Metadata keys that are not schema keys are dropped (an EachOf or a
    ColName coordinate is not a location); an event with no schema key left is
    skipped. Nothing is written on a read-only database.
    """
    if not events:
        return 0
    t0 = time.perf_counter()
    if getattr(db, "read_only", False):
        Log.debug(f"[outcomes] {function_name}: read-only database; {len(events)} outcome(s) not stored")
        return 0
    schema_keys = list(db.dataset_schema_keys)
    at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for e in events:
        meta, failure = e["metadata"], e["failure"]
        keys = {k: schema_str(meta[k]) for k in schema_keys if meta.get(k) is not None}
        if not keys:
            continue
        no_data = bool(failure.get("no_data"))
        rows.append([
            function_name,
            list(output_types),
            *[keys.get(k) for k in schema_keys],
            NO_DATA if no_data else FAILED,
            None if no_data else failure.get("blame"),
            None if no_data else bool(failure.get("probable")),
            None if no_data else str(failure.get("reason") or "")[:2000],
            at,
        ])
    if not rows:
        Log.debug(f"[outcomes] {function_name}: {len(events)} event(s), none at a schema location")
        return 0
    cols = ["function_name", "output_types", *schema_keys, "outcome", "blame", "probable", "reason", "recorded_at"]
    col_sql = ", ".join('"' + c.replace('"', '""') + '"' for c in cols)
    one = "(" + ", ".join("?" for _ in cols) + ")"
    for i in range(0, len(rows), _CHUNK):
        chunk = rows[i:i + _CHUNK]
        db._duck.execute(
            f"INSERT INTO {TABLE} ({col_sql}) VALUES " + ", ".join(one for _ in chunk),
            [v for row in chunk for v in row],
        )
    n_failed = sum(1 for r in rows if r[len(schema_keys) + 2] == FAILED)
    Log.info(
        f"[outcomes] {function_name} -> {output_types}: stored {len(rows)} location(s) "
        f"({n_failed} failed, {len(rows) - n_failed} no data) in "
        f"{(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return len(rows)


def latest_outcomes(duck, schema_keys: list[str], variable: str) -> list[Outcome]:
    """The current outcome per (function, location) of every run that listed
    *variable* among its outputs. ``[]`` before the table exists."""
    if not duck.table_exists(TABLE):
        return []
    key_cols = ", ".join(f'"{k}"' for k in schema_keys)
    rows = duck.fetchall(
        f"""
        SELECT function_name, output_types, {key_cols}, outcome, blame, probable, reason, recorded_at
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY function_name, {key_cols}
                ORDER BY recorded_at DESC, rowid DESC
            ) AS rn
            FROM {TABLE}
            WHERE list_contains(output_types, ?)
        ) WHERE rn = 1
        """,
        [variable],
    )
    n = len(schema_keys)
    out = []
    for row in rows:
        keys = {k: str(v) for k, v in zip(schema_keys, row[2:2 + n]) if v is not None}
        outcome, blame, probable, reason, at = row[2 + n:]
        out.append(Outcome(
            function_name=row[0],
            output_types=tuple(row[1] or ()),
            keys=keys,
            outcome=outcome,
            blame=blame,
            probable=bool(probable),
            reason=reason,
            recorded_at=at,
        ))
    return out


def outcome_for(outcomes: list[Outcome], combo: dict) -> Outcome | None:
    """The outcome that explains *combo* (the shared most-specific rule)."""
    from scidb.store.exclusions import most_specific_match

    return most_specific_match(outcomes, combo)
