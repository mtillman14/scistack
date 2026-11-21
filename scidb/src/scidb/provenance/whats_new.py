"""What did this run add or change? (the run report's "What's new", GitHub #28)

Facts, never stored flags:

* **New** = an (output variable, location) whose *first ever* record was
  saved by this run: the first ``_record_save`` at that location falls
  inside the run. So the emphasis appears exactly once: re-running later
  shows no new locations.
* **Saved again** = a location the run saved that already had a record,
  with whether the value changed (the run's record there is a different
  record id, i.e. different content or lineage, than the one before).
* **Dataset composition, before → after**, per output variable: the
  location count, and per key the number of levels, and per parent the
  child counts that changed ("subject 1: 10 → 20 trials").

New locations are written compactly by ``inventory.describe_locations``
(the collapsed-block rule's owner), so a block reads ``subject=1,
trial=11–20``; the full list comes with them.

Times: ``_record_save.timestamp`` is local-clock ISO text, the run's
``started_at`` naive UTC; the run's window is converted to local text once
(:func:`_local_window`).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from typing import TYPE_CHECKING, Any

from scidb.model.log import Log

if TYPE_CHECKING:
    from scidb.provenance.run_summary import RunSummary
    from scidb.store.database import DatabaseManager

#: New locations listed in full per variable (the compact lines are always whole).
LIST_MAX = 500
#: Parents whose child count changed, listed per key pair.
CHANGES_MAX = 20
#: Slack after the run's recorded duration (the summary is written after the save).
_SLACK = timedelta(seconds=1)


@dataclass
class VariableNews:
    variable: str
    keys: list[str]
    new_locations: list[dict] = field(default_factory=list)
    new_compact: list[str] = field(default_factory=list)
    resaved_changed: int = 0
    resaved_unchanged: int = 0
    locations_before: int = 0
    locations_after: int = 0
    #: {key: (levels before, levels after)}
    levels: dict[str, tuple[int, int]] = field(default_factory=dict)
    #: ["subject 1: 10 → 20 trials", ...]
    changes: list[str] = field(default_factory=list)

    def headline(self) -> str:
        """``RMS: 10 new locations — subject=1, trial=11–20``."""
        n = len(self.new_locations)
        if not n:
            return f"{self.variable}: no new locations"
        where = "; ".join(self.new_compact[:3]) + ("; …" if len(self.new_compact) > 3 else "")
        return f"{self.variable}: {n} new location{'' if n == 1 else 's'} — {where}"

    def to_dict(self) -> dict:
        return {
            "variable": self.variable,
            "keys": list(self.keys),
            "headline": self.headline(),
            "new_count": len(self.new_locations),
            "new_compact": list(self.new_compact),
            "new_locations": [dict(x) for x in self.new_locations[:LIST_MAX]],
            "new_truncated": len(self.new_locations) > LIST_MAX,
            "resaved_changed": self.resaved_changed,
            "resaved_unchanged": self.resaved_unchanged,
            "locations_before": self.locations_before,
            "locations_after": self.locations_after,
            "levels": {k: list(v) for k, v in self.levels.items()},
            "changes": list(self.changes),
        }


def _local_window(summary: "RunSummary") -> tuple[str, str] | None:
    start = summary.started_at
    if start is None or not hasattr(start, "replace"):
        return None
    local = start.replace(tzinfo=timezone.utc).astimezone().replace(tzinfo=None)
    end = local + timedelta(milliseconds=summary.duration_ms or 0) + _SLACK
    return local.isoformat(), end.isoformat()


def whats_new(db: "DatabaseManager", summary: "RunSummary") -> list[VariableNews]:
    """Per output variable of the run: what it added and changed."""
    from scidb.provenance.inventory import describe_locations
    from scidb.provenance.query import _schema_locations

    t0 = time.perf_counter()
    window = _local_window(summary)
    if window is None:
        return []
    start, end = window
    duck = db._duck
    schema_keys = list(db.dataset_schema_keys)
    out: list[VariableNews] = []
    for variable in summary.output_types:
        rows = duck.fetchall(
            "SELECT r.schema_id, r.record_id, MIN(rs.timestamp), MAX(rs.timestamp) "
            "FROM _record r JOIN _record_save rs ON rs.record_id = r.record_id "
            "WHERE r.type = ? AND COALESCE(r.excluded, FALSE) = FALSE "
            "GROUP BY r.schema_id, r.record_id",
            [variable],
        )
        # Records saved during the run (first or again): one query.
        during = {
            r[0] for r in duck.fetchall(
                "SELECT DISTINCT rs.record_id FROM _record_save rs "
                "JOIN _record r ON r.record_id = rs.record_id "
                "WHERE r.type = ? AND rs.timestamp >= ? AND rs.timestamp <= ?",
                [variable, start, end],
            )
        }
        first_at: dict[Any, str] = {}
        saved_in_run: dict[Any, set] = {}
        existed_before: dict[Any, set] = {}
        for sid, rid, first, _last in rows:
            if sid not in first_at or first < first_at[sid]:
                first_at[sid] = first
            if first < start:
                existed_before.setdefault(sid, set()).add(rid)
            if rid in during:
                saved_in_run.setdefault(sid, set()).add(rid)
        locs = _schema_locations(duck, list(first_at), schema_keys)
        news = VariableNews(variable=variable, keys=[])
        populated = [k for k in schema_keys if any(locs.get(s, {}).get(k) is not None for s in first_at)]
        news.keys = populated
        before = [s for s, at in first_at.items() if at < start]
        after = [s for s, at in first_at.items() if at <= end]
        new = [s for s, at in first_at.items() if start <= at <= end]
        news.locations_before, news.locations_after = len(before), len(after)
        news.new_locations = [
            {k: str(v) for k, v in locs.get(s, {}).items() if v is not None} for s in new
        ]
        news.new_locations.sort(key=lambda d: [d.get(k, "") for k in populated])
        for s, rids in saved_in_run.items():
            if s in new:
                continue
            if rids & existed_before.get(s, set()):
                news.resaved_unchanged += 1
            else:
                news.resaved_changed += 1
        loc_of = lambda sids: [{k: str(v) for k, v in locs.get(s, {}).items() if v is not None} for s in sids]  # noqa: E731
        news.new_compact = describe_locations(populated, news.new_locations, loc_of(after))
        _composition(news, loc_of(before), loc_of(after), populated)
        out.append(news)
        Log.info(
            f"[whats_new] run={summary.run_id} {news.headline()} "
            f"(locations {news.locations_before} -> {news.locations_after}, saved again "
            f"{news.resaved_changed} changed / {news.resaved_unchanged} unchanged)"
        )
    Log.debug(f"[whats_new] run={summary.run_id} in {(time.perf_counter() - t0) * 1000:.1f} ms")
    return out


def _composition(news: VariableNews, before: list[dict], after: list[dict], keys: list[str]) -> None:
    for k in keys:
        b = len({loc.get(k) for loc in before if loc.get(k) is not None})
        a = len({loc.get(k) for loc in after if loc.get(k) is not None})
        news.levels[k] = (b, a)
        if a != b:
            news.changes.append(f"{k}s: {b} → {a}")
    for parent, child in zip(keys, keys[1:]):
        def per_parent(locs):
            counts: dict[str, set] = {}
            for loc in locs:
                if loc.get(parent) is not None and loc.get(child) is not None:
                    counts.setdefault(loc[parent], set()).add(loc[child])
            return {p: len(c) for p, c in counts.items()}

        b, a = per_parent(before), per_parent(after)
        changed = [p for p in a if a[p] != b.get(p, 0)]
        for p in changed[:CHANGES_MAX]:
            news.changes.append(f"{parent} {p}: {b.get(p, 0)} → {a[p]} {child}s")
        if len(changed) > CHANGES_MAX:
            news.changes.append(f"… and {len(changed) - CHANGES_MAX} more {parent}s")
