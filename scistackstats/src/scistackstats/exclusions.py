"""What a test left out, and the one wording of it (Stage A).

csv-stats records every row or subject it could not use in
``n.excluded_detail``: a paired test can't use a subject missing one of the
two levels; a repeated-measures test can't use one missing any within level
(``core._Prepared.complete_subjects``). Until 2026-10-19 that record reached
only a truncated table note ("… dropped: SS01, …" — at most 10 names), the
n column showed the n USED with no denominator, and scidb.log said nothing.

User rule (2026-10-19): data is never dropped silently. This module is the
one owner of reading that record and saying it; ``run`` attaches the result
to each Comparison / OmnibusResult and logs it, ``format`` puts the sentence
in the table notes and "used of offered" in the n column.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: csv-stats warning codes this module restates in full. ``format`` skips the
#: engine's own (truncated) message for these, so a note is never doubled.
RESTATED_CODES = frozenset({"subjects_dropped_incomplete", "rows_dropped_missing"})


@dataclass
class Exclusions:
    #: What the test was handed: units (with a subject) or rows.
    offered: int
    #: What it used.
    used: int
    #: "subjects" or "rows" — what ``offered`` / ``used`` count.
    counts: str
    #: ``[{"unit": name, "reason": why}]``, every one, in csv-stats' order.
    units: list[dict[str, str]] = field(default_factory=list)
    #: ``[{"rows": k, "reason": why}]`` for row-wise drops (missing values).
    rows: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    def sentence(self) -> str:
        """``"2 of 17 subjects excluded: SS05 (missing a within-subject level), …"``."""
        parts = []
        if self.units:
            named = ", ".join(f"{u['unit']} ({u['reason']})" for u in self.units)
            parts.append(f"{len(self.units)} of {self.offered} subjects excluded: {named}")
        for r in self.rows:
            parts.append(f"{r['rows']} row(s) excluded ({r['reason']})")
        return "; ".join(parts)

    def n_text(self) -> str:
        """``"15 of 17"`` for the n column."""
        return f"{self.used} of {self.offered}"


def from_record(n: dict[str, Any] | None, offered: int, counts: str) -> Exclusions | None:
    """The exclusions in a csv-stats core record's ``n``, or None when the
    test used everything it was handed."""
    if not n:
        return None
    detail = list(n.get("excluded_detail") or [])
    if not detail:
        return None
    units = [{"unit": str(d["subject"]), "reason": str(d.get("reason", ""))} for d in detail if "subject" in d]
    rows = [{"rows": int(d["rows"]), "reason": str(d.get("reason", ""))} for d in detail if "rows" in d]
    used_key = "subjects" if counts == "subjects" else "total"
    used = n.get(used_key)
    if used is None:
        used = offered - (len(units) if counts == "subjects" else sum(r["rows"] for r in rows))
    return Exclusions(offered=int(offered), used=int(used), counts=counts, units=units, rows=rows)
