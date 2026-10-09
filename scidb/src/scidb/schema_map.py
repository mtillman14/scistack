"""
The schema key map: importing a project into a DIFFERENT schema.

Portability Stage 6 (``docs/claude/portability.md`` "Schema on import").
Import proposes the exporter's schema; the user may enter their own, and the
exporter's is then ignored. Code never sees the schema; everything else that
NAMES a schema key goes through ONE map, exporter key -> recipient key or
``None`` (no counterpart). Names present in both schemas map to themselves;
the user overrides the rest.

This module owns the map and the renaming of each SHAPE a key can appear in
-- a stated iteration level, a location selection (through
``scifor.locations.LocationFilter``, the owner of that shape), a PathInput
template's ``{placeholders}``, a table keyed by schema key. Each bundle
section applies it to its own data and reports what it dropped or flagged
(:class:`MapReport`), so there is no second list of "places that mention
schema keys" anywhere.

Values are never translated: a location selection's levels (``S02``) and a
where filter belong to the exporter's DATASET, so a selection is kept and
FLAGGED for review whenever the map changes anything.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from scifor.locations import LocationFilter
from scistacklog import Log

_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass
class MapReport:
    """What applying the map did, for one section's import report."""

    #: ``[(where, key)]``: a reference to a key with no counterpart, removed.
    dropped: list[tuple[str, str]] = field(default_factory=list)
    #: ``[(where, why)]``: kept, but worth the user's review.
    flagged: list[tuple[str, str]] = field(default_factory=list)

    def merge(self, other: "MapReport") -> None:
        self.dropped += other.dropped
        self.flagged += other.flagged

    def to_dict(self) -> dict:
        return {
            "dropped": [list(x) for x in self.dropped],
            "flagged": [list(x) for x in self.flagged],
        }


@dataclass(frozen=True)
class KeyMap:
    """Exporter schema key -> recipient schema key, or ``None``."""

    mapping: tuple[tuple[str, "str | None"], ...] = ()

    @classmethod
    def auto(
        cls,
        exporter_keys: list[str],
        recipient_keys: list[str],
        overrides: "dict[str, str | None] | None" = None,
    ) -> "KeyMap":
        """Match keys by name, then apply *overrides* (exporter -> recipient
        or ``None``). An override naming a key the recipient does not have is
        refused: the map must land on the recipient's schema."""
        recipient = set(recipient_keys)
        mapping = {k: (k if k in recipient else None) for k in exporter_keys}
        for old, new in (overrides or {}).items():
            if old not in mapping:
                raise ValueError(
                    f"key map: {old!r} is not one of the exporter's schema keys "
                    f"{exporter_keys}"
                )
            if new is not None and new not in recipient:
                raise ValueError(
                    f"key map: {new!r} is not one of the recipient's schema keys "
                    f"{recipient_keys}"
                )
            mapping[old] = new
        targets = [v for v in mapping.values() if v is not None]
        if len(targets) != len(set(targets)):
            raise ValueError(f"key map: two exporter keys map to one recipient key: {mapping}")
        km = cls(tuple(mapping.items()))
        Log.info(f"[schema_map] key map {km.describe()}")
        return km

    def as_dict(self) -> "dict[str, str | None]":
        return dict(self.mapping)

    @property
    def is_identity(self) -> bool:
        return all(old == new for old, new in self.mapping)

    def describe(self) -> str:
        return ", ".join(f"{old}->{new if new is not None else '(none)'}" for old, new in self.mapping) or "(empty)"

    def key(self, name: str) -> "str | None":
        """*name* mapped; a name that is not an exporter schema key (a
        variable column, a synthetic factor) is returned unchanged."""
        d = self.as_dict()
        return d[name] if name in d else name

    def _renames(self) -> dict[str, str]:
        return {old: new for old, new in self.mapping if new is not None and new != old}

    def _drops(self) -> set[str]:
        return {old for old, new in self.mapping if new is None}

    # ---- shapes ---------------------------------------------------------

    def level(self, stated: Any, where: str, report: MapReport) -> Any:
        """A stated iteration level (``None`` / ``[]`` / ``[keys]``): keys
        renamed, unmapped ones dropped. Dropping the LAST key leaves the node
        unset (``None``), so the schema-level default rule decides it there,
        rather than turning "per trial" into "one call"."""
        if stated is None or self.is_identity:
            return stated
        keys = [stated] if isinstance(stated, str) else list(stated)
        if not keys:
            return keys
        out = []
        for k in keys:
            new = self.key(k)
            if new is None:
                report.dropped.append((where, k))
            else:
                out.append(new)
        return out or None

    def locations(self, raw: Any, where: str, report: MapReport) -> Any:
        """A location selection (the ``locations=`` / ``schemaSelection``
        mapping): renamed and unmapped parts removed through
        ``LocationFilter``; FLAGGED whenever the map touched it, because its
        levels belong to the exporter's dataset."""
        if not raw or self.is_identity:
            return raw

        lf = LocationFilter.of(raw)
        touched = lf.keys() & (set(self._renames()) | self._drops())
        if not touched:
            return raw
        for k in sorted(lf.keys() & self._drops()):
            report.dropped.append((where, k))
        mapped = lf.without_keys(self._drops()).renamed(self._renames())
        report.flagged.append(
            (where, f"location selection uses keys {sorted(touched)}; its levels are the exporter's")
        )
        return mapped.to_dict()

    def template(self, template: str, where: str, report: MapReport) -> str:
        """A PathInput template: ``{placeholders}`` renamed; an unmapped one
        is left in place and FLAGGED (the recipient points the template at
        their own data anyway)."""
        if not template or self.is_identity:
            return template
        renames, drops = self._renames(), self._drops()

        def _sub(m: re.Match) -> str:
            name = m.group(1)
            if name in drops:
                report.flagged.append((where, f"template placeholder {{{name}}} has no counterpart"))
                return m.group(0)
            return "{" + renames.get(name, name) + "}"

        return _PLACEHOLDER.sub(_sub, template)

    def table(self, table: "dict | None", where: str, report: MapReport) -> "dict | None":
        """A table keyed by schema key (``[schema_keys]`` level order,
        ``[aliases]``/``[colors]`` entries): keys renamed, unmapped entries
        dropped; entries keyed by anything else are kept."""
        if not table or self.is_identity:
            return table
        out = {}
        for k, v in table.items():
            new = self.key(k)
            if new is None:
                report.dropped.append((where, k))
                continue
            out[new] = v
        return out

    def exact_strings(self, value: Any, where: str, report: MapReport) -> Any:
        """Every string (and dict key) EQUAL to an exporter key, renamed --
        for a structure whose schema-key fields are too many to list (a plot
        spec). FLAGGED whenever it changed or met a key with no counterpart,
        since a factor name that merely equals a key cannot be told apart."""
        if self.is_identity:
            return value
        renames, drops = self._renames(), self._drops()
        hits: set[str] = set()

        def walk(v):
            if isinstance(v, str):
                if v in renames or v in drops:
                    hits.add(v)
                return renames.get(v, v)
            if isinstance(v, list):
                return [walk(x) for x in v]
            if isinstance(v, dict):
                return {walk(k) if isinstance(k, str) else k: walk(x) for k, x in v.items()}
            return v

        out = walk(value)
        if hits:
            report.flagged.append((where, f"mentions schema keys {sorted(hits)}; review it"))
        return out
