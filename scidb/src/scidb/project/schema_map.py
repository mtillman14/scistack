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

The same map renames keys IN PLACE when a project's own keys change
(``scidb.project.key_rename``, ``docs/claude/changing-schema-keys.md``). There a
deleted key is *retired*, not unmapped: every reference to it is KEPT and
FLAGGED (never drop data silently), where an import drops it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Sequence

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
    #: Keys that are no longer schema keys but whose references are KEPT and
    #: FLAGGED rather than dropped (an in-place change, :meth:`renaming`).
    retired: tuple[str, ...] = ()
    #: True for a project's own key change (:meth:`renaming`), False for an
    #: import. Only the wording of what is flagged differs.
    in_place: bool = False

    @classmethod
    def renaming(
        cls, renames: "dict[str, str] | None", retired: "Sequence[str]" = ()
    ) -> "KeyMap":
        """The map of an in-place key change: *renames* (old -> new) applied,
        *retired* (deleted) keys kept and flagged wherever they are named."""
        renames = {o: n for o, n in (renames or {}).items() if o != n}
        km = cls(
            tuple(renames.items()),
            tuple(k for k in retired if k not in renames),
            in_place=True,
        )
        Log.info(f"[schema_map] in-place key map {km.describe()}")
        return km

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
        """Nothing renamed, dropped or retired: every shape returns its input."""
        return not self.retired and all(old == new for old, new in self.mapping)

    def describe(self) -> str:
        parts = [f"{old}->{new if new is not None else '(none)'}" for old, new in self.mapping]
        parts += [f"{k} (retired)" for k in self.retired]
        return ", ".join(parts) or "(empty)"

    def flag_retired(self, names, where: str, report: MapReport) -> None:
        """Flag each retired key among *names* (kept as written)."""
        for name in sorted(set(names) & set(self.retired)):
            report.flagged.append((where, f"names {name!r}, which is no longer a schema key"))

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
        self.flag_retired(keys, where, report)
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
        ``LocationFilter``. On an import it is FLAGGED whenever the map
        touched it, because its levels belong to the exporter's dataset; in
        place (:meth:`renaming`) the levels are the project's own, so a rename
        is not worth a review."""
        if not raw or self.is_identity:
            return raw

        lf = LocationFilter.of(raw)
        self.flag_retired(lf.keys(), where, report)
        touched = lf.keys() & (set(self._renames()) | self._drops())
        if not touched:
            return raw
        for k in sorted(lf.keys() & self._drops()):
            report.dropped.append((where, k))
        mapped = lf.without_keys(self._drops()).renamed(self._renames())
        if not self.in_place:
            report.flagged.append(
                (where, f"location selection uses keys {sorted(touched)}; its levels are the exporter's")
            )
        return mapped.to_dict()

    def template(self, template: str, where: str, report: MapReport) -> str:
        """A PathInput template: ``{placeholders}`` renamed; an unmapped or
        retired one is left in place and FLAGGED (the recipient points the
        template at their own data anyway)."""
        if not template or self.is_identity:
            return template
        renames, drops = self._renames(), self._drops()
        self.flag_retired(_PLACEHOLDER.findall(template), where, report)

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
        dropped, retired ones kept and flagged; entries keyed by anything
        else are kept."""
        if not table or self.is_identity:
            return table
        self.flag_retired(table, where, report)
        out = {}
        for k, v in table.items():
            new = self.key(k)
            if new is None:
                report.dropped.append((where, k))
                continue
            out[new] = v
        return out

    def config_tables(self, section: dict, report: MapReport) -> dict:
        """*section* (a parsed ``scistack.toml``) with every table keyed by
        schema key remapped through :meth:`table`. The ONE list of those
        tables, :data:`SCHEMA_KEYED_CONFIG_TABLES`: a bundle import and an
        in-place change both read it."""
        new = dict(section)
        for name in SCHEMA_KEYED_CONFIG_TABLES:
            if isinstance(new.get(name), dict):
                new[name] = self.table(new[name], f"scistack.toml [{name}]", report)
        return new

    def path_input_arm(self, arm: dict, where: str, report: MapReport) -> dict:
        """One PathInput arm (``{"template", "root_folder", "aliases",
        "key_regex", ...}``): the template's placeholders renamed, and its
        ``aliases`` / ``key_regex`` (both keyed by placeholder) following
        them. An entry under a key with no counterpart stays under its old
        name, as :meth:`template` leaves that placeholder in place, so no
        alias or pattern is orphaned or dropped."""
        if self.is_identity:
            return arm

        def keyed(table: "dict | None") -> dict:
            out = {}
            for key, value in (table or {}).items():
                new = self.key(key)
                out[new if new is not None else key] = value
            return out

        out = dict(arm)
        out["template"] = self.template(arm.get("template") or "", where, report)
        out["aliases"] = keyed(arm.get("aliases"))
        out["key_regex"] = keyed(arm.get("key_regex"))
        return out

    def key_list(self, keys: "list | None", where: str, report: MapReport) -> "list | None":
        """A plain list of key names (``[library] schema_keys``): renamed,
        unmapped ones dropped, retired ones kept and flagged."""
        if not keys or self.is_identity:
            return keys
        self.flag_retired(keys, where, report)
        out = []
        for k in keys:
            new = self.key(k)
            if new is None:
                report.dropped.append((where, k))
            else:
                out.append(new)
        return out

    def where_filters(self, rows: "list | None", where: str, report: MapReport) -> "list | None":
        """A node's ``whereFilters`` (``[{variable, column, op, value}]``): a
        ``column`` that names a key (a variable's key column) renamed. The
        variable, op and value are data, never keys."""
        if not rows or self.is_identity:
            return rows
        out = []
        for row in rows:
            column = row.get("column") if isinstance(row, dict) else None
            if not isinstance(column, str) or not column:
                out.append(row)
                continue
            self.flag_retired([column], where, report)
            new = self.key(column)
            if new is None:
                report.dropped.append((where, column))
                continue
            out.append({**row, "column": new} if new != column else row)
        return out

    def exact_strings(self, value: Any, where: str, report: MapReport) -> Any:
        """Every string (and dict key) EQUAL to an exporter key, renamed --
        for a structure whose schema-key fields are too many to list (a plot
        spec). FLAGGED whenever it changed or met a key with no counterpart
        (or a retired one), since a factor name that merely equals a key
        cannot be told apart."""
        if self.is_identity:
            return value
        renames, drops, retired = self._renames(), self._drops(), set(self.retired)
        hits: set[str] = set()
        retired_hits: set[str] = set()

        def walk(v):
            if isinstance(v, str):
                if v in renames or v in drops:
                    hits.add(v)
                elif v in retired:
                    retired_hits.add(v)
                return renames.get(v, v)
            if isinstance(v, list):
                return [walk(x) for x in v]
            if isinstance(v, dict):
                return {walk(k) if isinstance(k, str) else k: walk(x) for k, x in v.items()}
            return v

        out = walk(value)
        if hits:
            report.flagged.append((where, f"mentions schema keys {sorted(hits)}; review it"))
        self.flag_retired(retired_hits, where, report)
        return out


#: The ``scistack.toml`` tables keyed by schema-key name (value order,
#: display names, colours). Read by :meth:`KeyMap.config_tables`.
SCHEMA_KEYED_CONFIG_TABLES = ("schema_keys", "aliases", "colors")
