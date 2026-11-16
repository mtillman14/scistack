"""
Hiding PathInput locations in an exported bundle (issue #16).

``ExportOptions.hide_path_locations`` asks for a bundle that keeps WHICH files a
PathInput names (its name, template shape, aliases, key_regex) and loses WHERE
they are on the exporter's machine (``root_folder``, an absolute template's
leading folders, the exporter's project folder, and any such folder quoted in
history text). PathInput identity is the name alone, so no record, invocation
or call id changes.

**One owner.** :class:`PathRedaction` is built by ``export_project`` when the
option is on (``ExportContext.redaction`` is ``None`` otherwise). Each section
applies it to its own data -- the same shape as ``schema_map.KeyMap`` on
import -- and the folders it removes are remembered as *needles*, so one text
pass can replace them wherever else they were quoted. A needle is reported by
the PathInput's NAME, never by the folder itself (the report is not allowed to
re-leak what it hides).

Code is never rewritten: a needle still present in a bundle file after every
section is built REFUSES the export (:func:`leftovers` finds them).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from scistacklog import Log

#: The label of the exporter's project folder.
PROJECT = "the project folder"

#: A folder shorter than this (separators ignored) is not a safe needle: "/" or
#: "." would shred every text it is replaced in.
_MIN_NEEDLE = 3


def _spellings(folder: str) -> list[str]:
    """Every way a folder is written in a bundle: as given, with ``/``, with
    ``\\``, and the JSON-escaped backslash form."""
    base = folder.rstrip("/\\")
    forward, backward = base.replace("\\", "/"), base.replace("/", "\\")
    return list(dict.fromkeys([base, forward, backward, backward.replace("\\", "\\\\")]))


def _sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


@dataclass
class RedactionReport:
    """What a hide-mode export did, for the CLI and the GUI to show."""

    #: ``[(where, PathInput name)]`` -- a location removed from a declaration.
    hidden: list = field(default_factory=list)
    #: ``{where: replacements}`` -- needles replaced in text.
    replaced: dict = field(default_factory=dict)
    #: ``[(where, label, count)]`` -- needles still present (the export refuses).
    remaining: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "hidden": [list(h) for h in self.hidden],
            "replaced": dict(self.replaced),
            "remaining": [list(r) for r in self.remaining],
        }


class PathRedaction:
    def __init__(self, project_root: "Path | str | None" = None) -> None:
        #: ``{spelling: label}``
        self._needles: dict[str, str] = {}
        #: ``{table: rule(duck, redaction)}`` -- a table whose rows hold a
        #: location in a column of its own (see :meth:`add_table_rule`).
        self.table_rules: dict[str, Callable] = {}
        self.report = RedactionReport()
        if project_root is not None:
            self.note(str(project_root), PROJECT)

    # -- needles ------------------------------------------------------------

    def note(self, folder: "str | Path | None", label: str) -> None:
        """Remember *folder* as something to hide, under *label*."""
        if folder is None:
            return
        text = str(folder)
        if len(text.strip("/\\")) < _MIN_NEEDLE:
            if text.strip("/\\"):
                Log.debug(f"[redaction] {label}: {text!r} is too short to search for; not noted")
            return
        for s in _spellings(text):
            self._needles.setdefault(s, label)
        Log.debug(f"[redaction] needle for {label} ({len(self._needles)} spellings in all)")

    @property
    def labels(self) -> list[str]:
        return sorted(set(self._needles.values()))

    def _by_length(self) -> list[tuple[str, str]]:
        return sorted(self._needles.items(), key=lambda kv: (-len(kv[0]), kv[0]))

    @staticmethod
    def _token(label: str) -> str:
        return "<project>" if label == PROJECT else f"<{label}>"

    # -- declarations -------------------------------------------------------

    def template(self, name: str, template: str, root_folder: "str | None" = None) -> str:
        """*template* without its location; notes what was removed."""
        from scifor.pathinput import split_template_location

        label = f"PathInput {name}"
        self.note(root_folder, label)
        location, rest = split_template_location(template)
        if location:
            self.note(location, label)
        return rest

    def arm(self, name: str, arm: dict, where: str = "") -> dict:
        """One declared arm (``template``, ``root_folder``, ``aliases``,
        ``key_regex``) without its location."""
        out = dict(arm)
        out["template"] = self.template(name, arm.get("template", ""), arm.get("root_folder"))
        if arm.get("root_folder") is not None or out["template"] != arm.get("template"):
            self.report.hidden.append((where or "declaration", name))
        out["root_folder"] = None
        return out

    def display(self, name: str, shown: dict, where: str = "gui") -> dict:
        """A ``graph_builder.path_input_display`` dict without its locations."""
        out = dict(shown)
        primary = self.arm(name, shown, where)
        out["template"], out["root_folder"] = primary["template"], None
        out["alternate_templates"] = [
            {**alt, **{k: v for k, v in self.arm(name, alt, where).items() if k in ("template", "root_folder")}}
            for alt in shown.get("alternate_templates") or []
        ]
        return out

    def spec(self, text: str) -> str:
        """A stored ``PathInput.to_spec()`` JSON without its location; anything
        that is not a PathInput spec is returned as it is."""
        try:
            payload = json.loads(text)
        except (TypeError, ValueError):
            return text
        if not isinstance(payload, dict) or payload.get("__type") != "PathInput":
            return text
        name = str(payload.get("name") or "?")
        payload["template"] = self.template(name, payload.get("template", ""), payload.get("root_folder"))
        payload["root_folder"] = None
        return json.dumps(payload)

    # -- text ---------------------------------------------------------------

    def text(self, s: str, where: str = "") -> str:
        """*s* with every needle replaced by a neutral token."""
        n = 0
        for needle, label in self._by_length():
            if needle in s:
                n += s.count(needle)
                s = s.replace(needle, self._token(label))
        if n and where:
            self.report.replaced[where] = self.report.replaced.get(where, 0) + n
        return s

    def sql_replace(self, column_sql: str) -> str:
        """The SQL expression that does :meth:`text` to *column_sql*."""
        expr = column_sql
        for needle, label in self._by_length():
            expr = f"replace({expr}, {_sql_literal(needle)}, {_sql_literal(self._token(label))})"
        return expr

    def counts(self, s: str) -> dict[str, int]:
        """``{label: occurrences}`` of the needles in *s*."""
        out: dict[str, int] = {}
        for needle, label in self._by_length():
            if needle in s:
                out[label] = out.get(label, 0) + s.count(needle)
                s = s.replace(needle, "\0")
        return out

    # -- tables -------------------------------------------------------------

    def add_table_rule(self, table: str, rule: Callable) -> None:
        """A table whose rows hold a location the textual pass cannot treat
        alone (it would collide with a primary key, or needs the PathInput's
        name): ``rule(duck, redaction)`` rewrites it in the scratch copy."""
        self.table_rules[table] = rule


def _constant_rule(duck, redaction: PathRedaction) -> None:
    """PathInput specs in ``_constant`` lose their location; their record ids
    and content hashes come from the name and do not change."""
    rows = duck.fetchall(
        "SELECT record_id, value_repr FROM _constant WHERE value_type = 'PathInput'"
    )
    for record_id, value_repr in rows:
        new = redaction.spec(value_repr)
        if new != value_repr:
            duck.execute("UPDATE _constant SET value_repr = ? WHERE record_id = ?", [new, record_id])
            redaction.report.hidden.append(("history/_constant", record_id))
    Log.info(f"[redaction] _constant: {len(rows)} PathInput spec(s) read")


def redact_tables(
    files: "dict[str, bytes]", redaction: PathRedaction, where: str
) -> "tuple[dict[str, bytes], list]":
    """A table dump (``table_copy.dump``'s files) with every location hidden,
    and ``[(where, label, count)]`` of any needle still in a cell.

    The live database is never touched: the dump is loaded into a scratch
    in-memory database, rewritten there, and dumped again. Structural rules
    first (they note the folders only the rows know), then ONE textual pass
    over every VARCHAR column, so no list of "columns that may hold a path"
    can fall out of date."""
    from sciduckdb.connection import DuckConnection

    from scidb.project import table_copy

    spec = json.loads(files[table_copy.TABLES_FILE].decode("utf-8"))
    scratch = DuckConnection(":memory:")
    try:
        table_copy.load(scratch, files)
        tables = list(spec.get("tables", {}))
        rules = {"_constant": _constant_rule, **redaction.table_rules}
        for table in tables:
            if table in rules:
                rules[table](scratch, redaction)
        for table in tables:
            cols = scratch.fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ? AND data_type = 'VARCHAR'",
                [table],
            )
            for (col,) in cols:
                q = table_copy._q(col)
                tests = " OR ".join(f"contains({q}, {_sql_literal(n)})" for n in redaction._needles)
                if not tests:
                    continue
                n = scratch.fetchone(f"SELECT count(*) FROM {table_copy._q(table)} WHERE {tests}")[0]
                if n:
                    scratch.execute(
                        f"UPDATE {table_copy._q(table)} SET {q} = {redaction.sql_replace(q)} WHERE {tests}"
                    )
                    key = f"{where}/{table}"
                    redaction.report.replaced[key] = redaction.report.replaced.get(key, 0) + int(n)
                    Log.debug(f"[redaction] {key}.{col}: {n} cell(s) rewritten")
        left = []
        for table in tables:
            for (col,) in scratch.fetchall(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ?", [table]
            ):
                q = f"CAST({table_copy._q(col)} AS VARCHAR)"
                any_hit = " OR ".join(f"contains({q}, {_sql_literal(n)})" for n in redaction._needles)
                if not any_hit or not scratch.fetchone(
                    f"SELECT count(*) FROM {table_copy._q(table)} WHERE {any_hit}"
                )[0]:
                    continue
                for needle, label in redaction._by_length():
                    n = scratch.fetchone(
                        f"SELECT count(*) FROM {table_copy._q(table)} "
                        f"WHERE contains({q}, {_sql_literal(needle)})"
                    )[0]
                    if n:
                        left.append((f"{where}/{table}.{col}", label, int(n)))
        out = table_copy.dump(scratch, tables, list(spec.get("views", {})))
    finally:
        scratch.close()
    return out, left


def leftovers(sections: "dict[str, dict[str, bytes]]", redaction: PathRedaction, skip: set) -> list:
    """``[(member, label, count)]`` of needles still in a text member of a
    bundle. Binary members (Parquet, wheels) are not text; tables are checked
    cell by cell in :func:`redact_tables`."""
    out = []
    for section, files in sections.items():
        if section in skip:
            continue
        for rel, data in sorted(files.items()):
            if rel.endswith((".parquet", ".whl")):
                continue
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                continue
            for label, n in redaction.counts(text).items():
                out.append((f"{section}/{rel}", label, n))
    return out


def refusal(found: list) -> str:
    """The message of the export refused for *found* (``leftovers``)."""
    lines = [
        "the export hides PathInput locations, but these folders are still in the "
        "bundle (SciStack does not edit your code to remove them):"
    ]
    lines += [f"  {label}: {n} time(s) in {where}" for where, label, n in found]
    lines.append(
        "Declare the PathInput in the entities file without a root_folder (or remove the "
        "path from the code), or export without hiding locations."
    )
    return "\n".join(lines)
