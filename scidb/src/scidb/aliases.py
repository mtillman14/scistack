"""Display aliases: ``[aliases]`` in the project config — the ONE owner of its grammar.

A figure says ``BL``, ``F``, ``StepLength`` because that is what the data
holds. An alias is how the project says what those read as, once, for every
plot:

.. code-block:: toml

    # scistack.toml
    [aliases.session]                 # one entry per thing: a schema key,
    name = "Session"                  # a variable, "Var.Column", ColName…

    [aliases.session.levels]          # its values, keyed by level TEXT
    "BL" = "Baseline"
    "01" = "Visit 1"                  # "01" stays "01"

    [aliases."Demographics.Sex"]
    name = "Sex"
    levels = { "F" = "Female", "M" = "Male" }

    [aliases.StepLength]
    name = "Step length (cm)"

    # pyproject.toml: the same under [tool.scistack.aliases.…]

``name`` renames the thing where the figure says what it IS (an axis title, a
legend title); ``levels`` renames its values wherever they appear (ticks,
legend entries, brackets, panel titles). ``levels`` is a sub-table, never keys
beside ``name``, so a level literally called ``name`` is not ambiguous.

Display only: nothing here changes which rows are drawn or their order.
A plot may override any entry (``PlotSpec.aliases``, scistackplot); the merge
and the application are ``scistackplot.aliases``'. This module reads, checks
and writes the project layer — see docs/claude/plot-text-and-labels.md.

Why scidb owns it
-----------------
The names it keys on are scidb's (schema keys, variables), it lives in the same
config ``[schema_keys]`` does, and it is read the same way: live, through
:func:`scidb.schema_order.locate_config`, content cached on the file's mtime —
an edit reaches the next figure with no restart. ``scistackplot`` must work
without scidb, so it never reads the file; ``scistackplotdb`` hands it the
parsed table. The GUI's whole-file writer calls :func:`render_aliases_table`
rather than knowing the grammar itself.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable, TypedDict

from scifor.discovery import read_scistack_section

from .log import Log

#: TOML table holding the declarations, in both config formats.
SECTION = "aliases"

#: Synthetic factors a plot can draw that are neither schema keys nor
#: variables (docs/claude/synthetic-factors.md). Known, so not warned about.
SYNTHETIC_NAMES = frozenset({"ColName", "Variant"})


class AliasEntry(TypedDict, total=False):
    """One ``[aliases.<thing>]`` entry, normalised. Both keys optional."""

    name: str
    levels: dict[str, str]


#: ``{thing: entry}`` — the plain-data form every consumer receives. Plain
#: dicts, not a class: it crosses into scistackplot, which cannot import scidb
#: and reads it with its own ``Alias.from_dict``.
AliasTable = dict[str, AliasEntry]

#: ``{config path: (mtime, table)}``, like ``schema_order._cache``.
_cache: dict[str, tuple[float, AliasTable]] = {}

_ENTRY_KEYS = ("name", "levels")


def normalize(raw: Any, *, where: str = SECTION) -> AliasTable:
    """The canonical form of a raw ``[aliases]`` table, WARNing what it drops.

    The one reading of the grammar: :func:`project_aliases` parses with it and
    :func:`render_aliases_table` writes through it, so what the GUI writes is
    exactly what the next read returns. Never raises — the file is hand-edited,
    and one bad entry must not cost the rest.

    * An entry that is not a table, a key other than ``name``/``levels``, a
      ``levels`` that is not a table: WARN, dropped.
    * A number where text is expected becomes its text (a TOML ``1`` cannot
      carry a zero pad, so its text is unambiguous); anything else: WARN.
    * An empty alias (``""``) means nothing at the project level and is
      dropped quietly; an entry left with nothing is dropped.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        Log.warn(f"[aliases] [{where}] is not a table of [{where}.<name>] entries — ignoring it.")
        return {}

    table: AliasTable = {}
    for thing, body in raw.items():
        thing = str(thing)
        at = f"{where}.{_toml_key(thing)}"
        if not isinstance(body, dict):
            Log.warn(
                f"[aliases] [{at}] is not a table (write name = \"…\" and/or a "
                f"levels table under it) — ignoring it."
            )
            continue
        entry: AliasEntry = {}
        for key, value in body.items():
            if key not in _ENTRY_KEYS:
                Log.warn(
                    f"[aliases] {at}.{key} is not a setting (only 'name' and "
                    f"'levels'); levels go under [{at}.levels] — ignoring it."
                )
                continue
            if key == "name":
                text = _text(value, f"{at}.name")
                if text:
                    entry["name"] = text
                continue
            if not isinstance(value, dict):
                Log.warn(f"[aliases] {at}.levels is not a table of \"level\" = \"alias\" — ignoring it.")
                continue
            levels: dict[str, str] = {}
            for level, alias in value.items():
                text = _text(alias, f"{at}.levels.{_toml_key(str(level))}")
                if text:
                    levels[str(level)] = text
            if levels:
                entry["levels"] = levels
        if entry:
            table[thing] = entry
    return table


def _text(value: Any, at: str) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    Log.warn(f"[aliases] {at} is {type(value).__name__}, not text — ignoring it.")
    return None


def _read(config: Path) -> AliasTable:
    section = read_scistack_section(config) or {}
    return normalize(section.get(SECTION))


def aliases_in(config: Path) -> AliasTable:
    """The aliases declared in *config*, cached on its mtime.

    Logged at INFO once per read (once per edit of the file), so "why is my
    alias not showing?" starts from what was actually read.
    """
    key = str(config)
    try:
        mtime = config.stat().st_mtime
    except OSError:
        mtime = 0.0
    cached = _cache.get(key)
    if cached is not None and cached[0] == mtime:
        return cached[1]

    table = _read(config)
    if table:
        Log.info("[aliases] %s: %s", config, describe(table))
    else:
        Log.info("[aliases] %s has no [%s] table — figures show raw names", config, SECTION)
    _cache[key] = (mtime, table)
    return table


def project_aliases() -> AliasTable:
    """The aliases for THIS process's project, read live.

    The same config ``[schema_keys]`` comes from
    (:func:`scidb.schema_order.locate_config`: the pinned project root, else
    the working directory), so the two can never be read from different files.
    Cheap: the location is cached for ``LOCATE_TTL`` and the parse on mtime.
    """
    from .schema_order import locate_config

    config = locate_config()
    if config is None:
        return {}
    return aliases_in(config)


def clear_cache() -> None:
    """Forget every parsed config. For tests, and for a project switch."""
    _cache.clear()


def describe(table: AliasTable) -> str:
    """One line for the log: ``session (name, 3 levels), StepLength (name)``."""
    parts = []
    for thing, entry in table.items():
        bits = []
        if "name" in entry:
            bits.append("name")
        if entry.get("levels"):
            bits.append(f"{len(entry['levels'])} level(s)")
        parts.append(f"{thing} ({', '.join(bits)})")
    return ", ".join(parts)


def validate(
    table: AliasTable,
    *,
    schema_keys: Iterable[str],
    variables: Iterable[str],
) -> list[str]:
    """WARN about entries that cannot be doing what they were written for.

    Returns the warnings (tests read them). Nothing is refused: the file is
    hand-edited, and an alias for something not drawn yet is harmless.

    * An entry naming neither a schema key, a variable, ``Var.Column`` of a
      known variable, nor a synthetic factor — usually a typo, which would
      otherwise be completely silent.
    * A schema key and a variable with the same name: they share one entry
      (one entry per thing is the grammar), which is rarely what was meant.
    * Two levels of one entry with the same alias: two different marks would
      read the same, so the figure would refuse the plot that draws them.
    """
    keys = set(schema_keys)
    names = set(variables)
    warnings: list[str] = []
    for thing, entry in table.items():
        variable, _, column = thing.partition(".")
        known = (
            thing in keys
            or thing in names
            or thing in SYNTHETIC_NAMES
            or (column and variable in names)
        )
        if not known:
            warnings.append(
                f"[aliases.{_toml_key(thing)}] names no schema key, variable or "
                f"'Variable.Column' of this dataset — it applies only if a factor "
                f"with exactly this name is drawn (a grouping column is written "
                f"'Variable.Column')."
            )
        if thing in keys and thing in names:
            warnings.append(
                f"[aliases.{_toml_key(thing)}] is both a schema key and a variable "
                f"here, so one entry renames both."
            )
        levels = entry.get("levels") or {}
        seen: dict[str, str] = {}
        for level, alias in levels.items():
            if alias in seen:
                warnings.append(
                    f"[aliases.{_toml_key(thing)}.levels] gives {seen[alias]!r} and "
                    f"{level!r} the same alias {alias!r} — a plot drawing both is "
                    f"refused, since two marks would read the same."
                )
            else:
                seen[alias] = level
    for message in warnings:
        Log.warn(message)
    return warnings


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

#: A TOML bare key: written unquoted. Anything else (a dot, a space) is quoted.
_BARE_KEY = re.compile(r"^[A-Za-z0-9_-]+$")


def _toml_str(text: str) -> str:
    """A TOML basic string: backslash, quote and control characters escaped."""
    out = []
    for char in text:
        if char == "\\":
            out.append("\\\\")
        elif char == '"':
            out.append('\\"')
        elif char == "\n":
            out.append("\\n")
        elif char == "\t":
            out.append("\\t")
        elif ord(char) < 0x20 or ord(char) == 0x7F:
            out.append(f"\\u{ord(char):04x}")
        else:
            out.append(char)
    return '"' + "".join(out) + '"'


def _toml_key(key: str) -> str:
    return key if _BARE_KEY.match(key) else _toml_str(key)


def render_aliases_table(raw: Any, *, root: str = SECTION) -> str:
    """The ``[aliases]`` tables as TOML text ("" when there is nothing).

    Goes through :func:`normalize` first, so the file never holds what the
    reader would drop. Tables only, so it is safe anywhere AFTER the
    top-level keys — the caller emits it last (a table swallows every key
    below it). Level keys are always quoted: ``"01"`` must stay text, and a
    bare ``01`` is not a valid TOML key anyway. ``root`` is the table path,
    ``tool.scistack.aliases`` for a pyproject.
    """
    table = normalize(raw)
    blocks: list[str] = []
    for thing, entry in table.items():
        header = f"{root}.{_toml_key(thing)}"
        lines = [f"[{header}]"]
        if "name" in entry:
            lines.append(f"name = {_toml_str(entry['name'])}")
        blocks.append("\n".join(lines))
        if entry.get("levels"):
            level_lines = [f"[{header}.levels]"]
            level_lines.extend(
                f"{_toml_str(level)} = {_toml_str(alias)}"
                for level, alias in entry["levels"].items()
            )
            blocks.append("\n".join(level_lines))
    return "\n\n".join(blocks) + ("\n" if blocks else "")


def with_alias(
    raw: Any,
    thing: str,
    *,
    name: "str | None | object" = ...,
    level: "str | None" = None,
    alias: "str | None" = None,
) -> AliasTable:
    """*raw* with one alias set or cleared — the one edit the GUI makes.

    ``name=`` sets (text) or clears (None) the thing's own name; ``level=`` +
    ``alias=`` sets or clears (None/"") one level's alias. Omitted arguments
    leave that part alone. An entry left empty is removed. Returns the
    normalised table; nothing is written here.
    """
    table = normalize(raw)
    entry: AliasEntry = dict(table.get(thing, {}))  # type: ignore[assignment]
    if name is not ...:
        if name:
            entry["name"] = str(name)
        else:
            entry.pop("name", None)
    if level is not None:
        levels = dict(entry.get("levels") or {})
        if alias:
            levels[str(level)] = str(alias)
        else:
            levels.pop(str(level), None)
        if levels:
            entry["levels"] = levels
        else:
            entry.pop("levels", None)
    if entry:
        table[thing] = entry
    else:
        table.pop(thing, None)
    return table
