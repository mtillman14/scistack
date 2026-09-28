"""Mark colours: ``[colors]`` in the project config — the ONE owner of its grammar.

A project pins what colour a level is painted in, once, for every plot:

.. code-block:: toml

    # scistack.toml
    [colors]
    default = "#333333"              # the one colour of a figure nothing colours

    [colors.session]                 # one table per thing: a schema key, a
    "BL" = "#0072b2"                 # variable, "Var.Column", ColName, Variant…
    "FU" = "#d55e00"                 # keys are level TEXT, quoted: "01" stays "01"

    [colors."Demographics.Sex"]
    "F" = "#cc79a7"

    # pyproject.toml: the same under [tool.scistack.colors…]

Directly under ``[colors]`` a STRING is a setting (only ``default``) and a
TABLE is a thing, so a variable called ``default`` cannot collide with the
setting.

Separate from ``[aliases]`` (user, 2026-09-27): an alias is what a thing
READS as, a colour is how it is PAINTED — two concepts, two owners.

This module reads, checks the SHAPE of, and writes the project layer. A
colour's text is kept as text: what a colour may be written as is
``scistackplot.colors.parse_color``'s (scidb cannot import scistackplot), and
the GUI's writer canonicalises through it before calling
:func:`render_colors_table`. The merge with a plot's own pins and the
painting are scistackplot's. See docs/claude/plot-colors.md.

Read like ``[aliases]``: live, through
:func:`scidb.schema_order.locate_config`, content cached on the file's mtime.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from scifor.discovery import read_scistack_section

from .aliases import SYNTHETIC_NAMES, _toml_key, _toml_str
from .log import Log

#: TOML table holding the declarations, in both config formats.
SECTION = "colors"

#: The one setting directly under ``[colors]``.
DEFAULT_KEY = "default"


#: ``[colors]``, normalised: the SAME shape as the TOML — ``"default"`` ->
#: colour text, and ``thing`` -> ``{level text: colour text}``. A string
#: value is the setting, a dict value is a thing (see the module docstring),
#: so a thing called ``default`` or ``levels`` is never ambiguous. Plain
#: dicts: it crosses into scistackplot, which cannot import scidb
#: (``LongTable.project_colors`` splits it with the same type rule).
ColorTable = dict[str, "str | dict[str, str]"]


def default_of(table: ColorTable) -> str | None:
    """The single mark colour, or None."""
    value = table.get(DEFAULT_KEY)
    return value if isinstance(value, str) else None


def things_of(table: ColorTable) -> dict[str, dict[str, str]]:
    """``{thing: {level: colour}}`` — every table-valued entry."""
    return {thing: entry for thing, entry in table.items() if isinstance(entry, dict)}


#: ``{config path: (mtime, table)}``, like ``aliases._cache``.
_cache: dict[str, tuple[float, ColorTable]] = {}


def normalize(raw: Any, *, where: str = SECTION) -> ColorTable:
    """The canonical form of a raw ``[colors]`` table, WARNing what it drops.

    The one reading of the grammar: :func:`project_colors` parses with it and
    :func:`render_colors_table` writes through it. Never raises — the file is
    hand-edited, and one bad entry must not cost the rest.

    * A string directly under ``[colors]`` other than ``default``, or a
      value that is neither text nor a table: WARN, dropped.
    * A level's colour that is not text: WARN, dropped. (Whether the text IS
      a colour is scistackplot's check, made when a figure is drawn.)
    * An empty colour (``""``) means nothing at the project level and is
      dropped quietly; an emptied thing is dropped.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        Log.warn(f"[colors] [{where}] is not a table — ignoring it.")
        return {}
    table: ColorTable = {}
    for key, body in raw.items():
        key = str(key)
        if isinstance(body, dict):
            at = f"{where}.{_toml_key(key)}"
            entry: dict[str, str] = {}
            for level, colour in body.items():
                if isinstance(colour, str):
                    if colour.strip():
                        entry[str(level)] = colour.strip()
                    continue
                Log.warn(
                    f"[colors] {at}.{_toml_key(str(level))} is "
                    f"{type(colour).__name__}, not colour text like \"#0072b2\" — ignoring it."
                )
            if entry:
                table[key] = entry
            continue
        if key == DEFAULT_KEY:
            if isinstance(body, str):
                if body.strip():
                    table[DEFAULT_KEY] = body.strip()
                continue
            Log.warn(
                f"[colors] {where}.default is {type(body).__name__}, not colour text — ignoring it."
            )
            continue
        Log.warn(
            f"[colors] {where}.{_toml_key(key)} is not a setting (only 'default'); "
            f"a thing's level colours go in a [{where}.{_toml_key(key)}] table — ignoring it."
        )
    return table


def _read(config: Path) -> ColorTable:
    section = read_scistack_section(config) or {}
    return normalize(section.get(SECTION))


def colors_in(config: Path) -> ColorTable:
    """The colours declared in *config*, cached on its mtime, logged at INFO
    once per read (once per edit of the file)."""
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
        Log.info("[colors] %s: %s", config, describe(table))
    else:
        Log.info("[colors] %s has no [%s] table — figures use the palette", config, SECTION)
    _cache[key] = (mtime, table)
    return table


def project_colors() -> ColorTable:
    """The colours for THIS process's project, read live, from the same
    config ``[schema_keys]`` and ``[aliases]`` come from."""
    from .schema_order import locate_config

    config = locate_config()
    if config is None:
        return {}
    return colors_in(config)


def clear_cache() -> None:
    """Forget every parsed config. For tests, and for a project switch."""
    _cache.clear()


def describe(table: ColorTable) -> str:
    """One line for the log: ``default #333333, session (2 level(s))``."""
    parts = []
    if default_of(table):
        parts.append(f"default {default_of(table)}")
    for thing, entry in things_of(table).items():
        parts.append(f"{thing} ({len(entry)} level(s))")
    return ", ".join(parts)


def validate(
    table: ColorTable,
    *,
    schema_keys: Iterable[str],
    variables: Iterable[str],
) -> list[str]:
    """WARN about things that name nothing in this dataset — usually a typo,
    which would otherwise be completely silent. Returns the warnings. Nothing
    is refused: a colour for something not drawn yet is harmless."""
    keys = set(schema_keys)
    names = set(variables)
    warnings: list[str] = []
    for thing in things_of(table):
        variable, _, column = thing.partition(".")
        known = (
            thing in keys
            or thing in names
            or thing in SYNTHETIC_NAMES
            or (column and variable in names)
        )
        if not known:
            warnings.append(
                f"[colors.{_toml_key(thing)}] names no schema key, variable or "
                f"'Variable.Column' of this dataset — it applies only if a factor "
                f"with exactly this name is drawn (a grouping column is written "
                f"'Variable.Column')."
            )
    for message in warnings:
        Log.warn(message)
    return warnings


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def render_colors_table(raw: Any, *, root: str = SECTION) -> str:
    """The ``[colors]`` tables as TOML text ("" when there is nothing).

    Goes through :func:`normalize` first, so the file never holds what the
    reader would drop. Tables only, so it is safe anywhere AFTER the
    top-level keys — the caller emits it last. ``default`` sits under the
    ``[colors]`` header itself, before any thing's table. Level keys are
    always quoted (``"01"`` must stay text). ``root`` is
    ``tool.scistack.colors`` for a pyproject.
    """
    table = normalize(raw)
    blocks: list[str] = []
    default = default_of(table)
    if default:
        blocks.append(f"[{root}]\n{DEFAULT_KEY} = {_toml_str(default)}")
    for thing, entry in things_of(table).items():
        lines = [f"[{root}.{_toml_key(thing)}]"]
        lines.extend(f"{_toml_str(level)} = {_toml_str(colour)}" for level, colour in entry.items())
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks) + ("\n" if blocks else "")


def with_color(
    raw: Any,
    thing: "str | None",
    *,
    level: "str | None" = None,
    color: "str | None" = None,
) -> ColorTable:
    """*raw* with one colour set or cleared — the one edit the GUI makes.

    ``thing=None`` edits ``default`` (the single mark colour); otherwise
    ``level`` names the level. ``color`` of None/"" clears. An emptied thing
    is removed. Returns the normalised table; nothing is written here, and
    the colour text is taken as given (the caller canonicalises it).
    """
    table = dict(normalize(raw))
    if thing is None:
        if color:
            table[DEFAULT_KEY] = str(color)
        else:
            table.pop(DEFAULT_KEY, None)
        return table
    if level is None:
        raise ValueError("with_color: a thing's colour needs its level")
    entry = dict(things_of(table).get(thing, {}))
    if color:
        entry[str(level)] = str(color)
    else:
        entry.pop(str(level), None)
    if entry:
        table[thing] = entry
    else:
        table.pop(thing, None)
    return table
