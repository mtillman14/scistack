"""
The project config file, ``scistack.toml``: one renderer, one writer.

``scistack.toml`` is the only project config file (decision 2026-10-08;
``docs/claude/config-file-formats.md``). Its NAME and LOCATION are owned by
``scifor.discovery`` (``CONFIG_FILENAME``, ``config_path_at``); its TEXT is
owned here. Every writer -- ``scidb.project.init_project`` and each GUI edit
(Paths popup, entities file, glue dir, aliases, colors) -- loads the whole
file as a dict, changes the keys it owns, and hands the whole dict to
:func:`write`. Nothing builds config text anywhere else.

**Every key survives a write.** This module regenerates the file rather than
editing it in place (comments are not kept; the file is SciStack's, not the
user's packaging file), so a key it does not know about would otherwise be
silently deleted by the next GUI click. Known keys come out in a fixed order
with their established formatting; any other key is rendered generically
(nested dicts as inline tables). A value TOML cannot hold is reported with a
WARN naming the key, never dropped quietly.
"""

from __future__ import annotations

import datetime as _dt
import os
import re
from pathlib import Path
from typing import Any

from scistacklog import Log

#: Written at the top of every rendered file.
HEADER = (
    "# SciStack project configuration -- the only config file (pyproject.toml\n"
    "# is packaging only). Written by SciStack; hand-editing is fine, this file\n"
    "# is re-read on every scan, but comments are not kept when SciStack writes."
)

#: Top-level keys with a fixed position and established formatting.
TOP_LEVEL_ORDER = (
    "modules",
    "entities_file",
    "glue_dir",
    "variable_file",
    "packages",
    "auto_discover",
    "db",
)

#: ``[matlab]`` keys, in order.
MATLAB_ORDER = ("functions", "variables", "sources", "variable_dir", "entities_file")

#: Tables rendered last, in this order (a TOML table swallows every key after
#: its header, so all top-level keys must come first).
TABLE_ORDER = ("matlab", "schema_keys", "aliases", "colors")


def _toml_str(s: str) -> str:
    """A quoted TOML basic string, escaping backslashes (Windows paths) and
    double quotes."""
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _toml_array(items: list) -> str:
    """A multi-line array of strings (the established form for path lists)."""
    if not items:
        return "[]"
    inner = ",\n    ".join(_toml_str(str(item)) for item in items)
    return f"[\n    {inner},\n]"


_BARE_KEY = re.compile(r"[A-Za-z0-9_-]+")


def _bare_or_quoted(key: str) -> str:
    """A TOML key: bare when it may be (ASCII letters, digits, - and _ only),
    quoted otherwise."""
    return key if _BARE_KEY.fullmatch(key) else _toml_str(key)


class _Unrenderable(ValueError):
    pass


def _value(v: Any) -> str:
    """Generic TOML for a value of a key this module has no fixed form for."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return _toml_str(v)
    if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_value(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{ " + ", ".join(f"{_bare_or_quoted(str(k))} = {_value(x)}" for k, x in v.items()) + " }"
    raise _Unrenderable(f"{type(v).__name__} cannot be written to TOML")


def _generic_line(key: str, value: Any, where: str) -> "str | None":
    try:
        return f"{_bare_or_quoted(key)} = {_value(value)}"
    except _Unrenderable as e:
        Log.warn(f"[config_file] {where}{key}: {e}; this key was NOT written")
        return None


def render(section: dict) -> str:
    """The full text of ``scistack.toml`` for *section* (the whole config).

    ``modules`` is always written (``[]`` when empty); ``auto_discover`` only
    when False; ``packages`` only when non-empty; path keys only when set
    (``entities_file = ""`` is a real value: the explicit opt-out).
    """
    section = dict(section or {})
    lines = [HEADER, ""]

    lines.append(f"modules = {_toml_array(list(section.get('modules') or []))}")
    for key in ("entities_file", "glue_dir", "variable_file"):
        if section.get(key) is not None:
            lines.append(f"{key} = {_toml_str(str(section[key]))}")
    if section.get("packages"):
        lines.append(f"packages = {_toml_array(list(section['packages']))}")
    if section.get("auto_discover", True) is False:
        lines.append("auto_discover = false")
    if section.get("db") is not None:
        lines.append(f"db = {_toml_str(str(section['db']))}")

    # Any other top-level key, kept rather than deleted. Before the tables:
    # a key below a [table] header would land inside that table.
    extra = [k for k in section if k not in TOP_LEVEL_ORDER and k not in TABLE_ORDER]
    for key in extra:
        line = _generic_line(key, section[key], "")
        if line is not None:
            lines.append(line)

    matlab = dict(section.get("matlab") or {})
    matlab_lines = []
    for key in MATLAB_ORDER:
        value = matlab.get(key)
        if key in ("functions", "variables", "sources"):
            if value:
                matlab_lines.append(f"{key} = {_toml_array(list(value))}")
        elif value is not None:
            matlab_lines.append(f"{key} = {_toml_str(str(value))}")
    for key in (k for k in matlab if k not in MATLAB_ORDER):
        line = _generic_line(key, matlab[key], "matlab.")
        if line is not None:
            matlab_lines.append(line)
    if matlab_lines:
        lines += ["", "[matlab]", *matlab_lines]

    # Hand-authored level order (scidb.schema_order reads it, nothing writes it).
    schema_keys = section.get("schema_keys")
    if schema_keys:
        lines += ["", "[schema_keys]"]
        for key, levels in schema_keys.items():
            if isinstance(levels, (list, tuple)):
                lines.append(f"{_bare_or_quoted(str(key))} = {_toml_array(list(levels))}")
            else:
                Log.warn(
                    f"[config_file] schema_keys.{key}: expected a list of levels, "
                    f"got {type(levels).__name__}; this key was NOT written"
                )
    # scidb owns these grammars; the text comes from their renderers.
    if section.get("aliases"):
        from .aliases import render_aliases_table

        rendered = render_aliases_table(section["aliases"])
        if rendered:
            lines += ["", rendered.rstrip("\n")]
    if section.get("colors"):
        from .colors import render_colors_table

        rendered = render_colors_table(section["colors"])
        if rendered:
            lines += ["", rendered.rstrip("\n")]

    lines.append("")
    return "\n".join(lines)


def write(path: "Path | str", section: dict) -> Path:
    """Write *section* to *path* (atomically: a temp file, then replace) and
    return the path. Logs which keys were written."""
    path = Path(path)
    text = render(section)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)
    Log.info(f"[config_file] wrote {path} (keys: {sorted((section or {}).keys())})")
    _forget_cached_reads()
    return path


def _forget_cached_reads() -> None:
    """Every reader that caches what scistack.toml says, told it changed.

    The readers cache for a few seconds (``schema_order.locate_config``,
    including "no config here"; ``names.library_packages``), so without this
    a write followed at once by a read -- listing a library and seeding it in
    the same request -- saw the OLD answer. This module is the only writer,
    so it is the one place to invalidate."""
    from . import names, schema_order

    schema_order.clear_cache()
    names.clear_cache()
    Log.debug("[config_file] cleared cached config reads (schema_order, names)")
