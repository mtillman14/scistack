"""Declared level order for schema keys: ``[schema_keys]`` in the project config.

A schema key's levels have no inherent order. ``session`` is ``BL``, ``POST``,
``FU`` — chronological to the person who ran the study, alphabetical to
everything else, so every figure's x axis and every table's rows come out as
``BL, FU, POST`` unless someone says otherwise. Saying otherwise is what this
is:

.. code-block:: toml

    # scistack.toml
    [schema_keys]
    session = ["BL", "POST", "FU"]
    speed   = ["SSV", "FAST"]

    # pyproject.toml
    [tool.scistack.schema_keys]
    session = ["BL", "POST", "FU"]

The rule, in full:

* **Declared levels come first, in the declared order.**
* **Everything else is appended**, ordered by whatever the caller sorted by
  before. A level that appears after the file was written is therefore visible
  (at the end) rather than dropped — the plan's example, ``["a", "c"]`` plus an
  observed ``b``, gives ``["a", "c", "b"]``.
* **An undeclared key is untouched**, so adding this file changes only the keys
  it names.

Values are compared as TEXT. A declaration is a list of labels, and ``"01"``
must stay ``"01"`` rather than becoming ``1`` (docs/claude/schema-key-types.md);
a key declared ``numeric`` is canonicalized long before it reaches here, so the
two never fight.

Why scidb owns it
-----------------
Schema keys are scidb's concept, and the consumers span three layers — row
order in ``database._sort_by_schema``, factor levels in
``scistackplotdb.source``, the GUI's level lists. One reader here means one
answer everywhere (CLAUDE.md NOTE 3), reading the project config through
``scifor.discovery``, which is the pure parser every layer already shares.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from scifor.discovery import find_project_config, read_scistack_section

from .log import Log

#: TOML table holding the declarations, in both config formats.
SECTION = "schema_keys"

#: ``{config path: (mtime, order)}``. Cached on the file's mtime, like
#: ``entities.load_for_project``: repeated access costs one ``stat``, and a
#: hand edit is picked up on the next call with nothing to invalidate.
_cache: dict[str, tuple[float, dict[str, list[str]]]] = {}


def _read(config: Path) -> dict[str, list[str]]:
    section = read_scistack_section(config) or {}
    raw = section.get(SECTION)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        Log.warn(
            f"[schema_order] [{SECTION}] in {config} is not a table of "
            f"key = [levels] — ignoring it."
        )
        return {}

    order: dict[str, list[str]] = {}
    for key, values in raw.items():
        if not isinstance(values, (list, tuple)):
            Log.warn(
                f"[schema_order] {SECTION}.{key} is not a list of levels — "
                f"ignoring it."
            )
            continue
        # Text, and de-duplicated: a repeated level would otherwise appear
        # twice in an axis order and once in the data.
        levels = list(dict.fromkeys(str(value) for value in values))
        if levels:
            order[str(key)] = levels
    return order


def declared_level_order(start: "Path | str | None" = None) -> dict[str, list[str]]:
    """``{schema key: [levels]}`` declared by the project containing *start*.

    Empty when there is no project config, no ``[schema_keys]`` table, or
    nothing valid in it — in which case every caller keeps the ordering it had.
    """
    config = find_project_config(Path(start) if start is not None else Path.cwd())
    if config is None:
        return {}
    key = str(config)
    try:
        mtime = config.stat().st_mtime
    except OSError:
        mtime = 0.0
    cached = _cache.get(key)
    if cached is not None and cached[0] == mtime:
        return cached[1]

    order = _read(config)
    if order:
        Log.info(
            "[schema_order] %s declares level order for %s",
            config,
            ", ".join(f"{k} ({len(v)})" for k, v in order.items()),
        )
    _cache[key] = (mtime, order)
    return order


def clear_cache() -> None:
    """Forget every parsed config. For tests, and for a project switch."""
    _cache.clear()


def validate(order: dict[str, list[str]], schema_keys) -> None:
    """Warn about declarations that name something this dataset has no key for.

    A typo is otherwise completely silent: the declaration simply never
    matches, and the levels come out in the default order with nothing said.
    """
    unknown = [key for key in order if key not in set(schema_keys)]
    if unknown:
        Log.warn(
            f"[schema_order] [{SECTION}] declares {sorted(unknown)}, which "
            f"{'is not a schema key' if len(unknown) == 1 else 'are not schema keys'} "
            f"of this dataset ({list(schema_keys)}) — those declarations do nothing."
        )


def order_levels(key: str, values, *, declared: dict[str, list[str]], fallback):
    """*values* for *key*: declared levels first, then ``fallback(rest)``.

    ``fallback`` is the caller's OWN existing sort — natural-sort in one place,
    numeric in another — passed in rather than chosen here, so
    an undeclared key behaves exactly as it did before this module existed and
    a declaration cannot change anything but the order it names.
    """
    order = declared.get(key)
    unique = list(dict.fromkeys(values))
    if not order:
        return fallback(unique)

    as_text = {str(value): value for value in unique}
    first = [as_text[level] for level in order if level in as_text]
    rest = fallback([value for value in unique if str(value) not in set(order)])
    if rest:
        Log.debug(
            "[schema_order] '%s': %d declared level(s), %d appended in the "
            "default order (%s)",
            key,
            len(first),
            len(rest),
            rest[:5],
        )
    return [*first, *rest]


def level_rank(key: str, declared: dict[str, list[str]]):
    """A sort key for *key*'s levels: ``(0, position)`` for declared ones,
    ``(1, …)`` for the rest — so declared levels sort first, in order, and
    everything else keeps the caller's own ordering behind them.

    Returns ``None`` when the key is undeclared, so a caller can skip building
    a temporary column and sort exactly as it always has.
    """
    order = declared.get(key)
    if not order:
        return None
    position = {level: index for index, level in enumerate(order)}

    def rank(value: Any) -> tuple[int, int]:
        text = str(value)
        return (0, position[text]) if text in position else (1, 0)

    return rank
