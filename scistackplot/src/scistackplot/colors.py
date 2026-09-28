"""
The colours a user PINS on a figure's marks — the ONE owner of the colour
text grammar and of the plot-over-project merge.

Three layers, first match wins, per level of the factor a mark is painted by:

1. the plot's own (:attr:`PlotSpec.colors`, stored with the saved plot);
2. the project's (``[colors]`` in the project config; ``scidb.colors`` owns
   that TOML grammar and a table carries a live reader of it,
   :meth:`LongTable.project_colors`);
3. the palette (``render.base.palette_for``), at the level's DECLARED
   position — so pinning one level never moves another level's colour.

A figure with no colour layer paints every mark one colour:
``StyleOptions.mark_color`` (plot), else the project's ``[colors] default``,
else the palette's first colour.

A plot entry of ``""`` means "the palette here, even though the project pins
it", as a plot alias of ``""`` shows the raw text.

What a colour may be written as is :func:`parse_color`'s alone: ``#rgb``,
``#rrggbb``, ``rrggbb``, ``rgb(r, g, b)`` and matplotlib's named colours.
Everything is canonicalised to lowercase ``#rrggbb``. scidb cannot import
this package, so the project layer arrives as TEXT and is parsed here; the
GUI's writer canonicalises through :func:`parse_color` before it writes, so
the file only ever gets what this reads back.

Display only: identity, order and data never change. ``colors`` is a
plan-irrelevant spec field, so a colour edit re-renders and never re-reduces.
See docs/claude/plot-colors.md.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any, Iterable

from scistacklog import Log

LAYER = "scistackplot"

#: Where a colour came from — in logs and in the GUI's Colours section.
PLOT = "plot"
PROJECT = "project"
PALETTE = "palette"


class ColorError(ValueError):
    """Text that is not a colour this module understands."""


_HEX = re.compile(r"^#?([0-9a-fA-F]{6})$")
_HEX3 = re.compile(r"^#([0-9a-fA-F]{3})$")
_RGB = re.compile(r"^rgb\(\s*([^,()]+)\s*,\s*([^,()]+)\s*,\s*([^,()]+)\s*\)$", re.IGNORECASE)
#: matplotlib's property-cycle colours (``C0``…) depend on the rc in force at
#: draw time, so a pin written as one would not name one colour.
_CYCLE = re.compile(r"^C\d+$")


def parse_color(text: Any) -> str:
    """The canonical ``#rrggbb`` for ``text``, or :class:`ColorError`.

    Accepted: ``#rgb``, ``#rrggbb``, a bare ``rrggbb``, ``rgb(r, g, b)``
    with integer channels 0-255, and matplotlib's named colours (``red``,
    ``tab:blue``, ``xkcd:sky blue``) when matplotlib is importable. Refused
    with a sentence saying why: an alpha channel (a mark's fill opacity is
    ``render.base.fill_alpha``'s), a cycle reference (``C0``), anything else.
    """
    if not isinstance(text, str):
        raise ColorError(f"{text!r} is {type(text).__name__}, not colour text")
    value = text.strip()
    if not value:
        raise ColorError("an empty colour")
    found = _HEX.match(value)
    if found:
        return "#" + found.group(1).lower()
    found = _HEX3.match(value)
    if found:
        return "#" + "".join(ch * 2 for ch in found.group(1).lower())
    if re.match(r"^#([0-9a-fA-F]{4}|[0-9a-fA-F]{8})$", value):
        raise ColorError(
            f"{value!r} has an alpha channel; give the colour as #rrggbb "
            f"(fill opacity is set per plot kind)"
        )
    found = _RGB.match(value)
    if found:
        channels = []
        for part in found.groups():
            part = part.strip()
            if not re.match(r"^\d{1,3}$", part) or int(part) > 255:
                raise ColorError(
                    f"{value!r}: each rgb() channel must be a whole number 0-255"
                )
            channels.append(int(part))
        return "#" + "".join(f"{c:02x}" for c in channels)
    if _CYCLE.match(value):
        raise ColorError(
            f"{value!r} is a matplotlib cycle reference, which changes with the "
            f"style in force; give the colour itself"
        )
    try:
        from matplotlib.colors import to_hex, to_rgba
    except ImportError:
        raise ColorError(
            f"{value!r} is not #rrggbb or rgb(r, g, b) (named colours need matplotlib)"
        ) from None
    try:
        rgba = to_rgba(value)
    except (ValueError, TypeError):
        raise ColorError(
            f"{value!r} is not a colour (write #rrggbb, rgb(r, g, b) or a colour name)"
        ) from None
    if rgba[3] != 1.0:
        raise ColorError(f"{value!r} is not an opaque colour")
    return to_hex(rgba).lower()


def try_color(text: Any, where: str) -> str | None:
    """:func:`parse_color`, WARNing and returning None instead of raising —
    for a hand-edited file or a stored spec, where one bad entry must not
    cost the figure."""
    try:
        return parse_color(text)
    except ColorError as exc:
        Log.warn("colours: %s is ignored — %s", where, exc, layer=LAYER)
        return None


# ---------------------------------------------------------------------------
# The merged pins of one figure
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MarkColors:
    """The merged colour pins of one figure. Empty (the default) pins
    nothing, which is what a ``ResolvedPlot`` built in a test gets."""

    #: ``{thing: {level text: (#rrggbb, origin)}}``.
    levels: dict[str, dict[str, tuple[str, str]]] = field(default_factory=dict)
    #: The one colour of a figure with no colour layer, ``(#rrggbb, origin)``.
    single: tuple[str, str] | None = None
    #: ``{factor: lookup keys}`` — the SAME keys the aliases look up
    #: (``aliases.lookup_keys``): a grouping column ``Sex`` is pinned under
    #: ``Demographics.Sex`` first.
    keys: dict[str, tuple[str, ...]] = field(default_factory=dict)
    #: Entries dropped as not colours, ``"where: text"`` — for the log line.
    dropped: tuple[str, ...] = ()
    #: The figure's roles (set per figure by :meth:`for_figure`).
    color: str | None = None
    sample_color: str | None = None

    def _keys(self, thing: str) -> tuple[str, ...]:
        return self.keys.get(thing, (thing,))

    def pinned(self, factor: str | None, value: Any) -> tuple[str, str] | None:
        """``(#rrggbb, origin)`` pinned for ``value`` of ``factor``, or None."""
        if factor is None:
            return None
        text = _raw(value)
        for key in self._keys(factor):
            found = self.levels.get(key, {}).get(text)
            if found is not None:
                return found
        return None

    def mark(self, level: Any) -> str | None:
        """The pinned colour of a MARK: its colour level's pin, or — with no
        colour layer — the single mark colour. None = the palette decides."""
        if self.color is None:
            return self.single[0] if self.single else None
        found = self.pinned(self.color, level)
        return found[0] if found else None

    def sample(self, level: Any) -> str | None:
        """The pinned colour of an overlay level (``PlotSpec.sample_color``)."""
        found = self.pinned(self.sample_color, level)
        return found[0] if found else None

    def entry_key(self, thing: str) -> str:
        """The key a pin for ``thing`` is written under (as
        ``DisplayText.entry_key``), so the GUI writes where the lookup reads
        first."""
        return self._keys(thing)[0]

    def for_figure(self, *, color: str | None, sample_color: str | None) -> "MarkColors":
        return replace(self, color=color, sample_color=sample_color)


def _raw(value: Any) -> str:
    return "" if value is None else str(value)


def mark_colors(spec, table) -> MarkColors:
    """The plot's pins over the project's, for ``table``'s factors.

    Reads the project layer NOW (``table.project_colors()``), so an edit to
    the project config reaches the next resolve with nothing rebuilt.
    """
    from .aliases import lookup_keys

    project_default, project_levels = table.project_colors()
    return merge(
        project_default=project_default,
        project_levels=project_levels,
        plot_default=spec.style.mark_color,
        plot_levels=spec.colors,
        keys=lookup_keys(spec),
    )


def merge(
    *,
    project_default: str | None,
    project_levels: dict[str, dict[str, str]],
    plot_default: str | None,
    plot_levels: dict[str, dict[str, str]],
    keys: dict[str, tuple[str, ...]] | None = None,
) -> MarkColors:
    """Plot over project, level by level, each with its origin. A plot
    ``""`` removes the project's pin (the palette draws it); every value is
    parsed here and a bad one is dropped with a WARN."""
    dropped: list[str] = []

    def parsed(text: Any, where: str) -> str | None:
        colour = try_color(text, where)
        if colour is None:
            dropped.append(f"{where}={text!r}")
        return colour

    levels: dict[str, dict[str, tuple[str, str]]] = {}
    for thing in dict.fromkeys([*project_levels, *plot_levels]):
        merged: dict[str, tuple[str, str]] = {}
        for level, text in (project_levels.get(thing) or {}).items():
            colour = parsed(text, f"[colors.{thing}] {level!r}")
            if colour:
                merged[str(level)] = (colour, PROJECT)
        for level, text in (plot_levels.get(thing) or {}).items():
            if text == "":
                merged.pop(str(level), None)
                continue
            colour = parsed(text, f"the plot's {thing} {level!r}")
            if colour:
                merged[str(level)] = (colour, PLOT)
        if merged:
            levels[thing] = merged

    single: tuple[str, str] | None = None
    if plot_default is not None and plot_default != "":
        colour = parsed(plot_default, "the plot's mark colour")
        if colour:
            single = (colour, PLOT)
    if single is None and plot_default != "" and project_default:
        colour = parsed(project_default, "[colors] default")
        if colour:
            single = (colour, PROJECT)
    return MarkColors(levels=levels, single=single, keys=dict(keys or {}), dropped=tuple(dropped))


def log_summary(colors: MarkColors, table) -> None:
    """What the pins did to this table's factors, once per resolve.

    INFO names every factor with pins — how many of its levels are pinned,
    from which layer — the single mark colour, every pin naming no level in
    the data (usually a text mismatch: ``1`` vs ``"01"``), and every entry
    dropped as not a colour. Silent when nothing is pinned.
    """
    if not colors.levels and colors.single is None and not colors.dropped:
        return
    parts: list[str] = []
    unused: list[str] = []
    for info in table.factors:
        entries: dict[str, tuple[str, str]] = {}
        for key in reversed(colors._keys(info.name)):  # the first key wins
            entries.update(colors.levels.get(key, {}))
        if not entries:
            continue
        present = {_raw(value) for value in info.levels}
        hits = [level for level in entries if level in present]
        by_origin = {
            origin: sum(1 for level in hits if entries[level][1] == origin)
            for origin in (PROJECT, PLOT)
        }
        parts.append(
            f"{info.name} {len(hits)}/{len(present)} pinned "
            f"(project {by_origin[PROJECT]}, plot {by_origin[PLOT]})"
        )
        unused.extend(f"{info.name}={level!r}" for level in entries if level not in present)
    Log.info(
        "colours: levels %s; single mark colour %s%s%s",
        ", ".join(parts) or "none",
        f"{colors.single[0]} ({colors.single[1]})" if colors.single else "palette",
        (
            f"; {len(unused)} pin(s) name no level in the data ({', '.join(unused[:5])})"
            if unused
            else ""
        ),
        (
            f"; {len(colors.dropped)} dropped as not colours ({', '.join(colors.dropped[:5])})"
            if colors.dropped
            else ""
        ),
        layer=LAYER,
    )
    Log.debug("colours: levels=%s single=%s", colors.levels, colors.single, layer=LAYER)


def warn_duplicates(painted: Iterable[tuple[str, Any, str, str]]) -> list[str]:
    """WARN when two levels of one factor are painted the same colour and at
    least one of them is a pin (the palette alone never repeats within its
    length). ``painted`` is ``(factor, level, #rrggbb, origin)``. Returns the
    messages (tests read them). Never refuses: identical colours are legal,
    just rarely meant."""
    seen: dict[tuple[str, str], tuple[Any, str]] = {}
    messages: list[str] = []
    for factor, level, colour, origin in painted:
        key = (factor, colour)
        if key in seen:
            other, other_origin = seen[key]
            if _raw(other) == _raw(level):
                continue  # the same level, painted as mark AND overlay
            if origin == PALETTE and other_origin == PALETTE:
                continue
            messages.append(
                f"colours: {factor} {_raw(other)!r} ({other_origin}) and {_raw(level)!r} "
                f"({origin}) are both {colour} — the legend cannot tell them apart"
            )
        else:
            seen[key] = (level, origin)
    for message in messages:
        Log.warn(message, layer=LAYER)
    return messages
