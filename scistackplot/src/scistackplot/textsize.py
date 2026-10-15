"""
The size of every piece of text in a figure — the ONE owner.

ONE dial, fixed proportions (user, 2026-10-14; ADR D-2026-10-14-1). Every
element is drawn at ``reference x its ratio``, clamped into that element's
floor/ceiling (:func:`drawn_sizes`):

* the **reference** is ``TextSizes.base`` when fixed (drawn as typed, never
  clamped), else the number ``autosize`` searched;
* the **ratios** are the user's intended relative sizes: :data:`DEFAULT_TEXT_RATIOS`
  (THE table) overridden per element by ``TextSizes.ratios``. Sizing never
  alters them: a clamp changes what is drawn, never the stored ratio, so a
  larger canvas restores the proportions;
* each element's **floor/ceiling** defaults to the target's band
  (:data:`BANDS`) x its ratio, overridden in pt by ``TextSizes.floors`` /
  ``ceilings`` (:func:`element_bounds`);
* an element given its own pt size is FIXED: drawn as typed, outside the
  scaling, and pinned against the fitting's shrink.

:func:`resolve_sizes` turns a spec (and a settled reference) into a point
size per element, and every consumer reads that: ``render_matplotlib``
(through :func:`rc_params` plus the few explicit sizes matplotlib has no rc
key for), the generated seaborn code (the same :func:`rc_params` dict, emitted
as a literal), and the plotly preview (1 pt = 1 px). Renderers call
:func:`sizes_for`, which passes a resolved plot's settled reference, so no
consumer ever turns ``None`` into a number of its own and no consumer
multiplies by a ratio of its own. See docs/claude/plot-text-and-labels.md.

Pure, no matplotlib import.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Mapping

from scistacklog import Log

from .spec import TEXT_ELEMENTS, StyleOptions, TextSizes

if TYPE_CHECKING:
    from .resolved import ResolvedPlot

LAYER = "scistackplot"

#: Each element's intended size over the reference (user, 2026-10-14). THE
#: table: a fixed ``base`` and auto both draw these, and every default
#: floor/ceiling is the band times them. Axis titles and the legend title one
#: step (x1.2) over the body text, the figure title two (x1.44).
DEFAULT_TEXT_RATIOS: dict[str, float] = {
    "title": 1.44,
    "x_label": 1.2,
    "y_label": 1.2,
    "x_ticks": 1.0,
    "y_ticks": 1.0,
    "groups": 1.0,
    "legend": 1.0,
    "legend_title": 1.2,
    "differences": 1.0,
}

#: Every element, in the order the log and the GUI state them.
ELEMENTS: tuple[str, ...] = TEXT_ELEMENTS


@dataclass(frozen=True)
class Band:
    """A target's reference band in points: the body text (ratio 1) runs from
    ``floor_pt`` to ``ceiling_pt``; every element's default band is this
    times its ratio."""

    floor_pt: float
    ceiling_pt: float


#: One band per :data:`spec.TEXT_TARGETS` entry (user, 2026-10-06). Print: 8 pt
#: is the ladder's own legibility floor (``LabelPolicy.min_font_pt``), 12 pt the
#: usual journal maximum. Slide: read from across a room. The bands assume the
#: figure is placed at its saved size.
BANDS: dict[str, Band] = {
    "print": Band(floor_pt=8.0, ceiling_pt=12.0),
    "slide": Band(floor_pt=14.0, ceiling_pt=28.0),
}

#: Which side of its band a clamped element was held at.
FLOOR = "floor"
CEILING = "ceiling"


@dataclass(frozen=True)
class ResolvedSizes:
    """Every element's DRAWN size in points, and how each was decided."""

    #: The reference (the one dial): matplotlib's ``font.size`` for text that
    #: is no element (an annotation, "no data"), and ratio 1.
    base: float
    title: float
    x_label: float
    y_label: float
    x_ticks: float
    y_ticks: float
    groups: float
    legend: float
    legend_title: float
    #: A difference bar's label (``diffbars``).
    differences: float
    #: The elements set explicitly. Fitting never shrinks these.
    pinned: frozenset[str] = frozenset()
    #: ``{element: "floor" | "ceiling"}``: held at its bound at this reference.
    clamped: Mapping[str, str] = field(default_factory=dict)
    #: The ratios drawn with (defaults + the spec's own), per element.
    ratios: Mapping[str, float] = field(default_factory=dict)
    #: ``{element: (floor, ceiling)}`` in points (auto only; empty when fixed).
    bounds: Mapping[str, tuple[float, float]] = field(default_factory=dict)

    def is_pinned(self, element: str) -> bool:
        return element in self.pinned

    def legend_title_for(self, entry_pt: float) -> float:
        """The legend title's size when the entries are drawn at ``entry_pt``:
        a fixed title as typed; otherwise it keeps its drawn proportion to
        the entries, so a legend fit that shrinks them shrinks it too."""
        if self.is_pinned("legend_title") or self.legend <= 0:
            return self.legend_title
        return round(self.legend_title * entry_pt / self.legend, 3)

    def to_dict(self) -> dict:
        """JSON for the GUI (``layout.meta.text_sizes``): every element's drawn
        size, and which are fixed / clamped, with the ratios and bounds."""
        return {
            **{name: getattr(self, name) for name in ("base", *ELEMENTS)},
            "pinned": sorted(self.pinned),
            "clamped": dict(self.clamped),
            "ratios": dict(self.ratios),
            "bounds": {e: list(b) for e, b in self.bounds.items()},
        }

    def describe(self) -> str:
        """One line for the log: every size, pinned and clamped ones marked."""
        parts = [f"reference={_num(self.base)}"]
        for name in ELEMENTS:
            mark = "*" if name in self.pinned else ""
            held = f"({self.clamped[name]})" if name in self.clamped else ""
            parts.append(f"{name}={_num(getattr(self, name))}{mark}{held}")
        return " ".join(parts) + ("  (* fixed)" if self.pinned else "")


def text_ratios(text: TextSizes) -> dict[str, float]:
    """Every element's ratio: :data:`DEFAULT_TEXT_RATIOS` and the spec's own."""
    return {e: float(text.ratios.get(e, DEFAULT_TEXT_RATIOS[e])) for e in ELEMENTS}


def default_bounds(text: TextSizes) -> dict[str, tuple[float, float]]:
    """Each element's default ``(floor, ceiling)``: the target's band x its ratio."""
    band = BANDS[text.target]
    return {
        e: (round(band.floor_pt * r, 3), round(band.ceiling_pt * r, 3))
        for e, r in text_ratios(text).items()
    }


def element_bounds(text: TextSizes) -> dict[str, tuple[float, float]]:
    """Each element's ``(floor, ceiling)`` in points: the defaults, with the
    spec's own pt bounds over them. A user floor above the (default) ceiling
    lifts the ceiling to it, and a user ceiling under the (default) floor
    lowers the floor: what the user typed wins (logged at DEBUG)."""
    out = {}
    for element, (floor, ceiling) in default_bounds(text).items():
        floor = float(text.floors.get(element, floor))
        ceiling = float(text.ceilings.get(element, ceiling))
        if floor > ceiling:
            if element in text.floors:
                ceiling = floor
            else:
                floor = ceiling
            Log.debug(
                "text %s: floor and ceiling crossed; drawn at %gpt", element, floor, layer=LAYER
            )
        out[element] = (floor, ceiling)
    return out


def auto_elements(text: TextSizes) -> tuple[str, ...]:
    """The elements the reference scales: every one the user did not fix."""
    return tuple(e for e in ELEMENTS if getattr(text, e) is None)


def reference_range(text: TextSizes) -> tuple[float, float] | None:
    """``(bottom, top)`` of the references that change anything: from the
    smallest at which some scaled element is still above its floor to the
    largest at which some is still below its ceiling. None when every
    element is fixed (the reference then sizes nothing)."""
    elements = auto_elements(text)
    if not elements:
        return None
    ratios = text_ratios(text)
    bounds = element_bounds(text)
    bottom = min(bounds[e][0] / ratios[e] for e in elements)
    top = max(bounds[e][1] / ratios[e] for e in elements)
    return round(bottom, 3), round(top, 3)


def drawn_sizes(
    reference: float, text: TextSizes, *, clamp: bool = True
) -> tuple[dict[str, float], dict[str, str]]:
    """``({element: pt}, {element: "floor" | "ceiling"})`` at ``reference``.

    The one rule: a fixed element as typed; every other one
    ``reference x ratio``, clamped into :func:`element_bounds` (``clamp`` off
    for a fixed ``base``: manual is anything). Never writes anything back.
    """
    ratios = text_ratios(text)
    bounds = element_bounds(text) if clamp else {}
    sizes: dict[str, float] = {}
    clamped: dict[str, str] = {}
    for element in ELEMENTS:
        fixed = getattr(text, element)
        if fixed is not None:
            sizes[element] = float(fixed)
            continue
        value = reference * ratios[element]
        if clamp:
            floor, ceiling = bounds[element]
            if value < floor - 1e-9:
                value, clamped[element] = floor, FLOOR
            elif value > ceiling + 1e-9:
                value, clamped[element] = ceiling, CEILING
        sizes[element] = round(value, 3)
    return sizes, clamped


def resolve_sizes(
    style: StyleOptions | TextSizes, *, reference: float | None = None
) -> ResolvedSizes:
    """Every element's drawn size in points.

    ``reference`` is the auto reference (``autosize.AutoTextSize.reference``);
    a fixed ``base`` wins over it and is never clamped. Callers holding a
    resolved plot use :func:`sizes_for`, which passes it.
    """
    text = style.text if isinstance(style, StyleOptions) else style
    if text.base is not None:
        ref, clamp = float(text.base), False
    elif reference is not None:
        ref, clamp = float(reference), True
    else:
        ref, clamp = _unsettled_reference(text), True
    sizes, clamped = drawn_sizes(ref, text, clamp=clamp)
    pinned = frozenset(e for e in ELEMENTS if getattr(text, e) is not None)
    return ResolvedSizes(
        base=ref,
        **sizes,
        pinned=pinned,
        clamped=clamped,
        ratios=text_ratios(text),
        bounds=element_bounds(text) if clamp else {},
    )


def sizes_for(resolved: "ResolvedPlot") -> ResolvedSizes:
    """The sizes ``resolved`` is drawn at: its spec, and its settled auto
    reference (``ResolvedPlot.auto_text``). What every renderer reads."""
    auto = resolved.auto_text
    return resolve_sizes(
        resolved.spec.style, reference=auto.reference if auto is not None else None
    )


def rc_params(sizes: ResolvedSizes) -> dict[str, float]:
    """The ``rc_context`` every matplotlib text size flows from.

    Absolute points, not relative names, so a size never depends on which rc
    happens to be in force. ``axes.labelsize`` is the x title's size:
    matplotlib has one key for both axis titles, so the y title is passed
    explicitly (``fontsize=sizes.y_label``) wherever one is set.
    """
    return {
        "font.size": sizes.base,
        "figure.titlesize": sizes.title,
        "axes.titlesize": sizes.title,
        "axes.labelsize": sizes.x_label,
        "xtick.labelsize": sizes.x_ticks,
        "ytick.labelsize": sizes.y_ticks,
        "legend.fontsize": sizes.legend,
        "legend.title_fontsize": sizes.legend_title,
    }


def _num(value: float) -> str:
    return f"{value:g}"


def _unsettled_reference(text: TextSizes) -> float:
    """The reference for an AUTO size nobody settled: the top of its range.

    Renderers settle first (``autosize.settle``), so this is reached only by a
    caller sizing text outside a render — a size estimate, a test, a describe.
    Logged at DEBUG so a renderer that forgot to settle shows up in scidb.log
    as this line next to its own render line.
    """
    span = reference_range(text)
    top = span[1] if span is not None else BANDS[text.target].ceiling_pt
    Log.debug(
        "text size is auto but unsettled; sizing at the %s top reference %gpt",
        text.target,
        top,
        layer=LAYER,
    )
    return top
