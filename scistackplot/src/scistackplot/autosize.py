"""
The automatic text size: each element as big as the figure lays it out
cleanly, inside a band.

``TextSizes.base = None`` means AUTO (user, 2026-10-06). This module is the ONE
owner of the sizes that replace it. :func:`settle` searches, PER ELEMENT
(x ticks, y ticks, x title, y title, figure title, bracket rows, legend,
difference labels), for the largest size at which a real matplotlib layout
needs nothing destructive for THAT element, inside the band of the spec's
``target`` (:data:`BANDS`). The result rides on ``ResolvedPlot.auto_text``,
and every renderer reads it through ``textsize.sizes_for``. Nothing else turns
``None`` into sizes, so the preview, the saved file and the generated code
cannot choose differently.

**Per element, not one shared base** (user, 2026-10-06): a long x title must
not make the y tick labels small. Each problem a layout reports is blamed on
the element that has it (:meth:`LayoutReport.blame`), and only that element
steps down. The one exception is the plot-area rule (the panels keep
:data:`MIN_DATA_FRACTION` of the canvas): that is all the text at once, so
every unsettled element steps down.

**Why a search over real layouts and not a formula:** what makes text too big
is the labels this figure has, at this size. Long tick names, a 14-entry
legend, a facet grid's y titles all read out of the layout the export already
measures (``ticklabels.fit_labels``, the legend fit, ``GridReach``).

The rules (user, 2026-10-06):

* The band follows the DESTINATION, not the width. A point is a point on paper
  because the file is exactly W x H, and a 7.2 in figure may be a journal's
  double column or half a slide.
* No text goes below the band's floor. An element's ceiling is the band's
  ceiling times its usual ratio to the body text (title x1.2, brackets x0.833).
* Rotating tick labels and moving the legend below are problems, like
  shrinking and thinning: allowed only when the element's floor still needs
  them. The floor is then used and the existing fitting handles the rest.

The search is pure (the layout is injected as ``measure``), so its rule is
tested without matplotlib (``tests/test_autosize.py``). See
docs/claude/plot-text-and-labels.md, "Automatic size".
"""

from __future__ import annotations

import time
import weakref
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Callable, Mapping

from scistacklog import Log

from .spec import TEXT_TARGETS, TextSizes

if TYPE_CHECKING:
    from .resolved import ResolvedPlot

LAYER = "scistackplot"


@dataclass(frozen=True)
class Band:
    """The sizes auto may choose, in points at the figure's saved size."""

    #: No text goes below this.
    floor_pt: float
    #: The body text (ticks, axis titles, legend) goes no higher than this;
    #: other elements scale it by :data:`CEILING_RATIOS`.
    ceiling_pt: float


#: One band per :data:`spec.TEXT_TARGETS` entry (user, 2026-10-06). Print: 8 pt
#: is the ladder's own legibility floor (``LabelPolicy.min_font_pt``), 12 pt the
#: usual journal maximum. Slide: read from across a room, at slide-scale
#: widths. The bands assume the figure is placed at its saved size.
BANDS: dict[str, Band] = {
    "print": Band(floor_pt=8.0, ceiling_pt=12.0),
    "slide": Band(floor_pt=14.0, ceiling_pt=28.0),
}

#: The elements auto sizes, and each one's ceiling over the band's ceiling:
#: matplotlib's own ratios (``textsize.LARGE`` / ``SMALL``), so an all-auto
#: figure at the ceiling keeps the hierarchy a fixed base draws. The legend
#: title follows the legend entries, as it always has.
CEILING_RATIOS: dict[str, float] = {
    "title": 1.2,
    "x_label": 1.0,
    "y_label": 1.0,
    "x_ticks": 1.0,
    "y_ticks": 1.0,
    "groups": 0.833,
    "legend": 1.0,
    "differences": 1.0,
}
ELEMENTS: tuple[str, ...] = tuple(CEILING_RATIOS)

#: The candidate sizes are this far apart.
STEP_PT = 0.5

#: The panels (the axes boxes, summed) must keep at least this share of the
#: canvas. Past it, the text is eating the plot (user delegated, 2026-10-06).
MIN_DATA_FRACTION = 0.55

#: Neighbouring panels' text may touch by this much before it counts as overlap.
OVERLAP_TOLERANCE_PT = 0.5
#: Content past the canvas edge, as ``figure_file.OVERFLOW_TOLERANCE_IN``.
OVERFLOW_TOLERANCE_IN = 0.01

#: Rounds of "verify the combination, step down whatever still fails" after
#: the per-element search (the elements interact: smaller y ticks widen the
#: panels the x ticks sit under).
VERIFY_ROUNDS = 6

#: Fit steps that change nothing a reader loses: wrapping onto two lines,
#: dropping a numbered prefix, a wrapped legend title or shorter line samples.
#: Every OTHER step a fit reports is a problem.
HARMLESS_STEPS = frozenset({"strip_prefix", "wrap", "wrap_title", "short_handles", "pinned"})

#: Which elements' text sits on which side of a panel, for overlap blame.
_LEFT_SIDE = ("y_ticks", "y_label")
_BELOW_SIDE = ("x_ticks", "x_label", "groups")
#: Blame key meaning "every element still being searched".
ALL = "*"


@dataclass(frozen=True)
class LayoutReport:
    """What one layout needed, read off the rendered figure."""

    #: ``LabelFit.steps`` of the x ticks / bracket rows, and whether they fit.
    tick_steps: tuple[str, ...] = ()
    ticks_fit: bool = True
    bracket_steps: tuple[str, ...] = ()
    brackets_fit: bool = True
    #: The legend fit's steps (``"shrink"``, ``"below"``, ...).
    legend_steps: tuple[str, ...] = ()
    #: Worst run of y-side text into the panel on its left / x-side text into
    #: the panel below (``GridReach``).
    left_overlap_pt: float = 0.0
    below_overlap_pt: float = 0.0
    #: ``(element, inches)``: how far each element's own text reaches past
    #: the canvas edge, where it does.
    overflow_by_element: tuple[tuple[str, float], ...] = ()
    #: Overflow by something no element owns (beyond the attributed ones).
    unattributed_overflow_in: float = 0.0
    #: The visible axes boxes' area over the canvas area.
    data_fraction: float = 1.0

    def blame(self) -> dict[str, tuple[str, ...]]:
        """``{element: problems}``, in words for the log; ``ALL`` for a
        problem every element shares. Empty = the layout is clean."""
        found: dict[str, list[str]] = {}

        def add(element: str, problem: str) -> None:
            found.setdefault(element, []).append(problem)

        for problem in _step_problems("x ticks", self.tick_steps, self.ticks_fit):
            add("x_ticks", problem)
        for problem in _step_problems("brackets", self.bracket_steps, self.brackets_fit):
            add("groups", problem)
        for step in self.legend_steps:
            if step not in HARMLESS_STEPS:
                add("legend", "legend moves below" if step == "below" else f"legend {step}s")
        if self.left_overlap_pt > OVERLAP_TOLERANCE_PT:
            for element in _LEFT_SIDE:
                add(element, f"y-side text overlaps the next panel by {self.left_overlap_pt:.1f}pt")
        if self.below_overlap_pt > OVERLAP_TOLERANCE_PT:
            for element in _BELOW_SIDE:
                add(element, f"x-side text overlaps the panel below by {self.below_overlap_pt:.1f}pt")
        for element, inches in self.overflow_by_element:
            if inches > OVERFLOW_TOLERANCE_IN:
                add(element, f"{element} runs {inches:.2f}in off the canvas")
        if self.unattributed_overflow_in > OVERFLOW_TOLERANCE_IN:
            add(ALL, f"content runs {self.unattributed_overflow_in:.2f}in off the canvas")
        if self.data_fraction < MIN_DATA_FRACTION:
            add(
                ALL,
                f"plot area {100 * self.data_fraction:.0f}% of the canvas "
                f"(< {100 * MIN_DATA_FRACTION:.0f}%)",
            )
        return {element: tuple(dict.fromkeys(p)) for element, p in found.items()}


def _step_problems(what: str, steps: tuple[str, ...], fits: bool) -> list[str]:
    found = []
    for step in steps:
        if step in HARMLESS_STEPS:
            continue
        if step.startswith("rotate_"):
            found.append(f"{what} rotate {step.removeprefix('rotate_')}°")
        elif step == "thin":
            found.append(f"{what} thinned")
        else:
            found.append(f"{what} {step}")
    if not fits:
        found.append(f"{what} overlap")
    return found


@dataclass(frozen=True)
class AutoTextSize:
    """The sizes an auto spec draws at, and how they were chosen. On
    ``ResolvedPlot.auto_text``; ``textsize.sizes_for`` reads ``sizes``."""

    target: str
    #: ``{element: pt}`` for every element auto sized (pinned ones are absent:
    #: their own number wins), plus ``"base"``: matplotlib's ``font.size`` for
    #: text that is no element (an annotation, "no data") = the smaller tick size.
    sizes: Mapping[str, float]
    #: ``{element: reason}``: why the next size up failed. Absent = at its ceiling.
    binding: Mapping[str, str] = field(default_factory=dict)
    #: Elements whose floor still had problems (the fitting handles them).
    at_floor: tuple[str, ...] = ()
    #: ``(sizes, blame)`` per layout, in the order tried.
    tried: tuple[tuple[Mapping[str, float], Mapping[str, tuple[str, ...]]], ...] = ()
    width_in: float = 0.0
    height_in: float = 0.0
    elapsed_s: float = 0.0

    @property
    def layouts(self) -> int:
        return len(self.tried)

    def describe(self) -> str:
        """The INFO line's body: each element's size and what limited it."""
        parts = []
        for element in ELEMENTS:
            if element not in self.sizes:
                continue
            why = self.binding.get(element)
            floor = " at floor" if element in self.at_floor else ""
            parts.append(
                f"{element}={_num(self.sizes[element])}"
                + (f" ({why}{floor})" if why else "")
            )
        return (
            f"{self.target} band at {self.width_in:.2f} x {self.height_in:.2f} in: "
            + ", ".join(parts)
            + f"; {self.layouts} layout(s) in {1000 * self.elapsed_s:.0f} ms"
        )

    def to_dict(self) -> dict:
        """``layout.meta.text_sizes.auto`` for the GUI."""
        return {
            "target": self.target,
            "sizes": dict(self.sizes),
            "binding": dict(self.binding),
            "at_floor": list(self.at_floor),
            "layouts": self.layouts,
            "ms": round(1000 * self.elapsed_s),
        }


# ---------------------------------------------------------------------------
# The search (pure)


def element_band(element: str, target: str) -> tuple[float, float]:
    """``(floor, ceiling)`` of one element in points."""
    band = BANDS[target]
    ceiling = round(band.ceiling_pt * CEILING_RATIOS[element], 3)
    return min(band.floor_pt, ceiling), ceiling


def candidates(floor: float, ceiling: float) -> list[float]:
    """The sizes tried, LARGEST first: the ceiling down in :data:`STEP_PT`
    steps, ending exactly on the floor."""
    if floor >= ceiling:
        return [ceiling]
    sizes = [ceiling]
    while sizes[-1] - STEP_PT > floor + 1e-6:
        sizes.append(round(sizes[-1] - STEP_PT, 3))
    sizes.append(round(floor, 3))
    return sizes


@dataclass
class _Element:
    """One element's binary search over its own candidates (largest first).

    ``fail`` is the largest index known to have problems (-1 = none known),
    ``ok`` the smallest index known clean (``len`` = none known yet). The
    answer lies in ``(fail, ok]``; done when they are neighbours. With no
    clean index found, ``ok`` stays ``len`` and the floor is the answer.
    """

    sizes: list[float]
    fail: int = -1
    ok: int = -1  # set in __post_init__
    reason: str = ""
    #: The verify pass found problems at the floor itself.
    floor_failed: bool = False

    def __post_init__(self) -> None:
        self.ok = len(self.sizes)

    @property
    def done(self) -> bool:
        return self.ok - self.fail <= 1

    @property
    def probe(self) -> int:
        return self.chosen_index if self.done else (self.fail + self.ok) // 2 if self.fail >= 0 else 0

    @property
    def chosen_index(self) -> int:
        return min(self.ok, len(self.sizes) - 1)

    @property
    def at_floor_with_problems(self) -> bool:
        return self.ok == len(self.sizes)


def choose_sizes(
    *,
    target: str,
    elements: tuple[str, ...],
    measure: Callable[[dict[str, float]], LayoutReport],
    width_in: float = 0.0,
    height_in: float = 0.0,
    max_layouts: int = 16,
) -> AutoTextSize:
    """Each element's largest candidate whose layout blames it for nothing.

    ``measure(sizes)`` lays the figure out with ``{element: pt}`` and reports.
    Every element runs its own binary search on the SAME layouts: each layout
    probes every unfinished element at its midpoint, so the cost is about
    ``log2(candidates) + 1`` layouts, not that per element. The first layout
    is everything at its ceiling (most figures fit there: 1 layout). Then the
    combination is verified, and whatever it still blames steps down one
    candidate, at most :data:`VERIFY_ROUNDS` times.
    """
    started = time.perf_counter()
    if not elements:
        # Every element fixed: only matplotlib's font.size is left to choose.
        return AutoTextSize(target=target, sizes=with_base({}, target),
                            width_in=width_in, height_in=height_in)
    search = {e: _Element(candidates(*element_band(e, target))) for e in elements}
    tried: list[tuple[dict[str, float], dict[str, tuple[str, ...]]]] = []

    def layout(sizes: dict[str, float]) -> dict[str, tuple[str, ...]]:
        t0 = time.perf_counter()
        report = measure(dict(sizes))
        blamed = report.blame()
        tried.append((dict(sizes), blamed))
        Log.debug(
            "auto text size layout %d: %s -> %s in %.0f ms",
            len(tried),
            _sizes_text(sizes),
            "; ".join(f"{e}: {', '.join(p)}" for e, p in blamed.items()) or "clean",
            1000 * (time.perf_counter() - t0),
            layer=LAYER,
        )
        return blamed

    def problems_of(element: str, blamed: dict) -> tuple[str, ...]:
        return blamed.get(element, ()) + blamed.get(ALL, ())

    while not all(s.done for s in search.values()) and len(tried) < max_layouts:
        probes = {e: s.probe for e, s in search.items()}
        blamed = layout({e: search[e].sizes[i] for e, i in probes.items()})
        for element, state in search.items():
            if state.done:
                continue
            index = probes[element]
            found = problems_of(element, blamed)
            if found:
                state.fail = index
                state.reason = found[0]
            else:
                state.ok = index

    chosen = {e: s.sizes[s.chosen_index] for e, s in search.items()}
    # The elements interact, so the combination itself is checked. Usually
    # this is the layout the search already ended on, and then it is free.
    for _ in range(VERIFY_ROUNDS):
        last = tried[-1] if tried else None
        blamed = last[1] if last is not None and last[0] == chosen else layout(chosen)
        stepped = False
        for element, state in search.items():
            found = problems_of(element, blamed)
            if not found:
                continue
            state.reason = found[0]
            if state.chosen_index < len(state.sizes) - 1:
                state.fail = state.chosen_index
                state.ok = state.fail + 1
                chosen[element] = state.sizes[state.chosen_index]
                stepped = True
            else:
                state.fail = state.ok = len(state.sizes) - 1
                state.floor_failed = True
        if not stepped or len(tried) >= max_layouts:
            break

    binding = {e: s.reason for e, s in search.items() if s.reason}
    at_floor = tuple(
        e for e, s in search.items() if s.at_floor_with_problems or s.floor_failed
    )
    # Brackets are read with the ticks above them: never larger than they are.
    if "groups" in chosen and "x_ticks" in chosen:
        chosen["groups"] = min(chosen["groups"], chosen["x_ticks"])
    chosen = with_base(chosen, target)
    return AutoTextSize(
        target=target,
        sizes=chosen,
        binding=binding,
        at_floor=at_floor,
        tried=tuple(tried),
        width_in=width_in,
        height_in=height_in,
        elapsed_s=time.perf_counter() - started,
    )


def auto_elements(text: TextSizes) -> tuple[str, ...]:
    """The elements auto sizes: every one the user did not fix."""
    return tuple(e for e in ELEMENTS if getattr(text, e) is None)


# ---------------------------------------------------------------------------
# Settling a resolved plot


def with_sizes(resolved: "ResolvedPlot", result: AutoTextSize) -> "ResolvedPlot":
    """``resolved`` drawn at ``result``, its difference-bar siblings too
    (their labels are sized from the siblings)."""
    return replace(
        resolved,
        auto_text=result,
        difference_siblings=[with_sizes(s, result) for s in resolved.difference_siblings],
    )


def at_size(resolved: "ResolvedPlot", width_in: float, height_in: float) -> "ResolvedPlot":
    """``resolved`` laid out at another size (the preview's pane)."""
    style = resolved.spec.style
    if (width_in, height_in) == (style.width, style.height):
        return resolved
    return replace(
        resolved,
        spec=replace(resolved.spec, style=replace(style, width=width_in, height=height_in)),
    )


def is_auto(resolved: "ResolvedPlot") -> bool:
    return resolved.spec.style.text.base is None


#: ``(id(resolved), width, height, text)`` -> ``(weakref(resolved), result)``.
#: One resolve is laid out by the preview's decisions, then drawn by plotly,
#: then perhaps saved: the search runs once. The weak reference guards
#: against a recycled id.
_MEMO: "OrderedDict[tuple, tuple]" = OrderedDict()
_MEMO_SIZE = 32


def settle(
    resolved: "ResolvedPlot",
    *,
    width_in: float | None = None,
    height_in: float | None = None,
    measure: Callable[["ResolvedPlot"], LayoutReport] | None = None,
) -> "ResolvedPlot":
    """``resolved`` with its auto text sizes chosen (``auto_text`` set).

    A fixed ``base``, or a plot already settled, comes back unchanged. The
    search lays the figure out at ``width_in x height_in`` (the spec's size
    when omitted); the result keeps the spec's own size. ``measure`` defaults
    to the matplotlib layout (``render.mpl.layout_report``). Without
    matplotlib nothing can be measured: the ceilings are used and that is
    WARNed.
    """
    if not is_auto(resolved) or resolved.auto_text is not None:
        return resolved
    text = resolved.spec.style.text
    style = resolved.spec.style
    width = float(width_in or style.width)
    height = float(height_in or style.height)
    key = (id(resolved), width, height, text)
    hit = _MEMO.get(key)
    if hit is not None and hit[0]() is resolved:
        _MEMO.move_to_end(key)
        Log.debug("auto text size: reused (%s)", _sizes_text(hit[1].sizes), layer=LAYER)
        return with_sizes(resolved, hit[1])

    elements = auto_elements(text)
    if measure is None:
        try:
            from .render.mpl import layout_report as measure
        except ImportError as exc:
            ceilings = with_base({e: element_band(e, text.target)[1] for e in elements}, text.target)
            Log.warn(
                "auto text size: matplotlib is not available (%s); drawing every "
                "element at its %s ceiling, unmeasured",
                exc,
                text.target,
                layer=LAYER,
            )
            return with_sizes(resolved, AutoTextSize(target=text.target, sizes=ceilings))

    sized = at_size(resolved, width, height)

    def trial(sizes: dict[str, float]) -> LayoutReport:
        candidate = with_sizes(sized, AutoTextSize(target=text.target, sizes=with_base(sizes, text.target)))
        with Log.trial(f"auto-size trial {_sizes_text(sizes)}"):
            return measure(candidate)

    result = choose_sizes(
        target=text.target, elements=elements, measure=trial, width_in=width, height_in=height
    )
    Log.info(
        "auto text size%s: %s",
        f" ({resolved.figure_label})" if resolved.figure_label else "",
        result.describe(),
        layer=LAYER,
    )
    _MEMO[key] = (weakref.ref(resolved), result)
    while len(_MEMO) > _MEMO_SIZE:
        _MEMO.popitem(last=False)
    return with_sizes(resolved, result)


def apply(resolved: "ResolvedPlot", result: AutoTextSize | None) -> "ResolvedPlot":
    """``resolved`` drawn at a result already chosen (the preview applies the
    decisions' result rather than searching again). A fixed size wins."""
    if result is None or not is_auto(resolved):
        return resolved
    return with_sizes(resolved, result)


def text_sizes_meta(resolved: "ResolvedPlot", sizes) -> dict:
    """``layout.meta.text_sizes``: every element's size (``ResolvedSizes``),
    how the auto sizes were chosen (null when fixed), the target and the
    targets the GUI toggle offers."""
    auto = resolved.auto_text
    return {
        **sizes.to_dict(),
        "auto": auto.to_dict() if auto is not None else None,
        "target": resolved.spec.style.text.target,
        "targets": list(TEXT_TARGETS),
    }


def with_base(sizes: Mapping[str, float], target: str) -> dict[str, float]:
    """``sizes`` plus ``"base"``: matplotlib's ``font.size`` for text that is no
    element = the smaller tick size, else the band's ceiling. The one rule."""
    ticks = [sizes[e] for e in ("x_ticks", "y_ticks") if e in sizes]
    return {**sizes, "base": min(ticks) if ticks else BANDS[target].ceiling_pt}


def _sizes_text(sizes: Mapping[str, float]) -> str:
    return " ".join(f"{e}={_num(v)}" for e, v in sizes.items() if e != "base")


def _num(value: float) -> str:
    return f"{value:g}"
