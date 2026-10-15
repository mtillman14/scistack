"""
The automatic text size: ONE reference, as large as the figure lays it out
cleanly, every element keeping its intended ratio to it.

``TextSizes.base = None`` means AUTO (user, 2026-10-06). This module is the ONE
owner of the reference that replaces it. :func:`settle` searches the reference
(:func:`choose_reference`); every element is then drawn at
``textsize.drawn_sizes(reference)`` = ``reference x ratio`` clamped into its
floor/ceiling. The result rides on ``ResolvedPlot.auto_text`` and every
renderer reads it through ``textsize.sizes_for``. Nothing else turns ``None``
into sizes, so the preview, the saved file and the generated code cannot
choose differently.

**One dial, not a search per element** (user, 2026-10-14; ADR D-2026-10-14-1,
superseding D-2026-10-06-2's per-element search): one failing rule anywhere
steps ALL the scaled text down together, so the proportions the user intended
hold. A clamp (an element held at its floor or ceiling) changes what is drawn,
never the stored ratio.

**Why a search over real layouts and not a formula:** what makes text too big
is the labels this figure has, at this size. Long tick names, a 14-entry
legend, a facet grid's y titles all read out of the layout the export already
measures (``ticklabels.fit_labels``, the legend fit, ``GridReach``,
``mpl.drawn_ticks``).

A layout is clean when every rule passes (:meth:`LayoutReport.failures`):

1. no text runs into text: the categorical x tick fit needs no shrink, thin
   or rotation; numeric x and y drawn labels do not collide; nothing runs
   into the neighbouring panel (``GridReach``); the bracket rows fit; the
   legend fits at the right;
2. no text runs off the canvas (drawn labels only);
3. every panel shows at least :data:`MIN_TICK_LABELS` y tick labels;
4. every numeric x axis shows at least :data:`MIN_TICK_LABELS` x tick labels;
5. no y title is longer than its panel is tall.

At the bottom of the range with rules still failing, the bottom is drawn, the
existing fitting (rotate / thin / legend below) handles the rest, and the
INFO line names the rules that still fail.

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
from .textsize import BANDS, ELEMENTS, drawn_sizes, reference_range

if TYPE_CHECKING:
    from .resolved import ResolvedPlot

LAYER = "scistackplot"

#: The candidate references are this far apart (user, 2026-10-14).
STEP_PT = 0.5

#: Rules 3 and 4: a panel's y axis (and a numeric x axis) must keep at least
#: this many tick labels. matplotlib's locator drops ticks as the label font
#: grows, so a short panel at a big size can be left with one number.
MIN_TICK_LABELS = 3

#: Neighbouring text may touch by this much before it counts as overlap.
OVERLAP_TOLERANCE_PT = 0.5
#: Content past the canvas edge, as ``figure_file.OVERFLOW_TOLERANCE_IN``.
OVERFLOW_TOLERANCE_IN = 0.01

#: Fit steps that change nothing a reader loses: wrapping onto two lines,
#: dropping a numbered prefix, a wrapped legend title or shorter line samples.
#: Every OTHER step a fit reports is a failure.
HARMLESS_STEPS = frozenset({"strip_prefix", "wrap", "wrap_title", "short_handles", "pinned"})


@dataclass(frozen=True)
class LayoutReport:
    """What one layout needed, read off the rendered figure
    (``render.mpl.layout_report``)."""

    #: ``LabelFit.steps`` of the categorical x ticks / bracket rows, and
    #: whether they fit.
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
    #: Panels (``"(row,col)"``) whose numeric x / y drawn tick labels collide.
    x_tick_collisions: tuple[str, ...] = ()
    y_tick_collisions: tuple[str, ...] = ()
    #: ``(panel, count)`` of the panel showing the fewest drawn y tick labels,
    #: and of the numeric x axis showing the fewest; None = none measured.
    fewest_y_ticks: tuple[str, int] | None = None
    fewest_x_ticks: tuple[str, int] | None = None
    #: ``(panel, pt)``: a y title longer than its panel is tall, by how much.
    y_title_overruns: tuple[tuple[str, float], ...] = ()

    def failures(self) -> tuple[str, ...]:
        """Every rule this layout breaks, in words for the log. Empty = clean."""
        found: list[str] = []
        # 1. Text into text.
        found.extend(_step_problems("x ticks", self.tick_steps, self.ticks_fit))
        found.extend(_step_problems("brackets", self.bracket_steps, self.brackets_fit))
        for step in self.legend_steps:
            if step not in HARMLESS_STEPS:
                found.append("legend moves below" if step == "below" else f"legend {step}s")
        if self.x_tick_collisions:
            found.append(f"x tick labels collide in panel {', '.join(self.x_tick_collisions)}")
        if self.y_tick_collisions:
            found.append(f"y tick labels collide in panel {', '.join(self.y_tick_collisions)}")
        if self.left_overlap_pt > OVERLAP_TOLERANCE_PT:
            found.append(f"y-side text overlaps the next panel by {self.left_overlap_pt:.1f}pt")
        if self.below_overlap_pt > OVERLAP_TOLERANCE_PT:
            found.append(f"x-side text overlaps the panel below by {self.below_overlap_pt:.1f}pt")
        # 2. Off the canvas.
        for element, inches in self.overflow_by_element:
            if inches > OVERFLOW_TOLERANCE_IN:
                found.append(f"{element} runs {inches:.2f}in off the canvas")
        if self.unattributed_overflow_in > OVERFLOW_TOLERANCE_IN:
            found.append(f"content runs {self.unattributed_overflow_in:.2f}in off the canvas")
        # 3. Too tall / 4. too wide.
        if self.fewest_y_ticks is not None and self.fewest_y_ticks[1] < MIN_TICK_LABELS:
            panel, count = self.fewest_y_ticks
            found.append(f"panel {panel} shows {count} y tick label(s) (< {MIN_TICK_LABELS})")
        if self.fewest_x_ticks is not None and self.fewest_x_ticks[1] < MIN_TICK_LABELS:
            panel, count = self.fewest_x_ticks
            found.append(f"panel {panel} shows {count} x tick label(s) (< {MIN_TICK_LABELS})")
        # 5. A y title taller than its panel.
        for panel, over in self.y_title_overruns:
            if over > OVERLAP_TOLERANCE_PT:
                found.append(f"y title of panel {panel} is {over:.1f}pt taller than the panel")
        return tuple(dict.fromkeys(found))


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
    """The reference an auto spec draws at, and how it was chosen. On
    ``ResolvedPlot.auto_text``; ``textsize.sizes_for`` reads ``reference``."""

    target: str
    #: The chosen reference (pt). Every scaled element is drawn at
    #: ``textsize.drawn_sizes(reference)``.
    reference: float
    #: ``(bottom, top)`` searched; None when every element is fixed.
    span: tuple[float, float] | None = None
    #: ``{element: pt}`` drawn at ``reference`` (fixed ones as typed), for the
    #: log and the GUI. Derived; never written into the spec.
    sizes: Mapping[str, float] = field(default_factory=dict)
    #: ``{element: "floor" | "ceiling"}`` held at a bound at ``reference``.
    clamped: Mapping[str, str] = field(default_factory=dict)
    #: The rules that failed at the next step UP (why it is not larger).
    #: Empty = at the top of the range.
    limit: tuple[str, ...] = ()
    #: The rules that still fail at ``reference`` (only at the bottom: the
    #: fitting handles them). Empty = clean.
    failing: tuple[str, ...] = ()
    #: ``(reference, failures)`` per layout, in the order tried.
    tried: tuple[tuple[float, tuple[str, ...]], ...] = ()
    width_in: float = 0.0
    height_in: float = 0.0
    elapsed_s: float = 0.0

    @property
    def layouts(self) -> int:
        return len(self.tried)

    @property
    def at_bottom(self) -> bool:
        return self.span is not None and self.reference <= self.span[0] + 1e-9

    def describe(self) -> str:
        """The INFO line's body: the reference, each element's drawn size
        (clamped ones marked), and what limited it."""
        parts = []
        for element in ELEMENTS:
            if element not in self.sizes:
                continue
            held = f" (clamped {self.clamped[element]})" if element in self.clamped else ""
            parts.append(f"{element}={_num(self.sizes[element])}{held}")
        if self.failing:
            why = f"at the bottom, still failing: {'; '.join(self.failing)}"
        elif self.limit:
            why = f"next step up fails: {'; '.join(self.limit)}"
        else:
            why = "at the top"
        return (
            f"{self.target} at {self.width_in:.2f} x {self.height_in:.2f} in: "
            f"reference {_num(self.reference)}pt"
            + (f" of {_num(self.span[0])}-{_num(self.span[1])}" if self.span else "")
            + f" ({why}); "
            + ", ".join(parts)
            + f"; {self.layouts} layout(s) in {1000 * self.elapsed_s:.0f} ms"
        )

    def to_dict(self) -> dict:
        """``layout.meta.text_sizes.auto`` for the GUI."""
        return {
            "target": self.target,
            "reference": self.reference,
            "span": list(self.span) if self.span else None,
            "sizes": dict(self.sizes),
            "clamped": dict(self.clamped),
            "limit": list(self.limit),
            "failing": list(self.failing),
            "at_bottom": self.at_bottom,
            "layouts": self.layouts,
            "ms": round(1000 * self.elapsed_s),
        }


# ---------------------------------------------------------------------------
# The search (pure)


def candidates(bottom: float, top: float) -> list[float]:
    """The references tried, LARGEST first: the top down in :data:`STEP_PT`
    steps, ending exactly on the bottom."""
    if bottom >= top:
        return [top]
    refs = [top]
    while refs[-1] - STEP_PT > bottom + 1e-6:
        refs.append(round(refs[-1] - STEP_PT, 3))
    refs.append(round(bottom, 3))
    return refs


def choose_reference(
    *,
    text: TextSizes,
    measure: Callable[[float], LayoutReport],
    width_in: float = 0.0,
    height_in: float = 0.0,
) -> AutoTextSize:
    """The largest reference whose layout breaks no rule.

    ``measure(reference)`` lays the figure out at that reference and reports.
    A binary search over :func:`candidates`: the first layout is the top
    (most figures fit there: 1 layout), then about ``log2(candidates)`` more.
    Larger text never makes a layout cleaner, so the clean references are a
    run at the bottom of the list's order.
    """
    started = time.perf_counter()
    span = reference_range(text)
    if span is None:
        # Every element fixed: the reference only sizes text that is no element.
        reference = BANDS[text.target].ceiling_pt
        sizes, clamped = drawn_sizes(reference, text)
        return AutoTextSize(
            target=text.target, reference=reference, sizes=sizes, clamped=clamped,
            width_in=width_in, height_in=height_in,
        )
    refs = candidates(*span)
    tried: list[tuple[float, tuple[str, ...]]] = []
    seen: dict[int, tuple[str, ...]] = {}

    def probe(index: int) -> tuple[str, ...]:
        t0 = time.perf_counter()
        failures = measure(refs[index]).failures()
        seen[index] = failures
        tried.append((refs[index], failures))
        Log.debug(
            "auto text size layout %d: reference %gpt -> %s in %.0f ms",
            len(tried),
            refs[index],
            "; ".join(failures) or "clean",
            1000 * (time.perf_counter() - t0),
            layer=LAYER,
        )
        return failures

    # The answer lies in (fail, ok]: fail = largest index known to fail,
    # ok = smallest known clean (len = none known).
    fail, ok = -1, len(refs)
    if probe(0):
        fail = 0
        while ok - fail > 1:
            mid = (fail + ok) // 2
            if probe(mid):
                fail = mid
            else:
                ok = mid
    else:
        ok = 0
    chosen = min(ok, len(refs) - 1)
    reference = refs[chosen]
    failing = seen.get(chosen, ()) if ok == len(refs) else ()
    limit = seen.get(chosen - 1, ()) if chosen > 0 else ()
    sizes, clamped = drawn_sizes(reference, text)
    return AutoTextSize(
        target=text.target,
        reference=reference,
        span=span,
        sizes=sizes,
        clamped=clamped,
        limit=limit,
        failing=failing,
        tried=tuple(tried),
        width_in=width_in,
        height_in=height_in,
        elapsed_s=time.perf_counter() - started,
    )


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
    """``resolved`` with its auto reference chosen (``auto_text`` set).

    A fixed ``base``, or a plot already settled, comes back unchanged. The
    search lays the figure out at ``width_in x height_in`` (the spec's size
    when omitted); the result keeps the spec's own size. ``measure`` defaults
    to the matplotlib layout (``render.mpl.layout_report``). Without
    matplotlib nothing can be measured: the top of the range is used and that
    is WARNed.
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
        Log.debug("auto text size: reused (reference %gpt)", hit[1].reference, layer=LAYER)
        return with_sizes(resolved, hit[1])

    if measure is None:
        try:
            from .render.mpl import layout_report as measure
        except ImportError as exc:
            span = reference_range(text)
            top = span[1] if span else BANDS[text.target].ceiling_pt
            sizes, clamped = drawn_sizes(top, text)
            Log.warn(
                "auto text size: matplotlib is not available (%s); drawing at the "
                "%s top reference %gpt, unmeasured",
                exc,
                text.target,
                top,
                layer=LAYER,
            )
            return with_sizes(
                resolved,
                AutoTextSize(target=text.target, reference=top, span=span, sizes=sizes, clamped=clamped),
            )

    sized = at_size(resolved, width, height)

    def trial(reference: float) -> LayoutReport:
        candidate = with_sizes(sized, AutoTextSize(target=text.target, reference=reference))
        with Log.trial(f"auto-size trial reference {reference:g}pt"):
            return measure(candidate)

    result = choose_reference(text=text, measure=trial, width_in=width, height_in=height)
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
    """``layout.meta.text_sizes``: every element's drawn size
    (``ResolvedSizes``), how the auto reference was chosen (null when fixed),
    the target and the targets the GUI toggle offers."""
    auto = resolved.auto_text
    return {
        **sizes.to_dict(),
        "auto": auto.to_dict() if auto is not None else None,
        "target": resolved.spec.style.text.target,
        "targets": list(TEXT_TARGETS),
    }


def _num(value: float) -> str:
    return f"{value:g}"
