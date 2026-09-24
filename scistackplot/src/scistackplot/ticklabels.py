"""
Fitting axis labels into the room they have.

Tick labels and nested-axis bracket labels overlapped in saved figures
(spec/images/graph1.png, graph2.png): nothing ever compared a label's width
with the space between its tick and the next. This module is the ONE place
that decides what to do about it, and it is pure — labels, positions and a
``measure`` callable in, a :class:`LabelFit` out. No figure, no renderer.

That split is the same bargain :mod:`~scistackplot.xaxis` makes: the renderers
supply the measurements (matplotlib measures real text; the plotly preview
estimates it), and both apply the same decision, so they cannot each invent
their own rule.

The decision walks a ladder from least to most destructive and stops at the
first step where no two labels overlap:

``strip_prefix``
    NUMBERED labels only (:func:`is_numbered`: every label ends in digits,
    ``SS01..SS36``): drop the prefix they all share -> ``01..36``. Always
    applied first when enabled. Names are never touched (``L_HAM``/``R_HAM``
    stay as they are), and the cut is only at a natural boundary.
``wrap``
    Break at ``_ - / . space`` onto at most ``max_lines`` lines.
``shrink``
    Smaller font, never below :meth:`LabelPolicy.font_floor`.
``rotate_<deg>``
    45 then 90 degrees, single-line, largest font that fits.
``thin``
    NUMBERED labels only: show every k-th (first and last of each group
    always kept). A hidden ``07`` can be read off ``06`` and ``08``; a hidden
    name cannot, so names stop at 90 degrees and report ``fits=False``.

**One decision for many rows.** A faceted figure passes one :class:`LabelRow`
per panel; a candidate fits only if it fits in every row, so every panel of a
figure reads the same way (the reason the renderers used to pin rotation at 0).

Units are points throughout; nothing here knows about pixels or inches.
"""

from __future__ import annotations

import itertools
import math
import re
from dataclasses import dataclass
from typing import Callable, Sequence

from scistacklog import Log

LAYER = "scistackplot"

#: ``measure(text, font_pt) -> (width_pt, height_pt)`` for ONE line of text.
Measure = Callable[[str, float], "tuple[float, float]"]

#: Characters a label may be broken after (wrap) or cut at (affix strip).
SEPARATORS = " _-/."

#: Line pitch of multi-line text as a multiple of one line's height —
#: matplotlib's default ``Text`` linespacing, so a wrapped label is measured
#: at the height the export will draw it.
LINE_SPACING = 1.2

#: More words than this and :func:`wrap_label` only considers even splits
#: rather than every combination of break points.
_MAX_WRAP_TOKENS = 24

#: Overlaps smaller than this are rounding, not collisions.
_EPS = 1e-6


@dataclass(frozen=True)
class LabelRow:
    """One axis' labels and where each sits.

    ``labels[i] == ""`` is a position with no label (a spacer on a nested
    axis): it takes no room, so its neighbours may spread into it.
    """

    labels: Sequence[str]
    #: Centre of each label along the axis, in points, ascending.
    positions: Sequence[float]
    #: Inclusive ``(start, end)`` index spans thinning must respect: each span
    #: keeps its own first and last label. Empty = the whole row is one span.
    groups: Sequence[tuple[int, int]] = ()

    def __post_init__(self) -> None:
        if len(self.labels) != len(self.positions):
            raise ValueError(
                f"LabelRow: {len(self.labels)} labels but "
                f"{len(self.positions)} positions"
            )


def slot_row(
    labels: Sequence[str], slot_pt: float, groups: Sequence[tuple[int, int]] = ()
) -> LabelRow:
    """A row of evenly spaced labels, ``slot_pt`` apart (a categorical axis)."""
    return LabelRow(
        labels=list(labels),
        positions=[(i + 0.5) * slot_pt for i in range(len(labels))],
        groups=list(groups),
    )


@dataclass(frozen=True)
class LabelPolicy:
    """Which steps of the ladder are allowed, and their bounds."""

    #: Absolute legibility floor for the font.
    min_font_pt: float = 8.0
    #: ... and never below this fraction of the requested font either, so a
    #: 14pt figure does not quietly drop to 8pt labels.
    min_font_ratio: float = 0.7
    font_step_pt: float = 0.5
    max_lines: int = 2
    rotations: tuple[int, ...] = (45, 90)
    allow_thinning: bool = True
    strip_prefix: bool = True
    #: Clear space required between neighbouring labels.
    pad_pt: float = 2.0
    #: Settings the user fixed (``StyleOptions.tick_rotation`` /
    #: ``tick_font_size`` / ``tick_every``). A pinned value is used as given
    #: and never searched; the rest of the ladder still runs around it.
    #: A pinned ``every`` thins even names: the user asked for it.
    pin_rotation: int | None = None
    pin_font_pt: float | None = None
    pin_every: int | None = None

    @property
    def pinned(self) -> bool:
        return (
            self.pin_rotation is not None
            or self.pin_font_pt is not None
            or self.pin_every is not None
        )

    def font_floor(self, font_pt: float) -> float:
        """Smallest font the ladder may use. Never above the requested font."""
        return min(font_pt, max(self.min_font_pt, self.min_font_ratio * font_pt))


#: Tick labels: every step allowed.
TICK_POLICY = LabelPolicy()

#: Bracket labels on a nested axis: they sit under a span, so rotating or
#: dropping one would detach it from the bars it names. Shrink and wrap only,
#: and never strip: a bracket is read on its own.
BRACKET_POLICY = LabelPolicy(rotations=(), allow_thinning=False, strip_prefix=False)


@dataclass(frozen=True)
class LabelFit:
    """What to draw. Renderers apply it; they do not second-guess it."""

    #: Display text per row, per position: stripped, wrapped (``"\\n"``), and
    #: ``""`` where the label is hidden by thinning or was a spacer.
    rows: tuple[tuple[str, ...], ...]
    font_pt: float
    #: Degrees counter-clockwise; 0 is upright. A rotated label is
    #: right-anchored at its tick.
    rotation: int = 0
    #: 1 = every label shown; k = every k-th (see :func:`_thin_indices`).
    every: int = 1
    #: The shared prefix dropped from numbered labels ("SS"), or "".
    prefix: str = ""
    wrapped: bool = False
    #: False when the last allowed step still overlaps — the renderer WARNs.
    fits: bool = True
    #: Largest remaining overlap in points (<= 0 when it fits).
    worst_overlap_pt: float = 0.0
    #: The ladder steps that changed something, in order, e.g.
    #: ``("strip_prefix", "rotate_45")``.
    steps: tuple[str, ...] = ()

    def describe(self) -> str:
        """One log-line fragment naming the decision."""
        parts = []
        if self.prefix:
            parts.append(f"strip {self.prefix!r}")
        if self.wrapped:
            parts.append("wrap")
        parts.append(f"font {self.font_pt:g}pt")
        if self.rotation:
            parts.append(f"rotate {self.rotation}°")
        if self.every > 1:
            parts.append(f"every {self.every}")
        if not self.fits:
            parts.append(f"STILL OVERLAPS by {self.worst_overlap_pt:.1f}pt")
        return ", ".join(parts)


# ---------------------------------------------------------------------------
# Numbered labels and their shared prefix


#: A numbered label: wholly digits, or ending in a digit run (``07``,
#: ``SS07``, ``Trial_12``). The one test for both stripping and thinning.
_NUMBERED = re.compile(r"\d+$")


def is_numbered(labels: Sequence[str]) -> bool:
    """Whether every non-empty label is, or ends in, a number.

    Such labels are a sequence: a stripped ``SS`` or a hidden ``07`` can be
    read off the neighbours. A name (``L_HAM``, ``Digitimer``) cannot, so the
    ladder never strips or thins one.
    """
    present = [label for label in labels if label]
    return bool(present) and all(_NUMBERED.search(label) for label in present)


def _char_class(char: str) -> int:
    if char.isdigit():
        return 1
    if char.isalpha():
        return 0
    return 2


def _is_boundary(text: str, index: int) -> bool:
    """Whether ``text`` may be cut between ``index - 1`` and ``index``."""
    if index <= 0 or index >= len(text):
        return False
    before, after = text[index - 1], text[index]
    if before in SEPARATORS or after in SEPARATORS:
        return True
    return _char_class(before) != _char_class(after)


def _common_prefix(labels: Sequence[str]) -> str:
    first, last = min(labels), max(labels)
    n = 0
    while n < min(len(first), len(last)) and first[n] == last[n]:
        n += 1
    return first[:n]


def strip_prefix(labels: Sequence[str]) -> tuple[list[str], str]:
    """Drop the prefix every numbered label shares: ``SS01, SS03 -> 01, 03``.

    Returns ``(labels, prefix)``; ``""`` labels are left as they are. Nothing
    is stripped unless every label is numbered (:func:`is_numbered`) and there
    are at least two distinct ones. The cut is the longest shared prefix that
    ends at a natural boundary (after a separator, or where letters turn to
    digits) and leaves no label starting with a separator — so ``SS01``/``SS10``
    lose ``SS`` and ``01``/``02``/``10`` lose nothing. Distinct labels that
    share a prefix stay distinct without it, and a boundary is never at a
    label's end, so no label is merged or blanked.
    """
    distinct = sorted({label for label in labels if label})
    if len(distinct) < 2 or not is_numbered(distinct):
        return list(labels), ""
    for cut in range(len(_common_prefix(distinct)), 0, -1):
        if all(
            _is_boundary(label, cut) and label[cut] not in SEPARATORS
            for label in distinct
        ):
            prefix = distinct[0][:cut]
            return [label[cut:] if label else label for label in labels], prefix
    return list(labels), ""


# ---------------------------------------------------------------------------
# Wrapping


def _tokens(text: str) -> list[str]:
    """Words with their trailing separator attached (``"Six_"``, ``"MWT"``)."""
    sep = re.escape(SEPARATORS)
    return re.findall(rf"[^{sep}]+[{sep}]*|[{sep}]+", text)


def wrap_label(
    text: str, font_pt: float, measure: Measure, max_lines: int = 2
) -> str:
    """``text`` broken at natural boundaries so its widest line is narrowest.

    A label with no break point (``"Digitimer"``) comes back unchanged. Spaces
    at a break are dropped; ``_`` and ``-`` stay on the line they end, so the
    reader still sees the label was one word.
    """
    tokens = _tokens(text)
    if len(tokens) < 2 or max_lines < 2:
        return text

    def lines_for(cuts: Sequence[int]) -> list[str]:
        bounds = [0, *cuts, len(tokens)]
        return [
            "".join(tokens[a:b]).rstrip(" ") for a, b in zip(bounds, bounds[1:])
        ]

    def widest(lines: Sequence[str]) -> float:
        return max(measure(line, font_pt)[0] for line in lines)

    best = [text]
    best_width = measure(text, font_pt)[0]
    n_lines = min(max_lines, len(tokens))
    for count in range(2, n_lines + 1):
        if len(tokens) > _MAX_WRAP_TOKENS:
            step = len(tokens) / count
            candidates = [[round(step * i) for i in range(1, count)]]
        else:
            candidates = itertools.combinations(range(1, len(tokens)), count - 1)
        for cuts in candidates:
            lines = lines_for(cuts)
            width = widest(lines)
            if width < best_width - _EPS:
                best, best_width = lines, width
    return "\n".join(best)


# ---------------------------------------------------------------------------
# Geometry


def label_block(text: str, font_pt: float, measure: Measure) -> tuple[float, float]:
    """``(width, height)`` of a possibly multi-line label."""
    lines = text.split("\n")
    sizes = [measure(line, font_pt) for line in lines]
    width = max(w for w, _ in sizes)
    line_height = max(h for _, h in sizes)
    height = line_height * (1 + LINE_SPACING * (len(lines) - 1))
    return width, height


def _pair_overlap(
    a: tuple[float, float],
    b: tuple[float, float],
    distance: float,
    rotation: int,
    pad: float,
) -> float:
    """How far two neighbouring labels overlap (<= 0: they do not).

    Upright, the labels are side by side: half of each width plus the pad
    must fit in the distance between their centres. Rotated by ``theta`` and
    anchored at their ticks, they are parallel strips whose centre lines are
    ``distance * sin(theta)`` apart, so only their HEIGHTS compete — which is
    why rotation rescues long labels but not crowded ones.
    """
    if rotation == 0:
        need = (a[0] + b[0]) / 2.0 + pad
        return need - distance
    need_perp = (a[1] + b[1]) / 2.0 + pad
    return need_perp / math.sin(math.radians(rotation)) - distance


def _worst_overlap(
    rows: Sequence[Sequence[str]],
    positions: Sequence[Sequence[float]],
    font_pt: float,
    rotation: int,
    pad: float,
    measure: Measure,
) -> float:
    worst = -math.inf
    for texts, where in zip(rows, positions):
        shown = [i for i, text in enumerate(texts) if text]
        for i, j in zip(shown, shown[1:]):
            overlap = _pair_overlap(
                label_block(texts[i], font_pt, measure),
                label_block(texts[j], font_pt, measure),
                where[j] - where[i],
                rotation,
                pad,
            )
            worst = max(worst, overlap)
    return 0.0 if worst == -math.inf else worst


# ---------------------------------------------------------------------------
# Thinning


def _spans(row: LabelRow) -> list[tuple[int, int]]:
    if row.groups:
        return [tuple(span) for span in row.groups]
    return [(0, len(row.labels) - 1)] if row.labels else []


def _thin_indices(
    texts: Sequence[str],
    spans: Sequence[tuple[int, int]],
    every: int,
    fits_pair: Callable[[int, int], bool],
) -> set[int]:
    """Indices kept when showing every ``every``-th label of each span.

    Counted over LABELLED positions (spacers do not count), from each span's
    first label. Its last label is always kept too; if it collides with the
    regular label before it, that one gives way — the ends of a group are
    what a reader anchors on.
    """
    kept: set[int] = set()
    for start, end in spans:
        labelled = [i for i in range(start, end + 1) if texts[i]]
        if not labelled:
            continue
        shown = labelled[::every]
        last = labelled[-1]
        if shown[-1] != last:
            if len(shown) > 1 and not fits_pair(shown[-1], last):
                shown.pop()
            shown.append(last)
        kept.update(shown)
    return kept


# ---------------------------------------------------------------------------
# The ladder


def fit_labels(
    rows: Sequence[LabelRow] | LabelRow,
    font_pt: float,
    measure: Measure,
    policy: LabelPolicy = TICK_POLICY,
) -> LabelFit:
    """Decide how to draw ``rows`` so no two labels overlap.

    One decision for every row — see the module docstring for the ladder.
    When nothing allowed fits, the least-overlapping attempt comes back with
    ``fits=False`` so the renderer can draw it and WARN.
    """
    if isinstance(rows, LabelRow):
        rows = [rows]
    rows = list(rows)
    memo: dict[tuple[str, float], tuple[float, float]] = {}

    def measured(text: str, size: float) -> tuple[float, float]:
        key = (text, size)
        if key not in memo:
            memo[key] = measure(text, size)
        return memo[key]

    positions = [list(row.positions) for row in rows]
    texts = [list(row.labels) for row in rows]
    steps: list[str] = []
    prefix = ""
    numbered = is_numbered([label for row in texts for label in row])

    if policy.strip_prefix and numbered:
        flat = [label for row in texts for label in row]
        stripped, prefix = strip_prefix(flat)
        if prefix:
            steps.append("strip_prefix")
            it = iter(stripped)
            texts = [[next(it) for _ in row] for row in texts]

    floor = policy.font_floor(font_pt)
    sizes = [font_pt]
    size = font_pt - policy.font_step_pt
    while size > floor + _EPS:
        sizes.append(round(size, 3))
        size -= policy.font_step_pt
    if floor < font_pt - _EPS:
        sizes.append(floor)

    #: The least-overlapping attempt so far, returned when nothing fits.
    best: dict = {"worst": math.inf}

    def attempt(step, candidate, size, rotation=0, **extra) -> LabelFit | None:
        worst = _worst_overlap(
            candidate, positions, size, rotation, policy.pad_pt, measured
        )
        Log.debug(
            "label fit: %s at %gpt/%d° leaves worst overlap %.1fpt",
            step, size, rotation, worst, layer=LAYER,
        )
        fit = dict(
            rows=tuple(tuple(row) for row in candidate),
            font_pt=size,
            rotation=rotation,
            prefix=prefix,
            worst_overlap_pt=worst,
            **extra,
        )
        if worst < best["worst"]:
            best.update(worst=worst, fit=fit)
        if worst > _EPS:
            return None
        return LabelFit(steps=tuple(steps), **fit)

    def wrapped_at(size: float) -> list[list[str]]:
        return [
            [wrap_label(t, size, measured, policy.max_lines) if t else t for t in row]
            for row in texts
        ]

    can_wrap = policy.max_lines > 1

    if policy.pinned:
        return _done(
            _fit_pinned(policy, rows, texts, positions, font_pt, sizes, steps,
                        numbered, attempt, wrapped_at, best, measured)
        )

    # upright, as given
    fit = attempt("upright", texts, font_pt)
    if fit:
        return _done(fit)

    # wrap
    if can_wrap:
        candidate = wrapped_at(font_pt)
        if candidate != texts:
            steps.append("wrap")
            fit = attempt("wrap", candidate, font_pt, wrapped=True)
            if fit:
                return _done(fit)
            steps.pop()

    # shrink — single-line preferred at each size, then wrapped
    for size in sizes[1:]:
        steps.append("shrink")
        fit = attempt("shrink", texts, size)
        if fit:
            return _done(fit)
        if can_wrap:
            steps.insert(-1, "wrap")
            fit = attempt("wrap+shrink", wrapped_at(size), size, wrapped=True)
            if fit:
                return _done(fit)
            steps.remove("wrap")
        steps.pop()

    # rotate — single line (wrapping only thickens a rotated strip), largest
    # font that fits
    for rotation in policy.rotations:
        for size in sizes:
            steps.append(f"rotate_{rotation}")
            if size < font_pt:
                steps.append("shrink")
            fit = attempt(f"rotate_{rotation}", texts, size, rotation)
            if fit:
                return _done(fit)
            del steps[-2 if size < font_pt else -1 :]

    # thin — the smallest k that fits, at the largest font for that k
    rotation = policy.rotations[-1] if policy.rotations else 0
    # Numbered labels only: a hidden name cannot be read off its neighbours.
    if policy.allow_thinning and numbered:
        n_max = max((len(row) for row in texts), default=1)
        for every in range(2, n_max + 1):
            for size in sizes:
                thinned = [
                    _thinned_row(row, row_texts, where, every, size, rotation,
                                 policy.pad_pt, measured)
                    for row, row_texts, where in zip(rows, texts, positions)
                ]
                added = [f"rotate_{rotation}"] if rotation else []
                added += ["shrink"] if size < font_pt else []
                added.append("thin")
                steps.extend(added)
                fit = attempt(
                    f"thin every {every}", thinned, size, rotation,
                    every=every,
                )
                if fit:
                    return _done(fit)
                del steps[-len(added):]

    fit = LabelFit(steps=tuple(steps), fits=False, **best["fit"])
    return _done(fit)


def _thinned_row(row, texts, where, every, size, rotation, pad, measured):
    def fits_pair(i: int, j: int) -> bool:
        return _pair_overlap(
            label_block(texts[i], size, measured),
            label_block(texts[j], size, measured),
            where[j] - where[i],
            rotation,
            pad,
        ) <= _EPS

    kept = _thin_indices(texts, _spans(row), every, fits_pair)
    return [t if i in kept else "" for i, t in enumerate(texts)]


def _done(fit: LabelFit) -> LabelFit:
    Log.debug(
        "label fit: %s (steps: %s)",
        fit.describe(),
        ", ".join(fit.steps) or "none",
        layer=LAYER,
    )
    return fit


def _fit_pinned(
    policy, rows, texts, positions, font_pt, sizes, steps, numbered, attempt,
    wrapped_at, best, measured,
) -> LabelFit:
    """The ladder with the user's fixed settings held fixed.

    Same order of preference as the ladder (fewest labels hidden, then
    upright, then the largest font), searching only what was NOT pinned. When
    nothing fits, the pinned settings still win: the least-overlapping attempt
    comes back with ``fits=False``, never a quietly changed pin.
    """
    steps.append("pinned")
    fonts = [float(policy.pin_font_pt)] if policy.pin_font_pt else sizes
    rotations = (
        [int(policy.pin_rotation)] if policy.pin_rotation is not None else [0, *policy.rotations]
    )
    if policy.pin_every:
        everys = [int(policy.pin_every)]
    elif policy.allow_thinning and numbered:
        everys = list(range(1, max((len(row) for row in texts), default=1) + 1))
    else:
        everys = [1]
    for every in everys:
        for rotation in rotations:
            for size in fonts:
                variants = [(texts, False)]
                if rotation == 0 and policy.max_lines > 1:
                    variants.append((wrapped_at(size), True))
                for candidate, wrapped in variants:
                    if every > 1:
                        candidate = [
                            _thinned_row(row, labels, where, every, size, rotation,
                                         policy.pad_pt, measured)
                            for row, labels, where in zip(rows, candidate, positions)
                        ]
                    fit = attempt(
                        "pinned", candidate, size, rotation, every=every, wrapped=wrapped
                    )
                    if fit:
                        return fit
    return LabelFit(steps=tuple(steps), fits=False, **best["fit"])
