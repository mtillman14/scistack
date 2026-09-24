"""
The size of every piece of text in a figure — the ONE owner.

``StyleOptions.text`` (:class:`~scistackplot.spec.TextSizes`) says what the
user fixed; :func:`resolve_sizes` turns it into a point size per element, and
every consumer reads that: ``render_matplotlib`` (through :func:`rc_params`
plus the few explicit sizes matplotlib has no rc key for), the generated
seaborn code (the same :func:`rc_params` dict, emitted as a literal), and the
plotly preview (1 pt = 1 px). Nothing else derives a size from ``base``: a
renderer that multiplies ``base`` by its own ratio is a second owner, which is
how the plotly bracket labels came to be ``0.8 x`` while matplotlib's were
``small`` (0.833 x).

Pure, no matplotlib import: the ratios below ARE matplotlib's
``font_manager.font_scalings``, pinned by ``tests/test_textsize.py`` against
the real thing, so an unset size here is exactly what matplotlib's relative
name would have resolved to. See docs/claude/plot-text-and-labels.md.
"""

from __future__ import annotations

from dataclasses import dataclass, fields

from .spec import StyleOptions, TextSizes

#: matplotlib's ``font_scalings`` for the relative names its rc defaults use:
#: ``axes.labelsize`` / ``x|ytick.labelsize`` / ``legend.fontsize`` are
#: ``medium``, ``figure.titlesize`` / ``axes.titlesize`` are ``large``, and the
#: bracket rows were drawn ``small``.
MEDIUM = 1.0
LARGE = 1.2
SMALL = 0.833


@dataclass(frozen=True)
class ResolvedSizes:
    """Every element's size in points, and which of them the user fixed."""

    base: float
    title: float
    x_label: float
    y_label: float
    x_ticks: float
    y_ticks: float
    groups: float
    legend: float
    #: ``None`` when unset: the title follows the legend entries WHEREVER they
    #: end up, including after the legend fit shrank them — a number fixed
    #: here would leave a big title over shrunken entries.
    legend_title: float | None
    #: The elements set explicitly. Fitting never shrinks these.
    pinned: frozenset[str] = frozenset()

    def is_pinned(self, element: str) -> bool:
        return element in self.pinned

    def legend_title_for(self, entry_pt: float) -> float:
        """The legend title's size when the entries are drawn at ``entry_pt``."""
        return self.legend_title if self.legend_title is not None else entry_pt

    def to_dict(self) -> dict:
        """JSON for the GUI (``layout.meta.text_sizes``): every element's size,
        and which are fixed. ``legend_title`` is null when it follows the
        entries."""
        return {
            **{name: getattr(self, name) for name in _ELEMENTS},
            "pinned": sorted(self.pinned),
        }

    def describe(self) -> str:
        """One line for the log: every size, pinned ones marked."""
        parts = []
        for name in _ELEMENTS:
            value = getattr(self, name)
            if value is None:
                parts.append(f"{name}=legend")
                continue
            mark = "*" if name in self.pinned else ""
            parts.append(f"{name}={_num(value)}{mark}")
        return " ".join(parts) + ("  (* fixed)" if self.pinned else "")


#: Every element, in the order the log states them.
_ELEMENTS = tuple(f.name for f in fields(ResolvedSizes) if f.name != "pinned")


def resolve_sizes(style: StyleOptions | TextSizes) -> ResolvedSizes:
    """Every element's size in points, derived where the user set none."""
    text = style.text if isinstance(style, StyleOptions) else style
    base = float(text.base)

    def pick(value: float | None, derived: float) -> float:
        return float(value) if value is not None else round(derived, 3)

    x_ticks = pick(text.x_ticks, base * MEDIUM)
    legend = pick(text.legend, base * MEDIUM)
    pinned = frozenset(
        f.name for f in fields(TextSizes) if f.name != "base" and getattr(text, f.name) is not None
    )
    return ResolvedSizes(
        base=base,
        title=pick(text.title, base * LARGE),
        x_label=pick(text.x_label, base * MEDIUM),
        y_label=pick(text.y_label, base * MEDIUM),
        x_ticks=x_ticks,
        y_ticks=pick(text.y_ticks, base * MEDIUM),
        # Unset brackets scale with the ticks they sit under, so a fixed tick
        # size moves them too — they are read as one block.
        groups=pick(text.groups, x_ticks * SMALL),
        legend=legend,
        legend_title=float(text.legend_title) if text.legend_title is not None else None,
        pinned=pinned,
    )


def rc_params(sizes: ResolvedSizes) -> dict[str, float]:
    """The ``rc_context`` every matplotlib text size flows from.

    Absolute points, not relative names, so a size never depends on which rc
    happens to be in force. ``axes.labelsize`` is the x title's size:
    matplotlib has one key for both axis titles, so the y title is passed
    explicitly (``fontsize=sizes.y_label``) wherever one is set.
    ``legend.title_fontsize`` is only stated when fixed; matplotlib's own
    ``None`` already means "the entries' size".
    """
    params = {
        "font.size": sizes.base,
        "figure.titlesize": sizes.title,
        "axes.titlesize": sizes.title,
        "axes.labelsize": sizes.x_label,
        "xtick.labelsize": sizes.x_ticks,
        "ytick.labelsize": sizes.y_ticks,
        "legend.fontsize": sizes.legend,
    }
    if sizes.legend_title is not None:
        params["legend.title_fontsize"] = sizes.legend_title
    return params


def _num(value: float) -> str:
    return f"{value:g}"
