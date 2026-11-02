"""
How a figure's paper looks — the ONE owner.

"Paper" is everything that is not data: the background, the text colour, the
frame round each panel, the ticks, the grid, the error-bar ink. The exported
figure used to get all of it from matplotlib's rc defaults, and the plotly
preview from plotly.js's defaults, which differ on every point: plotly draws a
grey grid and a zero line, with no frame and no ticks, while matplotlib draws a
black box with outward ticks and no grid. So the preview was never the figure
you would save, however carefully its sizes and marks were matched.

Every consumer reads :data:`PAPER`:

* ``render_matplotlib`` opens :func:`figure_rc_params` (paper + text sizes) and
  then applies :func:`apply_paper_axes` to each panel. The explicit pass exists
  because matplotlib creates tick objects lazily. A save outside the
  ``rc_context`` (``write_figure``, plot_service) would otherwise build its
  ticks from whatever rc is in force at that moment, including a user's
  ``matplotlibrc``.
* the generated seaborn code emits the same rc dict and :func:`paper_axes_code`.
  The latter also undoes seaborn's default despine, which would otherwise
  export an open frame.
* the plotly preview writes :func:`plotly_axis_style`, :func:`plotly_layout_style`
  and :func:`plotly_error_style` into its layout (1 pt = 1 px, the convention
  ``textsize`` already follows).

The values ARE matplotlib's defaults, so turning this owner into existence
changed nothing in an exported file. They are only written down now.
Pure, no matplotlib import. See docs/claude/preview-paper-parity.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .textsize import ResolvedSizes, rc_params


@dataclass(frozen=True)
class PaperStyle:
    """Lengths in points; the preview reads them as pixels."""

    background: str = "#ffffff"
    text: str = "#000000"
    #: The frame round each panel (all four sides) and the ticks.
    axis_line: str = "#000000"
    axis_line_width: float = 0.8
    tick_direction: str = "out"
    tick_length: float = 3.5
    tick_width: float = 0.8
    grid: bool = False
    #: ``Axes.bar``'s ``ecolor`` default, and ``lines.linewidth``.
    error_bar_color: str = "#000000"
    error_bar_width: float = 1.5
    #: matplotlib ``capsize``: HALF the cap's length, which is plotly's
    #: ``error_y.width`` too.
    error_cap: float = 3.0
    #: The caps' own thickness: matplotlib draws a cap as a ``_`` marker whose
    #: edge width is ``lines.markeredgewidth``, not the bar's line width.
    error_cap_width: float = 1.0


PAPER = PaperStyle()


def paper_rc_params() -> dict[str, Any]:
    """The rc keys that draw :data:`PAPER` in matplotlib."""
    p = PAPER
    params: dict[str, Any] = {
        "figure.facecolor": p.background,
        "axes.facecolor": p.background,
        "savefig.facecolor": p.background,
        "text.color": p.text,
        "axes.labelcolor": p.text,
        "axes.titlecolor": p.text,
        "axes.edgecolor": p.axis_line,
        "axes.linewidth": p.axis_line_width,
        "axes.grid": p.grid,
    }
    for side in ("left", "right", "top", "bottom"):
        params[f"axes.spines.{side}"] = True
    for axis, near, far in (("xtick", "bottom", "top"), ("ytick", "left", "right")):
        params[f"{axis}.color"] = p.axis_line
        params[f"{axis}.labelcolor"] = p.text
        params[f"{axis}.direction"] = p.tick_direction
        params[f"{axis}.major.size"] = p.tick_length
        params[f"{axis}.major.width"] = p.tick_width
        params[f"{axis}.{near}"] = True
        params[f"{axis}.{far}"] = False
    return params


def figure_rc_params(sizes: ResolvedSizes) -> dict[str, Any]:
    """Everything a figure's ``rc_context`` states: the paper, then the text sizes."""
    return {**paper_rc_params(), **rc_params(sizes)}


def apply_paper_axes(ax) -> None:
    """Draw :data:`PAPER` on one matplotlib Axes, whatever rc is in force.

    ``tick_params`` persists on the axis, so ticks made later (lazily, at save
    time) take it too.
    """
    p = PAPER
    ax.set_facecolor(p.background)
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color(p.axis_line)
        spine.set_linewidth(p.axis_line_width)
    ax.tick_params(
        which="major",
        direction=p.tick_direction,
        length=p.tick_length,
        width=p.tick_width,
        color=p.axis_line,
        labelcolor=p.text,
    )
    ax.grid(p.grid)


def paper_axes_code(axes: str) -> list[str]:
    """:func:`apply_paper_axes` as generated code, for each axes in ``axes``
    (an expression: ``g.axes.flat``, ``[ax]``). The panels only, as
    ``render_matplotlib`` does: a colourbar keeps its own outline.

    The generated function cannot import scistackplot, so the same values are
    emitted as literals.
    """
    p = PAPER
    return [
        f"for _ax in {axes}:",
        f"    _ax.set_facecolor({p.background!r})",
        "    for _spine in _ax.spines.values():",
        "        _spine.set_visible(True)",
        f"        _spine.set_color({p.axis_line!r})",
        f"        _spine.set_linewidth({p.axis_line_width!r})",
        "    _ax.tick_params(",
        f'        which="major", direction={p.tick_direction!r}, length={p.tick_length!r},',
        f"        width={p.tick_width!r}, color={p.axis_line!r}, labelcolor={p.text!r},",
        "    )",
        f"    _ax.grid({p.grid!r})",
    ]


def plotly_axis_style() -> dict[str, Any]:
    """The keys every plotly x/y axis carries so it draws :data:`PAPER`."""
    p = PAPER
    return {
        "showline": True,
        # The box's far sides, without ticks, as matplotlib's spines are.
        "mirror": True,
        "linecolor": p.axis_line,
        "linewidth": p.axis_line_width,
        "ticks": "outside" if p.tick_direction == "out" else "inside",
        "ticklen": p.tick_length,
        "tickwidth": p.tick_width,
        "tickcolor": p.axis_line,
        "showgrid": p.grid,
        "zeroline": False,
    }


def plotly_layout_style() -> dict[str, Any]:
    """The figure-level plotly keys (the text colour goes in ``layout.font``)."""
    return {"paper_bgcolor": PAPER.background, "plot_bgcolor": PAPER.background}


def seaborn_err_kws() -> dict[str, Any]:
    """``err_kws`` that make seaborn's bar error bars matplotlib's.

    seaborn (0.13) draws each error bar as ONE two-point line, with no caps
    at ``capsize=0``. A ``_`` marker at both ends is exactly how
    ``Axes.errorbar`` draws its caps (``markersize = 2 x capsize``), so this
    reproduces them in points. seaborn's own ``capsize`` is a fraction of the
    category spacing, so it would change with the figure's width.
    """
    p = PAPER
    return {
        "color": p.error_bar_color,
        "linewidth": p.error_bar_width,
        "marker": "_",
        "markersize": 2 * p.error_cap,
        "markeredgewidth": p.error_cap_width,
    }


def plotly_error_style() -> dict[str, Any]:
    """The ink of a plotly ``error_y``, as ``Axes.bar`` draws its error bars."""
    p = PAPER
    return {"color": p.error_bar_color, "thickness": p.error_bar_width, "width": p.error_cap}


def describe_paper() -> str:
    """One phrase for the logs, so a preview and an export can be compared by eye."""
    p = PAPER
    return (
        f"paper {p.background} bg, {p.axis_line} frame {p.axis_line_width:g}pt, "
        f"ticks {p.tick_direction} {p.tick_length:g}pt, grid {'on' if p.grid else 'off'}"
    )
