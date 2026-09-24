"""
Renderers.

Both take a fully reduced :class:`~scistackplot.resolved.ResolvedPlot` and only
translate it. matplotlib is the export and pipeline path (it returns the
``Figure`` a scidb ``plot_`` endpoint must return); plotly is the interactive
path (it returns a plotly.js figure dict for the webview).
"""

from .base import Renderer

__all__ = ["Renderer", "layout_decisions", "render_matplotlib", "render_plotly"]


def render_matplotlib(resolved):
    """Draw with matplotlib; returns a ``matplotlib.figure.Figure``."""
    from .mpl import render

    return render(resolved)


def render_plotly(resolved, *, decisions=None, fixed_size_px=None) -> dict:
    """Build a plotly.js figure dict (no plotly package required).

    ``decisions`` are :func:`layout_decisions` output — the export's label and
    legend decisions, applied as they are; ``fixed_size_px`` draws the figure
    at that ``(width, height)`` instead of letting it fill its pane.
    """
    from .plotly_ import render

    return render(resolved, decisions=decisions, fixed_size_px=fixed_size_px)


def layout_decisions(resolved, *, width_in=None, height_in=None) -> dict:
    """The export's label and legend decisions at a size (matplotlib)."""
    from .mpl import layout_decisions as decide

    return decide(resolved, width_in=width_in, height_in=height_in)
