"""
Writing a figure to a file at exactly the size the spec asks for.

The one owner of "render and save". The GUI's Save used to call
``savefig(..., bbox_inches="tight")`` itself, and "tight" crops or grows the
canvas around whatever sticks out, so the saved file was never
``StyleOptions.width x height`` (spec/images/graph1.png, graph2.png). Here the
file is the figure: ``render_matplotlib`` already lays the panels, labels and
legend out INSIDE the canvas (``render.mpl._fit_x_labels`` /
``_apply_legend``), so nothing needs cropping.

If something still reaches past the canvas edge, it would be cut off. That
is logged as a WARN with the size, and never fixed by quietly changing the
file's size.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from os import PathLike
from typing import IO, Any

from scistacklog import Log

from .resolved import ResolvedPlot

LAYER = "scistackplot"

#: Content this close to (or past) the edge is rounding, not clipping.
OVERFLOW_TOLERANCE_IN = 0.01


@dataclass(frozen=True)
class WrittenFigure:
    """What :func:`write_figure` wrote, for the caller's log and reply."""

    width_in: float
    height_in: float
    dpi: float
    #: ``(width, height)`` in pixels — ``round(inches * dpi)``.
    pixels: tuple[int, int]
    #: How far drawn content reaches past the canvas, in inches (0 = none).
    overflow_in: float
    render_s: float
    write_s: float


def canvas_overflow(fig) -> float:
    """How far any drawn content reaches past the figure's edge, in inches.

    Measured on the figure's tight bounding box against the canvas: the
    distance ``bbox_inches="tight"`` would have grown the file by.
    """
    renderer = fig.canvas.get_renderer()
    box = fig.get_tightbbox(renderer)
    width, height = fig.get_size_inches()
    if box is None:
        return 0.0
    return max(0.0, -box.x0, -box.y0, box.x1 - width, box.y1 - height)


def write_figure(
    resolved: ResolvedPlot,
    target: str | PathLike | IO[bytes],
    *,
    dpi: float = 200,
    **savefig_kwargs: Any,
) -> WrittenFigure:
    """Render ``resolved`` with matplotlib and write it to ``target``.

    The file is ``style.width x style.height`` inches at ``dpi``. Passing
    ``bbox_inches`` is refused: it is the one argument that changes the size
    the spec states. Other ``savefig`` arguments (``format``, ``metadata``)
    pass through. The figure is closed afterwards.
    """
    if "bbox_inches" in savefig_kwargs:
        raise ValueError(
            "write_figure: bbox_inches changes the saved size; the file is the "
            "spec's StyleOptions.width x height by design"
        )
    import matplotlib.pyplot as plt

    from .render import render_matplotlib

    started = time.perf_counter()
    fig = render_matplotlib(resolved)
    rendered = time.perf_counter()
    try:
        overflow = canvas_overflow(fig)
        if overflow > OVERFLOW_TOLERANCE_IN:
            Log.warn(
                "figure content reaches %.2fin past the %.2f x %.2f in canvas "
                "and will be cut off in the file — enlarge the figure or "
                "reduce the text (the file keeps the requested size)",
                overflow,
                *fig.get_size_inches(),
                layer=LAYER,
            )
        fig.savefig(target, dpi=dpi, **savefig_kwargs)
        finished = time.perf_counter()
        width, height = (float(v) for v in fig.get_size_inches())
    finally:
        plt.close(fig)

    written = WrittenFigure(
        width_in=width,
        height_in=height,
        dpi=float(dpi),
        pixels=(round(width * dpi), round(height * dpi)),
        overflow_in=overflow,
        render_s=rendered - started,
        write_s=finished - rendered,
    )
    Log.info(
        "wrote figure %.2f x %.2f in at %g dpi = %d x %d px%s",
        width,
        height,
        dpi,
        *written.pixels,
        f" (content overflows by {overflow:.2f}in)" if overflow > OVERFLOW_TOLERANCE_IN else "",
        layer=LAYER,
    )
    return written
