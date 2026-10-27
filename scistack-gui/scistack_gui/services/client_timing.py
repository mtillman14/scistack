"""
Client-side (webview) timings, written into the shared log.

scistack.log records the backend's share of a Plot Studio ◀ ▶ step (plot_resolve
and its layout timings) but nothing either side of it: the panel's debounce
before the request leaves, and Plotly drawing the answer after it lands. On
2026-10-16 "the arrows are slow" could be explained only up to "resolved
bar: figure N rendered"; the rest of what the user waited for was invisible.

So the panel times each step (frontend PlotStudio/figureStepTiming.ts) and
POSTs it here, and it lands as one INFO line beside the RPC lines it
brackets. Compare ``round_trip_ms`` with the backend's ``layout_decisions``
TOTAL to see transport/queueing; ``queued_ms`` is the debounce plus any wait
behind a resolve already in flight; ``draw_ms`` is Plotly.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _ms(value: Any) -> int:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return -1


def report_client_timing(params: dict[str, Any]) -> dict[str, Any]:
    """Log one timed webview step.

    ``params``: ``where`` (what was timed), optional ``label`` and
    ``figure_index``, ``clicks``, and the phases ``queued_ms``,
    ``round_trip_ms``, ``draw_ms``, ``total_ms``. Returns what was logged.
    """
    where = str(params.get("where") or "webview")[:200]
    label = str(params.get("label") or "")[:200]
    index = params.get("figure_index")
    clicks = _ms(params.get("clicks"))
    queued, round_trip, draw, total = (
        _ms(params.get(k)) for k in ("queued_ms", "round_trip_ms", "draw_ms", "total_ms")
    )
    figure = f"figure {index}" if index is not None else "figure ?"
    if label:
        figure += f" ({label})"
    logger.info(
        "[client] %s %s: %d ms click->drawn = queued %d + round trip %d + draw %d"
        " (%d click(s))",
        where,
        figure,
        total,
        queued,
        round_trip,
        draw,
        clicks,
    )
    return {
        "where": where,
        "total_ms": total,
        "queued_ms": queued,
        "round_trip_ms": round_trip,
        "draw_ms": draw,
    }
