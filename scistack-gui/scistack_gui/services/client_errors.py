"""
Client-side (webview) errors, written into the shared log.

A React render error unmounts the whole webview tree and leaves a blank tab,
and nothing about it reaches ``scidb.log``: the Python side only sees requests,
and a crash in the browser sends none. On 2026-09-15 the grouping picker did
exactly that — the log showed the popup's ``get_pipeline``/``get_layout`` and
then silence, and the cause had to be re-derived from the source.

So the frontend's error boundaries POST what they caught here, and it lands in
the same log as everything else, at ERROR, with the component stack. The
payload is display text from our own bundle; it is truncated but otherwise
written as received.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: Component stacks are long and the log is line-oriented; keep enough to
#: name the component chain without paging the file.
MAX_FIELD = 4000


def _clip(value: Any) -> str:
    text = str(value or "").strip()
    return text if len(text) <= MAX_FIELD else text[: MAX_FIELD - 1] + "…"


def report_client_error(params: dict[str, Any]) -> dict[str, Any]:
    """Log one error the webview caught.

    ``params``: ``where`` (which boundary caught it — the tab or a popup),
    ``message``, optional ``stack`` (the JS stack) and ``component_stack``
    (React's). Returns what was logged so the caller can confirm the round
    trip.
    """
    where = _clip(params.get("where")) or "webview"
    message = _clip(params.get("message")) or "(no message)"
    stack = _clip(params.get("stack"))
    component_stack = _clip(params.get("component_stack"))

    # The scistack_gui layer logger already feeds scidb.log (scistacklog's
    # file sink), so this is one ERROR beside the RPC lines it interleaves with.
    logger.error("[client] render error in %s: %s", where, message)
    if component_stack:
        logger.error("[client] component stack (%s):\n%s", where, component_stack)
    if stack:
        logger.error("[client] JS stack (%s):\n%s", where, stack)
    return {"where": where, "message": message}
