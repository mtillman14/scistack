"""
Canvas refreshes, as the webview applied them, written into the shared log.

Symptom (user, 2026-10-22, after canvas-as-document): a node or edge just
placed disappears for a moment, then reappears. scistack.log shows each
``get_pipeline`` the server answered, but not which refresh the canvas
APPLIED, in what order, or what it wiped: the canvas replaces its node and
edge lists wholesale with every snapshot (frontend PipelineDAG
``fetchPipeline``), and the server answers each request on its own thread,
so an older snapshot can land after a newer edit.

The frontend (``components/DAG/canvasRefreshLog.ts``) numbers each refresh,
remembers each optimistically drawn node/edge until a snapshot carries it,
and reports every applied refresh here:

- INFO, one line per refresh: number, trigger (``dag_updated:db_watcher`` =
  the extension's ``.duckdb`` watcher, ``dag_updated:server`` = a server
  push, ``effect:graph_version=N`` = mount / scope switch / bumpGraph),
  counts, timings, and ``a newer refresh #N had started`` when one was in
  flight.
- INFO, a refresh that was NOT drawn: ``DISCARDED`` with the reason.
  ``scope_changed`` = the user had left that canvas by the time it came back;
  ``newer_applied`` = a newer refresh was already on screen. Drawing either
  put the wrong picture up (scistack-debug.log 2026-11-04 09:19:45.680, main
  drawn over an open submodule).
- WARN, one line per wiped item: ``stale_snapshot`` = the refresh started
  before the item's write was confirmed (the race; fix in the frontend /
  watcher); ``server_omitted`` = it started after the confirm and the
  server still left the item out (a build-side bug: document view, scope
  filter).

Reading it: a placement shows ``put_layout``/``put_edge`` lines from
layout_service, then refresh lines; the WARN names the refresh that wiped
the item, the next INFO with ``settled`` > 0 is the one that brought it back.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _int(value: Any, default: int = -1) -> int:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return default


def report_canvas_refresh(params: dict[str, Any]) -> dict[str, Any]:
    """Log one applied canvas refresh (and any item it wiped).

    ``params``: ``seq``, ``trigger``, ``scope``, ``nodes``, ``edges``,
    ``get_pipeline_ms``, ``get_layout_ms``, ``total_ms``, ``superseded_by``,
    ``settled``, ``folded``, ``dropped`` (a list of ``{kind, id, age_ms,
    confirm_after_start_ms, verdict}``) and ``discarded`` (why this refresh was
    not drawn, else None). Returns a summary of what was logged.
    """
    seq = _int(params.get("seq"))
    trigger = str(params.get("trigger") or "unknown")[:120]
    scope = str(params.get("scope") or "?")[:200]
    superseded_by = params.get("superseded_by")
    settled = _int(params.get("settled"), 0)
    folded = _int(params.get("folded"), 0)
    dropped = [d for d in (params.get("dropped") or []) if isinstance(d, dict)][:50]

    discarded = params.get("discarded")
    if discarded:
        logger.info(
            "[client] canvas refresh #%d (%s, scope=%s): DISCARDED, %s; not drawn, "
            "%d ms; newest refresh started #%d",
            seq,
            trigger,
            scope,
            str(discarded)[:40],
            _int(params.get("total_ms")),
            _int(superseded_by, seq),
        )
        return {"seq": seq, "trigger": trigger, "wiped": 0, "discarded": str(discarded)[:40]}

    logger.info(
        "[client] canvas refresh #%d (%s, scope=%s): applied %d node(s), %d "
        "edge(s); get_pipeline %d ms + get_layout %d ms = %d ms; pending "
        "settled %d, folded %d, wiped %d%s",
        seq,
        trigger,
        scope,
        _int(params.get("nodes"), 0),
        _int(params.get("edges"), 0),
        _int(params.get("get_pipeline_ms")),
        _int(params.get("get_layout_ms")),
        _int(params.get("total_ms")),
        settled,
        folded,
        len(dropped),
        f"; a newer refresh #{_int(superseded_by)} had started"
        if superseded_by is not None
        else "",
    )
    for d in dropped:
        confirm = d.get("confirm_after_start_ms")
        when = (
            "its write was still unconfirmed"
            if confirm is None
            else f"its write was confirmed {_int(confirm)} ms "
            + ("AFTER" if _int(confirm) > 0 else "before")
            + " this refresh started"
        )
        logger.warning(
            "[client] canvas refresh #%d (%s) wiped the just-placed %s %s "
            "(on screen %d ms; %s) -> %s",
            seq,
            trigger,
            str(d.get("kind") or "?")[:10],
            str(d.get("id") or "?")[:200],
            _int(d.get("age_ms")),
            when,
            str(d.get("verdict") or "?")[:40],
        )
    return {"seq": seq, "trigger": trigger, "wiped": len(dropped)}


def report_scope_navigation(params: dict[str, Any]) -> dict[str, Any]:
    """Log one step into a submodule, as the breadcrumb took it.

    ``params``: ``pipeline_id`` (the submodule), ``before`` and ``after`` (the path,
    as pipeline ids). An ``after`` that doesn't end by adding the submodule means
    it was already on the path, so the breadcrumb went back to it instead
    (``context/descendInto.ts``). Nothing else can see this: the same scope
    fetches nothing, so repeated crumbs left no trace (2026-11-04).
    """
    pipeline_id = str(params.get("pipeline_id") or "?")[:200]
    before = [str(p)[:200] for p in (params.get("before") or [])][:50]
    after = [str(p)[:200] for p in (params.get("after") or [])][:50]
    repeated = pipeline_id in before
    logger.info(
        "[client] breadcrumb: into %s, path %s -> %s%s",
        pipeline_id,
        before,
        after,
        " (already on the path, no new crumb)" if repeated else "",
    )
    return {"pipeline_id": pipeline_id, "repeated": repeated}
