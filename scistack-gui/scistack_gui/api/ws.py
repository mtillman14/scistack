"""
WebSocket endpoint: /ws

A WebSocket is a persistent two-way connection between the browser and the
server — unlike HTTP where the browser asks and the server answers once,
a WebSocket stays open so the server can push messages at any time.

We use it here to stream for_each stdout back to the frontend in real time,
and to notify the frontend when the DAG should refresh.

Messages sent to the frontend are JSON objects with a "type" field:
  {"type": "run_output", "run_id": "...", "text": "..."}
  {"type": "run_done",   "run_id": "...", "success": true}
  {"type": "dag_updated"}

Delivery architecture (rewritten 2026-07-18 — the stuck-"Running…" bug):
each connection owns a PER-CLIENT outbox queue and a pump task that reads
ONLY that queue; the pump is explicitly cancelled on disconnect. The old
design shared ONE queue among every pump ever started, and disconnects
leaked their pump (gather doesn't cancel siblings) — so after any
reconnect, an orphaned pump could win the race for a message and consume
it on behalf of a dead connection, silently losing run_done/dag_updated
(regression-tested in tests/test_ws.py::test_push_after_reconnect_...).

Two entry points:
  1. ``broadcast()``   — from ASYNC endpoint handlers; sends directly.
  2. ``push_message()`` — from BACKGROUND RUN THREADS; hops onto the event
     loop via call_soon_threadsafe and fans out to every client's outbox.
Every drop point logs, so a delivery failure can be traced in scistack.log
via the [ws] lines.

Multiple clients are legitimate: the server holds ONE project (db._db is a
module global), so N connections are always N views of the SAME project —
two windows on one pipeline. Each costs a full graph rebuild per mutation,
though, since every client re-fetches on ``dag_updated``.

Connection identity + liveness (2026-08-25): ``client connected (2 total)``
used to be unattributable — there was no way to tell two browser tabs from
one page that had somehow opened two sockets. Each connection now carries a
short id, its peer address, and the ``page_id`` the frontend reports in its
``hello`` frame. **Two connections sharing a page_id are one page** (a
socket leak); different page_ids are genuinely different tabs/windows.

Liveness: the frontend pings every PING_INTERVAL_S. A client that stops
pinging is reaped after CLIENT_SILENT_TIMEOUT_S. Before this, the handler
blocked forever on ``receive_text()`` and the ONLY way a client left
``_clients`` was a proper close frame — so a tab that died uncleanly (crash,
laptop sleep, network drop) stayed registered forever while every fan-out
kept enqueueing into an outbox nobody would ever drain.
"""

import asyncio
import itertools
import json
import logging
import time
from dataclasses import dataclass, field

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

router = APIRouter()

# How often the frontend is expected to ping (useWebSocket.ts PING_INTERVAL_MS
# — keep the two in step) and how long a silent client is tolerated. The
# timeout is deliberately several missed pings: reaping a live-but-briefly-
# stalled client just makes it reconnect, and a reconnect loop is worse than
# a late reap.
PING_INTERVAL_S = 20
CLIENT_SILENT_TIMEOUT_S = 90
REAP_INTERVAL_S = 15


@dataclass
class ClientState:
    """Per-connection state: its outbox, who it is, and when it last spoke."""

    outbox: asyncio.Queue
    cid: str
    peer: str
    user_agent: str
    last_seen: float
    # Reported by the frontend's ``hello`` frame. Two connections sharing a
    # page_id came from ONE page — the discriminator between "two tabs" and
    # "one page leaking sockets". None until hello arrives (or forever, for a
    # frontend build predating the hello frame).
    page_id: str | None = None
    url: str | None = None
    pump: asyncio.Task | None = field(default=None, repr=False)

    def describe(self) -> str:
        page = self.page_id or "unknown-page"
        return f"{self.cid} (page={page}, peer={self.peer})"


# One ClientState per connected client. Iterating the dict yields the
# WebSocket objects (dict keys), so `for client in list(_clients)` works.
_clients: dict[WebSocket, ClientState] = {}

_cid_counter = itertools.count(1)

# The running event loop — stored from async context so background threads
# can schedule work on it via call_soon_threadsafe.
# (asyncio.get_event_loop() raises RuntimeError in non-main threads in Python 3.10+)
_loop: asyncio.AbstractEventLoop | None = None

# Single reaper task, started with the first client and stopped with the
# last, so an idle server (and every TestClient websocket test) leaves no
# task pending on a loop that's about to close.
_reaper: asyncio.Task | None = None


def _log_for(msg: dict):
    """run_output is per-stdout-line chatty → DEBUG; everything else INFO."""
    return logger.debug if msg.get("type") == "run_output" else logger.info


def push_message(msg: dict) -> None:
    """
    Thread-safe: called from a background thread to deliver a message to
    every connected client. Uses call_soon_threadsafe so the fan-out runs
    on the event loop thread rather than in the background thread.

    In JSON-RPC server mode (VS Code extension), delegates to notify.py
    instead of the WebSocket path.
    """
    from scistack_gui.notify import _enabled as _jsonrpc_mode

    if _jsonrpc_mode:
        from scistack_gui.notify import push_message as _jsonrpc_push

        _jsonrpc_push(dict(msg))  # copy to avoid mutating caller's dict
        return
    if _loop is None:
        # No WebSocket client has EVER connected — nowhere to schedule the
        # fan-out. This loses run_output/run_done/dag_updated, so say so.
        logger.warning(
            "[ws] DROPPED %s message (run_id=%s): no event loop captured — "
            "no WebSocket client has connected yet",
            msg.get("type"),
            msg.get("run_id"),
        )
        return
    _loop.call_soon_threadsafe(_fanout_nowait, dict(msg))


def _fanout_nowait(msg: dict) -> None:
    """Runs ON the event loop: copy the message into every client outbox."""
    if not _clients:
        logger.warning(
            "[ws] DROPPED %s message (run_id=%s): no clients connected",
            msg.get("type"),
            msg.get("run_id"),
        )
        return
    for state in _clients.values():
        state.outbox.put_nowait(msg)
    _log_for(msg)(
        "[ws] fanned out %s (run_id=%s) to %d client outbox(es): %s",
        msg.get("type"),
        msg.get("run_id"),
        len(_clients),
        ", ".join(s.cid for s in _clients.values()),
    )


def _peer_of(websocket: WebSocket) -> str:
    """``host:port`` of the connection, or "?" if the transport hides it.

    The source PORT is what makes two connections from the same browser
    distinguishable at all in the absence of a page_id.
    """
    client = getattr(websocket, "client", None)
    if client is None:
        return "?"
    return f"{getattr(client, 'host', '?')}:{getattr(client, 'port', '?')}"


def _handle_client_frame(state: ClientState, raw: str) -> None:
    """Interpret an inbound frame. Only ``hello`` carries anything we keep.

    Anything unparseable is ignored on purpose: the frame's real job is to
    prove the client is alive (the caller already refreshed ``last_seen``),
    and a frontend build predating the hello/ping protocol sends nothing at
    all — which must stay merely unattributed, never an error.
    """
    try:
        msg = json.loads(raw)
    except (ValueError, TypeError):
        return
    if not isinstance(msg, dict) or msg.get("type") != "hello":
        return
    state.page_id = str(msg.get("page_id") or "") or None
    state.url = str(msg.get("url") or "") or None
    same_page = [
        s
        for s in _clients.values()
        if s is not state and s.page_id and s.page_id == state.page_id
    ]
    logger.info(
        "[ws] client %s identified: url=%s, user_agent=%r",
        state.describe(),
        state.url,
        state.user_agent,
    )
    if same_page:
        # The thing we could not tell from a bare "(2 total)": one page is
        # holding more than one socket. useWebSocket.ts keeps a module-level
        # singleton, so this means the module was evaluated twice (an HMR
        # artifact, or two bundles) — not two tabs.
        logger.warning(
            "[ws] client %s shares page_id with %s — ONE page is holding %d "
            "sockets (expected exactly 1). Every extra socket costs a full "
            "graph rebuild per mutation.",
            state.cid,
            ", ".join(s.cid for s in same_page),
            len(same_page) + 1,
        )


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global _loop
    _loop = asyncio.get_running_loop()  # capture from async context
    await websocket.accept()
    state = ClientState(
        outbox=asyncio.Queue(),
        cid=f"c{next(_cid_counter)}",
        peer=_peer_of(websocket),
        user_agent=websocket.headers.get("user-agent", "?"),
        last_seen=time.monotonic(),
    )
    _clients[websocket] = state
    logger.info(
        "[ws] client connected: %s (%d total: %s)",
        state.describe(),
        len(_clients),
        ", ".join(s.cid for s in _clients.values()),
    )
    # The pump reads ONLY this connection's outbox and is ALWAYS reaped in
    # the finally below — a disconnect can never leak a competing consumer.
    state.pump = asyncio.create_task(_pump_outbox(websocket, state))
    _ensure_reaper()
    try:
        # Inbound frames are the liveness signal: every one refreshes
        # last_seen so the reaper can tell a quiet client from a dead one.
        while True:
            raw = await websocket.receive_text()
            state.last_seen = time.monotonic()
            _handle_client_frame(state, raw)
    except WebSocketDisconnect:
        logger.info("[ws] client disconnected: %s", state.describe())
    except Exception:
        logger.exception("[ws] connection handler failed: %s", state.describe())
    finally:
        _drop_client(websocket, reason="closed")


def _drop_client(websocket: WebSocket, reason: str) -> None:
    """Deregister a connection and cancel its pump. Idempotent — both the
    handler's finally and the reaper can reach the same connection."""
    state = _clients.pop(websocket, None)
    if state is None:
        return
    if state.pump is not None:
        state.pump.cancel()
    logger.info(
        "[ws] client removed: %s (%s) — %d remaining%s",
        state.describe(),
        reason,
        len(_clients),
        f": {', '.join(s.cid for s in _clients.values())}" if _clients else "",
    )
    if not _clients:
        _stop_reaper()


def _ensure_reaper() -> None:
    global _reaper
    if _reaper is None or _reaper.done():
        _reaper = asyncio.create_task(_reap_stale_clients())


def _stop_reaper() -> None:
    """Clear the reaper. Never cancels the CALLING task: the reaper reaps
    the last client → _drop_client → here, and self-cancelling would raise
    CancelledError at its next await, skipping the socket close it still
    owes that client. It exits on its own instead (see the loop below).
    """
    global _reaper
    task, _reaper = _reaper, None
    if task is not None and task is not asyncio.current_task():
        task.cancel()


async def _reap_stale_clients() -> None:
    """Drop clients that have stopped pinging.

    Without this a client only ever leaves ``_clients`` by sending a close
    frame, so an uncleanly-dead tab stayed registered forever: fan-out kept
    filling its outbox, ``len(_clients)`` overstated the real audience, and
    the "fanned out to N" / "delivered" counts drifted apart with no
    explanation in the log.
    """
    global _reaper
    try:
        while True:
            await asyncio.sleep(REAP_INTERVAL_S)
            now = time.monotonic()
            for websocket, state in list(_clients.items()):
                silent_for = now - state.last_seen
                if silent_for <= CLIENT_SILENT_TIMEOUT_S:
                    continue
                logger.warning(
                    "[ws] reaping client %s: silent for %.0fs (> %ds) — no "
                    "ping since connect/last frame, treating as dead. A "
                    "frontend predating the ping protocol will be reaped and "
                    "reconnect on a loop; rebuild it.",
                    state.describe(),
                    silent_for,
                    CLIENT_SILENT_TIMEOUT_S,
                )
                _drop_client(websocket, reason=f"stale, silent {silent_for:.0f}s")
                try:
                    await websocket.close(code=1001)
                except Exception:  # already gone at the transport level
                    pass
            if not _clients:
                # Nothing left to watch. Exiting here (rather than being
                # cancelled) is what lets _stop_reaper stay self-cancel-free;
                # _ensure_reaper starts a fresh one on the next connect.
                return
    finally:
        if _reaper is asyncio.current_task():
            _reaper = None


async def _pump_outbox(websocket: WebSocket, state: ClientState):
    """Forward this connection's outbox to its client until cancelled."""
    while True:
        msg = await state.outbox.get()
        try:
            await websocket.send_json(msg)
            _log_for(msg)(
                "[ws] delivered %s (run_id=%s) to %s",
                msg.get("type"),
                msg.get("run_id"),
                state.cid,
            )
        except Exception as exc:
            logger.warning(
                "[ws] send failed for %s message to %s (%s); stopping pump "
                "for this client",
                msg.get("type"),
                state.cid,
                exc,
            )
            break


async def broadcast(msg: dict) -> None:
    """Send a message to all connected clients from async context.

    In JSON-RPC server mode (VS Code extension), delegates to notify.py.
    """
    from scistack_gui.notify import _enabled as _jsonrpc_mode

    if _jsonrpc_mode:
        from scistack_gui.notify import push_message as _jsonrpc_push

        _jsonrpc_push(dict(msg))
        return
    delivered = 0
    for client in list(_clients):
        state = _clients.get(client)
        try:
            await client.send_json(msg)
            delivered += 1
        except Exception as exc:
            logger.warning(
                "[ws] broadcast send failed for %s to %s: %s",
                msg.get("type"),
                state.cid if state else "?",
                exc,
            )
    logger.info(
        "[ws] broadcast %s to %d/%d client(s)",
        msg.get("type"),
        delivered,
        len(_clients),
    )
