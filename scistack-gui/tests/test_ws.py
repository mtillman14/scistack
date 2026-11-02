"""
Regression tests for the WebSocket push path (api/ws.py).

Background (2026-07-18): runs completed server-side in ~50ms but the GUI
stayed on "Running…" forever — run_output/run_done/dag_updated pushed by
the background run thread never reached the browser, while direct
``broadcast()`` calls from async endpoints did. The thread path
(``push_message`` → call_soon_threadsafe → asyncio queue → ``_pump_queue``)
has several silent drop points; these tests pin its contract:

  a message pushed from a plain background thread while a client is
  connected MUST reach that client.

No DB needed — a minimal FastAPI app with just the ws router.
"""

import asyncio
import threading

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from scistack_gui.runtime import ws_hub as ws_mod
from scistack_gui.api.ws import router as ws_router


@pytest.fixture
def clean_ws_state():
    """Isolate the module-global loop/clients between tests."""
    ws_mod._loop = None
    ws_mod._clients.clear()
    ws_mod._reaper = None
    yield
    ws_mod._loop = None
    ws_mod._clients.clear()
    ws_mod._reaper = None


def _stub_state(**kw) -> ws_mod.ClientState:
    """A ClientState for tests that register clients directly."""
    defaults = dict(
        outbox=asyncio.Queue(),
        cid="cstub",
        peer="127.0.0.1:0",
        user_agent="test",
        last_seen=0.0,
    )
    return ws_mod.ClientState(**{**defaults, **kw})


def _ws_app() -> FastAPI:
    app = FastAPI()
    app.include_router(ws_router)
    return app


class TestPushMessageThreadPath:
    def test_push_from_thread_reaches_connected_client(self, clean_ws_state):
        client = TestClient(_ws_app())
        with client.websocket_connect("/ws") as sock:
            done = {"type": "run_done", "run_id": "abc12345", "success": True}
            t = threading.Thread(target=ws_mod.push_message, args=(done,))
            t.start()
            t.join()
            msg = sock.receive_json()
            assert msg == done

    def test_message_order_preserved(self, clean_ws_state):
        client = TestClient(_ws_app())
        with client.websocket_connect("/ws") as sock:
            msgs = [
                {"type": "run_output", "run_id": "r1", "text": "line 1\n"},
                {"type": "run_output", "run_id": "r1", "text": "line 2\n"},
                {"type": "run_done", "run_id": "r1", "success": True},
                {"type": "dag_updated"},
            ]

            def _push_all():
                for m in msgs:
                    ws_mod.push_message(m)

            t = threading.Thread(target=_push_all)
            t.start()
            t.join()
            received = [sock.receive_json() for _ in msgs]
            assert received == msgs

    def test_push_before_any_client_is_dropped_not_raised(self, clean_ws_state):
        # No client has ever connected: _loop is None. The message is lost
        # by design (logged as a WARNING) — but must never raise from the
        # run thread.
        ws_mod.push_message({"type": "run_done", "run_id": "orphan"})

    def test_push_reaches_all_connected_clients(self, clean_ws_state):
        """Fan-out: with two live connections (e.g. two browser tabs), a
        pushed message must reach BOTH — under the old shared-queue design
        one pump consumed each message and raced the other."""
        client = TestClient(_ws_app())
        with (
            client.websocket_connect("/ws") as s1,
            client.websocket_connect("/ws") as s2,
        ):
            done = {"type": "run_done", "run_id": "both", "success": True}
            t = threading.Thread(target=ws_mod.push_message, args=(done,))
            t.start()
            t.join()
            assert s1.receive_json() == done
            assert s2.receive_json() == done

    def test_push_after_reconnect_reaches_new_client(self, clean_ws_state):
        """The stuck-run scenario shape: first page connection goes away
        (reload), a second connects, then a run thread pushes. The pump
        task from the dead connection must not eat the message."""
        client = TestClient(_ws_app())
        with client.websocket_connect("/ws"):
            pass  # connect + immediately disconnect (page reload)
        with client.websocket_connect("/ws") as sock2:
            done = {"type": "run_done", "run_id": "second", "success": True}
            t = threading.Thread(target=ws_mod.push_message, args=(done,))
            t.start()
            t.join()
            msg = sock2.receive_json()
            assert msg == done


class TestConnectionIdentity:
    """``client connected (2 total)`` was unattributable: nothing in the log
    could distinguish two browser tabs from one page holding two sockets.
    Each connection now carries an id, a peer address, and the page_id the
    frontend reports in its hello frame.
    """

    def test_each_connection_gets_a_distinct_id(self, clean_ws_state):
        client = TestClient(_ws_app())
        with (
            client.websocket_connect("/ws"),
            client.websocket_connect("/ws"),
        ):
            cids = [s.cid for s in ws_mod._clients.values()]
            assert len(cids) == 2
            assert len(set(cids)) == 2, f"ids must be unique, got {cids}"

    def test_hello_frame_records_page_id(self, clean_ws_state):
        client = TestClient(_ws_app())
        with client.websocket_connect("/ws") as sock:
            sock.send_json({"type": "hello", "page_id": "abc123", "url": "http://x/"})
            # Round-trip a pushed message to be sure the server consumed the
            # hello frame before asserting on it.
            ws_mod.push_message({"type": "dag_updated"})
            sock.receive_json()
            state = next(iter(ws_mod._clients.values()))
            assert state.page_id == "abc123"
            assert state.url == "http://x/"

    def test_two_sockets_from_one_page_are_detectable(self, clean_ws_state):
        """The discriminator: same page_id from two connections means one
        page is holding two sockets, which useWebSocket.ts's singleton is
        supposed to make impossible."""
        client = TestClient(_ws_app())
        with (
            client.websocket_connect("/ws") as s1,
            client.websocket_connect("/ws") as s2,
        ):
            for sock in (s1, s2):
                sock.send_json({"type": "hello", "page_id": "samepage"})
            ws_mod.push_message({"type": "dag_updated"})
            s1.receive_json()
            s2.receive_json()
            page_ids = [s.page_id for s in ws_mod._clients.values()]
            assert page_ids == ["samepage", "samepage"]

    def test_unparseable_frame_is_ignored_not_fatal(self, clean_ws_state):
        """A frontend build predating the hello protocol sends nothing at
        all, and anything it does send must never kill the connection."""
        client = TestClient(_ws_app())
        with client.websocket_connect("/ws") as sock:
            sock.send_text("not json")
            sock.send_json({"type": "ping"})
            ws_mod.push_message({"type": "dag_updated"})
            assert sock.receive_json() == {"type": "dag_updated"}
            state = next(iter(ws_mod._clients.values()))
            assert state.page_id is None


class TestLiveness:
    """A client used to leave ``_clients`` ONLY via a close frame, so a tab
    that died uncleanly stayed registered forever while fan-out kept filling
    an outbox nobody would drain."""

    def test_inbound_frame_refreshes_last_seen(self, clean_ws_state):
        client = TestClient(_ws_app())
        with client.websocket_connect("/ws") as sock:
            state = next(iter(ws_mod._clients.values()))
            state.last_seen = 0.0
            sock.send_json({"type": "ping"})
            ws_mod.push_message({"type": "dag_updated"})
            sock.receive_json()
            assert state.last_seen > 0.0

    def test_drop_client_is_idempotent(self, clean_ws_state):
        """Both the handler's finally and the reaper can reach the same
        connection; the second call must be a no-op, not a KeyError."""

        class StubClient:
            pass

        stub = StubClient()
        ws_mod._clients[stub] = _stub_state()
        ws_mod._drop_client(stub, reason="first")
        ws_mod._drop_client(stub, reason="second")
        assert stub not in ws_mod._clients

    def test_reaper_stops_when_last_client_leaves(self, clean_ws_state):
        """An idle server must leave no task pending on a loop about to
        close (otherwise every websocket test trails a destroyed-task
        warning)."""
        client = TestClient(_ws_app())
        with client.websocket_connect("/ws"):
            assert ws_mod._reaper is not None
        assert ws_mod._clients == {}
        assert ws_mod._reaper is None

    def test_ping_interval_is_under_the_silence_timeout(self):
        """The frontend's PING_INTERVAL_MS is pinned to this constant; if
        the timeout ever drops below the ping period, every healthy client
        gets reaped and reconnects on a loop."""
        assert ws_mod.PING_INTERVAL_S * 2 <= ws_mod.CLIENT_SILENT_TIMEOUT_S
        assert ws_mod.REAP_INTERVAL_S < ws_mod.CLIENT_SILENT_TIMEOUT_S


class TestBroadcastDirectPath:
    # _clients maps websocket -> outbox queue; broadcast iterates the KEYS
    # and sends directly, so stubs stand in for websockets here.

    def test_broadcast_reaches_stub_clients(self, clean_ws_state):
        sent = []

        class StubClient:
            async def send_json(self, msg):
                sent.append(msg)

        ws_mod._clients[StubClient()] = _stub_state()
        asyncio.run(ws_mod.broadcast({"type": "dag_updated"}))
        assert sent == [{"type": "dag_updated"}]

    def test_broadcast_survives_one_dead_client(self, clean_ws_state):
        sent = []

        class DeadClient:
            async def send_json(self, msg):
                raise RuntimeError("socket closed")

        class LiveClient:
            async def send_json(self, msg):
                sent.append(msg)

        ws_mod._clients[DeadClient()] = _stub_state(cid="cdead")
        ws_mod._clients[LiveClient()] = _stub_state(cid="clive")
        asyncio.run(ws_mod.broadcast({"type": "dag_updated"}))
        assert sent == [{"type": "dag_updated"}]
