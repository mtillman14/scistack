"""Shared helpers for the browser tests (opt-in: needs Playwright).

``pip install playwright && playwright install chromium``, then
``pytest scistack-gui/tests/e2e -v -rs``. Without Playwright every test here
skips. Each server runs on a free, non-default port; the page's WebSocket
follows it (``frontend/src/hooks/wsUrl.ts``).

Selectors are ``data-testid`` attributes (grug follow-up 12), so wording
changes in the GUI do not break these tests. Every step logs what it waits
on, so a failure names its step; the server's own log is in the project
folder (``server.log``) and is named in assertion messages.
"""

from __future__ import annotations

import json
import logging
import socket
import subprocess
import sys
import time
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import pytest

try:  # the test modules skip themselves when it is missing
    from playwright import sync_api
except ImportError:  # pragma: no cover - opt-in dependency
    sync_api = None

logger = logging.getLogger("gui_e2e")

STEP_TIMEOUT_MS = 60_000


def free_port() -> int:
    """A port nothing is listening on, never the default 8765."""
    while True:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        if port != 8765:
            return port


def wait_http(url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                if r.status == 200:
                    return
        except OSError as exc:  # not listening yet
            last = exc
        time.sleep(0.25)
    raise TimeoutError(f"server never answered {url}: {last}")


def seed(project: Path, script: str) -> None:
    """Run a seeding script in its own process (it must print SEEDED)."""
    t0 = time.monotonic()
    out = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=300, cwd=project
    )
    assert "SEEDED" in out.stdout, out.stderr[-3000:]
    logger.info("seeded %s in %.1fs", project, time.monotonic() - t0)


@contextmanager
def gui_server(project: Path, args: list[str]):
    """``python -m scistack_gui <args> --port <free> --no-browser`` in *project*."""
    port = free_port()
    url = f"http://127.0.0.1:{port}/"
    log_path = project / "server.log"
    log = log_path.open("w")
    proc = subprocess.Popen(
        [sys.executable, "-m", "scistack_gui", *args, "--port", str(port), "--no-browser"],
        cwd=project,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        t0 = time.monotonic()
        wait_http(url, timeout=120)
        logger.info("server up on %s in %.1fs (log: %s)", url, time.monotonic() - t0, log_path)
        yield {"project": project, "log": log_path, "url": url}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()


def make_page():
    """Generator behind the ``page`` fixture (below): a Chromium page
    that records uncaught page errors and WebSocket frames (``page.smoke_errors``,
    ``page.smoke_frames``)."""
    with sync_api.sync_playwright() as pw:
        browser = pw.chromium.launch()
        pg = browser.new_page()
        errors: list[str] = []
        frames: list[dict] = []
        pg.on("pageerror", lambda exc: errors.append(str(exc)))

        def on_ws(ws):
            def on_frame(payload):
                try:
                    frames.append(json.loads(payload))
                except (TypeError, ValueError):
                    pass

            ws.on("framereceived", on_frame)

        pg.on("websocket", on_ws)
        pg.set_default_timeout(STEP_TIMEOUT_MS)
        pg.smoke_errors = errors  # type: ignore[attr-defined]
        pg.smoke_frames = frames  # type: ignore[attr-defined]
        try:
            yield pg
        finally:
            if errors:
                logger.error("page errors: %s", errors)
            browser.close()


def open_canvas(page, server, min_nodes: int = 3) -> None:
    logger.info("step open_canvas: %s, waiting for >=%d canvas nodes", server["url"], min_nodes)
    page.goto(server["url"])
    page.wait_for_function(
        f"document.querySelectorAll('.react-flow__node').length >= {min_nodes}"
    )
    dismiss_graph_notice(page)


#: How long open_canvas waits for the one-time graph notice before deciding
#: there is none.
NOTICE_WAIT_MS = 3_000


def dismiss_graph_notice(page) -> bool:
    """Close the modal graph notice (PipelineDAG's GraphWarnings: "Added from
    recorded runs" / "Nodes are wired the same") if it shows. A fresh
    project's first load adopts recorded steps onto the canvas and says so
    once; the overlay covers the whole page until "Got it"."""
    button = page.get_by_role("button", name="Got it", exact=True)
    try:
        button.wait_for(state="visible", timeout=NOTICE_WAIT_MS)
    except Exception:
        return False
    title = page.get_by_text("Added from recorded runs").or_(page.get_by_text("wired the same")).first
    logger.info("step dismiss_graph_notice: %s", title.inner_text() if title.count() else "(untitled)")
    button.click()
    button.wait_for(state="detached")
    return True


def node(page, kind: str, label: str):
    """A canvas node by kind (``function``/``variable``) and label."""
    return page.locator(f'[data-testid="node-{kind}"][data-label="{label}"]').first


def wait_for_frame(page, kind: str, timeout_s: float = STEP_TIMEOUT_MS / 1000) -> dict | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        hit = next((f for f in page.smoke_frames if f.get("type") == kind), None)
        if hit is not None:
            return hit
        page.wait_for_timeout(200)
    return None




#: How long a click may wait for its target to become clickable before the
#: test names what covers it (shorter than STEP_TIMEOUT_MS on purpose).
CLICK_TIMEOUT_MS = 15_000

_TOP_ELEMENT_JS = """
(el) => {
  const r = el.getBoundingClientRect();
  const top = document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2);
  if (!top) return 'nothing at the target point';
  const chain = [];
  for (let e = top; e && chain.length < 6; e = e.parentElement) {
    const id = e.getAttribute('data-testid');
    const cls = (e.className && typeof e.className === 'string') ? e.className : '';
    const st = e.getAttribute('style') || '';
    chain.push(`<${e.tagName.toLowerCase()}${id ? ' data-testid=' + id : ''}${cls ? ' class=' + cls : ''}${st ? ' style=' + st.slice(0, 160) : ''}>`);
  }
  return 'covered by: ' + chain.join(' < ') + '\\ntext: ' + (top.innerText || '').slice(0, 400);
}
"""


def screenshot(page, server, name: str) -> Path:
    """Save the page next to server.log and return the path."""
    path = Path(server["project"]) / f"{name}.png"
    try:
        page.screenshot(path=str(path), full_page=True)
    except Exception as exc:  # a diagnostic must never hide the real failure
        logger.warning("screenshot %s failed: %s", path, exc)
    return path


def click(page, server, locator, what: str, **kwargs) -> None:
    """``locator.click()``, but when something covers the target, fail naming
    the covering element (testid/class/style chain and its text) and save a
    screenshot, instead of a bare 60 s timeout."""
    logger.info("step click: %s", what)
    try:
        locator.click(timeout=CLICK_TIMEOUT_MS, **kwargs)
    except Exception as exc:
        try:
            cover = locator.evaluate(_TOP_ELEMENT_JS)
        except Exception as inner:
            cover = f"(could not inspect the target: {inner})"
        shot = screenshot(page, server, f"click-failed-{what.replace(' ', '-')}")
        raise AssertionError(
            f"could not click {what}: {type(exc).__name__}\n{cover}\n"
            f"screenshot: {shot}\nserver log: {server['log']}"
        ) from exc


def require_playwright() -> None:
    """Skip the calling test module without Playwright; FAIL instead when
    ``SCISTACK_E2E_REQUIRED`` is set (the CI gui-e2e job), so a broken install
    there can never pass as 'all skipped'."""
    import os

    if os.environ.get("SCISTACK_E2E_REQUIRED"):
        if sync_api is None:
            raise RuntimeError(
                "SCISTACK_E2E_REQUIRED is set but Playwright is not importable: "
                "pip install playwright && playwright install --with-deps chromium"
            )
        return
    pytest.importorskip("playwright.sync_api")


@pytest.fixture
def page():
    """The Chromium page each test drives. Test modules import it by name
    (``from gui_e2e import page``). It is not in a conftest.py: e2e/ has no
    ``__init__.py``, so a conftest here would be imported as the bare module
    ``conftest`` and shadow ``tests/conftest.py`` for every GUI test that
    does ``from conftest import RawSignal``."""
    yield from make_page()
