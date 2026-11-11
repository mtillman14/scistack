"""GUI smoke paths in a real browser (grug stage gui_smoke_e2e,
docs/claude/grugbrain-principles.md).

pytest covers the backend in depth, but nothing automated loads the BUILT
frontend bundle against the real server, so "pytest passes, GUI unchecked"
was the usual state. These are the few key paths grug asks for, not a UI
test suite:

* the canvas renders the seeded pipeline, with no uncaught page error;
* ▶ Run on a function node finishes with ``run_done`` success (read off the
  WebSocket, so it does not depend on run-log markup);
* Open Plot Studio on a variable reaches ``/api/plot/describe``.

Opt-in: needs Playwright (``pip install playwright && playwright install
chromium``). The server runs on a free, non-default port, so the Run test
also proves the WebSocket follows the page's port (``frontend/src/hooks/wsUrl.ts``;
it was hard-coded to 8765 until 2026-11-10). Run from the workspace root:

    pytest scistack-gui/tests/e2e -v

Each step logs what it waits on, so a failure names its step.
"""

from __future__ import annotations

import json
import logging
import socket
import subprocess
import sys
import textwrap
import time
import urllib.request
from pathlib import Path

import pytest

sync_api = pytest.importorskip("playwright.sync_api")

logger = logging.getLogger("gui_smoke")

STEP_TIMEOUT_MS = 60_000

MODULE = textwrap.dedent(
    '''
    import numpy as np

    from scidb import BaseVariable


    class SmokeRaw(BaseVariable):
        pass


    class SmokeOut(BaseVariable):
        pass


    def smoke_double(signal):
        return np.asarray(signal, dtype=float) * 2.0
    '''
)

SEED = textwrap.dedent(
    '''
    import sys

    import numpy as np

    sys.path.insert(0, {project!r})
    from scidb import configure_database, for_each
    from smoke_steps import SmokeOut, SmokeRaw, smoke_double

    db = configure_database({db!r}, ["subject"])
    for s in ("1", "2"):
        SmokeRaw.save(np.arange(5.0), subject=s)
    for_each(smoke_double, {{"signal": SmokeRaw}}, [SmokeOut], subject=[])
    db.close()
    print("SEEDED")
    '''
)


def _free_port() -> int:
    """A port nothing is listening on, never the default 8765."""
    while True:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        if port != 8765:
            return port


def _wait_http(url: str, timeout: float) -> None:
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


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    port = _free_port()
    url = f"http://127.0.0.1:{port}/"
    project = tmp_path_factory.mktemp("smoke")
    (project / "smoke_steps.py").write_text(MODULE)
    db = project / "smoke.duckdb"

    t0 = time.monotonic()
    seed = subprocess.run(
        [sys.executable, "-c", SEED.format(project=str(project), db=str(db))],
        capture_output=True,
        text=True,
        timeout=300,
        cwd=project,
    )
    assert "SEEDED" in seed.stdout, seed.stderr[-3000:]
    logger.info("seeded %s in %.1fs", db, time.monotonic() - t0)

    log = (project / "server.log").open("w")
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "scistack_gui", str(db),
            "--module", str(project / "smoke_steps.py"),
            "--port", str(port), "--no-browser",
        ],
        cwd=project,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        t0 = time.monotonic()
        _wait_http(url, timeout=120)
        logger.info("server up on %s in %.1fs (log: %s)", url, time.monotonic() - t0, log.name)
        yield {"project": project, "log": Path(log.name), "url": url}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()


@pytest.fixture
def page(server):
    with sync_api.sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        errors: list[str] = []
        frames: list[dict] = []
        page.on("pageerror", lambda exc: errors.append(str(exc)))

        def on_ws(ws):
            def on_frame(payload):
                try:
                    frames.append(json.loads(payload))
                except (TypeError, ValueError):
                    pass

            ws.on("framereceived", on_frame)

        page.on("websocket", on_ws)
        page.set_default_timeout(STEP_TIMEOUT_MS)
        page.smoke_errors = errors  # type: ignore[attr-defined]
        page.smoke_frames = frames  # type: ignore[attr-defined]
        try:
            yield page
        finally:
            if errors:
                logger.error("page errors: %s", errors)
            browser.close()


def _open_canvas(page, server) -> None:
    logger.info("step open_canvas: %s, waiting for >=3 canvas nodes", server["url"])
    page.goto(server["url"])
    page.wait_for_function("document.querySelectorAll('.react-flow__node').length >= 3")


def test_canvas_renders_without_page_errors(page, server):
    _open_canvas(page, server)
    labels = page.locator(".react-flow__node").all_inner_texts()
    text = " ".join(labels)
    for name in ("SmokeRaw", "SmokeOut", "smoke_double"):
        assert name in text, f"{name} not on the canvas; nodes: {labels} (log: {server['log']})"
    assert page.smoke_errors == [], page.smoke_errors


def test_run_one_function_finishes(page, server):
    _open_canvas(page, server)
    node = page.locator(".react-flow__node", has_text="smoke_double").first
    logger.info("step run: clicking ▶ Run on smoke_double")
    node.get_by_role("button", name="▶ Run").first.click()

    deadline = time.monotonic() + STEP_TIMEOUT_MS / 1000
    done = None
    while done is None and time.monotonic() < deadline:
        done = next((f for f in page.smoke_frames if f.get("type") == "run_done"), None)
        page.wait_for_timeout(200)
    assert done is not None, f"no run_done on the WebSocket (log: {server['log']})"
    assert done.get("success") is True, done
    assert page.smoke_errors == [], page.smoke_errors


def test_plot_studio_opens_on_a_variable(page, server):
    _open_canvas(page, server)
    logger.info("step plot_studio: selecting SmokeOut, then Open Plot Studio")
    page.locator(".react-flow__node", has_text="SmokeOut").first.click()
    with page.expect_response(lambda r: "/api/plot/describe" in r.url) as described:
        page.get_by_role("button", name="Open Plot Studio →").click()
    assert described.value.status == 200, described.value.text()[:2000]
    assert page.smoke_errors == [], page.smoke_errors
