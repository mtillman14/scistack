"""create_app must survive a missing or half-written frontend build.

vite empties its outDir before writing, so during ``npm run build``
``static/`` exists with no ``assets/`` inside. create_app used to guard on
``static/`` alone and then mount ``static/assets``, so StaticFiles raised
``RuntimeError: Directory '.../static/assets' does not exist`` and every test
that built the app mid-rebuild errored (2026-09-23).
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from scistack_gui.app import _mount_frontend


def _app_with_api() -> FastAPI:
    app = FastAPI()

    @app.get("/api/ping")
    def ping():
        return {"ok": True}

    return app


def test_static_dir_without_assets_serves_the_api_only(tmp_path, caplog):
    static = tmp_path / "static"
    static.mkdir()  # the mid-build state: outDir emptied, nothing written yet

    app = _app_with_api()
    _mount_frontend(app, static)  # must not raise

    with TestClient(app) as c:
        assert c.get("/api/ping").json() == {"ok": True}
    assert "frontend build incomplete" in caplog.text


def test_missing_static_dir_serves_the_api_only(tmp_path):
    app = _app_with_api()
    _mount_frontend(app, tmp_path / "static")

    with TestClient(app) as c:
        assert c.get("/api/ping").json() == {"ok": True}


def test_complete_build_is_served(tmp_path):
    static = tmp_path / "static"
    (static / "assets").mkdir(parents=True)
    (static / "assets" / "index-abc.js").write_text("console.log(1)")
    (static / "index.html").write_text("<html>scistack</html>")

    app = _app_with_api()
    _mount_frontend(app, static)

    with TestClient(app) as c:
        assert c.get("/api/ping").json() == {"ok": True}
        assert "console.log(1)" in c.get("/assets/index-abc.js").text
        assert "scistack" in c.get("/some/client/route").text
