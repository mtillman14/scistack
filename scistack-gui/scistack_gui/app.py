"""
FastAPI application factory.

Mounts all API routers under /api and (in production) serves the pre-built
React frontend as static files from scistack_gui/static/.
"""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from scistack_gui.api.artifacts import router as artifacts_router
from scistack_gui.api.bootstrap import router as bootstrap_router
from scistack_gui.api.builtin_functions import router as builtin_functions_router
from scistack_gui.api.glue import router as glue_router
from scistack_gui.api.layout import router as layout_router
from scistack_gui.api.pipeline import router as pipeline_router
from scistack_gui.api.plot import router as plot_router
from scistack_gui.api.project import router as project_router
from scistack_gui.api.provenance import router as provenance_router
from scistack_gui.api.registry import router as registry_router
from scistack_gui.api.run import router as run_router
from scistack_gui.api.schema import router as schema_router
from scistack_gui.api.scopes import router as scopes_router
from scistack_gui.api.variables import router as variables_router
from scistack_gui.api.ws import router as ws_router

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(title="SciStack GUI", version="0.1.0")

    # Allow the Vite dev server (localhost:5173) to call the backend during
    # development. In production, both are served from the same origin so this
    # middleware has no effect.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(bootstrap_router, prefix="/api")
    app.include_router(pipeline_router, prefix="/api")
    app.include_router(registry_router, prefix="/api")
    app.include_router(schema_router, prefix="/api")
    app.include_router(run_router, prefix="/api")
    app.include_router(layout_router, prefix="/api")
    app.include_router(variables_router, prefix="/api")
    app.include_router(project_router, prefix="/api")
    app.include_router(scopes_router, prefix="/api")
    app.include_router(artifacts_router, prefix="/api")
    app.include_router(builtin_functions_router, prefix="/api")
    app.include_router(glue_router, prefix="/api")
    app.include_router(plot_router, prefix="/api")
    app.include_router(provenance_router, prefix="/api")
    app.include_router(ws_router)

    _mount_frontend(app, Path(__file__).parent / "static")
    return app


def _mount_frontend(app: FastAPI, static_dir: Path) -> None:
    """Serve the pre-built React frontend, if a complete build is present.
    During development the Vite dev server handles this instead.

    Guards on the directories actually mounted, not just ``static/``: vite
    empties its outDir before writing, so mid-build ``static/`` exists with
    no ``assets/`` in it, and ``StaticFiles`` raises ``RuntimeError:
    Directory '.../static/assets' does not exist`` from ``create_app`` —
    which errored every test that built the app during a rebuild
    (2026-09-23). A missing frontend must never take the API down with it.
    """
    assets_dir = static_dir / "assets"
    index_html = static_dir / "index.html"
    if not (assets_dir.is_dir() and index_html.is_file()):
        logger.warning(
            "create_app: frontend build incomplete or absent (assets=%s, "
            "index.html=%s) — serving the API only. Rebuild with "
            "`cd scistack-gui/frontend && npm run build`.",
            assets_dir.is_dir(),
            index_html.is_file(),
        )
        return

    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/{full_path:path}")
    def serve_frontend(full_path: str):
        """Catch-all: serve index.html for any non-API route (React handles routing)."""
        return FileResponse(index_html)


app = create_app()
