"""
FastAPI application factory.

Mounts all API routers under /api and (in production) serves the pre-built
React frontend as static files from scistack_gui/static/.
"""

from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from scistack_gui.api.pipeline import router as pipeline_router
from scistack_gui.api.registry import router as registry_router
from scistack_gui.api.schema import router as schema_router
from scistack_gui.api.run import router as run_router
from scistack_gui.api.layout import router as layout_router
from scistack_gui.api.ws import router as ws_router


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

    app.include_router(pipeline_router, prefix="/api")
    app.include_router(registry_router, prefix="/api")
    app.include_router(schema_router, prefix="/api")
    app.include_router(run_router, prefix="/api")
    app.include_router(layout_router, prefix="/api")
    app.include_router(ws_router)

    # Serve the pre-built React frontend if the static folder exists.
    # During development the Vite dev server handles this instead.
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/assets", StaticFiles(directory=static_dir / "assets"), name="assets")

        @app.get("/{full_path:path}")
        def serve_frontend(full_path: str):
            """Catch-all: serve index.html for any non-API route (React handles routing)."""
            return FileResponse(static_dir / "index.html")

    return app


app = create_app()
