"""
CLI entry point: scistack-gui <path/to/experiment.duckdb>

What happens:
  1. Validate the .duckdb path
  2. Read schema keys from the existing DB
  3. Initialise the shared DatabaseManager (scistack_gui.db)
  4. Start uvicorn on localhost:8765
  5. Open the browser
"""

import argparse
import sys
import webbrowser
from pathlib import Path

import uvicorn


def main():
    parser = argparse.ArgumentParser(
        prog="scistack-gui",
        description="Launch the SciStack GUI for a pipeline database.",
    )
    parser.add_argument(
        "db_path",
        type=Path,
        help="Path to the SciStack .duckdb file (e.g. experiment.duckdb)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port to serve on (default: 8765)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Don't open the browser automatically",
    )
    args = parser.parse_args()

    db_path = args.db_path.resolve()
    if not db_path.exists():
        print(f"Error: database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    # Initialise the shared DB connection before uvicorn starts.
    # Import here so that the module-level singleton is set before the app
    # imports its routers.
    from scistack_gui.db import init_db
    try:
        db = init_db(db_path)
        print(f"Opened database: {db_path}")
        print(f"Schema keys: {db.dataset_schema_keys}")
    except Exception as e:
        print(f"Error opening database: {e}", file=sys.stderr)
        sys.exit(1)

    url = f"http://localhost:{args.port}"
    print(f"SciStack GUI running at {url}")

    if not args.no_browser:
        # Open after a short delay to let uvicorn bind the port
        import threading
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    uvicorn.run(
        "scistack_gui.app:app",
        host="localhost",
        port=args.port,
        log_level="warning",  # suppress uvicorn's per-request logs
    )


if __name__ == "__main__":
    main()
