"""
CLI entry point: scistack-gui [path/to/experiment.duckdb]

What happens:
  1. If a db_path is given: import pipeline code, open (or create, with
     --schema-keys) the database via scistack_gui.bootstrap.
  2. If no db_path is given: skip straight to step 3 with no project loaded
     — the browser opens onto the project-creation wizard
     (POST /api/bootstrap/create, /api/bootstrap/open), which runs the same
     bootstrap sequence from a running server.
  3. Start uvicorn on localhost:8765
  4. Open the browser
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
        nargs="?",
        default=None,
        help="Path to the SciStack .duckdb file (e.g. experiment.duckdb). "
        "If omitted, the GUI opens onto a wizard to create or open one.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port to serve on (default: 8765)",
    )
    parser.add_argument(
        "--module",
        "-m",
        type=Path,
        default=None,
        help="Path to your pipeline .py file (single-file mode).",
    )
    parser.add_argument(
        "--project",
        "-p",
        type=Path,
        default=None,
        help="Path to scistack.toml or the directory containing one "
        "(project mode — reads scistack.toml).",
    )
    parser.add_argument(
        "--schema-keys",
        type=str,
        default=None,
        help="Comma-separated schema keys; if provided and db_path "
        "does not exist, a new database is created.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=None,
        help="Directory to treat as the project root when no "
        "scistack.toml exists yet. Determines where a new "
        "scistack.toml and entities file are written; defaults to the "
        "working directory.",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Don't open the browser automatically",
    )
    args = parser.parse_args()

    if args.project_root is not None:
        from scistack_gui.config import set_project_root_hint

        set_project_root_hint(args.project_root)

    if args.module and args.project:
        print("Error: --module and --project are mutually exclusive.", file=sys.stderr)
        sys.exit(1)

    if args.db_path is not None:
        db_path = args.db_path.resolve()
        schema_keys = None
        if args.schema_keys:
            schema_keys = [k.strip() for k in args.schema_keys.split(",") if k.strip()]

        if not db_path.exists() and not schema_keys:
            print(f"Error: database file not found: {db_path}", file=sys.stderr)
            sys.exit(1)

        from scistack_gui.bootstrap import open_or_create_project

        try:
            result = open_or_create_project(
                db_path,
                schema_keys=schema_keys,
                module=args.module,
                project=args.project,
            )
        except (FileNotFoundError, ValueError, FileExistsError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error opening database: {e}", file=sys.stderr)
            sys.exit(1)

        print(f"Opened database: {db_path}")
        print(f"Schema keys: {result.schema_keys}")
        print(
            f"Loaded: {result.functions_loaded} functions, "
            f"{result.variables_loaded} variables"
        )
        if result.matlab_functions_loaded or result.matlab_variables_loaded:
            print(
                f"MATLAB: {result.matlab_functions_loaded} functions, "
                f"{result.matlab_variables_loaded} variables"
            )
        for w in result.warnings:
            print(f"Warning: {w}", file=sys.stderr)
    else:
        print("No database given — open the browser to create or open one.")

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
