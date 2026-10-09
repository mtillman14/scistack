"""
``scistack`` CLI entry point.

Usage:
    scistack init [PATH] [--name NAME] [--schema-keys subject session ...]
    scistack export [OUT] [--db DB] [--data] [--no-history]
    scistack import BUNDLE [--into DIR] [--schema ...] [--map OLD=NEW] ...
    scistack bundle-info BUNDLE
    scistack library [list | add NAME | remove NAME | create ... | copy NAME | build DIR]
    scistack install [PROJECT]    # what an imported project needs (after trust)
    scistack verify [--against X] # = scidb verify: your re-run vs the exporter's history
    scistack db <command> ...      # alias for the ``scidb`` CLI

``init`` is a thin front end over ``scidb.project.init_project``, the one
owner of "make this folder a SciStack project" (the GUI calls the same
function when it creates a database). It only creates what is missing, so it
is safe to run in an existing folder and to run twice.

``export`` / ``import`` / ``bundle-info`` are ``scistack_gui.bundle_cli``'s
parsers, mounted here (the GUI runs the same module in a subprocess, and
scistack-gui must never depend on this package).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["verify"]:
        # One implementation: the read-only scidb CLI owns verify.
        from scidb.inspect.cli import main as scidb_main

        return scidb_main(argv)

    parser = argparse.ArgumentParser(
        prog="scistack",
        description="SciStack project tooling.",
    )
    sub = parser.add_subparsers(dest="command")

    init = sub.add_parser(
        "init",
        help="Make a folder a SciStack project (creates only what is missing).",
    )
    init.add_argument(
        "path",
        nargs="?",
        type=Path,
        default=Path.cwd(),
        help="Project folder (default: the current directory; created if absent).",
    )
    init.add_argument(
        "--name",
        default=None,
        help="Package name (default: pyproject.toml's [project].name, else the "
        "folder name made into a valid identifier).",
    )
    init.add_argument(
        "--schema-keys",
        nargs="+",
        default=None,
        help="Also create the project database with these schema keys "
        "(top-down, e.g. subject session trial).",
    )
    init.add_argument(
        "--db",
        default=None,
        help="Database file name for --schema-keys (default: <name>.duckdb in "
        "the project folder).",
    )

    # --- export / import / bundle-info (owned by scistack_gui.bundle_cli) ---
    from scistack_gui.bundle_cli import add_bundle_subparsers
    from scistack_gui.bundle_cli import dispatch as bundle_dispatch

    add_bundle_subparsers(sub)

    from scistack_gui.library_cli import add_library_subparsers

    add_library_subparsers(sub)

    # --- db (alias for the scidb CLI; wiring lives in scidb.inspect.cli) ---
    try:
        from scidb.inspect.cli import add_db_subparser

        add_db_subparser(sub)
    except ImportError:
        pass

    args = parser.parse_args(argv)

    if args.command == "init":
        return _cmd_init(args)

    if getattr(args, "_bundle_cmd", None) is not None:
        return bundle_dispatch(args)

    if getattr(args, "_library_cmd", None) is not None:
        return args._library_cmd(args)

    if args.command == "db":
        dispatch = getattr(args, "_dispatch", None)
        if dispatch is not None:
            return dispatch(args)

    parser.print_help()
    return 1


def _cmd_init(args: argparse.Namespace) -> int:
    from scidb.project import init_project

    try:
        report = init_project(args.path, name=args.name)
    except (ValueError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    for path in report.created:
        print(f"created  {_rel(path, report.root)}")
    for path in report.kept:
        print(f"kept     {_rel(path, report.root)}")

    if args.schema_keys:
        db_path = report.root / (args.db or f"{report.package}.duckdb")
        if db_path.exists():
            print(f"kept     {_rel(db_path, report.root)} (database already exists)")
        else:
            try:
                _create_database(db_path, args.schema_keys)
            except Exception as e:  # surfaced, never swallowed
                print(f"Error creating {db_path}: {e}", file=sys.stderr)
                return 1
            print(f"created  {_rel(db_path, report.root)} (schema: {', '.join(args.schema_keys)})")

    for warning in report.warnings:
        print(f"warning: {warning}", file=sys.stderr)
    print(f"SciStack project {report.package!r} ready at {report.root}")
    return 0


def _create_database(db_path: Path, schema_keys: list[str]) -> None:
    """Create the project's DuckDB file, leaving no configured database
    behind (this is a tool, not the user's pipeline)."""
    from scidb.database import clear_current_database

    from scidb import configure_database

    db = configure_database(db_path, schema_keys)
    db.close()
    clear_current_database()


def _rel(path: Path, root: Path) -> str:
    try:
        return Path(path).relative_to(root).as_posix()
    except ValueError:
        return str(path)


if __name__ == "__main__":
    sys.exit(main())
