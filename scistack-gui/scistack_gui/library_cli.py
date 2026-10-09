"""
``scistack library list | add NAME | remove NAME``: the project's libraries
from the terminal (portability Stage 10a).

The same owners as the GUI's Libraries list: ``config.add_package`` /
``remove_package`` (the ``packages`` list in scistack.toml) and
``scidb.library.read_library`` (what a library offers). Like the bundle
commands (``bundle_cli``), they live in the GUI package and ``scistack``
mounts them. Nothing is installed: a listed package that is not importable
is reported with what to do.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


def add_library_subparsers(sub: argparse._SubParsersAction) -> None:
    lib = sub.add_parser("library", help="List, add or remove the project's libraries.")
    lib.add_argument(
        "--project", type=Path, default=None,
        help="Project folder (default: the current directory).",
    )
    lib.add_argument("--json", action="store_true", help="Print the result as JSON.")
    cmds = lib.add_subparsers(dest="library_command")
    cmds.add_parser("list", help="Each listed library: schema keys, pipelines, MATLAB, problems.")
    add = cmds.add_parser("add", help="List an installed package as a library (scistack.toml packages).")
    add.add_argument("name", help="Its import name (e.g. gait_tools).")
    rm = cmds.add_parser("remove", help="Unlist a library (its pipelines stay on your canvases).")
    rm.add_argument("name")
    lib.set_defaults(_library_cmd=dispatch)


def _listed(project: Path) -> list[str]:
    from scifor.discovery import project_config_at, read_scistack_section

    config = project_config_at(project)
    section = read_scistack_section(config) if config else {}
    return [p for p in (section or {}).get("packages", []) if isinstance(p, str)]


def _describe(project: Path) -> list[dict]:
    from scidb.library import read_library

    out = []
    for name in _listed(project):
        top = name.split(".")[0]
        if importlib.util.find_spec(top) is None:
            out.append({"library": name, "installed": False, "pipelines": [], "schema_keys": None,
                        "matlab": False, "errors": [f"{top} is not importable from {sys.executable}"]})
            continue
        info = read_library(top)
        out.append({
            "library": name,
            "installed": True,
            "schema_keys": info.schema_keys,
            "pipelines": [p.name for p in info.pipelines],
            "matlab": info.matlab_dir is not None,
            "errors": info.errors,
        })
    return out


def dispatch(args: argparse.Namespace) -> int:
    from scistack_gui.config import add_package, remove_package

    project = (args.project or Path.cwd()).resolve()
    cmd = getattr(args, "library_command", None) or "list"
    try:
        if cmd == "add":
            add_package(None, args.name, project=project)
        elif cmd == "remove":
            remove_package(None, args.name, project=project)
    except (ValueError, FileNotFoundError, OSError) as e:
        if args.json:
            print(json.dumps({"ok": False, "error": str(e)}))
        else:
            print(f"Error: {e}", file=sys.stderr)
        return 1
    libraries = _describe(project)
    if args.json:
        print(json.dumps({"ok": True, "libraries": libraries}))
        return 0
    if cmd == "add":
        print(f"listed {args.name} in {project / 'scistack.toml'}")
    elif cmd == "remove":
        print(f"unlisted {args.name}")
    if not libraries:
        print("no libraries listed")
    for lib in libraries:
        status = "" if lib["installed"] else "  (NOT INSTALLED)"
        print(f"{lib['library']}{status}")
        if lib["schema_keys"]:
            print(f"  schema: {', '.join(lib['schema_keys'])}")
        for p in lib["pipelines"]:
            print(f"  pipeline: {p}")
        if lib["matlab"]:
            print("  MATLAB functions: yes")
        for e in lib["errors"]:
            print(f"  problem: {e}")
    if any(not lib["installed"] for lib in libraries):
        print(f"Install missing libraries into this Python: {sys.executable} -m pip install ...")
    return 0
