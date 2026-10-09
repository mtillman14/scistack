"""
``scistack library list | add NAME | remove NAME | create``: the project's libraries
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
    create = cmds.add_parser(
        "create",
        help="Share a submodule as a new library package (code, pipeline, declarations).",
    )
    create.add_argument("--from-submodule", required=True, metavar="NAME_OR_ID",
                        help="The submodule's name (or pipeline id when names repeat).")
    create.add_argument("--into", required=True, type=Path, help="New, empty folder for the library.")
    create.add_argument("--name", default=None, help="Library (import) name; default from the submodule's name.")
    create.add_argument("--db", default=None, help="Project database (default: as scistack export finds it).")
    copy = cmds.add_parser(
        "copy",
        help="Make my own copy: copy a library into this project; its pipelines become editable.",
    )
    copy.add_argument("name")
    copy.add_argument("--db", default=None, help="Project database (default: as scistack export finds it).")
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


def _create(args: argparse.Namespace) -> dict:
    """Open the project WITH discovery (the generator needs the registry,
    as export does) and share the submodule."""
    from scidb.inspect.cli import CLIError, resolve_db_path

    from scistack_gui import pipeline_store as ps
    from scistack_gui.headless import open_for_export
    from scistack_gui.services.library_share import share_as_library, share_defaults

    try:
        db_path, _ = resolve_db_path(args.db)
    except CLIError as e:
        raise ValueError(str(e)) from e
    db = open_for_export(db_path, project=args.project)
    wanted = args.from_submodule
    matches = [p for p in ps.list_all_pipelines(db) if wanted in (p["pipeline_id"], p["name"])]
    if not matches:
        raise ValueError(f"no submodule named {wanted!r}")
    if len(matches) > 1:
        raise ValueError(
            f"{len(matches)} pipelines are named {wanted!r}; pass one of their ids: "
            f"{[m['pipeline_id'] for m in matches]}"
        )
    pid = matches[0]["pipeline_id"]
    name = args.name or share_defaults(db, pid)["name"]
    return share_as_library(db, pid, args.into, name).to_dict()


def dispatch(args: argparse.Namespace) -> int:
    from scistack_gui.config import add_package, remove_package

    project = (args.project or Path.cwd()).resolve()
    cmd = getattr(args, "library_command", None) or "list"
    if cmd == "copy":
        from scidb.inspect.cli import CLIError, resolve_db_path

        from scistack_gui.headless import open_for_export
        from scistack_gui.services.library_copy import make_own_copy

        try:
            db_path, _ = resolve_db_path(args.db)
            db = open_for_export(db_path, project=args.project)
            report = make_own_copy(db, args.name).to_dict()
        except (CLIError, ValueError, OSError) as e:
            if args.json:
                print(json.dumps({"ok": False, "error": str(e)}))
            else:
                print(f"Error: {e}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps({"ok": True, "report": report}))
            return 0
        print(f"copied {args.name} into {report['python_dir']}")
        if report["matlab_dir"]:
            print(f"  MATLAB: {report['matlab_dir']}")
        print(f"  {len(report['files'])} file(s); editable now: "
              f"{', '.join(report['released_pipelines']) or 'no pipelines'}")
        for v in report["declared_variables"]:
            print(f"  declared Variable {v}")
        for w in report["warnings"]:
            print(f"  warning: {w}", file=sys.stderr)
        return 0
    if cmd == "create":
        try:
            report = _create(args)
        except (ValueError, OSError) as e:
            if args.json:
                print(json.dumps({"ok": False, "error": str(e)}))
            else:
                print(f"Error: {e}", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps({"ok": True, "report": report}))
            return 0
        print(f"shared '{report['pipeline']}' as library {report['library']} at {report['dest']}")
        print(f"  functions: {', '.join(report['functions'].values()) or '-'}")
        for label in report["kept_labels"]:
            print(f"  from another library: {label}")
        for n in report["parameters"] + report["path_inputs"]:
            print(f"  default for: {n}")
        if report["dependencies"]:
            print(f"  depends on: {', '.join(report['dependencies'])}")
        for w in report["warnings"]:
            print(f"  warning: {w}", file=sys.stderr)
        print(f"install: {report['install']}")
        return 0
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
