"""
``scistack export`` / ``scistack import`` / ``scistack bundle-info``: whole
project bundles from the terminal (``.claude/plan-portability.md`` Stage 8).

The commands live here, not in the ``scistack`` package, because the GUI
needs them too and ``scistack-gui`` must never depend on ``scistack``: the
VS Code extension's "Import Project Bundle" command runs
``python -m scistack_gui.bundle_cli import ... --json`` in a separate process
(an import opens the NEW project's database, and a GUI session's process
already holds its own). ``scistack/__main__.py`` mounts the same parsers
through :func:`add_bundle_subparsers`, so there is one implementation.

Every flag's default is read from its owner, never restated:
``scidb.bundle.ExportOptions`` for export and ``import_project``'s signature
for ``--history``. The work itself is ``headless``'s compositions.

With ``--json``, the result is printed to stdout as ONE line of JSON, last
(the extension parses the last line); the console log is held at WARN on
stderr, and the INFO narrative goes to ``scidb.log`` as usual.
"""

from __future__ import annotations

import argparse
import inspect
import json
import sys
from pathlib import Path

#: ExportOptions field -> (its ``--X/--no-X`` flag, what it includes). The one
#: list of options a user is offered: the CLI's flags and the GUI's export
#: checkboxes (``api/bundles.get_export_options``) both read it. A field
#: missing here is not offered yet (``include_wheelhouse``: Stage 10).
EXPORT_FLAGS = {
    "include_history": ("history", "History: what was run, on which inputs, with which code"),
    "include_data": ("data", "Data: every saved result (can be large)"),
    "include_wheelhouse": (
        "wheelhouse",
        "Wheelhouse: a wheel of each library the project uses, so it installs without being published",
    ),
}


def export_choices() -> list[dict]:
    """``[{"name", "flag", "label", "default"}]`` for each offered option,
    each default read from ``ExportOptions``."""
    from scidb.bundle import ExportOptions

    defaults = ExportOptions()
    return [
        {"name": f, "flag": flag, "label": label, "default": getattr(defaults, f)}
        for f, (flag, label) in EXPORT_FLAGS.items()
    ]


class BundleCLIError(Exception):
    """A user-facing error: printed as one line, exit code 1."""


def _import_history_default() -> bool:
    from scidb.bundle import import_project

    return inspect.signature(import_project).parameters["import_history"].default


def _on_off(value: bool) -> str:
    return "on" if value else "off"


def add_bundle_subparsers(sub: argparse._SubParsersAction) -> None:
    """Add ``export``, ``import`` and ``bundle-info`` to a subparsers group
    (the ``scistack`` CLI's, or this module's own ``main``)."""
    from scidb.bundle import EXTENSION

    export =sub.add_parser(
        "export",
        help=f"Write this project as a {EXTENSION} bundle (code, config, canvas, history).",
    )
    export.add_argument(
        "out",
        nargs="?",
        type=Path,
        default=None,
        help=f"Bundle file to write (default: <database name>{EXTENSION} in the current folder).",
    )
    export.add_argument(
        "--db",
        default=None,
        help="Project database (default: SCIDB_DATABASE, scistack.toml's db, or the "
        "single .duckdb in the current folder).",
    )
    for choice in export_choices():
        export.add_argument(
            f"--{choice['flag']}",
            dest=choice["name"],
            action=argparse.BooleanOptionalAction,
            default=choice["default"],
            help=f"{choice['label']} (default: {_on_off(choice['default'])}).",
        )
    export.add_argument("--json", action="store_true", help="Print the result as JSON.")
    export.set_defaults(_bundle_cmd=_cmd_export)

    imp = sub.add_parser(
        "import",
        help=f"Make a NEW project from a {EXTENSION} bundle. Runs none of its code "
        "unless --check-code.",
    )
    imp.add_argument("bundle", type=Path, help=f"The {EXTENSION} file.")
    imp.add_argument(
        "--into",
        type=Path,
        default=None,
        help="Folder for the new project (default: ./<package>); must hold no "
        "project yet.",
    )
    imp.add_argument(
        "--schema",
        nargs="+",
        default=None,
        metavar="KEY",
        help="Your schema keys, top-down (default: the exporter's).",
    )
    imp.add_argument(
        "--map",
        action="append",
        default=[],
        metavar="OLD=NEW",
        help="Exporter key -> your key, repeatable. Keys with the same name map "
        "to themselves; OLD= drops a key.",
    )
    imp.add_argument(
        "--path-root",
        action="append",
        default=[],
        metavar="NAME=FOLDER",
        help="Your copy of a PathInput's raw files, repeatable (never copied).",
    )
    history_default = _import_history_default()
    imp.add_argument(
        "--history",
        dest="import_history",
        action=argparse.BooleanOptionalAction,
        default=history_default,
        help=f"Import the bundle's history (default: {_on_off(history_default)}).",
    )
    imp.add_argument(
        "--check-code",
        action="store_true",
        help="After importing, load the project's code and list canvas nodes "
        "whose function or variable is missing. Imports the bundle's code: "
        "needs --trust or a yes at the prompt.",
    )
    imp.add_argument(
        "--trust",
        action="store_true",
        help="Trust the bundle's code: install what the project needs that is missing "
        "(all or nothing, after a full check) and allow --check-code without asking.",
    )
    imp.add_argument(
        "--no-install",
        action="store_true",
        help="With --trust, do not install anything (run 'scistack install' later).",
    )
    imp.add_argument("--json", action="store_true", help="Print the report as JSON.")
    imp.set_defaults(_bundle_cmd=_cmd_import)

    info = sub.add_parser(
        "bundle-info",
        help=f"Describe a {EXTENSION} bundle (schema, PathInputs, sections, "
        "environment). Runs no code.",
    )
    info.add_argument("bundle", type=Path)
    info.add_argument("--json", action="store_true", help="Print the result as JSON.")
    info.set_defaults(_bundle_cmd=_cmd_info)

    inst = sub.add_parser(
        "install",
        help="Install what an imported project needs that this environment lacks "
        "(only missing packages; stops without changing anything on any conflict).",
    )
    inst.add_argument("project", nargs="?", type=Path, default=None,
                      help="The project folder (default: the current directory).")
    inst.add_argument("--yes", action="store_true",
                      help="Trust the project's packages without asking (installing runs their code).")
    inst.add_argument("--json", action="store_true", help="Print the result as JSON.")
    inst.set_defaults(_bundle_cmd=_cmd_install)


def dispatch(args: argparse.Namespace) -> int:
    """Run a parsed bundle command; 0 on success, 1 on a reported error."""
    from scidb.bundle import BundleError
    from scistacklog import Log

    console_level = Log.get_level("console")
    if getattr(args, "json", False):
        # stdout carries exactly one JSON line; keep the console quiet.
        Log.set_level("WARN", sink="console")
    try:
        result = args._bundle_cmd(args)
    except (BundleCLIError, BundleError, ValueError, OSError) as e:
        Log.error(f"[bundle_cli] {args.command}: {e}")
        if getattr(args, "json", False):
            print(json.dumps({"ok": False, "error": str(e)}))
        else:
            print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        Log.set_level(console_level, sink="console")
    if getattr(args, "json", False):
        print(json.dumps({"ok": True, **result}, default=str))
    return 0


def _parse_pairs(pairs: list[str], what: str, *, empty_is_none: bool) -> dict:
    out: dict = {}
    for pair in pairs:
        if "=" not in pair:
            raise BundleCLIError(f"{what} {pair!r} is not NAME=VALUE")
        name, value = pair.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not name:
            raise BundleCLIError(f"{what} {pair!r} has no name")
        if name in out:
            raise BundleCLIError(f"{what} gives {name!r} twice")
        if not value and not empty_is_none:
            raise BundleCLIError(f"{what} {pair!r} has no value")
        out[name] = value or None
    return out


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


def _cmd_export(args: argparse.Namespace) -> dict:
    from scidb.bundle import EXTENSION, ExportOptions
    from scidb.inspect.cli import CLIError, resolve_db_path

    from scistack_gui.headless import export_project_bundle

    try:
        db_path, source = resolve_db_path(args.db)
    except CLIError as e:
        raise BundleCLIError(str(e)) from e
    if not Path(db_path).is_file():
        raise BundleCLIError(f"Database not found: {db_path} (from {source})")
    options = ExportOptions(**{f: getattr(args, f) for f in EXPORT_FLAGS})
    out = args.out or Path.cwd() / f"{Path(db_path).stem}{EXTENSION}"
    path = export_project_bundle(db_path, out, options=options)
    if not args.json:
        size = path.stat().st_size
        print(f"exported {db_path} ({source})")
        print(f"      to {path} ({size:,} bytes)")
        print(f" options {', '.join(f'{f}={getattr(options, f)}' for f in EXPORT_FLAGS)}")
    return {"path": str(path), "options": options.to_dict()}


# ---------------------------------------------------------------------------
# import
# ---------------------------------------------------------------------------


def _confirm_trust(bundle: Path) -> bool:
    if not sys.stdin.isatty():
        return False
    answer = input(
        f"--check-code imports and runs the module-level code of {bundle.name}'s "
        "package. Trust it? [y/N] "
    )
    return answer.strip().lower() in ("y", "yes")


def _cmd_import(args: argparse.Namespace) -> dict:
    from scidb.bundle import read_bundle

    from scistack_gui.headless import check_code, import_project_bundle

    key_map = _parse_pairs(args.map, "--map", empty_is_none=True)
    path_roots = _parse_pairs(args.path_root, "--path-root", empty_is_none=False)
    if args.check_code and not args.trust and not _confirm_trust(args.bundle):
        # Decided before anything is written, so a refusal leaves no half import.
        raise BundleCLIError(
            "--check-code imports the bundle's code; pass --trust to allow it"
        )
    into = args.into
    if into is None:
        package = (read_bundle(args.bundle).manifest.get("project") or {}).get("package")
        into = Path.cwd() / (package or args.bundle.stem)
    report = import_project_bundle(
        args.bundle,
        into,
        schema_keys=args.schema,
        key_map=key_map or None,
        path_roots=path_roots or None,
        import_history=args.import_history,
    )
    installed = None
    if args.trust and not args.no_install:
        from scidb.environment import install_project_requirements

        installed = install_project_requirements(report.root).to_dict()
    checked = check_code(report.db_path, project=report.root) if args.check_code else None
    if not args.json:
        _print_import_report(report, checked)
        _print_install(installed, report.root, trusted=args.trust)
    return {"report": report.to_dict(), "check_code": checked, "install": installed}


def _print_install(installed: "dict | None", root, *, trusted: bool) -> None:
    if installed is None:
        if not trusted:
            print(f"Nothing installed. Once you trust it: scistack install \"{root}\"")
        return
    status = installed["status"]
    if status == "installed":
        print(f"  installed: {', '.join(installed['installed'])}")
    elif status == "nothing":
        print("  install: nothing missing")
    else:
        print(f"  install {status}: {installed['reason']}", file=sys.stderr)
        for c in installed["conflicts"]:
            print(f"    conflict: {c}", file=sys.stderr)
        if installed["command"]:
            print(f"  to install by hand: {installed['command']}", file=sys.stderr)


def _cmd_install(args: argparse.Namespace) -> dict:
    from scidb.environment import install_project_requirements

    root = (args.project or Path.cwd()).resolve()
    if not args.yes and not _confirm_trust_install(root):
        raise BundleCLIError("installing runs the packages' code; pass --yes to allow it")
    installed = install_project_requirements(root).to_dict()
    if not args.json:
        _print_install(installed, root, trusted=True)
    return {"install": installed}


def _confirm_trust_install(root: Path) -> bool:
    if not sys.stdin.isatty():
        return False
    answer = input(
        f"Installing what {root.name} needs runs those packages' code. Trust them? [y/N] "
    )
    return answer.strip().lower() in ("y", "yes")


def _print_import_report(report, checked: "dict | None") -> None:
    root = Path(report.root)

    def rel(p) -> str:
        try:
            return Path(p).relative_to(root).as_posix()
        except ValueError:
            return str(p)

    print(f"Imported package {report.package!r} into {root}")
    print(f"  schema: {', '.join(report.schema_keys)}")
    for p in report.created:
        print(f"  created {rel(p)}")
    for name, section in sorted(report.sections.items()):
        print(f"  {name}: {_summarise(section)}")
    schema = report.sections.get("schema") or {}
    for where, key in schema.get("dropped") or []:
        print(f"  dropped: {where} referred to {key!r}, which has no counterpart")
    for where, why in schema.get("flagged") or []:
        print(f"  review:  {where}: {why}")
    env = report.sections.get("env") or {}
    if env.get("install"):
        print(f"  install: {env['install']}")
    if checked is not None:
        if checked["unresolved_labels"]:
            print(f"  code: NOT FOUND for {', '.join(checked['unresolved_labels'])}")
        else:
            print(
                f"  code: every canvas node found ({checked['functions_loaded']} "
                f"function(s), {checked['variables_loaded']} variable(s))"
            )
        for w in checked["warnings"]:
            print(f"  code warning: {w}")
    for w in report.warnings:
        print(f"warning: {w}", file=sys.stderr)
    print(f"Open {report.db_path} in the SciStack GUI to continue.")


def _summarise(section) -> str:
    """One line per section: counts for lists, values for scalars."""
    if not isinstance(section, dict):
        return str(section)
    parts = []
    for k, v in section.items():
        if isinstance(v, (list, tuple, dict)):
            parts.append(f"{k}={len(v)}")
        else:
            parts.append(f"{k}={v}")
    return ", ".join(parts) if parts else "-"


# ---------------------------------------------------------------------------
# bundle-info
# ---------------------------------------------------------------------------


def _cmd_info(args: argparse.Namespace) -> dict:
    from scistack_gui.headless import preview_bundle

    info = preview_bundle(args.bundle)
    if not args.json:
        print(f"{info['path']}")
        print(f"  package {info['package']!r}, schema {', '.join(info['schema_keys'])}")
        print(f"  options {info['options']}")
        for name, s in info["sections"].items():
            print(f"  {name}: {s['files']} file(s), {s['bytes']:,} bytes")
        for p in info["path_inputs"]:
            roots = ", ".join(p["root_folders"]) or "(none)"
            print(f"  PathInput {p['name']}: {' | '.join(p['templates'])}  root {roots}")
        env = info.get("environment") or {}
        if env.get("install"):
            print(f"  install: {env['install']}")
    return {"info": info}


def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m scistack_gui.bundle_cli",
        description="SciStack project bundles (also: scistack export/import/bundle-info).",
    )
    add_bundle_subparsers(parser.add_subparsers(dest="command"))
    args = parser.parse_args(argv)
    if getattr(args, "_bundle_cmd", None) is None:
        parser.print_help()
        return 1
    return dispatch(args)


if __name__ == "__main__":
    sys.exit(main())
