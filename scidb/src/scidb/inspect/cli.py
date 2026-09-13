"""``scidb`` CLI — thin rendering shell over scidb.inspect.

Read commands (status, vars, schema, pipeline, variants, trace, runs,
state, locations, show, sql, exclusions) open the database strictly read-only via
``Inspector``. Write commands (exclude, include — and future declarative
writes) are registered with ``_write_handler`` instead of ``_handler``,
which routes them through ``mutate.Mutator`` (a per-invocation read-write
session with lock-contention mapped to a one-line error). That split is the
whole write seam: new write capabilities add a Mutator method + a
``_write_handler`` command and inherit session handling, audit logging, and
error mapping (see mutate.py's checklist).

Every command supports ``--json`` (machine-readable ``dataclasses.asdict``
output). Also mountable as the ``scistack db`` alias via ``add_db_subparser``.
"""

from __future__ import annotations

import argparse
import ast
import dataclasses
import json
import os
import sys
import time
from pathlib import Path

from ..exceptions import DatabaseLockedError
from ..log import Log
from . import render
from .api import Inspector
from .mutate import Mutator, lock_errors_mapped
from .pick import PickAborted, drill_down


class CLIError(Exception):
    """User-facing CLI failure; message printed to stderr, exit code 1."""


# ---------------------------------------------------------------------------
# Database discovery
# ---------------------------------------------------------------------------


def _pyproject_db(start: Path) -> str | None:
    """Find ``[tool.scistack] db = "…"`` in the nearest pyproject.toml upward."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            Log.debug("scidb cli: no tomllib/tomli — skipping pyproject.toml discovery")
            return None
    for directory in [start, *start.parents]:
        pyproject = directory / "pyproject.toml"
        if not pyproject.is_file():
            continue
        try:
            with open(pyproject, "rb") as f:
                data = tomllib.load(f)
        except Exception as e:
            Log.warn(f"scidb cli: could not parse {pyproject}: {e}")
            continue
        db = data.get("tool", {}).get("scistack", {}).get("db")
        if db:
            return str((directory / db).resolve()) if not Path(db).is_absolute() else db
    return None


def resolve_db_path(db_flag: str | None, cwd: Path | None = None) -> tuple[str, str]:
    """Resolve the database path → (path, source). Discovery order:
    --db flag > SCIDB_DATABASE env > pyproject [tool.scistack] db > single
    *.duckdb in cwd.
    """
    cwd = cwd or Path.cwd()
    if db_flag:
        return str(db_flag), "--db flag"
    env = os.environ.get("SCIDB_DATABASE")
    if env:
        return env, "SCIDB_DATABASE env var"
    from_pyproject = _pyproject_db(cwd)
    if from_pyproject:
        return from_pyproject, "pyproject.toml [tool.scistack] db"
    candidates = sorted(cwd.glob("*.duckdb"))
    if len(candidates) == 1:
        return str(candidates[0]), "single *.duckdb in cwd"
    if len(candidates) > 1:
        names = ", ".join(c.name for c in candidates)
        raise CLIError(
            f"Multiple .duckdb files in {cwd}: {names}. "
            f"Pick one with --db or set SCIDB_DATABASE."
        )
    raise CLIError(
        "No database found. Pass --db PATH, set SCIDB_DATABASE, add "
        '[tool.scistack] db = "..." to pyproject.toml, or run in a '
        "directory containing exactly one .duckdb file."
    )


def _parse_kv(pairs: list[str]) -> dict[str, str]:
    """Parse ``key=value`` args. Values stay strings — schema key values are
    stored as strings (zero-padded keys like "01" must survive verbatim)."""
    out: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise CLIError(f"Expected key=value, got {pair!r}")
        key, value = pair.split("=", 1)
        out[key] = value
    return out


def _parse_kv_lists(pairs: list[str]) -> dict[str, list[str]]:
    """Like _parse_kv, but repeated keys accumulate into lists
    (``subject=S01 subject=S02`` → {"subject": ["S01", "S02"]})."""
    out: dict[str, list[str]] = {}
    for pair in pairs:
        if "=" not in pair:
            raise CLIError(f"Expected key=value, got {pair!r}")
        key, value = pair.split("=", 1)
        out.setdefault(key, []).append(value)
    return out


def _coerce_non_schema(metadata: dict, schema_keys) -> dict:
    """Literal-eval NON-schema values so ``low_hz=20`` matches the stored
    int 20 (branch-param matching is exact-typed equality). Schema-key values
    are returned verbatim: schema columns are stored as strings and
    zero-padded values like "01" must never be auto-converted."""
    out: dict = {}
    for key, value in metadata.items():
        if key in schema_keys:
            out[key] = value
        else:
            try:
                out[key] = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                out[key] = value
    return out


_STYLE_PRESETS = {"default": render.DEFAULT_STYLE, "ascii": render.ASCII_STYLE}


def _resolve_style(args) -> render.RenderStyle:
    name = getattr(args, "style", None) or os.environ.get("SCIDB_STYLE") or "default"
    preset = _STYLE_PRESETS.get(name)
    if preset is None:
        raise CLIError(
            f"Unknown render style {name!r}; choose from {sorted(_STYLE_PRESETS)}"
        )
    if _want_color(getattr(args, "no_color", False), sys.stdout.isatty()):
        preset = render.with_ansi_colors(preset)
    return preset


def _want_color(no_color: bool, isatty: bool) -> bool:
    """Color state tags only on a real terminal, and never under --no-color
    (piped/captured output stays byte-plain — including --json and -o files)."""
    return isatty and not no_color


def _as_payload(result):
    """A result's JSON shape.

    ``dataclasses.asdict`` silently drops ``@property`` values, so a dataclass
    whose headline numbers are derived (``LocationTree.total`` / ``green`` /
    ``verdict``) would serialize without them. Its own ``to_dict`` wins when it
    has one — which keeps those numbers defined once, on the tree, instead of
    recomputed at every edge that serializes it.
    """
    to_dict = getattr(result, "to_dict", None)
    return to_dict() if callable(to_dict) else dataclasses.asdict(result)


def _emit_json(result) -> None:
    if isinstance(result, list):
        payload = [_as_payload(r) for r in result]
    else:
        payload = _as_payload(result)
    print(json.dumps(payload, indent=2, default=str))


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def _cmd_status(insp: Inspector, args) -> None:
    overview = insp.overview()
    if args.json:
        _emit_json(overview)
    else:
        print(render.render_overview(overview))


def _cmd_vars(insp: Inspector, args) -> None:
    if args.type:
        detail = insp.variable(args.type)
        _emit_json(detail) if args.json else print(
            render.render_variable_detail(detail)
        )
    else:
        variables = insp.variables()
        _emit_json(variables) if args.json else print(
            render.render_variables(variables)
        )


def _cmd_schema(insp: Inspector, args) -> None:
    tree = insp.schema_tree()
    if args.json:
        _emit_json(tree)
    elif args.tree:
        print(render.render_schema_tree(tree, style=_resolve_style(args)))
    else:
        print(render.render_schema_summary(tree))


def _cmd_pipeline(insp: Inspector, args) -> None:
    graph = insp.pipeline(output_type=args.type)
    style = _resolve_style(args)
    if args.json or args.format == "json":
        text = json.dumps(dataclasses.asdict(graph), indent=2, default=str)
    elif args.format == "tree":
        text = render.render_pipeline_tree(
            graph,
            expand_variants=args.variants,
            include_values=args.values,
            style=style,
        )
    elif args.format == "mermaid":
        text = render.render_pipeline_mermaid(graph, style=style)
    else:  # dot
        text = render.render_pipeline_dot(graph, style=style)
    if args.output is not None:
        Path(args.output).write_text(text + "\n")
        print(f"wrote {args.output}", file=sys.stderr)
    else:
        print(text)


def _cmd_variants(insp: Inspector, args) -> None:
    variants = insp.variants(args.name)
    if args.json:
        _emit_json(variants)
    else:
        print(render.render_variants_table(variants))


def _stderr_chooser(title: str, labels) -> int:
    """Numbered menu on stderr, choice from stdin — stdout stays clean so
    ``$(scidb pick …)`` captures only the record_id."""
    print(title, file=sys.stderr)
    for i, label in enumerate(labels, 1):
        print(f"  {i}. {label}", file=sys.stderr)
    while True:
        sys.stderr.write(f"choice [1-{len(labels)}, q to cancel]: ")
        sys.stderr.flush()
        try:
            raw = input().strip()
        except EOFError:
            raise PickAborted()
        if raw.lower() in ("q", "quit"):
            raise PickAborted()
        if raw.isdigit() and 1 <= int(raw) <= len(labels):
            return int(raw) - 1
        print(f"invalid choice: {raw!r}", file=sys.stderr)


def _cmd_pick(insp: Inspector, args) -> None:
    schema_keys = insp._db.dataset_schema_keys
    metadata = _coerce_non_schema(_parse_kv(args.metadata), schema_keys)
    type_name = args.type

    try:
        if type_name is None:
            if not args.interactive:
                raise CLIError(
                    "pick needs a variable type (or --interactive to choose one)"
                )
            variables = [v for v in insp.variables() if v.record_count > 0]
            if not variables:
                raise CLIError("No variables with records in this database")
            idx = _stderr_chooser(
                "Select variable:",
                [f"{v.name}   ({v.record_count} records)" for v in variables],
            )
            type_name = variables[idx].name

        candidates = insp.pick(type_name, **metadata)
        if not candidates:
            raise CLIError(f"No {type_name} records match {metadata or '(any)'}")

        if args.json:
            _emit_json(candidates)
        elif args.table:
            print(render.render_pick_table(candidates, schema_keys))
        elif args.interactive:
            chosen = drill_down(candidates, schema_keys, _stderr_chooser)
            print(chosen.record_id)
        elif len(candidates) == 1:
            print(candidates[0].record_id)
        else:
            # Ambiguous non-interactive pick must fail so $(…) gets nothing —
            # the disambiguation table goes to stderr.
            print(render.render_pick_table(candidates, schema_keys), file=sys.stderr)
            raise CLIError(
                f"{len(candidates)} records match — narrow with schema keys / "
                f"branch params, or use --interactive / --table / --json."
            )
    except PickAborted:
        raise CLIError("selection cancelled")


def _cmd_exclusions(insp: Inspector, args) -> None:
    exclusions = insp.exclusions()
    if args.json:
        _emit_json(exclusions)
    else:
        print(render.render_exclusions(exclusions, insp._db.dataset_schema_keys))


def _cmd_exclude(mut: Mutator, args) -> None:
    result = mut.exclude_schema(args.reason, **_parse_kv(args.schema))
    _emit_json(result) if args.json else print(render.render_mutation_result(result))


def _cmd_include(mut: Mutator, args) -> None:
    result = mut.include_schema(args.reason, **_parse_kv(args.schema))
    _emit_json(result) if args.json else print(render.render_mutation_result(result))


def _cmd_sql(insp: Inspector, args) -> None:
    result = insp.sql(args.query)
    if args.json:
        _emit_json(result)
    else:
        print(render.format_table(result.columns, result.rows))
        print(f"({result.row_count} rows)")


def _cmd_trace(insp: Inspector, args) -> None:
    if args.type is None and args.record_id is None:
        raise CLIError(
            "trace needs a variable type (plus key=val filters) or --record-id"
        )
    metadata = _coerce_non_schema(
        _parse_kv(args.metadata), insp._db.dataset_schema_keys
    )
    tree = insp.trace(
        args.type, record_id=args.record_id, include_audit=args.audit, **metadata
    )
    if args.json:
        _emit_json(tree)
    else:
        print(render.render_trace(tree, style=_resolve_style(args)))


def _cmd_report(insp: Inspector, args) -> None:
    if args.json:
        _emit_json(
            insp.report(fn=args.fn, variable=args.var, all_versions=args.all_versions)
        )
        return
    out_dir = args.output or (
        f"scidb-report-{Path(insp._db.dataset_db_path).stem}-{time.strftime('%Y%m%d')}"
    )
    index = insp.write_report(
        out_dir,
        fn=args.fn,
        variable=args.var,
        all_versions=args.all_versions,
        copy_artifacts=not args.no_copy,
        embed=args.embed,
    )
    print(f"Report written: {index}")


def _cmd_runs(insp: Inspector, args) -> None:
    runs = insp.runs(fn=args.fn, limit=args.limit)
    if args.json:
        _emit_json(runs)
    else:
        print(render.render_runs_table(runs))


def _cmd_state(insp: Inspector, args) -> None:
    if args.pathinput:
        if not args.fn:
            raise CLIError("state --pathinput requires a function name")
        grid = _parse_kv_lists(args.metadata)
        states = insp.pathinput_state(args.fn, **grid)
    else:
        if args.metadata:
            raise CLIError("key=value grid args only apply with --pathinput")
        states = insp.node_state(args.fn)
    if args.json:
        _emit_json(states)
    else:
        print(
            render.render_node_states(
                states, show_missing=args.missing, style=_resolve_style(args)
            )
        )


def _cmd_locations(insp: Inspector, args) -> None:
    # Branch-param values are literal-eval'd (low_hz=20 must match the stored
    # int), and no key here is ever a schema key — a location filter is the
    # positional grid, a variant is `fn.param`.
    variant = _coerce_non_schema(_parse_kv(args.variant or []), [])
    grid = _parse_kv_lists(args.metadata)
    tree = insp.locations(
        args.type, variant=variant or None, problems_only=args.problems, **grid
    )
    if args.json:
        _emit_json(tree)
    else:
        print(
            render.render_location_tree(
                tree, style=_resolve_style(args), depth=args.depth
            )
        )


def _cmd_show(insp: Inspector, args) -> None:
    metadata = _coerce_non_schema(
        _parse_kv(args.metadata), insp._db.dataset_schema_keys
    )
    records = insp.records(
        args.type,
        latest=not args.versions,
        include_excluded=args.include_excluded,
        include_values=args.values,
        **metadata,
    )
    if args.json:
        _emit_json(records)
    else:
        print(render.render_records(records, insp._db.dataset_schema_keys))


# ---------------------------------------------------------------------------
# Parser wiring
# ---------------------------------------------------------------------------


def _add_global_args(
    parser: argparse.ArgumentParser, suppress_defaults: bool = False
) -> None:
    """Add the global flags.

    ``suppress_defaults=True`` is for the copies attached to subcommand
    parsers: since Python 3.9 a subparser parses into a *fresh* namespace and
    copies every value back, so a subcommand-level default (db=None,
    json=False) would clobber a flag parsed before the subcommand. With
    default=SUPPRESS, a flag the user didn't pass after the subcommand never
    enters the sub-namespace and the root-parsed value survives.
    """
    d = {"default": argparse.SUPPRESS} if suppress_defaults else {}
    parser.add_argument(
        "--db", **d, help="Path to the .duckdb database (else discovered)."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        **d,
        help="Emit machine-readable JSON instead of tables.",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        **d,
        help="Disable colored output (Phase 1 output is uncolored).",
    )
    parser.add_argument(
        "--style",
        choices=["default", "ascii"],
        **d,
        help="Render style preset (or set SCIDB_STYLE). "
        "'ascii' avoids Unicode box-drawing characters.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        **d,
        help="Verbose logging (facade call timing etc.).",
    )


def _global_parent() -> argparse.ArgumentParser:
    """Global flags as a parents=[] parser so they are accepted both before
    and after the subcommand (``scidb --json vars`` and ``scidb vars --json``)."""
    parent = argparse.ArgumentParser(add_help=False)
    _add_global_args(parent, suppress_defaults=True)
    return parent


def _add_commands(
    sub: argparse._SubParsersAction, parent: argparse.ArgumentParser
) -> None:
    p = sub.add_parser(
        "status",
        parents=[parent],
        help="Database overview: counts, size, last activity.",
    )
    p.set_defaults(_handler=_cmd_status)

    p = sub.add_parser(
        "vars", parents=[parent], help="List variable types, or detail for one."
    )
    p.add_argument(
        "type", nargs="?", default=None, help="Variable type name for a detailed view."
    )
    p.set_defaults(_handler=_cmd_vars)

    p = sub.add_parser(
        "schema", parents=[parent], help="Schema keys and realized hierarchy."
    )
    p.add_argument(
        "--tree", action="store_true", help="Render the full hierarchy tree."
    )
    p.set_defaults(_handler=_cmd_schema)

    p = sub.add_parser(
        "pipeline",
        parents=[parent],
        help="The pipeline DAG: functions, variables, variants, state.",
    )
    p.add_argument(
        "--type",
        default=None,
        help="Restrict to this variable type and everything upstream.",
    )
    p.add_argument(
        "--variants",
        action="store_true",
        help="Expand each step into one line per constants variant.",
    )
    p.add_argument(
        "--values",
        action="store_true",
        help="Show input params and PathInput specs per step.",
    )
    p.add_argument(
        "--format",
        choices=["tree", "mermaid", "dot", "json"],
        default="tree",
        help="Output format (default: tree).",
    )
    p.add_argument(
        "-o", "--output", default=None, help="Write output to a file instead of stdout."
    )
    p.set_defaults(_handler=_cmd_pipeline)

    p = sub.add_parser(
        "variants",
        parents=[parent],
        help="Coexisting variants of a variable type or function.",
    )
    p.add_argument("name", help="Variable type or function name.")
    p.set_defaults(_handler=_cmd_variants)

    p = sub.add_parser(
        "trace", parents=[parent], help="Full upstream provenance of one record."
    )
    p.add_argument(
        "type",
        nargs="?",
        default=None,
        help="Variable type name (omit when using --record-id).",
    )
    p.add_argument(
        "metadata",
        nargs="*",
        help="key=value filters. Schema-key values match verbatim; "
        "other values are parsed as Python literals "
        "(low_hz=20 matches the stored int 20).",
    )
    p.add_argument(
        "--record-id",
        default=None,
        help="Trace this exact record instead of resolving by metadata.",
    )
    p.add_argument(
        "--audit",
        action="store_true",
        help="Append the execution audit (who ran it, when, where=).",
    )
    p.set_defaults(_handler=_cmd_trace)

    p = sub.add_parser(
        "report",
        parents=[parent],
        help="Collect finalized endpoint (plot_/stat_) records "
        "into a self-contained HTML report folder "
        "(figures + per-test stats tables + provenance).",
    )
    p.add_argument(
        "--fn", default=None, help="Only this endpoint function (e.g. plot_gait)."
    )
    p.add_argument(
        "--var",
        default=None,
        help="Only this output variable type. NOTE: filters on the "
        "PRODUCING fn prefix, so only endpoint-produced "
        "records of the type appear.",
    )
    p.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output directory (default: ./scidb-report-<dbname>-<date>).",
    )
    p.add_argument(
        "--all-versions",
        action="store_true",
        help="Include superseded record versions (default: latest per variant).",
    )
    p.add_argument(
        "--no-copy",
        action="store_true",
        help="Link original artifact paths instead of copying "
        "them into <outdir>/artifacts/ (report not portable).",
    )
    p.add_argument(
        "--no-embed",
        dest="embed",
        action="store_false",
        default=True,
        help="Never inline images into index.html (always link).",
    )
    p.set_defaults(_handler=_cmd_report)

    p = sub.add_parser(
        "runs", parents=[parent], help="Execution audit log (_run), newest first."
    )
    p.add_argument("--fn", default=None, help="Only runs of this function.")
    p.add_argument(
        "-n", "--limit", type=int, default=50, help="Maximum rows (default: 50)."
    )
    p.set_defaults(_handler=_cmd_runs)

    p = sub.add_parser(
        "state", parents=[parent], help="Green/red run state per pipeline function."
    )
    p.add_argument(
        "fn", nargs="?", default=None, help="Function name (omit for all functions)."
    )
    p.add_argument(
        "metadata",
        nargs="*",
        help="Iteration grid for --pathinput (repeat keys for "
        "lists: subject=S01 subject=S02).",
    )
    p.add_argument(
        "--missing",
        action="store_true",
        help="List the missing schema combos per red node.",
    )
    p.add_argument(
        "--pathinput",
        action="store_true",
        help="Discovery-based check for a PathInput loader: "
        "files on disk ∩ grid − exclusions vs realized.",
    )
    p.set_defaults(_handler=_cmd_state)

    p = sub.add_parser(
        "locations",
        parents=[parent],
        help="Per-location status for one variable: green/amber/red/grey.",
    )
    p.add_argument("type", help="Variable type name.")
    p.add_argument(
        "metadata",
        nargs="*",
        help="Iteration grid for PathInput discovery (repeat keys for lists: "
        "subject=S01 subject=S02). Only consulted for loader-produced variables.",
    )
    p.add_argument(
        "--variant",
        action="append",
        metavar="fn.param=value",
        help="Pin a branch param or code version (repeatable): "
        "bandpass.low_hz=20, __code__=latest.",
    )
    p.add_argument(
        "--problems",
        action="store_true",
        help="Only the branches holding an amber or red location "
        "(counts stay intact).",
    )
    p.add_argument(
        "--depth",
        type=int,
        default=0,
        metavar="N",
        help="Stop printing below depth N (0 = whole tree).",
    )
    p.set_defaults(_handler=_cmd_locations)

    p = sub.add_parser(
        "show", parents=[parent], help="Records at a location (latest per variant)."
    )
    p.add_argument("type", help="Variable type name.")
    p.add_argument(
        "metadata",
        nargs="*",
        help="key=value filters (schema keys and branch params; "
        "values are matched as strings).",
    )
    p.add_argument(
        "--versions",
        action="store_true",
        help="Show every saved version, not just the latest per variant.",
    )
    p.add_argument(
        "--include-excluded",
        action="store_true",
        help="Include records marked excluded.",
    )
    p.add_argument(
        "--values",
        action="store_true",
        help="Add a compact value preview per record (storage form).",
    )
    p.set_defaults(_handler=_cmd_show)

    p = sub.add_parser(
        "sql",
        parents=[parent],
        help="Read-only SQL escape hatch (rendered as a table).",
    )
    p.add_argument("query", help="The SELECT to run (writes fail: read-only).")
    p.set_defaults(_handler=_cmd_sql)

    p = sub.add_parser(
        "pick",
        parents=[parent],
        help="Resolve a variable output to its record_id "
        "(prints only the id — composable in $(…)).",
    )
    p.add_argument(
        "type",
        nargs="?",
        default=None,
        help="Variable type (omit with --interactive to choose one).",
    )
    p.add_argument(
        "metadata", nargs="*", help="key=value filters (same rules as show/trace)."
    )
    p.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Drill down via menus (variable → schema keys → "
        "variant); menus on stderr, record_id on stdout.",
    )
    p.add_argument(
        "--table",
        action="store_true",
        help="List all candidates as a table instead of selecting.",
    )
    p.set_defaults(_handler=_cmd_pick)

    p = sub.add_parser(
        "exclusions",
        parents=[parent],
        help="List currently-excluded schema combinations.",
    )
    p.set_defaults(_handler=_cmd_exclusions)

    p = sub.add_parser(
        "exclude",
        parents=[parent],
        help="Exclude a schema combination from every analysis "
        "(write; omitted keys are wildcards).",
    )
    p.add_argument(
        "schema", nargs="+", help="key=value schema keys (values verbatim strings)."
    )
    p.add_argument(
        "--reason",
        required=True,
        help="Why this data is excluded (stored in the audit trail).",
    )
    p.set_defaults(_write_handler=_cmd_exclude)

    p = sub.add_parser(
        "include",
        parents=[parent],
        help="Re-include a previously excluded combination (write; history preserved).",
    )
    p.add_argument(
        "schema", nargs="+", help="key=value schema keys of the exact excluded keyset."
    )
    p.add_argument(
        "--reason",
        required=True,
        help="Why this data is re-included (stored in the audit trail).",
    )
    p.set_defaults(_write_handler=_cmd_include)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scidb",
        description="Inspect a scidb database (read-only).",
    )
    _add_global_args(parser)
    _add_commands(parser.add_subparsers(dest="command"), _global_parent())
    return parser


def add_db_subparser(sub: argparse._SubParsersAction) -> None:
    """Mount these commands as the ``scistack db`` alias (owning-layer rule:
    scistack/__main__.py calls this; all logic stays in scidb)."""
    db_parser = sub.add_parser("db", help="Inspect a scidb database (read-only).")
    _add_global_args(db_parser)
    _add_commands(db_parser.add_subparsers(dest="db_command"), _global_parent())
    db_parser.set_defaults(_dispatch=dispatch)


def dispatch(args: argparse.Namespace) -> int:
    """Run a parsed inspect command. Shared by ``scidb`` and ``scistack db``.

    Read commands (``_handler``) get a read-only Inspector; write commands
    (``_write_handler``) get a per-invocation read-write Mutator — the seam
    every future write capability plugs into.
    """
    handler = getattr(args, "_handler", None)
    write_handler = getattr(args, "_write_handler", None)
    if handler is None and write_handler is None:
        build_parser().print_help()
        return 1
    if args.verbose:
        Log.set_level("DEBUG")
    else:
        # stdout carries the command's results; keep stderr quiet unless
        # something is actually wrong. The file sink keeps the INFO narrative.
        Log.set_level("WARN", sink="console")
    try:
        db_path, source = resolve_db_path(args.db)
        if not Path(db_path).is_file():
            raise CLIError(f"Database not found: {db_path} (from {source})")
        # Same convention as configure_database: scidb.log next to the db file.
        if Log.get_path() is None:
            from scidb.log import attach_log_file

            attach_log_file(db_path)
        mode = "write" if write_handler else "read-only"
        Log.info(f"scidb cli: db={db_path} (resolved via {source}, {mode})")
        if args.verbose:
            print(f"database: {db_path} (via {source}, {mode})", file=sys.stderr)
        if write_handler is not None:
            with Mutator.open(db_path) as mut, lock_errors_mapped(db_path):
                write_handler(mut, args)
        else:
            with Inspector.open(db_path) as insp:
                handler(insp, args)
        return 0
    except (CLIError, DatabaseLockedError) as e:
        print(f"Error: {e}", file=sys.stderr)
        Log.error(f"scidb cli failed: {type(e).__name__}: {e}")
        return 1
    except ValueError as e:
        # Primitive-level validation (unknown key, already excluded, …).
        print(f"Error: {e}", file=sys.stderr)
        Log.error(f"scidb cli failed: {type(e).__name__}: {e}")
        return 1
    except Exception as e:
        # DuckDB lock contention, malformed db, unknown variable, bad filters…
        print(f"Error: {e}", file=sys.stderr)
        Log.error(f"scidb cli failed: {type(e).__name__}: {e}")
        return 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return dispatch(args)


if __name__ == "__main__":
    sys.exit(main())
