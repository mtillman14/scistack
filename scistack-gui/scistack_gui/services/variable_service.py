"""
Variable service: variable creation, and the record / plot-data queries
the sidebar draws from.

Used by both server.py's JSON-RPC handler (VS Code extension) and
api/variables.py's FastAPI route, so validation/target-file/MATLAB-fallback
behavior can't drift between the two transports.
"""

from __future__ import annotations

import keyword
import logging

from fastapi import HTTPException
from scidb.store.database import DatabaseManager

from scistack_gui.stores import history

logger = logging.getLogger(__name__)


def create_variable(
    name: str, docstring: str | None = None, language: str = "python"
) -> dict:
    """Validate, write a new BaseVariable subclass, and refresh the registry.

    Works for both Python and MATLAB variables.

    Returns:
        {"ok": True, "name": name} on success,
        {"ok": False, "error": message} on failure.
    """
    from scidb import BaseVariable
    from scistack_gui.matlab import registry as matlab_registry
    from scistack_gui.project import registry

    name = name.strip()
    logger.debug("create_variable: name=%r, language=%r", name, language)

    # --- Validation ---
    if not name or not name.isidentifier() or keyword.iskeyword(name):
        return {"ok": False, "error": f"'{name}' is not a valid class name."}
    if name.startswith("_"):
        return {
            "ok": False,
            "error": "Variable names must not start with an underscore.",
        }
    if name in BaseVariable._all_subclasses:
        # Always say WHERE. This message used to be the bare "already
        # exists", which was unfalsifiable from the user's side when the
        # registry held a name nothing on disk declared -- the reason
        # BaseVariable.unregister and registry._variable_sources now exist.
        # Naming the source makes a stale registration visibly wrong instead
        # of just baffling.
        source = registry._variable_sources.get(name)
        where = f" It is declared in {source}." if source else (
            " Nothing in this project declares it, so it is a stale "
            "registration — hit 🔄 Refresh Code and try again."
        )
        logger.info(
            "[variable_service] Refusing to create '%s': already registered "
            "(source=%s)",
            name,
            source or "<untracked>",
        )
        return {
            "ok": False,
            "error": f"A variable named '{name}' already exists.{where}",
        }

    # MATLAB variable creation. When the project has a TOML entities file,
    # the declaration goes THERE and the classdef is materialized from it --
    # one declaration, in the language-neutral file, plus the stub MATLAB
    # needs to reference the type. Without one (a MATLAB-only project that
    # never configured an entities file), the classdef is still the
    # declaration, as before.
    if language == "matlab":
        return _create_matlab_variable(name, docstring)

    # Python variable creation.
    from scistack_gui.services.target_file_service import (
        ensure_scidb_import,
        get_or_create_target_file,
        is_toml_target,
        write_variable,
    )

    target_file, target_err = get_or_create_target_file()

    if target_file is None:
        # No Python target — fall back to MATLAB if configured.
        if matlab_registry.has_matlab_config() and matlab_registry._config is not None:
            # No longer requires a configured variable_dir: the classdef
            # destination falls back to scimatlab.stubs.variable_stub_dir.
            return _create_matlab_variable(name, docstring)
        return {"ok": False, "error": target_err}

    if is_toml_target(target_file):
        if docstring:
            # A value-less list has nowhere to put one (plan D4). Logged
            # rather than dropped silently.
            logger.warning(
                "[variable_service] Dropping docstring %r for %r: the TOML "
                "entities file declares variables as bare names; declare the "
                "class in Python if you need a docstring",
                docstring,
                name,
            )
        logger.info(
            "[variable_service] create_variable: name=%r target=%s", name, target_file
        )
        err = write_variable(target_file, name)
        if err:
            return err
        return {"ok": True, "name": name}

    lines = ["\n"]
    if docstring:
        escaped = docstring.replace('"""', '\\"\\"\\"')
        lines.append(
            f'class {name}(scidb.BaseVariable):\n    """{escaped}"""\n    pass\n'
        )
    else:
        lines.append(f"class {name}(scidb.BaseVariable):\n    pass\n")

    try:
        ensure_scidb_import(target_file)
        history.note_write(target_file)
        with open(target_file, "a") as f:
            f.writelines(lines)
    except OSError as e:
        return {"ok": False, "error": f"Failed to write to module file: {e}"}

    # Narrow by default: the append above went into one file, so re-reading
    # that file is the whole reload. Never refresh_registries() here -- see
    # target_file_service._reload_after_write for the dispatch and the cost.
    from scistack_gui.services.target_file_service import _reload_after_write

    error = _reload_after_write(target_file)
    if error is not None:
        return {"ok": False, "error": error}

    return {"ok": True, "name": name}


def _create_matlab_variable(name: str, docstring: str | None = None) -> dict:
    """Create a MATLAB classdef variable file and register the surrogate.

    With a TOML entities file configured, the *declaration* is written there
    first and the classdef becomes a materialized stub of it -- so the same
    variable is visible to Python without being declared twice. The classdef
    is still written here rather than left to the next scan, because the
    user expects the file to exist the moment they hit create.
    """
    from scistack_gui.matlab import registry as matlab_registry
    from scistack_gui.services.target_file_service import (
        get_or_create_target_file,
        is_toml_target,
        write_variable,
    )

    if matlab_registry._config is None:
        return {
            "ok": False,
            "error": "No MATLAB configuration loaded for this project.",
        }

    entities_file, _ = get_or_create_target_file()
    if is_toml_target(entities_file):
        err = write_variable(entities_file, name)
        if err:
            return err
        logger.info(
            "[variable_service] Declared MATLAB variable %r in the entities "
            "file; materializing its classdef",
            name,
        )

    # An explicit [matlab] variable_dir wins; otherwise the classdef goes
    # where scimatlab materializes stubs, which is the directory
    # +scidb/entities.m adds to the MATLAB path at run time. Falling back
    # rather than refusing is the point: a project can declare its
    # variables in the entities file and never configure a variable_dir.
    target_dir = matlab_registry._config.matlab_variable_dir
    if target_dir is None:
        from scimatlab.stubs import variable_stub_dir

        from scistack_gui.project import registry as _registry

        target_dir = variable_stub_dir(_registry.get_project_root())
    if target_dir is None:
        return {
            "ok": False,
            "error": (
                "Nowhere to write the classdef: configure "
                "[matlab] variable_dir in scistack.toml, or an entities_file "
                "for its default location."
            ),
        }
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"{name}.m"

    if target_file.exists():
        return {"ok": False, "error": f"File already exists: {target_file}"}

    # scimatlab owns the stub's text, so a GUI-created classdef and one
    # materialized from the entities file at run time are byte-identical
    # (CLAUDE.md NOTE 3). A docstring is appended as a comment because the
    # shared text has nowhere to carry one.
    from scimatlab.stubs import classdef_text, invalid_name_reason

    reason = invalid_name_reason(name)
    if reason is not None:
        return {
            "ok": False,
            "error": f"'{name}' cannot be a MATLAB class: {reason}.",
        }

    body = classdef_text(name)
    if docstring:
        body = body.replace("end\n", f"    % {docstring}\nend\n")

    try:
        history.note_write(target_file)
        target_file.write_text(body, encoding="utf-8")
    except OSError as e:
        return {"ok": False, "error": f"Failed to write .m file: {e}"}

    # Overwriting an existing .m could reuse the (mtime_ns, size) a previous
    # scan cached this path under, which would make a LATER refresh_registries()
    # serve the pre-write classification. Registration below is direct, so
    # this only guards that future scan -- see matlab_parser's cache.
    from scistack_gui.matlab.parser import invalidate_classify_cache

    invalidate_classify_cache(target_file)

    # Register just this one file. The full refresh_registries()
    # this used to call re-classified every configured .m source (14.9 s for
    # 303 files on a real project) to learn about a file whose path we are
    # holding -- see target_file_service._reload_after_write.
    try:
        matlab_registry._register_matlab_variable(name, target_file)
    except Exception as e:
        return {"ok": False, "error": f"File written but registration failed: {e}"}

    return {"ok": True, "name": name}


def _format_variant_label(
    branch_params: dict,
    fn_name: str | None = None,
    fn_version: str | None = None,
    is_latest: bool | None = None,
) -> str:
    """
    Produce a concise human-readable label for one variant.

    branch_params keys are namespaced as "fn_name.param" (for constants) or bare
    names (for dynamic discriminators). Strip the function prefix when all keys
    share the same function, so the display stays compact.

    ``fn_version`` is scidb's function-version discriminator — set only when the
    variable type actually holds records produced by more than one version of its
    function's source (see ``provenance_query.variant_identity_batch``). When it
    is set the label MUST say so: those records are otherwise indistinguishable,
    and showing "(raw)" twice is what sent a user plotting the wrong data. The
    function name is spelled out alongside it because "v2" on its own does not
    tell you *what* changed.
    """
    parts = []
    for k, v in sorted(branch_params.items()):
        short_k = k.split(".")[-1] if "." in k else k
        parts.append(f"{short_k}={v}")
    label = ", ".join(parts)

    if fn_version:
        version = f"{fn_name} {fn_version}" if fn_name else fn_version
        if is_latest:
            version += " (latest)"
        # Constants first — they are the variant the user configured. The code
        # version is the one they did not, so it reads as the qualifier it is.
        label = f"{label} · {version}" if label else version

    return label or "(raw)"


def get_variable_records(variable_name: str, db: DatabaseManager) -> dict:
    """
    Return all records for a variable type with schema key values and variant info.

    Response shape:
      {
        "schema_keys": ["subject", "session"],
        "records": [
          {"subject": "1", "session": "pre", "branch_params": {...}, "variant_label": "..."},
          ...
        ],
        "variants": [
          {"label": "...", "branch_params": {...}, "record_count": 4},
          ...
        ]
      }
    """
    schema_keys: list[str] = db._duck.dataset_schema

    # The pre-bipartite _record_metadata table (with its embedded
    # branch_params column) is gone: record identity lives on the _record
    # entity and branch_params are DERIVED from the provenance graph.
    # Query the variable's data table joined to _record/_schema, then batch
    # the branch-params walk (never per-record — the N+1 trap).

    # Validate the name against the _variables registry table — unknown
    # variables return the empty shape (and the {name}_data interpolation
    # below only ever receives a known-registered name).
    known = {
        row[0] for row in db._duck.fetchall("SELECT variable_name FROM _variables")
    }
    if variable_name not in known:
        logger.info("get_variable_records: unknown variable %r", variable_name)
        return {"schema_keys": schema_keys, "records": [], "variants": []}

    schema_select = ", ".join(f's."{k}"' for k in schema_keys)
    if schema_select:
        schema_select = ", " + schema_select
    query = f"""
        SELECT DISTINCT t.record_id{schema_select}
        FROM "{variable_name}_data" t
        JOIN _record r ON t.record_id = r.record_id
        LEFT JOIN _schema s ON r.schema_id = s.schema_id
        WHERE r.excluded IS DISTINCT FROM TRUE
        ORDER BY {", ".join(f's."{k}"' for k in schema_keys) or "t.record_id"}
    """
    try:
        rows = db._duck.fetchall(query)
    except Exception as exc:
        logger.warning("get_variable_records(%s) query failed: %s", variable_name, exc)
        raise HTTPException(status_code=404, detail=str(exc))

    record_ids = [row[0] for row in rows]
    from scidb.provenance.query import variant_identity_batch

    # Not branch_params_batch: two records produced by different versions of the
    # same function have IDENTICAL branch params, so grouping on those alone
    # collapsed them into one "(raw)" variant and hid the difference entirely.
    ident_by_record = variant_identity_batch(db._duck, record_ids)

    records = []
    for row in rows:
        record_id = row[0]
        schema_vals = dict(zip(schema_keys, row[1:], strict=False))
        ident = ident_by_record.get(record_id, {})
        bp = ident.get("branch_params", {})
        records.append(
            {
                **{
                    k: str(schema_vals[k]) if schema_vals[k] is not None else None
                    for k in schema_keys
                },
                "branch_params": bp,
                "fn_name": ident.get("fn_name"),
                "fn_hash": ident.get("fn_hash"),
                "fn_version": ident.get("fn_version"),
                "is_latest": ident.get("is_latest"),
                "saved_at": ident.get("saved_at"),
                "variant_label": _format_variant_label(
                    bp,
                    ident.get("fn_name"),
                    ident.get("fn_version"),
                    ident.get("is_latest"),
                ),
            }
        )

    # Build the variant summary: group by the record's point in variant space
    # (`bindings.variant_signature`, the ONE recipe — see
    # docs/claude/variant-space.md §4) *and* producing function hash. The hash
    # is what separates two runs of an edited function — without it they merge
    # into a single row whose count is the sum of both.
    from scidb.run.bindings import variant_signature

    variant_map: dict[tuple, dict] = {}
    for rec in records:
        key = (variant_signature(rec["branch_params"]), rec["fn_hash"])
        if key not in variant_map:
            variant_map[key] = {
                "branch_params": rec["branch_params"],
                "fn_name": rec["fn_name"],
                "fn_hash": rec["fn_hash"],
                "fn_version": rec["fn_version"],
                "is_latest": rec["is_latest"],
                "record_count": 0,
            }
        variant_map[key]["record_count"] += 1
        # is_latest is resolved per schema location, so one version can be the
        # latest at some locations and not others. For the summary row, "latest
        # somewhere" is the useful reading — it means re-running would not
        # replace all of these records.
        if rec["is_latest"]:
            variant_map[key]["is_latest"] = True

    # Label AFTER aggregating, not from the first record in the group: the
    # is_latest above can flip to True partway through, and a row whose label
    # omits "(latest)" while its is_latest says True is exactly the kind of
    # quiet inconsistency this whole change exists to remove.
    variants = list(variant_map.values())
    for v in variants:
        v["label"] = _format_variant_label(
            v["branch_params"], v["fn_name"], v["fn_version"], v["is_latest"]
        )

    # Latest versions first — but only reorder when code versions are actually
    # in play. Sorting unconditionally would also reshuffle a variable holding
    # both raw and function-produced records (is_latest None vs True), which has
    # nothing to do with this feature.
    if any(v["fn_version"] for v in variants):
        variants.sort(key=lambda v: not v["is_latest"])

    if len(variants) > 1:
        logger.info(
            "get_variable_records(%s): %d record(s) in %d variant(s) — %s",
            variable_name,
            len(records),
            len(variants),
            [f"{v['label']} x{v['record_count']}" for v in variants],
        )

    return {
        "schema_keys": schema_keys,
        "records": records,
        "variants": variants,
    }


# ---- Default plotting by schema level (to-do #4) ------------------------------


def _numeric_plot_kind(sample: object) -> "str | None":
    """Classify a variable's data column for the default-plot mechanism
    from one SAMPLE VALUE already fetched via the duckdb Python client
    (not by parsing a SQL type-name string) — see
    plan-default-plotting-by-schema-level.md for why: the duckdb client's
    own Python type for a row value (float/int for a scalar column, list
    for a LIST column) is the ground truth actually observed, with no
    dependency on knowing DuckDB's information_schema type-string spelling
    for list/array columns across versions.

    Returns "scalar", "1d", or None (not eligible). A column is uniformly
    typed by DuckDB, so sampling any one row's value classifies the whole
    column.
    """
    if isinstance(sample, bool):  # bool is an int subclass — exclude explicitly
        return None
    if isinstance(sample, (int, float)):
        return "scalar"
    if isinstance(sample, list) and sample and all(
        isinstance(v, (int, float)) and not isinstance(v, bool) for v in sample
    ):
        return "1d"
    return None


def get_variable_plot_data(variable_name: str, db: DatabaseManager) -> dict:
    """
    Raw points for the sidebar's default plot (to-do #4) — every record's
    schema key values + its scalar/1D-numeric value, unaggregated. The
    frontend groups/averages by whichever schema keys the user leaves
    checked; shipping raw points keeps that instant (no round trip per
    schema-level toggle).

    Response shape:
      {
        "eligible": bool,
        "reason": str | None,             # why not, when eligible=False
        "kind": "scalar" | "1d" | None,
        "schema_keys": ["subject", "session"],
        "points": [
          {"subject": "1", "session": "pre", "value": 0.42},
          ...
        ]
      }
    """
    schema_keys: list[str] = db._duck.dataset_schema
    empty = {"eligible": False, "reason": None, "kind": None,
             "schema_keys": schema_keys, "points": []}

    known = {
        row[0] for row in db._duck.fetchall("SELECT variable_name FROM _variables")
    }
    if variable_name not in known:
        logger.info("get_variable_plot_data: unknown variable %r", variable_name)
        return {**empty, "reason": "unknown variable"}

    data_table = f"{variable_name}_data"
    cols = db._duck.fetchall(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = ? AND column_name != 'record_id' "
        "ORDER BY ordinal_position",
        [data_table],
    )
    if len(cols) != 1:
        return {**empty, "reason": "not a scalar/1D variable (multi-column data)"}
    value_col = cols[0][0]

    schema_select = ", ".join(f's."{k}"' for k in schema_keys)
    if schema_select:
        schema_select = ", " + schema_select
    query = f"""
        SELECT t.record_id, t."{value_col}"{schema_select}
        FROM "{data_table}" t
        JOIN _record r ON t.record_id = r.record_id
        LEFT JOIN _schema s ON r.schema_id = s.schema_id
        WHERE r.excluded IS DISTINCT FROM TRUE
    """
    try:
        rows = db._duck.fetchall(query)
    except Exception as exc:
        logger.warning("get_variable_plot_data(%s) query failed: %s", variable_name, exc)
        raise HTTPException(status_code=404, detail=str(exc))

    if not rows:
        return {**empty, "reason": "no records yet"}

    kind = _numeric_plot_kind(rows[0][1])
    if kind is None:
        return {**empty, "reason": f"not scalar/1D numeric (got {type(rows[0][1]).__name__})"}

    points = []
    for row in rows:
        value = row[1]
        schema_vals = dict(zip(schema_keys, row[2:], strict=False))
        points.append({
            **{
                k: (str(v) if v is not None else None)
                for k, v in schema_vals.items()
            },
            "value": value,
        })

    logger.info(
        "get_variable_plot_data(%s): kind=%s, %d point(s)",
        variable_name, kind, len(points),
    )
    return {
        "eligible": True,
        "reason": None,
        "kind": kind,
        "schema_keys": schema_keys,
        "points": points,
    }


def input_columns(variable_type: str, db=None) -> dict:
    """The columns a consumer of *variable_type* will actually receive.

    Not guessable from the canvas — it depends on how the variable stores its
    data:

    * a **DataFrame**-stored variable arrives under the user's own column
      names; there is no column named after the class;
    * a **dict**-stored (multi_column) variable arrives as one column per key;
    * a **scalar or array** arrives as schema-key columns plus ONE data column
      named after the class (``view_name()``).

    Read live from ``_variables.dtype`` on every panel open rather than
    scaffolded anywhere — a cached list goes stale the moment the variable is
    re-saved with a different shape, and both consumers of this (the glue
    panel and the function node's Inputs picker) re-read it every time.

    Variables own this, not glue. It grew up inside ``glue_service`` because
    glue was the first caller; a second caller with the same question is a
    reason to move it to the layer that owns the answer, not to copy it
    (``feedback_avoid_scifor_scidb_duplication``).

    Returns ``{"ok": True, "variable_type", "schema_keys", "data_columns",
    "note"}``, or ``{"ok": False, "error"}`` when no database is open.
    """
    from scistack_gui.runtime import db as db_module

    try:
        active = db if db is not None else db_module.get_db()
    except Exception:
        active = None
    if active is None:
        return {"ok": False, "error": "No database is open."}

    schema_keys = list(active.dataset_schema_keys)
    try:
        dtype = active.get_dtype_meta(variable_type)
    except Exception as exc:
        logger.debug(
            "[variables] dtype lookup failed for %s: %s", variable_type, exc
        )
        dtype = None

    mode = (dtype or {}).get("mode") if isinstance(dtype, dict) else None
    if mode == "dataframe":
        data_columns = list((dtype or {}).get("columns", {}) or {})
        note = "This variable stores a DataFrame, so its own column names arrive."
    elif mode == "multi_column":
        data_columns = list((dtype or {}).get("columns", {}) or {})
        note = "This variable stores a dict; one column per key arrives."
    else:
        data_columns = [variable_type]
        note = (
            f"This variable stores a scalar or array, so its data arrives in one "
            f"column named '{variable_type}'."
        )

    logger.info(
        "[variables] input_columns(%s): mode=%s, %d data column(s), "
        "%d schema key(s)",
        variable_type,
        mode or "scalar/array",
        len(data_columns),
        len(schema_keys),
    )
    return {
        "ok": True,
        "variable_type": variable_type,
        "schema_keys": schema_keys,
        "data_columns": data_columns,
        "note": note,
    }
