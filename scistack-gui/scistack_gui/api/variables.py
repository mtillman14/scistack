"""
Variable-related API endpoints.

GET  /api/variables/{variable_name}/records — records + variant summary
POST /api/variables/create                 — define a new BaseVariable subclass
"""

import json
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from scidb.database import DatabaseManager

from scistack_gui.db import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


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


@router.get("/variables/{variable_name}/records")
def get_variable_records(variable_name: str, db: DatabaseManager = Depends(get_db)):
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
        row[0] for row in db._duck._fetchall("SELECT variable_name FROM _variables")
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
        rows = db._duck._fetchall(query)
    except Exception as exc:
        logger.warning("get_variable_records(%s) query failed: %s", variable_name, exc)
        raise HTTPException(status_code=404, detail=str(exc))

    record_ids = [row[0] for row in rows]
    from scidb.provenance_query import variant_identity_batch

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

    # Build the variant summary: group by branch_params JSON *and* producing
    # function hash. The hash is what separates two runs of an edited function —
    # without it they merge into a single row whose count is the sum of both.
    variant_map: dict[tuple, dict] = {}
    for rec in records:
        key = (json.dumps(rec["branch_params"], sort_keys=True), rec["fn_hash"])
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


@router.get("/variables/{variable_name}/plot-data")
def get_variable_plot_data(variable_name: str, db: DatabaseManager = Depends(get_db)):
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
        row[0] for row in db._duck._fetchall("SELECT variable_name FROM _variables")
    }
    if variable_name not in known:
        logger.info("get_variable_plot_data: unknown variable %r", variable_name)
        return {**empty, "reason": "unknown variable"}

    data_table = f"{variable_name}_data"
    cols = db._duck._fetchall(
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
        rows = db._duck._fetchall(query)
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


# ---- Column list -------------------------------------------------------------


@router.get("/variables/{variable_name}/columns")
def get_variable_columns(
    variable_name: str, db: DatabaseManager = Depends(get_db)
) -> dict:
    """Which columns a consumer of this variable receives.

    Read live by the function node's Inputs column picker and by the glue
    panel, on every open — a variable re-saved with a different shape changes
    this answer, and neither surface caches it.

    Response: ``{"ok", "variable_type", "data_columns", "schema_keys",
    "note"}``. A scalar/array-stored variable reports its single class-named
    column plus the note explaining why, rather than an empty list, so the
    picker can say something true instead of looking broken.
    """
    from scistack_gui.services import variable_service

    return variable_service.input_columns(variable_name, db=db)


# ---- Create new variable type -------------------------------------------------


class CreateVariableRequest(BaseModel):
    name: str
    docstring: str | None = None


@router.post("/variables/create")
async def create_variable(req: CreateVariableRequest) -> dict:
    """
    Define a new BaseVariable subclass by appending it to the user's module file,
    then refresh the registry so it's immediately available.

    Delegates to ``services.variable_service.create_variable`` -- the single
    source of truth also used by the JSON-RPC (VS Code extension) path in
    server.py -- so validation/target-file/MATLAB-fallback behavior can't
    drift between the two transports.

    Deliberately does NOT broadcast ``dag_updated``: a freshly declared type
    has no DB records yet, so it cannot appear as a canvas node (variable
    nodes come from ``list_variables``, not from the type registry — see
    ``graph_builder.build_variable_nodes``), and nothing in the frontend
    renders a per-node "declared" state that a broadcast would need to
    refresh (the undeclared case is only ever surfaced at the run boundary,
    see ``layout_service.write_manual_node``). Broadcasting here used to
    force every connected client through a full pipeline refetch+relayout
    plus registry/path-input/parameter/hidden-pipeline refetches for a
    change the canvas can't show — the same "rebuild for nothing" cost the
    position-only-write guard in ``api/pipeline.py`` exists to avoid. The
    caller updates its own sidebar registry state directly instead, same as
    ``create_parameter``/``create_path_input`` already do.
    """
    from scistack_gui.services.variable_service import create_variable as _create

    name = req.name.strip()
    logger.info("create_variable request: name=%r docstring=%r", name, req.docstring)

    return _create(name, req.docstring)
