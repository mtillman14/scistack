"""PipelineGraph — the type-level pipeline DAG for observability.

Shapes ``provenance_query.pipeline_variants`` into display-ready nodes and
edges. Grouping: one FunctionNode per **pipeline step** = (function_name,
variable-input wiring). Constants are aggregated across the step's variants
— note this is deliberately coarser than ``call_id`` (which folds constants
in), so a two-value sweep renders as one step with two variants, matching
the GUI's mental model of "the bandpass step". Each variant keeps its own
``call_id``. PathInput specs are displayed but excluded from the grouping
key (a template change does not fork a variant — decided WON'T-DO
2026-06-21).

Node state (green/red) reuses ``state.py`` §9c semantics
(``expected_invocations_for_function`` vs ``present_invocation_schema_pairs``):

- ``state_basis="live_fn"``   — a function object was supplied via
  ``fn_registry``: full semantics, including detecting source edits.
- ``state_basis="stored_hash"`` — standalone (CLI) mode: uses the most
  recently *run* function_hash, so missing/partial coverage and re-saved
  inputs are detected, but a source edit since the last run is NOT.
- ``state_basis="none"``      — no stored hash at all; state="unknown".

The variant-aggregation concept is moved down from scistack-gui
``domain/graph_builder.aggregate_variants`` (owning-layer rule); migrating
the GUI onto this module is a noted follow-up.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..exceptions import NotFoundError
from ..log import Log
from ..provenance import parse_path_input_spec

if TYPE_CHECKING:
    from ..database import DatabaseManager


# ---------------------------------------------------------------------------
# Dataclasses (JSON-serializable via dataclasses.asdict)
# ---------------------------------------------------------------------------


@dataclass
class VariantSummary:
    function_name: str
    call_id: str
    output_type: str
    output_num: int | None
    input_types: dict[str, str]  # param → type name (PathInput params → spec string)
    constants: dict[str, str]  # param → display string of the value
    record_count: int
    #: ``distribute=…[, as_table=[…]]`` — the third variant axis after
    #: constants and code. Two rows identical everywhere else and different
    #: here are two genuinely different variants (the flags are folded into
    #: invocation_id), which is why omitting this made them look like a
    #: duplicate row (docs/claude/run-option-variants.md, "Not done / open").
    run_options: str | None = None


@dataclass
class FunctionNode:
    id: str  # fn__{name}__{8-hex step hash}
    function_name: str
    input_params: dict[str, str]  # param → variable type (PathInputs excluded)
    path_inputs: dict[str, str]  # param → template string
    output_types: list[str]
    constants: dict[str, list[str]]  # param → sorted distinct value strings
    variant_count: int  # distinct constants combinations
    call_ids: list[str]
    record_count: int
    state: str  # "green" | "red" | "unknown"
    state_counts: dict[str, int]  # {"up_to_date": N, "missing": N} when known
    state_basis: str  # "live_fn" | "stored_hash" | "none"
    variants: list[VariantSummary] = field(default_factory=list)


@dataclass
class VariableNode:
    id: str  # var__{name}
    name: str
    record_count: int  # distinct non-excluded records
    produced_by: list[str] = field(default_factory=list)  # FunctionNode ids
    consumed_by: list[str] = field(default_factory=list)


@dataclass
class PipelineEdge:
    source: str  # node id
    target: str
    param: str | None = None  # var→fn edges
    output_num: int | None = None  # fn→var edges


@dataclass
class PipelineGraph:
    variables: list[VariableNode] = field(default_factory=list)
    functions: list[FunctionNode] = field(default_factory=list)
    edges: list[PipelineEdge] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def var_node_id(name: str) -> str:
    return f"var__{name}"


def parse_path_input(value: str) -> dict | None:
    """``scidb.provenance.parse_path_input_spec`` under this module's name."""
    return parse_path_input_spec(value)


def _value_str(value) -> str:
    """Display string for a constant value: JSON-ish so strings keep quotes."""
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return str(value)


def _step_id(fn_name: str, wiring_key) -> str:
    digest = hashlib.sha256(
        json.dumps(wiring_key, sort_keys=True, default=str).encode()
    ).hexdigest()[:8]
    return f"fn__{fn_name}__{digest}"


def _stored_function_hashes(duck) -> dict[str, str]:
    """fn_name → most recently *run* function_hash (falls back to any stored
    hash for invocations that never got a _run row)."""
    rows = duck._fetchall(
        "SELECT i.function_name, i.function_hash, MAX(r.timestamp) AS ts "
        "FROM _invocation i "
        "LEFT JOIN _run_invocation ri ON ri.invocation_id = i.invocation_id "
        "LEFT JOIN _run r ON r.run_id = ri.run_id "
        "WHERE i.function_name <> '__save__' "
        "GROUP BY i.function_name, i.function_hash"
    )
    best: dict[str, tuple] = {}
    for fn_name, fn_hash, ts in rows:
        # None timestamps sort before any real one.
        rank = (ts is not None, ts)
        if fn_name not in best or rank > best[fn_name][0]:
            best[fn_name] = (rank, fn_hash)
    return {fn: h for fn, (_, h) in best.items()}


def _node_states(db, fn_names, fn_registry=None) -> dict[str, dict]:
    """fn_name → {state, counts, basis, missing_schema_ids}.

    See the module docstring for what each basis means; missing_schema_ids
    are the schema locations of the expected-but-absent invocations (used by
    the state command to say *which* combos need running).
    """
    from .. import provenance_query
    from ..foreach_config import function_hash_for

    stored = _stored_function_hashes(db._duck)
    states: dict[str, dict] = {}
    for fn_name in fn_names:
        fn_obj = (fn_registry or {}).get(fn_name)
        if fn_obj is not None:
            # Same recipe the save path stored — see function_hash_for. A MATLAB
            # handle carries its .m digest explicitly; AST-hashing its `.fcn`
            # name-holder would match nothing ever written.
            fn_hash = function_hash_for(fn_obj)
            basis = "live_fn"
        elif fn_name in stored:
            fn_hash = stored[fn_name]
            basis = "stored_hash"
        else:
            states[fn_name] = {
                "state": "unknown",
                "counts": {},
                "basis": "none",
                "missing_schema_ids": [],
            }
            continue

        expected = provenance_query.expected_invocations_for_function(
            db,
            fn_name,
            fn_hash,
        )
        present = provenance_query.present_invocation_schema_pairs(
            db._duck,
            {inv_id for inv_id, _sid in expected},
        )
        missing_pairs = [pair for pair in expected if pair not in present]
        state = "green" if expected and not missing_pairs else "red"
        counts = {
            "up_to_date": len(expected) - len(missing_pairs),
            "missing": len(missing_pairs),
        }
        Log.debug(
            f"inspect graph: node {fn_name} state={state} basis={basis} "
            f"({counts['up_to_date']} up-to-date, {counts['missing']} missing)"
        )
        states[fn_name] = {
            "state": state,
            "counts": counts,
            "basis": basis,
            "missing_schema_ids": sorted(
                (sid for _inv, sid in missing_pairs),
                key=lambda s: (s is None, s),
            ),
        }
    return states


def _variable_record_counts(duck) -> dict[str, int]:
    rows = duck._fetchall(
        "SELECT type, COUNT(*) FROM _record "
        "WHERE type NOT IN ('__constant__', '__pathinput__', '__glue__') "
        "AND NOT COALESCE(excluded, FALSE) "
        "GROUP BY type"
    )
    return {t: int(n) for t, n in rows}


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def build_pipeline_graph(
    db: DatabaseManager,
    output_type: str | None = None,
    fn_registry: dict | None = None,
) -> PipelineGraph:
    from .. import provenance_query

    duck = db._duck
    raw_variants = provenance_query.pipeline_variants(duck)

    # --- group variants into steps: (fn_name, variable-input wiring) ---
    steps: dict[tuple, dict] = {}
    for v in raw_variants:
        var_inputs: dict[str, str] = {}
        path_inputs: dict[str, str] = {}
        for param, type_val in v["input_types"].items():
            pi = parse_path_input(type_val)
            if pi is not None:
                path_inputs[param] = pi["template"] or type_val
            else:
                var_inputs[param] = type_val

        # PathInput params contribute their *name* only — the spec is display
        # data, not step identity (template change ≠ new variant).
        wiring_key = (
            v["function_name"],
            tuple(sorted(var_inputs.items())),
            tuple(sorted(path_inputs)),
        )
        step = steps.get(wiring_key)
        if step is None:
            step = steps[wiring_key] = {
                "fn_name": v["function_name"],
                "input_params": var_inputs,
                "path_inputs": dict(path_inputs),
                "outputs": {},  # output_type → set(output_num)
                "constants": {},  # param → set(display str)
                "constant_keys": set(),  # distinct constants dicts
                "call_ids": set(),
                "record_count": 0,
                "variants": [],
            }
        step["outputs"].setdefault(v["output_type"], set()).add(v["output_num"])
        for param, value in v["constants"].items():
            step["constants"].setdefault(param, set()).add(_value_str(value))
        step["constant_keys"].add(
            tuple(sorted((k, _value_str(val)) for k, val in v["constants"].items()))
        )
        step["call_ids"].add(v["call_id"])
        step["record_count"] += int(v["record_count"])
        step["variants"].append(
            VariantSummary(
                function_name=v["function_name"],
                call_id=v["call_id"],
                output_type=v["output_type"],
                output_num=v["output_num"],
                input_types=dict(v["input_types"]),
                constants={k: _value_str(val) for k, val in v["constants"].items()},
                record_count=int(v["record_count"]),
                run_options=v.get("run_options"),
            )
        )

    # --- node state per function name (shared across its steps) ---
    fn_names = {s["fn_name"] for s in steps.values()}
    states = _node_states(db, fn_names, fn_registry=fn_registry)

    # --- assemble nodes + edges ---
    graph = PipelineGraph()
    counts = _variable_record_counts(duck)
    var_nodes: dict[str, VariableNode] = {}

    def var_node(name: str) -> VariableNode:
        if name not in var_nodes:
            node = VariableNode(
                id=var_node_id(name),
                name=name,
                record_count=counts.get(name, 0),
            )
            var_nodes[name] = node
            graph.variables.append(node)
        return var_nodes[name]

    for wiring_key, step in sorted(
        steps.items(), key=lambda kv: (kv[1]["fn_name"], kv[0][1], kv[0][2])
    ):
        fid = _step_id(step["fn_name"], wiring_key)
        st = states[step["fn_name"]]
        state, state_counts, basis = st["state"], st["counts"], st["basis"]
        step["variants"].sort(
            key=lambda s: (
                s.output_type,
                s.output_num if s.output_num is not None else -1,
                sorted(s.constants.items()),
            )
        )
        node = FunctionNode(
            id=fid,
            function_name=step["fn_name"],
            input_params=step["input_params"],
            path_inputs=step["path_inputs"],
            output_types=sorted(step["outputs"]),
            constants={
                k: sorted(vals) for k, vals in sorted(step["constants"].items())
            },
            variant_count=len(step["constant_keys"]),
            call_ids=sorted(step["call_ids"]),
            record_count=step["record_count"],
            state=state,
            state_counts=state_counts,
            state_basis=basis,
            variants=step["variants"],
        )
        graph.functions.append(node)

        for param, var_type in sorted(step["input_params"].items()):
            vn = var_node(var_type)
            if fid not in vn.consumed_by:
                vn.consumed_by.append(fid)
            graph.edges.append(PipelineEdge(source=vn.id, target=fid, param=param))
        for out_type, out_nums in sorted(step["outputs"].items()):
            vn = var_node(out_type)
            if fid not in vn.produced_by:
                vn.produced_by.append(fid)
            for num in sorted(out_nums, key=lambda n: (n is None, n)):
                graph.edges.append(
                    PipelineEdge(source=fid, target=vn.id, output_num=num)
                )

    # Variables that exist in the DB but appear in no pipeline step.
    for name in sorted(counts):
        var_node(name)
    graph.variables.sort(key=lambda n: n.name)

    if output_type is not None:
        _restrict_to_ancestors(graph, output_type)

    Log.debug(
        f"inspect graph: {len(graph.variables)} variables, "
        f"{len(graph.functions)} steps, {len(graph.edges)} edges"
        + (f" (restricted to ancestors of {output_type})" if output_type else "")
    )
    return graph


def _restrict_to_ancestors(graph: PipelineGraph, output_type: str) -> None:
    """Keep only ``output_type``'s variable node and everything upstream of it
    (mutates the graph in place)."""
    target = var_node_id(output_type)
    if all(v.id != target for v in graph.variables):
        raise NotFoundError(
            f"Variable type {output_type!r} not found in the pipeline graph"
        )
    preds: dict[str, set[str]] = {}
    for e in graph.edges:
        preds.setdefault(e.target, set()).add(e.source)

    keep = {target}
    frontier = [target]
    while frontier:
        node = frontier.pop()
        for pred in preds.get(node, ()):
            if pred not in keep:
                keep.add(pred)
                frontier.append(pred)

    graph.variables = [v for v in graph.variables if v.id in keep]
    graph.functions = [f for f in graph.functions if f.id in keep]
    graph.edges = [e for e in graph.edges if e.source in keep and e.target in keep]
    for v in graph.variables:
        v.produced_by = [i for i in v.produced_by if i in keep]
        v.consumed_by = [i for i in v.consumed_by if i in keep]
