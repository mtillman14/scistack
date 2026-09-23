"""
Pure graph-building logic for the pipeline DAG.

Builds React Flow nodes and edges from pre-fetched data. No I/O — works
entirely on plain Python data structures (dicts, lists, sets, strings).
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field

from scidb.provenance import compute_wiring_id as _compute_wiring_id
from scidb.provenance import parse_path_input_spec as _parse_path_input_spec
from scidb.provenance import strip_path_input_specs as _strip_path_input_specs
from scidb.parameter import parameter_node_name

from scistack_gui.ids import (
    FN_ID_PREFIX,
    IN_HANDLE_PREFIX,
    PARAM_ID_PREFIX,
    PATH_INPUT_ID_PREFIX,
    ROOT_SCOPE,
    VAR_ID_PREFIX,
    fn_node_id,
    in_handle,
    out_handle,
    param_handle,
    param_node_id,
    parse_fn_node_id,
    parse_placement_id,
    path_input_node_id,
    placement_id,
    strip_placement,
    var_node_id,
)
from scistack_gui.domain.scope_filter import DECLARED_ONLY

logger = logging.getLogger(__name__)


FnKey = tuple[str, str]
"""(fn_name, call_id) — uniquely identifies a for_each call site.

Two for_each() invocations of the same fn that differ in inputs, constants,
where, distribute, or as_table produce different call_ids and therefore
different FnKeys. Since 2026-07-18 the CANVAS no longer shows one node per
call site: call sites are grouped by WIRING (see ``wiring_id`` /
``group_call_sites_by_wiring``) and render as variant rows inside one
node — a new constant value forks a new call_id in scidb (by design) but
lands in the SAME canvas node. State computation stays per call site.
"""


@dataclass
class AggregatedData:
    """Aggregated pipeline data from DB variants.

    Function-keyed fields use ``FnKey = (fn_name, call_id)`` so the same
    function reused from multiple for_each call sites appears as multiple
    distinct entries (and therefore multiple distinct function nodes).

    ``const_fns`` keeps a per-FnKey set of which call sites use each
    constant — that determines which call-site node receives the
    constant→function edge.
    """

    all_var_types: set[str] = field(default_factory=set)
    fn_input_params: dict[FnKey, dict] = field(
        default_factory=lambda: defaultdict(dict)
    )
    fn_outputs: dict[FnKey, set] = field(default_factory=lambda: defaultdict(set))
    const_counts: dict[str, dict] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )
    const_fns: dict[str, set] = field(default_factory=lambda: defaultdict(set))
    fn_constants: dict[FnKey, set] = field(default_factory=lambda: defaultdict(set))
    path_inputs: dict[str, dict] = field(default_factory=dict)
    """Keyed by the PathInput's SOURCE-DECLARED name (resolved via
    ``resolve_path_input_name``, content-matched against the registry) —
    NOT the function parameter name, which can differ (``RAW_EMG`` bound to
    a ``signal`` param). Each entry: ``{"template", "root_folder",
    "alternate_templates", "functions": set[tuple[FnKey, param_name]]}`` —
    the per-membership ``param_name`` is needed because one PathInput can
    feed differently-named params across different functions."""
    fn_variants_map: dict[FnKey, list] = field(
        default_factory=lambda: defaultdict(list)
    )
    fn_parameter_names: dict[FnKey, dict[str, str]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    """``{fkey: {argument: Parameter node name}}`` — which Parameter node each
    constant argument belongs to, as scidb resolved it
    (``get_aggregated_variants`` / ``scidb.parameter.parameter_node_name``).
    ``const_counts``/``const_fns`` are keyed by NODE; ``fn_constants`` and
    variant rows by ARGUMENT (the handle). The two differ when a Parameter
    declared ``gaitrite_config`` feeds ``gaitRiteConfig`` (cleanup-audit B1);
    translate with :meth:`constant_node` / :meth:`constant_args`, never by
    assuming they are equal."""

    def constant_node(self, fkey: FnKey, argument: str) -> str:
        """The Parameter node a call site's constant *argument* belongs to."""
        return self.fn_parameter_names.get(fkey, {}).get(argument) or argument

    def constant_args(self, fkey: FnKey, node: str) -> list[str]:
        """The arguments of *fkey* that Parameter *node* fills (handles)."""
        args = [
            a for a in sorted(self.fn_constants.get(fkey, set()))
            if self.constant_node(fkey, a) == node
        ]
        return args or [node]


def edge_dedup_key(
    source: str, target: str, target_handle: str | None = None
) -> tuple:
    """The identity of a *connection*, independent of the edge's id.

    Two edges with this same key describe the same wire and must never both
    be rendered. Compare on BARE ids: DB-derived keys are always canonical,
    while a graduated manual edge can carry a ``::scope`` placement suffix
    (graduate_manual_node -> placement_id). resolve_scope_view resolves each
    endpoint into the viewing scope afterwards, so two edges over the same
    canonical pair are the same connection.

    PathInput sources also key on the parameter the edge fills: ``pi_name``
    and ``param_name`` can differ, so unlike var/const edges the source id
    does not encode the parameter by itself.

    This is the single definition used both to dedup manual edges against
    DB-derived ones inside build_edges AND to re-check that invariant after
    something rewrites edge endpoints mid-build (graduation, the legacy
    wiring migration) — see drop_superseded_manual_edges.
    """
    src = strip_placement(source)
    tgt = strip_placement(target)
    handle = target_handle or ""
    if src.startswith(PATH_INPUT_ID_PREFIX) and handle.startswith(IN_HANDLE_PREFIX):
        return (src, tgt, handle[len(IN_HANDLE_PREFIX) :])
    return (src, tgt)


def is_manual_edge(edge: dict) -> bool:
    """Whether a built edge came from a manual (user-drawn) row.

    build_edges tags manual edges with ``data.manual``; DB-derived edges
    carry no ``data`` at all.
    """
    return bool((edge.get("data") or {}).get("manual"))


def drop_superseded_manual_edges(edges: list[dict]) -> tuple[list[dict], list[dict]]:
    """Remove manual edges that now duplicate a DB-derived edge.

    build_edges already applies this rule, but it runs BEFORE the graduation
    of manual nodes — at which point a manual edge still names the manual
    node ids (``fn__read_csv__2qxdue``) while the DB-derived edge names the
    call-site ids, so the two don't compare equal and BOTH are emitted.
    Graduation then rewrites the manual edge's endpoints onto the DB-derived
    ids (in the DB via pipeline_store.rename_edge_endpoints, and in-memory so
    the response isn't missing edges), which is exactly what turns it into a
    duplicate. The legacy wiring migration's edge rewrites do the same thing.

    So: whenever endpoints are rewritten mid-build, re-run this. The
    invariant it protects is that a build's response equals what an
    immediate rebuild would produce — without it the canvas draws two
    identical wires until the next unrelated refresh, and deleting one
    leaves its twin (the permanent-duplicate failure that endpoint dedup
    was introduced to fix in the first place).

    The manual row itself is untouched in the DB (hide, never delete): if
    the DB-derived edge later disappears, the manual edge renders again.

    Returns (kept_edges, dropped_edges).
    """
    db_keys = {
        edge_dedup_key(e["source"], e["target"], e.get("targetHandle"))
        for e in edges
        if not is_manual_edge(e)
    }
    kept, dropped = [], []
    for e in edges:
        if is_manual_edge(e) and (
            edge_dedup_key(e["source"], e["target"], e.get("targetHandle")) in db_keys
        ):
            dropped.append(e)
        else:
            kept.append(e)
    return kept, dropped


@dataclass
class GraduationAction:
    """Side-effect to execute after merge_manual_nodes (pure return value)."""

    old_id: str
    new_id: str


# ---------------------------------------------------------------------------
# Wiring grouping (one canvas node per function + input/output shape)
# ---------------------------------------------------------------------------

_STATE_WORST_ORDER = {"red": 0, "pending": 1, "green": 2}


def strip_path_input_params(input_params: dict) -> dict:
    """*input_params* without the entries that are really PathInput specs —
    ``scidb.provenance.strip_path_input_specs``, kept under the GUI's name."""
    return _strip_path_input_specs(input_params)


def wiring_id(fn_name: str, input_params: dict, out_types, path_inputs: dict) -> str:
    """16-hex id for a function's WIRING — the id a canvas node is keyed by
    and the subject every ``_intent`` statement is about.

    **The recipe is scidb's** (``scidb.provenance.compute_wiring_id``), the
    same way ``call_id`` is: one owner below every caller, computed here by
    IMPORT rather than predicted. Until 2026-09-20 this module held the only
    copy, and each time the canvas and the run path disagreed the fix was a
    normalisation patch on this side (``strip_path_input_params``); the
    normalisation now lives inside the recipe, so both views hash alike by
    construction. The bytes are unchanged — node ids key saved layout
    positions and scope membership.
    """
    return _compute_wiring_id(fn_name, input_params, out_types, path_inputs)


def identity_token(fn_name: str, wiring: str) -> str:
    """A ``token_for`` that says a node's identity token IS its wiring.

    **The seam for allocated node ids** (D-2026-09-22-1,
    ``docs/claude/node-identity.md``). Every function below that used to
    derive a node id as ``fn__{fn}__{wiring_id}`` now asks a ``token_for``
    callable for the trailing segment instead.

    **``token_for`` is a REQUIRED parameter and this is not its default.**
    Deriving a node id from its wiring is the bug the whole change removes;
    if forgetting to pass the mapping silently fell back to doing exactly
    that, the regression would be invisible — no error, no log line, just the
    old behaviour. A missing argument is a ``TypeError`` at the call site
    instead (user decision 2026-09-23, the same reasoning as D-2026-09-22-5).

    So this exists for callers for which the equivalence is TRUE and worth
    stating: the pure-domain tests, which have no nodes and no database and
    are testing the graph shape rather than identity. Passing it there reads
    as an assumption declared, not a default inherited.

    Why a callable rather than a dict: the mapping is per graph build, is
    computed from the database, and is not available to the pure layer.
    """
    return wiring


def path_input_bindings_by_fkey(path_inputs: dict) -> dict[FnKey, dict[str, str]]:
    """Invert ``AggregatedData.path_inputs`` into ``{fkey: {param: declared
    PathInput name}}`` — the per-call-site shape ``wiring_id`` needs.

    ``path_inputs`` is keyed by DECLARED name with a ``functions`` list of
    ``(fkey, param_name)`` memberships, which is the wrong way round for
    every wiring_id call site.
    """
    out: dict[FnKey, dict[str, str]] = {}
    for pi_name, pi in (path_inputs or {}).items():
        for fkey, param_name in pi["functions"]:
            out.setdefault(fkey, {})[param_name] = pi_name
    return out


def group_call_sites_by_wiring(
    agg: AggregatedData,
    run_states: dict[str, str],
    pending_constants: dict[str, set] | None = None,
    *,
    token_for,
    is_current,
    manual_edges: "list[dict] | tuple" = (),
    manual_nodes: "dict[str, dict] | None" = None,
    hidden_edge_ids: "set[str] | frozenset[str]" = frozenset(),
) -> tuple[AggregatedData, dict[str, str], dict[str, list[str]]]:
    """Re-key the aggregated call-site data to (fn_name, node token) groups.

    User decision 2026-07-18: one canvas node per function + wiring;
    constant-value call sites become variant rows INSIDE the node (each row
    keeps its own per-call-site state chip — the no-blur guarantee moves
    from separate nodes to separate chips). scidb call identity is
    untouched; this is presentation-layer grouping only.

    Args:
        agg: call-site-keyed aggregate (NOT mutated).
        run_states: propagate_run_states output — per-call-site
            ``fn__{fn}__{call_id}`` keys plus ``var__*`` keys.
        pending_constants: staged values ({name: {value, ...}}); each gets a
            SYNTHESIZED ``staged`` variant row on every group that uses the
            constant, so the value is visible in the node it will land in.

    Returns:
        (grouped AggregatedData,
         node_states — run_states re-keyed to group node ids, group own
         state = worst member state (red < pending < green — including any
         synthesized pending row), re-propagated through the DAG (domain.
         run_state.propagate_run_states) on the GROUPED wiring so a staged
         pending value cascades to every downstream var__/fn__ node, not
         just the group it was staged on,
         member_map — {group_node_id: [legacy member node ids]} for the
         one-time position/edge adoption).

    ``token_for(fn_name, wiring_id) -> token`` decides the id the group is
    keyed by, and ``is_current(fn_name, wiring_id)`` says whether that wiring
    is the node's shape NOW or one it merely used to run as. Both come from
    the build's ``IdentityPlan`` and both are REQUIRED — deriving a node id
    from its wiring is the bug this removes, and a default that quietly did it
    would make the regression invisible (see ``identity_token``).

    Under allocation, TWO wirings can map to ONE token — a node that was
    rewired and then run. Its call sites all land in one group, so it keeps
    its saved position, config and scope membership instead of a second node
    appearing beside it; but only the CURRENT wiring's call sites contribute
    its inputs, outputs and constants (``docs/claude/node-identity.md``).
    """
    pending_constants = pending_constants or {}
    grouped = AggregatedData()
    grouped.all_var_types = agg.all_var_types
    grouped.const_counts = agg.const_counts
    grouped.path_inputs = {}

    fkey_to_gkey: dict[FnKey, FnKey] = {}
    member_map: dict[str, list[str]] = {}
    group_member_states: dict[FnKey, list[str]] = defaultdict(list)

    pi_by_fkey = path_input_bindings_by_fkey(agg.path_inputs)
    for fkey in sorted(agg.fn_input_params.keys()):
        fn, cid = fkey
        wid = wiring_id(
            fn,
            agg.fn_input_params[fkey],
            agg.fn_outputs.get(fkey, set()),
            pi_by_fkey.get(fkey, {}),
        )
        token = token_for(fn, wid)
        gkey = (fn, token)
        fkey_to_gkey[fkey] = gkey

        # A node's SHAPE comes from its current wiring only — never the union
        # of every shape it has ever run as. A node that was rewired owns its
        # older wirings (they are its history, and its variant rows below
        # carry them), but drawing handles and edges for a shape the user
        # rewired away from would put a node on the canvas that is not the
        # node they see. `is_current` is the identity plan's answer; with the
        # default `token_for` every wiring is its own token and so always
        # current, which is the pre-allocation behaviour exactly.
        if is_current(fn, wid):
            grouped.fn_input_params[gkey].update(agg.fn_input_params[fkey])
            grouped.fn_outputs[gkey] |= set(agg.fn_outputs.get(fkey, set()))
            grouped.fn_constants[gkey] |= set(agg.fn_constants.get(fkey, set()))
            grouped.fn_parameter_names[gkey].update(
                agg.fn_parameter_names.get(fkey, {})
            )
        else:
            # Still reserve the group, so a node whose every call site is
            # historical does not vanish from the canvas between the run that
            # rewired it and the next one.
            grouped.fn_input_params.setdefault(gkey, {})
            grouped.fn_outputs.setdefault(gkey, set())
            grouped.fn_constants.setdefault(gkey, set())

        member_state = run_states.get(fn_node_id(fn, cid))
        if member_state:
            group_member_states[gkey].append(member_state)
        for row in agg.fn_variants_map.get(fkey, []):
            grouped.fn_variants_map[gkey].append(
                {
                    **row,
                    "call_id": cid,
                    **({"state": member_state} if member_state else {}),
                }
            )

        member_map.setdefault(fn_node_id(fn, token), []).append(fn_node_id(fn, cid))

    # const/path-input edge targets follow their call sites into the groups.
    for const_name, fkeys in agg.const_fns.items():
        grouped.const_fns[const_name] = {fkey_to_gkey.get(f, f) for f in fkeys}
    for pi_name, pi in agg.path_inputs.items():
        grouped.path_inputs[pi_name] = {
            **pi,
            "functions": {
                (fkey_to_gkey.get(f, f), pname) for (f, pname) in pi["functions"]
            },
        }

    # Staged pending values: a synthesized row per (constant, value) on
    # every group that uses the constant and hasn't already run it — a
    # group whose own real call sites already cover this exact value
    # (e.g. it just got run) must not be re-flagged pending merely because
    # a SIBLING wiring sharing the same constant node hasn't caught up yet
    # (see pending_value_group_coverage / auto_clean_pending_constants).
    coverage = pending_value_group_coverage(pending_constants, agg)
    # Pending values are keyed by the Parameter NODE (its UI writes them);
    # the synthesized row is keyed by the ARGUMENT, like every real row.
    for gkey in list(grouped.fn_constants.keys()):
        for arg in sorted(grouped.fn_constants[gkey]):
            const_name = grouped.constant_node(gkey, arg)
            for pval in sorted(pending_constants.get(const_name, set())):
                if gkey in coverage.get((const_name, pval), set()):
                    continue
                grouped.fn_variants_map[gkey].append(
                    {
                        "constants": {arg: pval},
                        "state": "pending",
                        "staged": True,
                    }
                )
                if "pending" not in group_member_states[gkey]:
                    group_member_states[gkey].append("pending")

    # Group own state = worst member state (including any synthesized
    # pending row) — then re-propagated through the DAG on the GROUPED
    # wiring so a staged pending value cascades downstream to every
    # var__/fn__ node that depends on it, not just the group it was staged
    # on. A plain "worst member state" assignment (the old approach) only
    # ever set the group's OWN node — a pending row on bandpass_filter
    # never reached var__FilteredSignal, since pass 1's DAG propagation
    # (domain.api.pipeline._compute_run_states) necessarily runs BEFORE
    # this staged row even exists.
    from scistack_gui.domain.run_state import propagate_run_states

    group_own_states = {
        gkey: min(states, key=lambda s: _STATE_WORST_ORDER.get(s, 0))
        for gkey, states in group_member_states.items()
        if states
    }
    # Same rule as pass 1 (api/pipeline._build_graph): the cascade follows the
    # edges visible on the canvas, drawn ones included. Applied again here
    # because this pass re-propagates on the GROUPED wiring, which is rebuilt
    # from the recorded call sites and so carries no manual bindings either.
    # `grouped.fn_input_params` itself is left alone — it is what the node ids
    # were derived from.
    node_states = propagate_run_states(
        group_own_states,
        input_params_with_manual_edges(
            grouped.fn_input_params,
            grouped.fn_outputs,
            grouped.fn_constants,
            grouped.path_inputs,
            manual_edges,
            token_for,
            manual_nodes=manual_nodes,
            hidden_edge_ids=hidden_edge_ids,
        ),
        grouped.fn_outputs,
    )

    n_groups = len(grouped.fn_input_params)
    n_sites = len(agg.fn_input_params)
    if n_groups != n_sites:
        logger.info(
            "[graph_builder] wiring grouping: %d call site(s) -> %d node(s)",
            n_sites,
            n_groups,
        )
    return grouped, node_states, member_map


def legacy_position_adoptions(
    member_map: dict[str, list[str]],
    positions_by_scope: dict[str, dict],
) -> tuple[list[dict], list[str]]:
    """One-time migration plan (pure): pre-grouping documents saved
    positions under per-call-site node ids; the group node adopts the first
    member position found (keeping its SCOPE — position location IS scope
    membership) and every legacy key is dropped.

    Returns (adoptions [{new_id, scope, x, y}], drop_ids [legacy ids]).
    """
    adoptions: list[dict] = []
    drop_ids: list[str] = []
    for group_id, legacy_ids in member_map.items():
        placed = any(group_id in pos for pos in positions_by_scope.values())
        for legacy_id in legacy_ids:
            if legacy_id == group_id:
                continue
            for scope, positions in positions_by_scope.items():
                if legacy_id not in positions:
                    continue
                if not placed:
                    xy = positions[legacy_id]
                    adoptions.append(
                        {
                            "new_id": group_id,
                            "scope": scope,
                            "x": xy.get("x", 0),
                            "y": xy.get("y", 0),
                        }
                    )
                    placed = True
                if legacy_id not in drop_ids:
                    drop_ids.append(legacy_id)
    return adoptions, drop_ids


def legacy_edge_rewrites(
    member_map: dict[str, list[str]],
    manual_edges: list[dict],
) -> list[dict]:
    """Manual edges whose endpoints reference legacy per-call-site node ids,
    rewritten to the group node id (pure; caller persists via the edge
    upsert)."""
    legacy_to_group = {
        legacy_id: group_id
        for group_id, legacy_ids in member_map.items()
        for legacy_id in legacy_ids
        if legacy_id != group_id
    }
    rewrites = []
    for edge in manual_edges:
        new_source = legacy_to_group.get(edge["source"])
        new_target = legacy_to_group.get(edge["target"])
        if new_source or new_target:
            rewrites.append(
                {
                    **edge,
                    "source": new_source or edge["source"],
                    "target": new_target or edge["target"],
                }
            )
    return rewrites


def parse_path_input(value: str) -> dict | None:
    """If *value* (from __inputs) represents a PathInput spec, return
    ``{"template": ..., "root_folder": ...}``, else ``None``.

    The parser is scidb's (``scidb.provenance.parse_path_input_spec``); this
    name is kept for the GUI's call sites. Three copies of it existed before
    2026-09-20 (here, ``scidb.inspect.graph``, ``scidb.database``).
    """
    return _parse_path_input_spec(value)


def _path_input_content_variants(obj) -> list[tuple[str, str | None]]:
    """(template, root_folder) pairs *obj* can match a historical DB value
    against — one for a bare PathInput, one per alternative for an EachOf
    of PathInputs (alternate templates)."""
    from scifor import EachOf, PathInput

    if isinstance(obj, PathInput):
        candidates = [obj]
    elif isinstance(obj, EachOf):
        candidates = [a for a in obj.alternatives if isinstance(a, PathInput)]
    else:
        return []
    return [
        (c.path_template, str(c.root_folder) if c.root_folder is not None else None)
        for c in candidates
    ]


def path_input_display(obj) -> dict:
    """{"template", "root_folder", "alternate_templates"} for a registry
    PathInput/EachOf-of-PathInput object — what a ``pathInput__`` node
    shows. The first alternative is the primary template; the rest render
    as ``alternate_templates`` (same shape the old layout.json-authored
    version used)."""
    from scifor import EachOf, PathInput

    alts = obj.alternatives if isinstance(obj, EachOf) else [obj]
    primary, *rest = [a for a in alts if isinstance(a, PathInput)] or [obj]
    return {
        "template": primary.path_template,
        "root_folder": (
            str(primary.root_folder) if primary.root_folder is not None else None
        ),
        "alternate_templates": [
            {
                "template": a.path_template,
                "root_folder": str(a.root_folder) if a.root_folder is not None else None,
            }
            for a in rest
        ],
    }


def _is_project_root(root_folder: "str | None", project_root) -> bool:
    """True when *root_folder* names *project_root*.

    A PathInput rooted at the project root is indistinguishable, in what it
    resolves to, from one with no root at all — see
    ``resolve_path_input_name`` step 3. Compared as resolved paths so a
    trailing slash or an unnormalized path does not decide the answer.
    """
    if not root_folder or project_root is None:
        return False
    from pathlib import Path

    try:
        return Path(root_folder).resolve() == Path(project_root).resolve()
    except OSError:  # pragma: no cover - unresolvable path on this machine
        return str(root_folder) == str(project_root)


def resolve_path_input_name(
    observed: dict,
    registry: "dict[str, object]",
    history: "dict[tuple, str] | None" = None,
    project_root=None,
) -> tuple[str, dict]:
    """Match a DB-history-observed ``{"template", "root_folder"}`` against
    the source-scanned PathInput registry by CONTENT (there's no name in
    DB history — ``PathInput.to_key()`` only serializes template/
    root_folder, never the module-level name it's bound to). Returns
    ``(registry_name, display_dict)``.

    Four strategies, in order:

    1. **Live registry content-match** — the template a declaration
       currently holds.
    2. **Recorded history** (D7) — ``{(template, root_folder): name}`` from
       ``pipeline_store.list_path_input_history``, covering templates a GUI
       edit has since overwritten. Without this, editing a template detaches
       every run recorded against the old one, because content-matching is
       the ONLY link between a run and a node. The display still comes from
       the CURRENT declaration, so the node shows what source says now while
       keeping its history attached.
    3. **Project-root-rooted match** — a run whose ``root_folder`` is exactly
       the project root, matched against a declaration that has none. A
       rootless PathInput resolves against the project root anyway, so the two
       name the same files; this is the same input recorded two ways.
       Generated MATLAB commands used to substitute the project root for a
       missing ``root_folder`` (see ``api.matlab_command._format_path_input``),
       so every run made that way is on disk under the rooted key. The
       generator no longer does that, but the recorded rows are permanent —
       without this step they keep an ``__unresolved__`` ghost node beside
       the declaration that produced them, forever. Skipped when the caller
       passes no ``project_root`` — there is then nothing to normalize against.
    4. **Unresolved** — a synthetic ``__unresolved__:{template}`` key with a
       WARN. Now genuinely rare, and meaning what it was designed to mean:
       the declaration was removed, or renamed *and* re-templated, so there
       is nothing left to attribute it to. The node still renders
       best-effort from the historical value.
    """
    key = (observed["template"], observed.get("root_folder"))
    for name, obj in registry.items():
        if key in _path_input_content_variants(obj):
            return name, path_input_display(obj)

    historical_name = (history or {}).get(key)
    if historical_name is not None and historical_name in registry:
        logger.info(
            "[graph_builder] PathInput usage matched a PREVIOUS template of "
            "%r (template=%r) — attributing history to the current node",
            historical_name,
            observed["template"],
        )
        return historical_name, path_input_display(registry[historical_name])

    if _is_project_root(observed.get("root_folder"), project_root):
        rootless = (observed["template"], None)
        for name, obj in registry.items():
            if rootless in _path_input_content_variants(obj):
                logger.info(
                    "[graph_builder] PathInput usage recorded with the project "
                    "root as its root_folder (template=%r) — attributing to the "
                    "rootless declaration %r, which resolves to the same files",
                    observed["template"],
                    name,
                )
                return name, path_input_display(obj)
        historical_name = (history or {}).get(rootless)
        if historical_name is not None and historical_name in registry:
            logger.info(
                "[graph_builder] PathInput usage recorded with the project root "
                "as its root_folder matched a PREVIOUS template of %r "
                "(template=%r)",
                historical_name,
                observed["template"],
            )
            return historical_name, path_input_display(registry[historical_name])

    logger.warning(
        "[graph_builder] PathInput usage with no matching source "
        "declaration: template=%r root_folder=%r — renamed or removed?",
        observed["template"],
        observed.get("root_folder"),
    )
    return f"__unresolved__:{observed['template']}", {
        "template": observed["template"],
        "root_folder": observed.get("root_folder"),
        "alternate_templates": [],
    }


def convert_scidb_path_inputs(
    scidb_path_inputs: dict,
    path_input_registry: "dict[str, object]",
    path_input_history: "dict[tuple, str] | None" = None,
    project_root=None,
) -> dict[str, dict]:
    """``db.get_aggregated_variants()["path_inputs"]`` (keyed by PARAM NAME
    — raw DB-history extraction, no knowledge of source code) ->
    ``AggregatedData.path_inputs`` shape (keyed by resolved registry name,
    ``"functions"`` as ``set[(FnKey, param_name)]``).

    Single shared conversion — do not re-inline this at a new call site;
    ``api/pipeline.py`` and ``execution_service.disconnected_report_entries``
    both need the exact same resolution (registry name, not param name) for
    their hidden-edge-id lookups to line up with what ``build_edges``
    actually produced. That includes ``project_root``: a caller that omits it
    resolves one PathInput to a different name than a caller that passes it,
    and the two sides stop lining up.
    """
    result: dict[str, dict] = {}
    for param_name, pi_data in scidb_path_inputs.items():
        pi_name, display = resolve_path_input_name(
            {"template": pi_data["template"], "root_folder": pi_data["root_folder"]},
            path_input_registry,
            path_input_history,
            project_root,
        )
        entry_functions = {(tuple(f), param_name) for f in pi_data["functions"]}
        existing = result.get(pi_name)
        if existing is None:
            result[pi_name] = {**display, "functions": entry_functions}
        else:
            existing["functions"] |= entry_functions
    return result


def seed_undiscovered_path_inputs(
    path_inputs: dict[str, dict], registry: "dict[str, object]"
) -> dict[str, dict]:
    """Add every registry PathInput that has no DB run history yet (so it
    still appears as an available, unconnected node) — the source-scanned
    replacement for the old layout.json-authored ``overlay_saved_path_inputs``.
    Mutates ``path_inputs`` in place and returns it."""
    for name, obj in registry.items():
        if name not in path_inputs:
            path_inputs[name] = {**path_input_display(obj), "functions": set()}
    return path_inputs


def aggregate_variants(
    variants: list[dict],
    listed_var_names: set[str],
    path_input_registry: "dict[str, object] | None" = None,
    path_input_history: "dict[tuple, str] | None" = None,
    project_root=None,
) -> AggregatedData:
    """Parse DB variants into aggregated data structures.

    Function-keyed fields use ``FnKey = (fn_name, call_id)`` so the same
    function reused from multiple for_each call sites becomes multiple
    entries.  call_id is taken from the variant dict (added by
    ``list_pipeline_variants``).

    Args:
        variants: From db.list_pipeline_variants().
        listed_var_names: Variable names from db.list_variables() to fill in
            types that exist but haven't been run through for_each.
        path_input_registry: ``registry.get_path_inputs_registry()`` — used
            to resolve a historically-recorded PathInput value (template/
            root_folder only, no name) back to its source-declared name via
            content matching (see ``resolve_path_input_name``).
        project_root: ``registry.get_project_root()`` — lets a run recorded
            with the project root as its ``root_folder`` attribute to a
            rootless declaration (``resolve_path_input_name`` step 3).

    Returns:
        AggregatedData with all parsed fields.
    """
    logger.info(
        "[graph_builder] aggregate_variants: processing %d variant(s)", len(variants)
    )
    path_input_registry = path_input_registry or {}
    agg = AggregatedData()

    for v in variants:
        fn = v["function_name"]
        cid = v.get("call_id", "")
        if not cid:
            # Legacy variant without call_id — skip rather than collide
            # other call sites under an empty key.  Logged so we notice.
            logger.warning(
                "aggregate_variants: variant missing call_id, skipping: fn=%s out=%s",
                fn,
                v.get("output_type"),
            )
            continue
        fkey: FnKey = (fn, cid)
        out = v["output_type"]
        inputs = v["input_types"]
        constants = v["constants"]
        count = v["record_count"]

        agg.all_var_types.add(out)

        for param_name, type_val in inputs.items():
            pi = parse_path_input(type_val)
            if pi is not None:
                pi_name, display = resolve_path_input_name(
                    pi, path_input_registry, path_input_history, project_root
                )
                existing = agg.path_inputs.get(pi_name)
                if existing is None:
                    agg.path_inputs[pi_name] = {
                        **display,
                        "functions": {(fkey, param_name)},
                    }
                else:
                    existing["functions"].add((fkey, param_name))
            else:
                agg.all_var_types.add(type_val)
                agg.fn_input_params[fkey][param_name] = type_val

        agg.fn_outputs[fkey].add(out)

        # Ensure fkey is tracked even with only PathInput/constant inputs
        if fkey not in agg.fn_input_params:
            agg.fn_input_params[fkey] = {}

        recorded = v.get("parameter_names") or {}
        for k, val in constants.items():
            node = parameter_node_name(k, recorded)
            agg.fn_parameter_names[fkey].setdefault(k, node)
            agg.const_counts[node][str(val)] += count
            agg.const_fns[node].add(fkey)
            agg.fn_constants[fkey].add(k)

        # Per-call-site variant list (currently always one entry per FnKey
        # because list_pipeline_variants groups by version_keys, but kept
        # as a list to match the existing settings-panel contract).
        agg.fn_variants_map[fkey].append(
            {
                "constants": constants,
                "input_types": inputs,
                "output_type": out,
                "record_count": count,
            }
        )

    # Add variable types from the DB that weren't in any for_each run.
    agg.all_var_types |= listed_var_names
    logger.debug(
        "[graph_builder] added %d variable type(s) from list_variables",
        len(listed_var_names),
    )

    logger.info(
        "[graph_builder] aggregate_variants complete: %d variants → %d var types, %d call sites, %d constants, %d path inputs",
        len(variants),
        len(agg.all_var_types),
        len(agg.fn_outputs),
        len(agg.const_counts),
        len(agg.path_inputs),
    )
    return agg


def filter_hidden(
    agg: AggregatedData, hidden_ids: set[str], strip_var_type_values: bool = True
) -> AggregatedData:
    """Remove hidden nodes from the aggregated data (mutates in place).

    Args:
        agg: Aggregated data to filter.
        hidden_ids: Set of node IDs the user has explicitly deleted.
        strip_var_type_values: Whether to also scrub hidden variable TYPES out
            of ``fn_outputs``/``fn_input_params`` VALUES for every surviving
            call site (as opposed to only dropping dict entries for call
            sites that are themselves hidden by fn id). Callers computing
            ``wiring_id`` (graph_builder.wiring_id — hashes fn name +
            input/output var types) from this agg MUST pass False: a
            function's wiring — and therefore its canvas node id, which
            anchors its saved scope placement/position — must stay stable
            regardless of which of its already-produced variables the user
            has hidden in this particular scope's view, or the node loses
            its placement and disappears from non-root scopes the moment one
            of its output leaves is hidden (see plan-scope-hidden-nodes-edges
            postmortem). Pass True (the default) once identity has already
            been fixed by grouping, to strip phantom hidden ports for
            display.

    Returns:
        The same AggregatedData, mutated.
    """
    logger.info(
        "[graph_builder] filter_hidden: filtering %d hidden node(s) "
        "(strip_var_type_values=%s)",
        len(hidden_ids),
        strip_var_type_values,
    )

    hidden_var_types = {
        nid.replace(VAR_ID_PREFIX, "", 1) for nid in hidden_ids if nid.startswith(VAR_ID_PREFIX)
    }
    # fn IDs in hidden_ids are composite ``fn__{fn_name}__{call_id}``.
    # Parse into FnKeys; ignore IDs that don't match (legacy/manual).
    hidden_fkeys: set[FnKey] = set()
    for nid in hidden_ids:
        parsed = parse_fn_node_id(nid)
        if parsed is not None:
            hidden_fkeys.add(parsed)
    hidden_const_names = {
        nid.replace(PARAM_ID_PREFIX, "", 1)
        for nid in hidden_ids
        if nid.startswith(PARAM_ID_PREFIX)
    }
    hidden_path_names = {
        nid.replace(PATH_INPUT_ID_PREFIX, "", 1)
        for nid in hidden_ids
        if nid.startswith(PATH_INPUT_ID_PREFIX)
    }

    agg.all_var_types -= hidden_var_types

    if strip_var_type_values:
        for fkey in list(agg.fn_outputs.keys()):
            agg.fn_outputs[fkey] -= hidden_var_types

        for fkey in list(agg.fn_input_params.keys()):
            agg.fn_input_params[fkey] = {
                p: t
                for p, t in agg.fn_input_params[fkey].items()
                if t not in hidden_var_types
            }

    for fkey in hidden_fkeys:
        agg.fn_input_params.pop(fkey, None)
        agg.fn_outputs.pop(fkey, None)
        agg.fn_constants.pop(fkey, None)

    for cname in hidden_const_names:
        agg.const_counts.pop(cname, None)
        agg.const_fns.pop(cname, None)

    for pname in hidden_path_names:
        agg.path_inputs.pop(pname, None)

    if hidden_ids:
        logger.info(
            "[graph_builder] filter_hidden complete: removed %d var, %d fn, %d const, %d pathInput",
            len(hidden_var_types),
            len(hidden_fkeys),
            len(hidden_const_names),
            len(hidden_path_names),
        )
        logger.debug(
            "[graph_builder] hidden nodes: var=%s fn=%s const=%s pathInput=%s",
            hidden_var_types,
            sorted(hidden_fkeys),
            hidden_const_names,
            hidden_path_names,
        )
    return agg


def _wiring_group_key(agg: "AggregatedData", fkey: FnKey) -> tuple[str, str]:
    """(fn_name, wiring_id) for the call site's canvas node — see wiring_id()."""
    fn, _cid = fkey
    return (
        fn,
        wiring_id(
            fn,
            agg.fn_input_params.get(fkey, {}),
            agg.fn_outputs.get(fkey, set()),
            path_input_bindings_by_fkey(agg.path_inputs).get(fkey, {}),
        ),
    )


def _fkey_has_constant_value(
    agg: "AggregatedData", fkey: FnKey, const_name: str, pval: str
) -> bool:
    # const_name is the Parameter NODE; rows are keyed by the argument(s) it fills.
    args = agg.constant_args(fkey, const_name)
    return any(
        str(row.get("constants", {}).get(arg)) == pval
        for row in agg.fn_variants_map.get(fkey, [])
        for arg in args
        if arg in row.get("constants", {})
    )


def pending_value_group_coverage(
    pending_constants: dict[str, set[str]],
    agg: "AggregatedData",
) -> dict[tuple[str, str], set[tuple[str, str]]]:
    """For every staged (constant, pending_value) pair, the set of wiring
    groups (see ``_wiring_group_key``) that already have a REAL call site
    recording that exact value.

    A constant can feed multiple function nodes that share a function name
    but are wired to different inputs/outputs (e.g. compute_rolling_vo2 fed
    by RawVO2 in one node, RawHeartRate in another — each its own canvas
    node/wiring, see group_call_sites_by_wiring). Shared by
    ``auto_clean_pending_constants`` (deciding when a value is no longer
    pending ANYWHERE) and ``group_call_sites_by_wiring`` (deciding whether
    to synthesize a staged row for one SPECIFIC wiring, even while the
    value is still pending for a sibling one).

    Returns:
        {(const_name, pending_value): {wiring_group_key, ...}}
    """
    coverage: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for const_name, pvals in pending_constants.items():
        consuming_fkeys = agg.const_fns.get(const_name, set())
        for pval in pvals:
            coverage[(const_name, pval)] = {
                _wiring_group_key(agg, fkey)
                for fkey in consuming_fkeys
                if _fkey_has_constant_value(agg, fkey, const_name, pval)
            }
    return coverage


def auto_clean_pending_constants(
    pending_constants: dict[str, set[str]],
    agg: "AggregatedData",
) -> tuple[dict[str, set[str]], list[tuple[str, str]]]:
    """Remove pending values once every wiring that consumes them has run.

    A naive "is this value in the DB anywhere" check blurs across wirings
    that share a constant node: as soon as ONE of them ran with the new
    value, the check saw the value in the DB and cleared the pending flag
    for ALL of them — silently un-marking the OTHER (never re-run) wiring
    as no-longer-pending, even though it's still showing its old, stale
    value. Removal must wait until every wiring group that references the
    constant has its own real call site recording that exact value.

    Returns:
        Tuple of (cleaned pending_constants, list of (name, value) to remove from DB).
    """
    removals: list[tuple[str, str]] = []
    coverage = pending_value_group_coverage(pending_constants, agg)
    for const_name in list(pending_constants.keys()):
        consuming_fkeys = agg.const_fns.get(const_name, set())
        required_groups = {_wiring_group_key(agg, fkey) for fkey in consuming_fkeys}
        still_pending: set[str] = set()
        for pval in pending_constants[const_name]:
            covered_groups = coverage[(const_name, pval)]
            # required_groups being empty means no real call site references
            # this constant yet — never auto-clean on that vacuous truth.
            if required_groups and required_groups <= covered_groups:
                removals.append((const_name, pval))
            else:
                still_pending.add(pval)
        pending_constants[const_name] = still_pending
    if removals:
        logger.debug("auto_clean_pending_constants: removing %s", removals)
    return pending_constants, removals


def build_variable_nodes(
    all_var_types: set[str],
    record_counts: dict[str, int],
    run_states: dict[str, str],
) -> list[dict]:
    """Build React Flow variable nodes."""
    logger.info(
        "[graph_builder] build_variable_nodes: building %d variable node(s)",
        len(all_var_types),
    )
    nodes = []
    for vtype in sorted(all_var_types):
        data: dict = {
            "label": vtype,
            "total_records": record_counts.get(vtype, 0),
        }
        state = run_states.get(var_node_id(vtype), "green")
        data["run_state"] = state
        nodes.append(
            {
                "id": var_node_id(vtype),
                "type": "variableNode",
                "position": {"x": 0, "y": 0},
                "data": data,
            }
        )
    logger.debug("[graph_builder] built %d variable node(s)", len(nodes))
    return nodes


def render_value_group_label(kind: str, spec: dict, count: int) -> str:
    """The compact label for a generated value set — ``0:2:20 — 11 values``.

    Rendered HERE, backend-side, and shipped in the node's ``data``, because
    both surfaces that show it (``ParameterNode`` on the canvas and
    ``ParameterSettingsPanel`` in the sidebar, which receives the node's
    ``values`` as a prop) then render the identical string by construction.
    Two frontend implementations of "the same repr" would only have to agree
    by convention.

    Range sets use colon notation, which reads natively to the MATLAB half
    of this project. A pasted list has no range to state, so it shows its
    first few members and the count.
    """
    if kind == "range":
        start, step, end = spec.get("start"), spec.get("step"), spec.get("end")
        if start is not None and step is not None and end is not None:
            span = f"{_trim_number(start)}:{_trim_number(step)}:{_trim_number(end)}"
            return f"{span} — {count} values"
    members = [str(m) for m in (spec.get("members") or [])]
    if members:
        shown = ", ".join(members[:6])
        return f"{shown} — {count} values" if len(members) > 6 else shown
    return f"{count} values"


def _trim_number(n) -> str:
    """``2.0`` -> ``2``, ``0.5`` -> ``0.5`` — a generated range is usually
    whole, and ``0.0:2.0:20.0`` reads worse than ``0:2:20``."""
    if isinstance(n, float) and n.is_integer():
        return str(int(n))
    return str(n)


def _collapse_value_group(
    const_name: str,
    values: list[dict],
    group: "dict | None",
    declared: set,
) -> list[dict]:
    """Fold *group*'s members into a single row, or leave *values* alone.

    A group survives only while it still describes what source declares: if
    any member has left the declaration (a hand edit to the entities file, a
    removal in the panel), the grouping is stale and every value renders
    individually. Reconciliation is a READ-side check on purpose — source is
    the truth for which values exist, and a stale group must never be able
    to hide a value that is really declared.

    Rows outside the group (values added individually afterwards, and DB
    history rows) keep their existing shape untouched, which is what keeps
    their presentation identical to before this feature.
    """
    if not group:
        return values
    members = [str(m) for m in group.get("values", [])]
    if not members:
        return values
    missing = [m for m in members if m not in declared]
    if missing:
        logger.info(
            "[graph_builder] parameter %r: ignoring a stale generated set — "
            "%d of its %d value(s) are no longer declared in source (%s)",
            const_name,
            len(missing),
            len(members),
            missing,
        )
        return values

    member_set = set(members)
    grouped = [v for v in values if v["value"] in member_set]
    if not grouped:
        return values
    rest = [v for v in values if v["value"] not in member_set]

    row = {
        "kind": "generated",
        "value": render_value_group_label(
            group.get("kind", "list"), group.get("spec") or {}, len(members)
        ),
        "members": members,
        # Shipped so the panel's Generate section can re-seed its inputs with
        # the generation that produced what is on screen, instead of its
        # hardcoded 0/10/1 defaults.
        "spec": group.get("spec") or {},
        "record_count": sum(v["record_count"] for v in grouped),
        # Unchecked as soon as ANY member is hidden: the set is the unit the
        # user toggles, so "partly excluded" is not a state its one checkbox
        # can represent. No tri-state — the only way to reach a mixed set is
        # to hide members individually before generating, and the next toggle
        # resolves it either way.
        "checked": all(v["checked"] for v in grouped),
        "is_current_source_value": all(
            v.get("is_current_source_value", False) for v in grouped
        ),
    }
    logger.debug(
        "[graph_builder] parameter %r: %d value(s) collapsed into one "
        "generated row (%s)",
        const_name,
        len(grouped),
        row["value"],
    )
    # The group leads, individually added values follow — the set was written
    # first, in one action, and the rest were appended to it.
    return [row, *rest]


def is_declared_in_entities_file(
    source_file: "str | None", entities_file: "str | None"
) -> bool:
    """Whether *source_file* (a Parameter's ``source_file``) is the
    configured writable entities file -- the single comparison behind both
    the canvas node's and the sidebar row's "declared in entities file"
    flag, kept in one place so the two never drift."""
    return source_file is not None and entities_file is not None and source_file == entities_file


def build_parameter_nodes(
    const_counts: dict[str, dict],
    pending_constants: dict[str, set[str]],
    source_parameters: "dict[str, object] | None" = None,
    hidden_values: "dict[str, set[str]] | None" = None,
    value_groups: "dict[str, dict] | None" = None,
    entities_file: "str | None" = None,
) -> list[dict]:
    """Build React Flow **Parameter** nodes.

    A Parameter is a named thing with one or more values -- one class,
    ``scidb.Parameter``, whatever the count. Adding a value is adding an
    argument, so a node never changes type or id under the user (D6).

    source_parameters: ``{name: Parameter}`` from
    ``registry.get_parameters_registry()``. Every declared value is merged
    in, so a source edit surfaces in the GUI immediately. A Parameter with
    no DB history and no pending value still gets a node (parallels
    ``seed_undiscovered_path_inputs``). Rows whose value matches a currently
    declared one are tagged ``is_current_source_value: True`` -- even if the
    row already existed as DB history or a pending value -- so the frontend
    can badge them apart from stale historical rows. Values that have LEFT
    source but have run history stay visible and simply lose the badge: the
    DB is the record of what actually ran.

    hidden_values: {const_name: {hidden value strings}} from
    ``pipeline_store.list_hidden_parameter_values`` — every value row gets a
    ``"checked"`` bool (``value not in hidden_values.get(const_name, ...)``)
    so ``ParameterNode.tsx``'s checkbox reflects PERSISTED state instead of
    a hardcoded true. Applies to every value, whether the Parameter holds
    one or many.

    value_groups: ``{param_name: {"kind", "spec", "values"}}`` from
    ``pipeline_store.get_parameter_value_groups`` — values written in one go
    by the panel's "Replace values" button. Each group collapses into ONE
    row carrying ``kind: "generated"``, a compact label and a ``members``
    list; every other row keeps the exact shape it has always had, so values
    added one at a time render exactly as before. See
    :func:`_collapse_value_group`.

    entities_file: the configured writable entities file path (as a string),
    used only to set each node's ``declared_in_entities_file`` flag via
    :func:`is_declared_in_entities_file` -- lets the GUI offer "refresh from
    file" only where a re-read of that file can actually change the value.
    """
    source_parameters = source_parameters or {}
    hidden_values = hidden_values or {}
    value_groups = value_groups or {}
    # One shape for every Parameter: its declared values, stringified.
    source_values: dict[str, list] = {
        name: [str(v) for v in p.values] for name, p in source_parameters.items()
    }
    all_names = sorted(set(const_counts) | set(source_values))
    logger.info(
        "[graph_builder] build_parameter_nodes: building %d parameter node(s)",
        len(all_names),
    )
    # A node that exists ONLY because history names it. Legitimate for a
    # Parameter since removed from source, but it is also exactly what a run
    # recorded under an argument name instead of the declared Parameter looks
    # like (cleanup-audit B1) — so it is named, not left to be counted.
    history_only = sorted(set(const_counts) - set(source_values))
    if history_only and source_values:
        logger.info(
            "[graph_builder] build_parameter_nodes: %d node(s) come from run "
            "history alone, not a declaration: %s (declared: %s)",
            len(history_only),
            history_only,
            sorted(source_values),
        )
    nodes = []
    for const_name in all_names:
        hidden_for_name = hidden_values.get(const_name, set())
        values = [
            {"value": val, "record_count": cnt, "checked": val not in hidden_for_name}
            for val, cnt in sorted(const_counts.get(const_name, {}).items())
        ]
        existing_values = {v["value"] for v in values}
        for pval in sorted(pending_constants.get(const_name, set())):
            if pval not in existing_values:
                values.append(
                    {
                        "value": pval,
                        "record_count": 0,
                        "checked": pval not in hidden_for_name,
                    }
                )
                existing_values.add(pval)
        # Values source currently declares. Ones that have LEFT source but
        # have DB history stay visible — the DB is the record of what
        # actually ran (decision #2 of plan-constant-source-of-truth); they
        # simply lose the badge.
        current_source = source_values.get(const_name, [])

        # "New" here means "declared in source, no DB records yet" — which
        # stays true on EVERY rebuild for any Parameter that hasn't been run,
        # so this is steady-state bookkeeping, not an event. Logged once per
        # parameter at debug; at INFO it was three lines per build forever
        # (5% of a real session's log, examples/vo2max/scidb.log).
        merged_source_values = []
        for src_val in current_source:
            if src_val not in existing_values:
                merged_source_values.append(src_val)
                values.append(
                    {
                        "value": src_val,
                        "record_count": 0,
                        "checked": src_val not in hidden_for_name,
                    }
                )
                existing_values.add(src_val)
        if merged_source_values:
            logger.debug(
                "[graph_builder] parameter %r: merged %d source-declared "
                "value(s) with no DB records yet: %s",
                const_name,
                len(merged_source_values),
                merged_source_values,
            )
        source_set = set(current_source)
        for v in values:
            if v["value"] in source_set:
                v["is_current_source_value"] = True

        # Last, so the group sees final record counts, checked state and
        # source badges and can fold them into its single row.
        values = _collapse_value_group(
            const_name, values, value_groups.get(const_name), source_set
        )

        param = source_parameters.get(const_name)
        source_file = getattr(param, "source_file", None) if param is not None else None
        source_line = getattr(param, "source_line", None) if param is not None else None

        nodes.append(
            {
                "id": param_node_id(const_name),
                "type": "parameterNode",
                "position": {"x": 0, "y": 0},
                "data": {
                    "label": const_name,
                    "values": values,
                    "source_file": source_file,
                    "source_line": source_line,
                    "declared_in_entities_file": is_declared_in_entities_file(
                        source_file, entities_file
                    ),
                    # No DB history and no GUI-added value: it exists only
                    # because source declares it. Such a node is on a canvas
                    # only where it has been placed (scope_filter).
                    DECLARED_ONLY: const_name not in const_counts
                    and not pending_constants.get(const_name),
                },
            }
        )
    logger.debug("[graph_builder] built %d parameter node(s)", len(nodes))
    return nodes


def build_path_input_nodes(path_inputs: dict[str, dict]) -> list[dict]:
    """Build React Flow path input nodes.

    ``path_inputs`` is keyed by the PathInput's SOURCE-DECLARED name (see
    ``AggregatedData.path_inputs`` / ``resolve_path_input_name`` /
    ``seed_undiscovered_path_inputs``) — no longer the function parameter
    name, and no longer layout.json-authored (see
    ``docs/claude/code-discovery-categories.md``).
    """
    logger.info(
        "[graph_builder] build_path_input_nodes: building %d path input node(s)",
        len(path_inputs),
    )
    nodes = []
    for pi_name in sorted(path_inputs.keys()):
        pi = path_inputs[pi_name]
        nodes.append(
            {
                "id": path_input_node_id(pi_name),
                "type": "pathInputNode",
                "position": {"x": 0, "y": 0},
                "data": {
                    "label": pi_name,
                    "template": pi["template"],
                    "root_folder": pi.get("root_folder"),
                    "alternate_templates": pi.get("alternate_templates", []),
                    # Seeded from the registry with no run history
                    # (seed_undiscovered_path_inputs) — see DECLARED_ONLY.
                    DECLARED_ONLY: not pi.get("functions"),
                },
            }
        )
    logger.debug("[graph_builder] built %d path input node(s)", len(nodes))
    return nodes




def build_function_nodes(
    fn_input_params: dict[FnKey, dict],
    fn_outputs: dict[FnKey, set],
    fn_constants: dict[FnKey, set],
    fn_variants_map: dict[FnKey, list],
    fn_params_map: dict[str, list[str]],
    run_states: dict[str, str],
    matlab_functions: set[str],
    saved_configs: dict[str, dict | None],
    matlab_output_order: dict[str, list[str]] | None = None,
    matlab_param_to_class: dict[str, dict[str, str]] | None = None,
    node_configs: dict[str, dict] | None = None,
) -> list[dict]:
    """Build React Flow function nodes — one per aggregate key.

    Keys arrive as ``(fn_name, wiring_id)`` when the caller grouped call
    sites via ``group_call_sites_by_wiring`` (the canvas default since
    2026-07-18); the builder is agnostic and works for raw
    ``(fn_name, call_id)`` keys too (unit tests, ungrouped callers).
    ``data.call_id`` is set to the key's suffix either way — the node id
    always ends with it.

    Args:
        fn_input_params: {(fn_name, call_id): {param: var_type}}.
        fn_outputs: {(fn_name, call_id): {output_types}}.
        fn_constants: {(fn_name, call_id): {constant_param_names}}.
        fn_variants_map: {(fn_name, call_id): [variant_dicts]} for settings panel.
        fn_params_map: {fn_name: [all_sig_params]} from registry.  Keyed by
            fn_name only because the function's signature does not vary
            across call sites.
        run_states: {node_id: state} keyed by composite ``fn__{fn}__{cid}`` IDs.
        matlab_functions: Set of MATLAB function names.
        saved_configs: {fn_name: config_dict or None} from manual nodes.  Same
            saved config applies to every call site of fn_name.  The FALLBACK
            source -- see node_configs.
        node_configs: {node_id: config_dict} from the _node_config table, the
            authoritative per-node source.  Keyed by the composite node id, so
            unlike saved_configs it can hold a setting for a node that has
            already run, and it distinguishes two call sites of one function.
            Checked first; saved_configs answers when it has no entry.
        matlab_output_order: {fn_name: [output_names in signature order]}.
        matlab_param_to_class: {fn_name: {param_name: class_name}} — explicit
            mapping from MATLAB signature param names to connected Variable class
            names. Used to decide which declared params got wired up so their
            handles are rendered (handle id `out__{param_name}`).
    """
    logger.info(
        "[graph_builder] build_function_nodes: building %d function node(s)",
        len(fn_input_params),
    )
    nodes = []
    # Sort by (fn_name, call_id) for stable output across runs.
    for fkey in sorted(fn_input_params.keys()):
        fn, cid = fkey
        # Order by registry signature declaration order (falling back to
        # alphabetical for any param not in the signature, e.g. stale DB
        # data) so DB-derived handle order matches the manual-node path,
        # which already orders by signature (pipeline.py _fn_params_from_registry).
        sig_order = fn_params_map.get(fn, [])
        order_key = {name: i for i, name in enumerate(sig_order)}
        input_params = dict(
            sorted(
                fn_input_params[fkey].items(),
                key=lambda kv: (order_key.get(kv[0], len(sig_order)), kv[0]),
            )
        )
        constant_params = sorted(
            fn_constants.get(fkey, set()),
            key=lambda name: (order_key.get(name, len(sig_order)), name),
        )

        # Fill in any params the DB didn't capture.
        known = set(input_params) | set(constant_params)
        for name in fn_params_map.get(fn, []):
            if name not in known:
                input_params[name] = ""

        # MATLAB fns render handles in MATLAB-signature order using param names
        # (e.g. "time", "force_left"). Non-MATLAB fns use the class names directly.
        actual_outputs = fn_outputs.get(fkey, set())
        if fn in matlab_functions and matlab_output_order:
            declared = matlab_output_order.get(fn, [])
            p2c = (matlab_param_to_class or {}).get(fn, {})
            connected_classes = set(p2c.values()) | actual_outputs
            # Signature order, but only for params that actually map to a class
            # (either via an explicit edge or a DB variant).
            out_types = [
                p for p in declared if p in p2c or p2c.get(p) in connected_classes
            ]
            if not out_types:
                out_types = list(declared)
            # Any class in DB variants that is not covered by the declared
            # signature is a real anomaly — log it so we can see it.
            covered = {p2c.get(p) for p in out_types if p in p2c}
            orphan = actual_outputs - covered - {None}
            if orphan:
                # Not cosmetic: build_edges falls back to sourceHandle
                # 'out__{class}' when p2c has no entry, while this node
                # renders handles named 'out__{param}'. React Flow drops an
                # edge whose sourceHandle does not exist on its source node,
                # so the canvas shows the function disconnected from an
                # output variable that run_state still marks green (states
                # propagate over node ids, not handles).
                logger.warning(
                    "[graph_builder] matlab fn=%s call_id=%s: DB variants %s "
                    "have no declared param mapping (matlab_param_to_class=%s) "
                    "— its output edge(s) will target handle(s) %s while this "
                    "node renders %s, and will not render",
                    fn,
                    cid,
                    sorted(orphan),
                    p2c,
                    sorted(out_handle(o) for o in orphan),
                    sorted(out_handle(p) for p in out_types),
                )
            logger.debug(
                "[graph_builder] matlab fn=%s call_id=%s handles=%s param→class=%s",
                fn,
                cid,
                out_types,
                p2c,
            )
        else:
            out_types = sorted(actual_outputs)

        node_id = fn_node_id(fn, cid)
        fn_data: dict = {
            "label": fn,
            "call_id": cid,
            "variants": fn_variants_map.get(fkey, []),
            "input_params": input_params,
            "output_types": out_types,
            "constant_params": constant_params,
        }
        state = run_states.get(node_id)
        if state:
            fn_data["run_state"] = state
        if fn in matlab_functions:
            fn_data["language"] = "matlab"

        # Apply saved config (schemaSelection, schemaLevel, whereFilters,
        # runOptions). This node's own config wins; the fn_name-keyed manual
        # config is the fallback for a node that has never been saved under
        # its composite id. Without the node_id lookup, a function that has
        # run has no reachable config at all and every toggle silently resets
        # on rebuild -- see docs/claude/gui-run-options-flow.md.
        saved = (node_configs or {}).get(node_id) or saved_configs.get(fn)
        _apply_saved_config(fn_data, saved)

        nodes.append(
            {
                "id": node_id,
                "type": "functionNode",
                "position": {"x": 0, "y": 0},
                "data": fn_data,
            }
        )
    logger.debug("[graph_builder] built %d function node(s)", len(nodes))
    return nodes


def build_edges(
    fn_input_params: dict[FnKey, dict],
    fn_outputs: dict[FnKey, set],
    const_fns: dict[str, set],
    path_inputs: dict[str, dict],
    manual_edges: list[dict],
    hidden_ids: set[str],
    matlab_param_to_class: dict[str, dict[str, str]] | None = None,
    hidden_edge_ids: set[str] | None = None,
    fn_parameter_names: "dict[FnKey, dict[str, str]] | None" = None,
) -> list[dict]:
    """Build React Flow edges (DB-derived + manual).

    Edges target/source the per-call-site node IDs (``fn__{fn}__{cid}``)
    so an input variable that feeds two different call sites of the same
    function produces two distinct edges.

    Args:
        fn_input_params: {(fn_name, call_id): {param: var_type}}.
        fn_outputs: {(fn_name, call_id): {output_types}}.
        const_fns: {Parameter node name: {(fn_name, call_id), ...}}.
        fn_parameter_names: {fkey: {argument: Parameter node name}}
            (``AggregatedData.fn_parameter_names``). An edge leaves the
            NODE and lands on the ARGUMENT's handle; they differ when a
            Parameter feeds an argument of another name (cleanup-audit B1).
            Absent → the node name is the argument, the pre-2026-09-23 shape.
        path_inputs: {param_name: {"functions": set[FnKey], ...}}.
        manual_edges: List of manual edge dicts from pipeline_store.
        hidden_ids: Set of hidden node IDs.
        matlab_param_to_class: {fn_name: {param_name: class_name}} — for MATLAB
            fns, the explicit mapping from signature param name to connected
            Variable class. Drives sourceHandle=out__{param_name} for output
            edges instead of the class-name-based handle.
        hidden_edge_ids: Set of edge IDs the user has explicitly hidden (see
            pipeline_store.hide_edge) — excluded from the DB-derived edges
            they'd otherwise regenerate every rebuild. Never deletes data,
            only excludes rendering (and, for inbound edges, marks the
            target wiring "disconnected" — see hidden_wirings).
    """
    logger.info(
        "[graph_builder] build_edges: building edges from DB-derived data and manual edges"
    )
    edges = []
    seen_edges: set[tuple] = set()
    p2c_all = matlab_param_to_class or {}
    hidden_edge_ids = hidden_edge_ids or set()

    # Variable → function edges (one per call-site target).
    logger.debug("[graph_builder] building variable → function edges")
    hidden_var_to_fn = 0
    for fkey, params in fn_input_params.items():
        fn, cid = fkey
        target_id = fn_node_id(fn, cid)
        for param_name, in_type in params.items():
            key = (var_node_id(in_type), target_id)
            if key not in seen_edges:
                seen_edges.add(key)
                edge_id = f"e__{in_type}__{fn}__{cid}"
                if edge_id in hidden_edge_ids:
                    hidden_var_to_fn += 1
                    continue
                edges.append(
                    {
                        "id": edge_id,
                        "source": var_node_id(in_type),
                        "target": target_id,
                        "targetHandle": in_handle(param_name),
                    }
                )
    var_to_fn_count = len(edges)
    logger.debug(
        "[graph_builder] built %d variable → function edge(s) (%d hidden)",
        var_to_fn_count,
        hidden_var_to_fn,
    )

    # Function → variable edges.  For MATLAB fns, use the param↔class mapping
    # (call-site-independent) so sourceHandle=out__{param_name}.
    logger.debug("[graph_builder] building function → variable edges")
    hidden_fn_to_var = 0
    for fkey, out_types in fn_outputs.items():
        fn, cid = fkey
        source_id = fn_node_id(fn, cid)
        class_to_param = {c: p for p, c in p2c_all.get(fn, {}).items()}
        for out_type in out_types:
            key = (source_id, var_node_id(out_type))
            if key in seen_edges:
                continue
            seen_edges.add(key)
            edge_id = f"e__{fn}__{cid}__{out_type}"
            if edge_id in hidden_edge_ids:
                hidden_fn_to_var += 1
                continue
            param = class_to_param.get(out_type)
            source_handle = out_handle(param) if param else out_handle(out_type)
            edges.append(
                {
                    "id": edge_id,
                    "source": source_id,
                    "target": var_node_id(out_type),
                    "sourceHandle": source_handle,
                }
            )
    fn_to_var_count = len(edges) - var_to_fn_count
    logger.debug(
        "[graph_builder] built %d function → variable edge(s) (%d hidden)",
        fn_to_var_count,
        hidden_fn_to_var,
    )

    # Constant → function edges (one per call site that uses the constant).
    logger.debug("[graph_builder] building constant → function edges")
    hidden_const_to_fn = 0
    names_by_fkey = fn_parameter_names or {}
    renamed_edges: list[str] = []
    for const_name, fkeys in const_fns.items():
        for fkey in fkeys:
            fn, cid = fkey
            target_id = fn_node_id(fn, cid)
            args = [
                a for a, n in sorted(names_by_fkey.get(fkey, {}).items())
                if n == const_name
            ] or [const_name]
            for arg in args:
                key = (param_node_id(const_name), target_id, arg)
                if key in seen_edges:
                    continue
                seen_edges.add(key)
                # Keyed by the ARGUMENT (the handle it fills), which is what
                # hidden_wirings and the disconnected report spell — and the
                # same id as before whenever node and argument share a name.
                edge_id = f"e__{arg}__{fn}__{cid}"
                if edge_id in hidden_edge_ids:
                    hidden_const_to_fn += 1
                    continue
                if arg != const_name:
                    renamed_edges.append(f"{const_name}->{fn}.{arg}")
                edges.append(
                    {
                        "id": edge_id,
                        "source": param_node_id(const_name),
                        "target": target_id,
                        "targetHandle": param_handle(arg),
                    }
                )
    if renamed_edges:
        logger.info(
            "[graph_builder] %d Parameter edge(s) feed an argument of another "
            "name (declared Parameter -> fn.argument): %s",
            len(renamed_edges),
            ", ".join(renamed_edges),
        )
    const_to_fn_count = len(edges) - var_to_fn_count - fn_to_var_count
    logger.debug(
        "[graph_builder] built %d constant → function edge(s) (%d hidden)",
        const_to_fn_count,
        hidden_const_to_fn,
    )

    # PathInput → function edges. ``pi_name`` (the source-declared name) and
    # ``param_name`` (the function's parameter it fills) can differ, so both
    # must be part of the dedup/edge-id key — unlike var/const edges, the
    # source id no longer encodes the parameter by itself.
    logger.debug("[graph_builder] building pathInput → function edges")
    hidden_path_to_fn = 0
    for pi_name, pi in path_inputs.items():
        for fkey, param_name in pi["functions"]:
            fn, cid = fkey
            target_id = fn_node_id(fn, cid)
            key = (path_input_node_id(pi_name), target_id, param_name)
            if key not in seen_edges:
                seen_edges.add(key)
                edge_id = f"e__{pi_name}__{param_name}__{fn}__{cid}"
                if edge_id in hidden_edge_ids:
                    hidden_path_to_fn += 1
                    continue
                edges.append(
                    {
                        "id": edge_id,
                        "source": path_input_node_id(pi_name),
                        "target": target_id,
                        "targetHandle": in_handle(param_name),
                    }
                )
    path_to_fn_count = (
        len(edges) - var_to_fn_count - fn_to_var_count - const_to_fn_count
    )
    logger.debug(
        "[graph_builder] built %d pathInput → function edge(s) (%d hidden)",
        path_to_fn_count,
        hidden_path_to_fn,
    )

    # Merge manually-created edges.
    logger.debug("[graph_builder] merging %d manual edge(s)", len(manual_edges))
    db_edge_count = len(edges)
    superseded = 0
    for me in manual_edges:
        if me["source"] in hidden_ids or me["target"] in hidden_ids:
            continue
        if me["id"] in hidden_edge_ids:
            continue
        if any(e["id"] == me["id"] for e in edges):
            continue
        # Endpoint dedup, not just id dedup. A manual edge keeps its random
        # ``manual__xxxx`` id forever, and on graduation its endpoints are
        # rewritten onto the DB-derived node ids (pipeline_store.
        # rename_edge_endpoints) — so it ends up describing the very same
        # connection as a ``e__...`` edge while never colliding by id. The
        # canvas then drew both, permanently, and deleting one left its twin.
        # The row stays in _pipeline_edges (hide, never delete): if the
        # DB-derived edge later disappears, this renders again.
        #
        # NOTE: this pass only catches manual edges that ALREADY name the
        # DB-derived ids. Ones graduated later in this same build are caught
        # by drop_superseded_manual_edges, which keys on the same function.
        dedup_key = edge_dedup_key(
            me["source"],
            me["target"],
            me.get("targetHandle") or me.get("target_handle"),
        )
        if dedup_key in seen_edges:
            superseded += 1
            logger.debug(
                "[graph_builder] manual edge %s superseded by the DB-derived "
                "edge for the same connection (%s -> %s)",
                me["id"],
                me["source"],
                me["target"],
            )
            continue
        edge: dict = {
            "id": me["id"],
            "source": me["source"],
            "target": me["target"],
            "data": {"manual": True},
        }
        if me.get("sourceHandle"):
            edge["sourceHandle"] = me["sourceHandle"]
        if me.get("targetHandle"):
            edge["targetHandle"] = me["targetHandle"]
        edges.append(edge)
    manual_edge_count = len(edges) - db_edge_count
    logger.debug("[graph_builder] added %d manual edge(s)", manual_edge_count)

    total_hidden = (
        hidden_var_to_fn + hidden_fn_to_var + hidden_const_to_fn + hidden_path_to_fn
    )
    logger.info(
        "[graph_builder] build_edges complete: %d total edges (%d DB-derived, "
        "%d manual, %d hidden, %d manual superseded by DB-derived)",
        len(edges),
        db_edge_count,
        manual_edge_count,
        total_hidden,
        superseded,
    )
    return edges


# ---------------------------------------------------------------------------
# Disconnected wirings — hiding an INBOUND edge (variable/constant/
# pathInput -> function) means that wiring is missing a required input
# entirely, not just decluttered from the canvas. Every call site sharing
# that wiring is affected (run_state forced red, execution blocked) — see
# domain.run_state.propagate_run_states(disconnected_fkeys=...) and
# domain.variant_resolver.reconcile_manual_inputs. Hiding an OUTBOUND
# (function -> variable) edge is deliberately excluded here: it's cosmetic
# only, since the function's real output still exists in the DB either way.
# ---------------------------------------------------------------------------


def inbound_edge_candidates(
    fn: str, wid: str, var_types=(), const_names=(), path_names=()
) -> list[str]:
    """Candidate inbound edge ids (var/const/pathInput -> fn) for one
    wiring — the same id shape build_edges constructs, reusable anywhere a
    caller needs to check "is this call site's required input hidden?"
    without needing the edge to already exist (hidden_wirings,
    variant_resolver.reconcile_manual_inputs)."""
    return (
        [f"e__{vt}__{fn}__{wid}" for vt in var_types]
        + [f"e__{cn}__{fn}__{wid}" for cn in const_names]
        + [f"e__{pn}__{fn}__{wid}" for pn in path_names]
    )


def inbound_edge_candidates_by_handle(
    fn: str, wid: str, input_params: dict, const_names=(), path_names=()
) -> dict[str, str]:
    """Same candidate inbound edge ids as ``inbound_edge_candidates``, but
    mapped to the ``target_handle`` each one feeds (``in__{param}`` /
    ``param__{name}``) — the id shape alone doesn't say WHICH input a hidden
    edge blocks, and callers reconciling hidden edges against manual
    reconnects need that to check per-handle coverage rather than a flat
    yes/no (see hidden_wirings, variant_resolver.reconcile_manual_inputs).

    ``input_params``: {param_name: var_type-or-list-of-var_types}, same
    shape as a call site's ``fn_input_params`` entry / a DB variant's
    ``input_types``. A list-valued param contributes one candidate per
    element, all mapped to that param's single handle.
    """
    result: dict[str, str] = {}
    for param_name, type_val in input_params.items():
        handle = in_handle(param_name)
        types = type_val if isinstance(type_val, (list, set, tuple)) else [type_val]
        for vt in types:
            result[f"e__{vt}__{fn}__{wid}"] = handle
    for cname in const_names:
        result[f"e__{cname}__{fn}__{wid}"] = param_handle(cname)
    for pname in path_names:
        result[f"e__{pname}__{fn}__{wid}"] = in_handle(pname)
    return result


def manual_edge_handle_index(
    manual_edges: "list[dict] | tuple",
) -> dict[tuple[str, str, str], dict]:
    """Index manual edges by the (fn_name, node token, target_handle) call
    site they currently feed — the "is this exact input handle covered by
    a manual reconnect?" lookup used by hidden_wirings/
    reconcile_manual_inputs to stop treating a hidden DB-derived edge
    as disconnected once the user has manually wired a replacement onto
    the same handle. Matches by parsing each edge's ``target`` the same
    way execution_service.derive_fn_targets already matches manual edges
    to function nodes (parse_fn_node_id, which strips any placement
    suffix first) — so a bare, wiring-grouped, or scope-placed target id
    all resolve to the same key.

    **The key's second element is the NODE's token, not a wiring id.** It
    always was — it is read off the ``target`` node id — and until
    2026-09-22 the two were the same string, which is why the parameter was
    called ``wid`` everywhere. Under allocated node ids they can differ, and
    every caller that computes a wiring must map it through ``token_for``
    before looking in here (``docs/claude/node-identity.md``).

    **Every** edge on a handle, not the last one scanned (2026-09-22). The
    index used to hold one edge per key, which silently lost the others: two
    manual edges on one handle showed only one source (the "Known limitations"
    entry in the doc above), and the supersession rewrite could only move one
    of them per build — so a node with two edges per handle needed two builds
    to migrate, doing half the work each time and logging it twice. Values are
    lists in scan order; every caller that only asks ``in`` is unaffected.
    """
    index: dict[tuple[str, str, str], list[dict]] = {}
    for edge in manual_edges:
        handle = edge.get("targetHandle")
        target = edge.get("target")
        if not handle or not target:
            continue
        parsed = parse_fn_node_id(target)
        if parsed is None:
            continue
        index.setdefault((parsed[0], parsed[1], handle), []).append(edge)
    logger.debug(
        "[graph_builder] manual_edge_handle_index: %d edge(s) on %d handle(s)",
        sum(len(v) for v in index.values()),
        len(index),
    )
    return index


def manual_input_overrides(
    fn: str,
    wid: str,
    input_params: dict,
    const_names,
    manual_index: dict[tuple[str, str, str], dict],
    manual_nodes: "dict[str, dict] | None" = None,
    hidden_edge_ids: "set[str] | frozenset[str]" = frozenset(),
) -> dict:
    """``{param: variable_type_or_list}`` — the VISIBLE variable sources of
    every ``in__<param>`` handle of the ``(fn, wid)`` call site that a manual
    variable edge lands on.

    ``wid`` is the **node's token** — the trailing segment of its id, which
    is what ``manual_index`` and the reconstructed edge ids are keyed by. It
    equalled the wiring id until allocated node ids (2026-09-22) and still
    does for every node that has not been rewired; a caller holding a WIRING
    must map it through ``token_for`` first.

    This is the one owner of the rule (docs/claude/manual-edges-on-history-
    nodes.md): **the edges visible on the DAG are the ground truth**, for
    display and for execution, whether they came from history or were drawn
    by the user. A history node's inputs are therefore resolved exactly as a
    fresh node's are (edge_resolver.resolve_function_edges): per handle, the
    variable sources of every visible edge — history types whose edge is
    not hidden, plus the manual edge's variable. One source → bare string;
    several → a list, which is EachOf; a handle with no manual edge is not
    in the result at all (history stands, hidden or not — the hidden-and-
    uncovered case stays "disconnected", see reconcile_manual_inputs).

    That covers, with no special cases: a parameter history never bound
    (added to the signature after the recorded runs — the grSides/
    Demographics ``n/a`` of 2026-09-15), a hidden history edge the user
    reconnected to a different variable (the reconnect flow hidden_wirings /
    variant_resolver already handled on its own), and a manual edge beside a
    still-visible history edge (EachOf of both — the same picture means the
    same thing on a fresh node; replacing is what hiding the edge is for).

    Shared by the display overlay (overlay_manual_inputs) and the execution
    path (variant_resolver.reconcile_manual_inputs) so the panel can never
    show a binding the run would not use, or vice versa.

    ``input_params``: the call site's RECORDED variable inputs, DB shape
    (bare string when single). ``manual_index``: manual_edge_handle_index.
    """
    from scistack_gui.domain.edge_resolver import node_id_to_var_label

    handle_map = inbound_edge_candidates_by_handle(
        fn, wid, input_params, const_names=const_names
    )
    # Visible history sources per handle, in recorded order.
    visible: dict[str, list[str]] = {}
    for param, type_val in input_params.items():
        handle = in_handle(param)
        types = type_val if isinstance(type_val, (list, set, tuple)) else [type_val]
        visible[handle] = [
            vt
            for vt in types
            if vt and f"e__{vt}__{fn}__{wid}" not in hidden_edge_ids
        ]
    hidden_handles = {h for eid, h in handle_map.items() if eid in hidden_edge_ids}

    overrides: dict = {}
    for (ifn, iwid, handle), edges in manual_index.items():
        if ifn != fn or iwid != wid or not handle.startswith(IN_HANDLE_PREFIX):
            continue
        sources = list(visible.get(handle, []))
        drawn = False
        for edge in edges:
            var_label = node_id_to_var_label(
                edge.get("source", ""), {}, manual_nodes or {}
            )
            if not var_label:
                # PathInput / Parameter / glue sources have their own binding
                # rules (edge_resolver); this rule is about variables only.
                continue
            if var_label in sources:
                # Re-drawing an edge history already shows, or the same
                # variable wired twice: nothing new to override
                # (layout_service.put_edge auto-unhides the first case).
                continue
            sources.append(var_label)
            drawn = True
        if drawn:
            # Two manual edges naming two variables on one handle is an
            # EachOf, exactly as history-plus-manual already was. Before
            # 2026-09-22 the index kept only one of them and the second was
            # silently dropped.
            overrides[handle[len(IN_HANDLE_PREFIX) :]] = (
                sources[0] if len(sources) == 1 else sources
            )

    if overrides:
        logger.debug(
            "[graph_builder] manual_input_overrides(%s, %s): %s (recorded=%s, "
            "hidden handles=%s)",
            fn,
            wid,
            overrides,
            input_params,
            sorted(hidden_handles),
        )
    return overrides


def input_params_with_manual_edges(
    fn_input_params: dict[FnKey, dict],
    fn_outputs: dict[FnKey, set],
    fn_constants: dict[FnKey, set],
    path_inputs: dict,
    manual_edges: "list[dict] | tuple",
    token_for,
    manual_nodes: "dict[str, dict] | None" = None,
    hidden_edge_ids: "set[str] | frozenset[str]" = frozenset(),
) -> dict[FnKey, dict]:
    """``fn_input_params`` with manual variable edges folded in — **for run-state
    propagation only**.

    The third consumer of "the edges visible on the DAG are the ground truth"
    (docs/claude/manual-edges-on-history-nodes.md). Display got it
    (``overlay_manual_inputs``) and execution got it
    (``variant_resolver.reconcile_manual_inputs``); COLOUR did not, and colour
    is computed from the recorded wiring ~200 lines before the overlay is
    applied to the built nodes. So a step fed only by an edge the user drew had
    no upstream at all as far as ``propagate_run_states`` could see, and red
    stopped dead at the last history edge: on 2026-09-22 ``loadGaitRiteOneFile``
    was red while ``grSides`` and ``calculateSymmetryOneVector`` downstream of
    it stayed green, because both read their inputs through drawn edges.

    **Never use this for identity.** ``wiring_id`` hashes ``input_params``, so
    folding an override into the dict a node id is derived from would rename the
    node, orphaning its saved position, scope membership and config (the
    placement-id lookup trap). The doc's rule is explicit — node identity does
    not change when an edge is drawn — and it is why this returns a copy that
    goes to ``propagate_run_states`` and nowhere else.

    Works on both run-state passes without a flag: the wiring id is recomputed
    from each entry and then mapped through ``token_for``, which for a
    per-call-site key derives the group it belongs to and for an already-grouped
    key returns that same key's token. Under allocated node ids the second case
    is what needs the mapping rather than the identity: a grouped key's params
    are the UNION of the wirings the node has run as, so recomputing gives the
    node's STATED wiring, and only ``token_for`` knows that this is still the
    same node (``docs/claude/node-identity.md``).
    """
    if not manual_edges:
        return fn_input_params
    manual_index = manual_edge_handle_index(manual_edges)
    if not manual_index:
        return fn_input_params

    pi_by_fkey = path_input_bindings_by_fkey(path_inputs)
    out: dict[FnKey, dict] = {}
    applied: dict[str, dict] = {}
    for fkey, params in fn_input_params.items():
        fn, _ = fkey
        token = token_for(
            fn,
            wiring_id(
                fn, params, fn_outputs.get(fkey, set()), pi_by_fkey.get(fkey, {})
            ),
        )
        overrides = manual_input_overrides(
            fn,
            token,
            params,
            fn_constants.get(fkey, set()),
            manual_index,
            manual_nodes,
            hidden_edge_ids,
        )
        out[fkey] = {**params, **overrides} if overrides else params
        if overrides:
            applied[fn_node_id(fn, token)] = overrides
    if applied:
        logger.info(
            "[graph_builder] run-state propagation follows %d manual edge "
            "binding(s): %s",
            len(applied),
            applied,
        )
    return out


def stated_wiring_claims(
    fn_input_params: dict[FnKey, dict],
    fn_outputs: dict[FnKey, set],
    fn_constants: dict[FnKey, set],
    path_inputs: dict,
    manual_edges: "list[dict] | tuple",
    manual_nodes: "dict[str, dict] | None",
    hidden_edge_ids: "set[str] | frozenset[str]",
    current_by_node: dict,
) -> dict[tuple[str, str], list[str]]:
    """``{(fn_name, stated_wiring): [node_id, ...]}`` — rule 2 of
    ``domain.node_identity``.

    A node's **stated** wiring is its recorded bindings with the edges the
    user has DRAWN folded in: the shape a run of it would record. So this
    answers "which node, if run right now, would produce that wiring?" —
    which is how a wiring appearing in history for the first time is
    recognised as a node that already exists rather than as a new one.

    It is deliberately computed from the stated wiring and not from a repair
    applied afterwards: before the run the node states W2 (the drawn edge is
    part of what it states), the run records W2, same node. The duplicate
    never forms, and it is prevented by the thing the user controls
    (``docs/claude/node-identity.md`` §7a).

    Only nodes that ALREADY EXIST can state anything — ``current_by_node`` is
    ``{node_id: its current wiring}`` straight from ``_node_wiring``, not an
    assignment being built. That is what makes ``resolve_identities`` a single
    pass: nothing minted in a pass can claim anything in the same pass,
    because a node whose id did not exist a moment ago has no edges drawn onto
    it.

    The node's CURRENT wiring is the one its drawn edges are folded into — not
    every shape it has ever run as. A node rewired twice states one wiring,
    the one it would record if run now.

    Pure: computes, never writes. A claim whose stated wiring equals the
    recorded one is dropped, since that is the node's own key and claiming it
    would say nothing.
    """
    if not manual_edges or not current_by_node:
        return {}
    manual_index = manual_edge_handle_index(manual_edges)
    if not manual_index:
        return {}
    # A LIST per wiring, not one node: two nodes can share a current shape
    # (the §7b ambiguity), and collapsing them here would hide it from the
    # resolution that is supposed to report it.
    nodes_of_wiring: dict[str, list[str]] = {}
    for node_id, wiring in sorted(current_by_node.items()):
        nodes_of_wiring.setdefault(wiring, []).append(node_id)

    pi_by_fkey = path_input_bindings_by_fkey(path_inputs)
    claims: dict[tuple[str, str], list[str]] = {}
    for fkey, params in fn_input_params.items():
        fn, _cid = fkey
        recorded = wiring_id(
            fn, params, fn_outputs.get(fkey, set()), pi_by_fkey.get(fkey, {})
        )
        prefix = f"{FN_ID_PREFIX}{fn}__"
        for node_id in nodes_of_wiring.get(recorded, ()):
            # A node id names its function: one that does not start with this
            # prefix runs something else (a wiring reused after a rename).
            if not str(node_id).startswith(prefix):
                continue
            token = str(node_id)[len(prefix) :]
            _claim_stated_wirings(
                claims,
                fn,
                token,
                node_id,
                params,
                recorded,
                fn_outputs.get(fkey, set()),
                fn_constants.get(fkey, set()),
                pi_by_fkey.get(fkey, {}),
                manual_index,
                manual_nodes,
                hidden_edge_ids,
            )
    if claims:
        logger.debug("[graph_builder] stated_wiring_claims: %s", claims)
    return claims


def _claim_stated_wirings(
    claims: dict,
    fn: str,
    token: str,
    node_id: str,
    params: dict,
    recorded: str,
    outputs,
    const_names,
    path_input_bindings: dict,
    manual_index: dict,
    manual_nodes,
    hidden_edge_ids,
) -> None:
    """One node's claims, appended to *claims* in place.

    Split out of :func:`stated_wiring_claims` so the per-node body reads as
    one thing: what does the run of THIS node, with the edges drawn on it,
    record?
    """
    overrides = manual_input_overrides(
        fn,
        token,
        params,
        const_names,
        manual_index,
        manual_nodes,
        hidden_edge_ids,
    )
    if not overrides:
        return
    # provenance records ONE variable type per input, so an EachOf override
    # splits into ONE WIRING PER SOURCE — and the node states every one of
    # them, not just the first. Claiming only the first is how the second
    # source's run would still fork a node: the run writes both shapes and
    # only one of them is spoken for.
    as_list = {
        param: (list(sources) if isinstance(sources, list) else [sources])
        for param, sources in overrides.items()
    }
    base = {**params, **{param: s[0] for param, s in as_list.items()}}
    stated_wirings = {wiring_id(fn, base, outputs, path_input_bindings)}
    # One param varied at a time rather than the full Cartesian product: a
    # drawn edge is per handle, and the product of several EachOf handles is a
    # combinatorial claim over shapes nothing has run.
    for param, sources in as_list.items():
        for source in sources:
            stated_wirings.add(
                wiring_id(
                    fn, {**base, param: source}, outputs, path_input_bindings
                )
            )
    for stated in stated_wirings:
        if stated == recorded:
            # The node's own key. Claiming it would say nothing.
            continue
        holders = claims.setdefault((fn, stated), [])
        if str(node_id) not in holders:
            holders.append(str(node_id))


def collect_manual_input_overrides(
    nodes: list[dict],
    fn_input_params: dict[FnKey, dict],
    fn_constants: dict[FnKey, set],
    manual_edges: "list[dict] | tuple",
    manual_nodes: "dict[str, dict] | None",
    hidden_edge_ids: "set[str] | frozenset[str]" = frozenset(),
) -> dict[str, dict[str, str]]:
    """``{node_id: {param: variable_label}}`` — manual_input_overrides
    evaluated for every wiring-grouped function node in *nodes*.

    Node ids at this point are the bare ``fn__{fn}__{wid}`` (build_function_
    nodes runs before scope resolution); manual edge targets may carry a
    ``::scope`` suffix, which manual_edge_handle_index strips.
    """
    manual_index = manual_edge_handle_index(manual_edges)
    if not manual_index:
        return {}
    result: dict[str, dict[str, str]] = {}
    for node in nodes:
        if node.get("type") != "functionNode":
            continue
        parsed = parse_fn_node_id(node["id"])
        if parsed is None:
            continue
        fn, wid = parsed
        fkey: FnKey = (fn, wid)
        overrides = manual_input_overrides(
            fn,
            wid,
            fn_input_params.get(fkey, {}),
            fn_constants.get(fkey, set()),
            manual_index,
            manual_nodes,
            hidden_edge_ids,
        )
        if overrides:
            result[node["id"]] = overrides
    return result


def overlay_manual_inputs(nodes: list[dict], overrides_by_node: dict[str, dict]) -> int:
    """Apply manual input overrides to the built function nodes' data:
    ``input_params[param]`` becomes the handle's visible source (what the
    handles, the Inputs column picker and the code exporter read) and
    ``manual_inputs`` carries the full override per param, so a consumer —
    or a log reader — can tell an overlay from a recorded binding. Returns
    the number of nodes touched.

    ``input_params`` stays ``{param: str}``: every frontend consumer types it
    that way, and DB-derived values are always bare (provenance records one
    variable type per input). An EachOf override (list) therefore shows its
    FIRST source there — the same choice the fresh-node path makes
    (``api/pipeline.py``: ``inferred_inputs = {p: ts[0]}``) — with the whole
    list in ``manual_inputs``. The Inputs column picker is per parameter, so
    one column set applies to every source of an EachOf handle either way
    (docs/claude/column-selection.md, Known limitations).

    One DEBUG line per touched node lists every parameter with its origin,
    because "why does this handle say what it says" is exactly the question
    that was unanswerable from scidb.log before (2026-09-15).
    """
    touched = 0
    if not overrides_by_node:
        return touched
    for node in nodes:
        overrides = overrides_by_node.get(node["id"])
        if not overrides:
            continue
        data = node["data"]
        params = dict(data.get("input_params") or {})
        for param, sources in overrides.items():
            params[param] = sources[0] if isinstance(sources, list) else sources
        data["input_params"] = params
        data["manual_inputs"] = {
            p: (list(s) if isinstance(s, list) else s) for p, s in overrides.items()
        }
        touched += 1
        origins = {
            p: ("manual" if p in overrides else ("history" if t else "unbound"))
            for p, t in params.items()
        }
        logger.debug(
            "[graph_builder] %s input_params after manual overlay: %s (origins=%s, "
            "manual_inputs=%s, constants=%s)",
            node["id"],
            params,
            origins,
            data["manual_inputs"],
            data.get("constant_params"),
        )
    return touched


def hidden_wirings(
    fn_input_params: dict[FnKey, dict],
    fn_outputs: dict[FnKey, set],
    fn_constants: dict[FnKey, set],
    path_inputs: dict[str, dict],
    hidden_edge_ids: set[str],
    token_for,
    manual_edges: "list[dict] | tuple" = (),
) -> set[tuple[str, str]]:
    """(fn_name, node token) pairs with at least one hidden inbound edge that
    is NOT currently covered by a manual reconnect.

    The second element is the NODE TOKEN, not the wiring — the two coincide
    unless the node was rewired and run (``identity_token`` /
    ``docs/claude/node-identity.md``). It has to be the token, because both
    things compared here are keyed by the node id: the reconstructed edge ids
    (``build_edges`` builds them from the post-grouping node id) and the
    manual-edge index (keyed by each edge's ``target``).

    Reconstructs each call site's candidate inbound edge ids the same way
    build_edges does (without needing edges to already exist) and checks
    them against ``hidden_edge_ids``. Works on the PRE-GROUPING agg (raw
    per-call-site FnKeys) — every call site sharing a wiring recomputes
    the same wiring_id, so the result is correct regardless of grouping.

    A hidden inbound edge only keeps its wiring "disconnected" if the
    target_handle it fed still has no manual edge wired onto it — the user
    reconnecting a DIFFERENT variable to the same handle (not the same
    source, which layout_service.put_edge already auto-unhides) must clear
    the disconnected state too. A wiring with MULTIPLE hidden handles stays
    disconnected until every one of them is covered (partial reconnection
    doesn't make it runnable) — see manual_edge_handle_index.
    """
    if not hidden_edge_ids:
        return set()
    manual_index = manual_edge_handle_index(manual_edges)
    pi_by_fkey = path_input_bindings_by_fkey(path_inputs)
    result: set[tuple[str, str]] = set()
    for fkey, params in fn_input_params.items():
        fn, _cid = fkey
        wid = token_for(
            fn,
            wiring_id(fn, params, fn_outputs.get(fkey, set()), pi_by_fkey.get(fkey, {})),
        )
        handle_map = inbound_edge_candidates_by_handle(
            fn, wid, params, const_names=fn_constants.get(fkey, set())
        )
        hidden_handles = {h for cid_, h in handle_map.items() if cid_ in hidden_edge_ids}
        if not hidden_handles:
            continue
        uncovered = [h for h in hidden_handles if (fn, wid, h) not in manual_index]
        if uncovered:
            result.add((fn, wid))
        else:
            logger.info(
                "[graph_builder] wiring (%s, %s) reconnected via manual edge(s) "
                "covering %s — clearing disconnected state",
                fn,
                wid,
                sorted(hidden_handles),
            )
    for pi_name, pi in path_inputs.items():
        for fkey, param_name in pi["functions"]:
            fn, _cid = fkey
            wid = token_for(
                fn,
                wiring_id(
                    fn,
                    fn_input_params.get(fkey, {}),
                    fn_outputs.get(fkey, set()),
                    pi_by_fkey.get(fkey, {}),
                ),
            )
            handle = in_handle(param_name)
            if f"e__{pi_name}__{param_name}__{fn}__{wid}" not in hidden_edge_ids:
                continue
            if (fn, wid, handle) in manual_index:
                logger.info(
                    "[graph_builder] wiring (%s, %s) pathInput '%s' reconnected via "
                    "manual edge — clearing disconnected state",
                    fn,
                    wid,
                    param_name,
                )
                continue
            result.add((fn, wid))
    if result:
        logger.info("[graph_builder] hidden_wirings: %s", sorted(result))
    return result


def wiring_disconnected_fkeys(
    fn_input_params: dict[FnKey, dict],
    fn_outputs: dict[FnKey, set],
    wirings: set[tuple[str, str]],
    path_inputs: dict[str, dict],
    token_for,
) -> set[FnKey]:
    """Map a (fn_name, node token) set back to raw pre-grouping call-site
    FnKeys — for feeding domain.run_state.propagate_run_states, which
    still operates per real call site at the point it runs.

    ``token_for`` must be the SAME one ``hidden_wirings`` was given, or the
    two sides of the comparison are keyed differently and every call site
    reads as connected.
    """
    if not wirings:
        return set()
    pi_by_fkey = path_input_bindings_by_fkey(path_inputs)
    result: set[FnKey] = set()
    for fkey, params in fn_input_params.items():
        fn, _cid = fkey
        wid = token_for(
            fn,
            wiring_id(fn, params, fn_outputs.get(fkey, set()), pi_by_fkey.get(fkey, {})),
        )
        if (fn, wid) in wirings:
            result.add(fkey)
    return result


def wirings_downstream_of(
    fn_input_params: dict[FnKey, dict],
    fn_outputs: dict[FnKey, set],
    seed_wirings: set[tuple[str, str]],
    path_inputs: dict[str, dict],
    token_for,
) -> set[tuple[str, str]]:
    """Every wiring that transitively consumes a seed wiring's output —
    used to report which OTHER functions become un-runnable as a
    consequence of a disconnected wiring (starved of an input) without
    being directly disconnected themselves. Returns only the downstream
    wirings, never the seeds (callers already have those)."""
    if not seed_wirings:
        return set()
    pi_by_fkey = path_input_bindings_by_fkey(path_inputs)
    wiring_outputs: dict[tuple[str, str], set] = {}
    wiring_inputs: dict[tuple[str, str], set] = {}
    for fkey, params in fn_input_params.items():
        fn, _cid = fkey
        wid = token_for(
            fn,
            wiring_id(fn, params, fn_outputs.get(fkey, set()), pi_by_fkey.get(fkey, {})),
        )
        wiring_inputs.setdefault((fn, wid), set()).update(params.values())
        wiring_outputs.setdefault((fn, wid), set()).update(fn_outputs.get(fkey, set()))

    affected = set(seed_wirings)
    frontier = set(seed_wirings)
    while frontier:
        produced: set = set()
        for w in frontier:
            produced |= wiring_outputs.get(w, set())
        next_frontier = set()
        for w, inputs in wiring_inputs.items():
            if w in affected:
                continue
            if inputs & produced:
                affected.add(w)
                next_frontier.add(w)
        frontier = next_frontier
    return affected - seed_wirings


def candidate_edge_id(
    source_id: str, target_id: str, target_handle: "str | None" = None
) -> str | None:
    """The deterministic DB-derived edge id a (source, target) node-id pair
    WOULD have in build_edges' output, without needing the edge to exist.

    Used to detect "the user just dragged a connection that recreates a
    previously-hidden DB-derived edge" (see layout_service.put_edge) —
    reconnecting the exact same nodes should unhide the original edge
    rather than create a redundant manual one. Returns None for pairs that
    aren't a recognized DB-derived category (a genuinely new connection).
    Both ids may be placement-qualified; only the bare ids matter here.

    ``target_handle`` is required for a ``pathInput__`` source: unlike
    var/const edges, a PathInput's declared name and the function parameter
    it fills can differ (see docs/claude/code-discovery-categories.md), so
    build_edges' real pathInput→fn edge id now encodes BOTH — without the
    handle there's no way to recover the parameter name, so this returns
    None (a safe degrade: the reconnect just creates a fresh manual edge
    instead of auto-unhiding).
    """
    src = strip_placement(source_id)
    tgt = strip_placement(target_id)
    if src.startswith(PATH_INPUT_ID_PREFIX):
        parsed = parse_fn_node_id(tgt)
        if parsed is None or not target_handle or not target_handle.startswith(IN_HANDLE_PREFIX):
            return None
        fn, wid = parsed
        pi_name = src.split("__", 1)[1]
        param_name = target_handle[len(IN_HANDLE_PREFIX) :]
        return f"e__{pi_name}__{param_name}__{fn}__{wid}"
    if src.startswith((VAR_ID_PREFIX, PARAM_ID_PREFIX)):
        parsed = parse_fn_node_id(tgt)
        if parsed is None:
            return None
        fn, wid = parsed
        x = src.split("__", 1)[1]
        return f"e__{x}__{fn}__{wid}"
    if tgt.startswith(VAR_ID_PREFIX):
        parsed = parse_fn_node_id(src)
        if parsed is None:
            return None
        fn, wid = parsed
        out_type = tgt[len(VAR_ID_PREFIX) :]
        return f"e__{fn}__{wid}__{out_type}"
    return None


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------


def find_cycle(
    edges: list[dict], new_source: str, new_target: str
) -> list[str] | None:
    """Would adding an edge new_source -> new_target close a cycle?

    ``edges`` is the FULL current graph for one scope (DB-derived + manual,
    as returned by services.pipeline_service.get_pipeline_graph) — checking
    manual edges alone would miss a cycle closed through existing
    DB-derived data-lineage edges. A self-loop (new_source == new_target)
    is always a cycle.

    Returns the cycle path new_source -> new_target -> ... -> new_source if
    adding the edge would create one, else None.
    """
    if new_source == new_target:
        return [new_source, new_target]

    adjacency: dict[str, list[str]] = defaultdict(list)
    for e in edges:
        adjacency[e["source"]].append(e["target"])

    # BFS forward from new_target: if new_source is reachable, the new edge
    # would close a loop back to itself.
    parent: dict[str, str] = {}
    frontier = deque([new_target])
    seen = {new_target}
    while frontier:
        current = frontier.popleft()
        if current == new_source:
            path = [current]
            while current != new_target:
                current = parent[current]
                path.append(current)
            path.reverse()
            return [new_source] + path
        for nxt in adjacency[current]:
            if nxt not in seen:
                seen.add(nxt)
                parent[nxt] = current
                frontier.append(nxt)
    return None


#: Every key FunctionSettingsPanel.updateNodeData persists. Rehydration has to
#: cover the same set the panel writes, or a setting saves successfully and
#: comes back missing -- which reads to the user as "it didn't save".
_SAVED_CONFIG_KEYS = (
    "schemaSelection",
    "schemaLevel",
    "whereFilters",
    "runOptions",
    # Per-parameter column picks — {param: {"columns": [...], "iterate": bool}}.
    # See docs/claude/column-selection.md §From the GUI. Omitting a key the
    # panel writes is the 2026-09-14 snap-back bug in miniature: it saves
    # fine, is never read back, and the control visibly reverts on the next
    # dag_updated refetch.
    "columnSelections",
)


def _apply_saved_config(node_data: dict, config: dict | None) -> None:
    """Copy a node's saved settings onto its React Flow ``data``."""
    if not config:
        return
    for key in _SAVED_CONFIG_KEYS:
        if key in config:
            node_data[key] = config[key]


def apply_placement_configs(
    nodes: list[dict], node_configs: dict[str, dict] | None
) -> int:
    """Rehydrate config saved under a node's FINAL (scope-resolved) id.

    ``build_function_nodes`` runs before ``scope_filter.resolve_scope_view``
    and so can only look a config up by the bare canonical id
    (``fn__{fn}__{cid}``). But the id the frontend saves under -- and the id
    ``execution_service._scope_function_node_ids`` runs under -- is the
    placement-qualified one (``fn__{fn}__{cid}::{scope}``) whenever the node
    has a qualified placement. Without this pass, a setting saved for such a
    node is written fine and simply never read back: every ``dag_updated``
    refetch hands the canvas a node with no ``schemaLevel``/``runOptions``
    and the checkbox visibly snaps back (2026-09-14, loadGaitRiteOneFile).

    Call it AFTER scope resolution, on the resolved node list. The
    per-placement config overrides whatever the bare-id pass already put on
    ``data``; a node without a qualified config keeps the bare-id result.
    Returns the number of nodes a qualified config was applied to.

    Also warns about ORPHAN configs -- rows whose node id matches no node in
    the resolved graph -- because that is exactly the signature of a fourth
    id shape nobody thought of, and in the 2026-09-14 case a single WARN
    here would have named the mismatch directly in scidb.log instead of it
    hiding behind a "checkbox flickers" report.
    See docs/claude/placement-qualified-ids.md.
    """
    if not node_configs:
        return 0
    graph_ids = {n["id"] for n in nodes}
    bare_ids = {strip_placement(nid) for nid in graph_ids}
    applied = 0
    for n in nodes:
        if n.get("type") != "functionNode":
            continue
        if parse_placement_id(n["id"]) is None:
            continue  # bare id: build_function_nodes already looked it up
        cfg = node_configs.get(n["id"])
        if cfg:
            _apply_saved_config(n["data"], cfg)
            applied += 1
            logger.debug(
                "[graph_builder] applied placement-qualified config for %s: keys=%s",
                n["id"],
                sorted(cfg),
            )
    orphans = sorted(
        nid
        for nid in node_configs
        if nid not in graph_ids and strip_placement(nid) not in bare_ids
    )
    if orphans:
        # Not necessarily wrong: a config may belong to a node in another
        # scope, or to a wiring that has since been hidden. But if the node
        # the user is toggling is in this list, the toggle can never stick.
        # A bare id that parses as fn__{fn}__{something-not-a-wiring-hash} is
        # a FRESH node's id: before 2026-09-15 graduation left its config
        # behind (pipeline_store.migrate_node_config now moves it), so these
        # are settings from an older session that must be re-applied by hand
        # on the graduated node.
        stale_fresh = [
            nid
            for nid in orphans
            if strip_placement(nid).startswith(FN_ID_PREFIX)
            and parse_fn_node_id(nid) is None
            and strip_placement(nid).count("__") >= 2
        ]
        logger.warning(
            "[graph_builder] %d saved node config(s) match no node in the "
            "resolved graph (by exact or bare id): %s -- a setting saved "
            "under one of these ids will not rehydrate%s",
            len(orphans),
            orphans,
            (
                f". {len(stale_fresh)} of them ({stale_fresh}) are fresh-node ids "
                "whose node graduated before config migration existed; "
                "re-apply those settings on the graduated node"
            )
            if stale_fresh
            else "",
        )
    return applied


def build_manual_node(
    node_id: str,
    meta: dict,
    pending_constants: dict[str, set[str]],
    manual_fn_state: str | None,
    resolved_input_params: dict[str, str] | None,
    resolved_output_types: list[str] | None,
    matlab_functions: set[str],
    node_config: dict | None = None,
) -> dict:
    """Build a single manual node dict.

    Args:
        node_id: The manual node ID.
        meta: {"type": ..., "label": ..., "config": ...} from pipeline_store.
        pending_constants: {const_name: {pending_values}}.
        manual_fn_state: Pre-computed run state for function nodes (or None).
        resolved_input_params: Pre-resolved {param: var_type} for function nodes.
        resolved_output_types: Pre-resolved output types for function nodes.
        matlab_functions: Set of MATLAB function names.
        node_config: This node's entry from the _node_config table, if any.
            Takes precedence over the legacy ``meta["config"]`` column.
    """
    fn_label = meta["label"]
    extra: dict = {}

    if meta["type"] == "variableNode":
        extra = {"total_records": 0, "run_state": "red"}
    elif meta["type"] == "parameterNode":
        pending_vals = [
            {"value": pval, "record_count": 0}
            for pval in sorted(pending_constants.get(fn_label, set()))
        ]
        extra = {"values": pending_vals}
    elif meta["type"] == "pathInputNode":
        extra = {"template": "", "root_folder": None, "alternate_templates": []}
    elif meta["type"] == "glueNode":
        # A glue node is a functionNode VARIANT: same in__{param} / out__
        # handle contract, so edges resolve with no new branch. What it does
        # NOT have is a run state — it is transient by construction, so a
        # badge would describe nothing and a red one would be a lie (D5).
        extra = {
            "input_params": resolved_input_params or {},
            "output_types": [],
            "per_schema_key": False,
        }
        if fn_label in matlab_functions:
            extra["language"] = "matlab"
    elif meta["type"] == "functionNode":
        extra = {
            "input_params": resolved_input_params or {},
            "output_types": list(resolved_output_types or []),
            "constant_params": [],
            "run_state": manual_fn_state or "red",
        }
        if fn_label in matlab_functions:
            extra["language"] = "matlab"

    node_data: dict = {"label": fn_label, **extra}
    if meta["type"] == "functionNode":
        # _node_config is where the panel writes now; meta["config"] is the
        # legacy _pipeline_nodes column, kept as the fallback so settings saved
        # before the split survive. See docs/claude/gui-run-options-flow.md.
        _apply_saved_config(node_data, node_config or meta.get("config"))

    return {
        "id": node_id,
        "type": meta["type"],
        "position": {"x": 0, "y": 0},
        "data": node_data,
    }


def merge_manual_nodes(
    existing_nodes: list[dict],
    manual_nodes: dict[str, dict],
    saved_positions: dict[str, dict],
) -> tuple[list[str], list[GraduationAction]]:
    """Determine which manual nodes to add and which to graduate.

    A manual function node graduates to its DB-derived counterpart only
    when there is exactly one DB node with the same (type, label).  If the
    same function name has multiple DB nodes (one per for_each call site),
    we cannot pick a canonical target unambiguously, so the manual node is
    kept as a separate node — the user can wire it up and run it to
    produce a real call site of its own.

    Graduation targets the manual node's OWN placement
    (``placement_id(canonical_id, meta["pipeline_id"])``), not the bare
    canonical id — this is what lets two manual nodes with the same label
    in DIFFERENT scopes (e.g. a duplicated pipeline re-running identical,
    unedited wiring) graduate independently instead of racing for one
    shared slot and stealing it from each other (the root cause fixed by
    this rework — see plan-placement-qualified-node-ids.md).

    Returns:
        Tuple of:
        - List of manual node IDs that should be added to the graph.
        - List of GraduationAction objects (side-effects for the service layer).
    """
    logger.info(
        "[graph_builder] merge_manual_nodes: processing %d manual node(s) against %d existing node(s)",
        len(manual_nodes),
        len(existing_nodes),
    )

    existing_ids = {n["id"] for n in existing_nodes}
    db_nodes_by_label: dict[tuple, list[str]] = {}
    for n in existing_nodes:
        key = (n["type"], n["data"]["label"])
        db_nodes_by_label.setdefault(key, []).append(n["id"])

    to_add: list[str] = []
    graduations: list[GraduationAction] = []

    for node_id, meta in manual_nodes.items():
        if node_id in existing_ids:
            continue
        key = (meta["type"], meta["label"])
        candidates = db_nodes_by_label.get(key, [])
        if len(candidates) == 1:
            canonical_id = candidates[0]
            target_id = placement_id(
                canonical_id, meta.get("pipeline_id") or ROOT_SCOPE
            )
            if target_id not in saved_positions:
                graduations.append(
                    GraduationAction(old_id=node_id, new_id=target_id)
                )
                continue
        elif len(candidates) > 1:
            logger.debug(
                "merge_manual_nodes: not graduating %s — %d DB nodes share label %r "
                "(multiple call sites)",
                node_id,
                len(candidates),
                meta["label"],
            )
        to_add.append(node_id)

    logger.info(
        "[graph_builder] merge_manual_nodes complete: %d to add, %d to graduate",
        len(to_add),
        len(graduations),
    )
    if graduations:
        logger.debug(
            "[graph_builder] graduations: %s",
            [(g.old_id, g.new_id) for g in graduations],
        )
    return to_add, graduations


# ---------------------------------------------------------------------------
# Intent the last run did not use
# ---------------------------------------------------------------------------

#: Why a stated selection is not what the data reflects.
UNUSED_SCRIPT_RUN = "script_run"  # the last run read source only (rule 3)
UNUSED_CHANGED_SINCE = "changed_since"  # stated after the last run, not yet run
UNUSED_NEVER_RUN = "never_run"  # no run recorded for this function at all


def mark_unused_intent(nodes: list[dict], latest_runs: dict) -> int:
    """Stamp ``unusedIntent`` on function nodes whose saved column selection
    is not what their most recent run actually bound.

    Decision A (``docs/claude/intent-and-fact.md`` §7): a script run ignores
    GUI intent. The canvas can therefore show a statement the last run never
    used, and that must not be silent — a chip describing a selection the
    data does not reflect is this whole model's bug class wearing a
    different hat. *latest_runs* is ``provenance_query.latest_runs``:
    ``{fn_name: {"origin", "selectors", ...}}``, batched by function so a
    render asks once.

    ``unusedIntent`` is ``{param: {"stated", "recorded", "origin", "reason"}}``
    — the two facts and why they differ, in the same spirit as the
    ``[selector-dropped]`` log line. Nothing is marked for a run older than
    the ``origin`` column (``origin`` is ``None``): "unknown" must never be
    reported as "ignored".

    Pure: no I/O. Returns the number of nodes marked.
    """
    from scidb.intent import ORIGIN_SCRIPT, normalize_columns, same_columns

    marked = 0
    for n in nodes:
        if n.get("type") != "functionNode":
            continue
        selections = (n.get("data") or {}).get("columnSelections") or {}
        if not selections:
            continue
        fn_name = n["data"].get("label")
        last = latest_runs.get(fn_name)
        unused: dict = {}
        for param, raw in selections.items():
            stated = normalize_columns(raw)
            if stated is None:
                continue
            if last is None:
                unused[param] = {
                    "stated": stated,
                    "recorded": None,
                    "origin": None,
                    "reason": UNUSED_NEVER_RUN,
                }
                continue
            recorded = (last.get("selectors") or {}).get(param)
            if same_columns(stated, recorded):
                continue
            origin = last.get("origin")
            if origin is None:
                continue  # older than the column: unknown, not ignored
            unused[param] = {
                "stated": stated,
                "recorded": recorded,
                "origin": origin,
                "reason": UNUSED_SCRIPT_RUN
                if origin == ORIGIN_SCRIPT
                else UNUSED_CHANGED_SINCE,
            }
        if unused:
            n["data"]["unusedIntent"] = unused
            marked += 1
            logger.info(
                "[graph_builder] %s: saved column selection not reflected by its "
                "last run — %s",
                n["id"],
                "; ".join(
                    f"{p}: stated {u['stated']}, last run ({u['origin'] or 'none'}) "
                    f"bound {u['recorded'] or 'whole variable'} [{u['reason']}]"
                    for p, u in unused.items()
                ),
            )
    return marked
