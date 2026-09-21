"""
Nested-pipeline scopes and hypotheses — the handler table for both
transports (``api/handlers.py``; plan-gui-nested-pipelines.md Part A).

    GET    /api/pipelines                      list_pipelines — VISIBLE scopes + use edges
    POST   /api/pipelines                      create_pipeline
    PUT    /api/pipelines/{pipeline_id}        rename_pipeline (root included)
    DELETE /api/pipelines/{pipeline_id}        delete_pipeline — hide (guards apply;
                                               never deletes data — pipeline_store.hide_pipeline)
    POST   /api/pipelines/{pipeline_id}/unhide unhide_pipeline
    GET    /api/pipelines/hidden               get_hidden_pipelines (restore panel)
    GET    /api/pipelines/{pipeline_id}/interface    get_pipeline_interface — the scope's ports
    GET    /api/pipelines/{pipeline_id}/hidden-ports get_hidden_ports
    POST   /api/pipelines/{pipeline_id}/hide-port    hide_port
    POST   /api/pipelines/{pipeline_id}/unhide-port  unhide_port
    POST   /api/pipelines/{pipeline_id}/extract      extract_to_submodule — a move, not a copy
    POST   /api/pipelines/{pipeline_id}/duplicate    duplicate_pipeline — fork its own nodes
    POST   /api/pipelines/{pipeline_id}/paste-nodes  paste_nodes
    GET    /api/pipelines/{pipeline_id}/export       export_pipeline
    GET    /api/pipelines/{pipeline_id}/export-code  export_pipeline_code
    POST   /api/pipelines/import                     import_pipeline
    GET    /api/pipelines/{pipeline_id}/plan         get_pipeline_plan
    POST   /api/pipelines/{pipeline_id}/run          start_pipeline_run
    POST   /api/pipelines/{pipeline_id}/uses         add_pipeline_use
    PUT    /api/pipeline-uses/{use_id}/binding       update_use_binding
    DELETE /api/pipeline-uses/{use_id}               remove_pipeline_use
    GET    /api/hypotheses                     list_hypotheses (tabs)
    POST   /api/hypotheses                     create_hypothesis
    PUT    /api/hypotheses/{pipeline_id}       update_hypothesis (research question,
                                               statement, evidence)
    DELETE /api/hypotheses/{pipeline_id}       delete_hypothesis — hide
    POST   /api/hypotheses/{pipeline_id}/duplicate  duplicate_hypothesis (new tab)

Store-level ValueErrors (cycles, last-visible-pipeline guard, duplicate
names, unknown ids, binding-key whitelist) map to HTTP 400 with the store's
message; the RPC transport reports them through its one error frame.
"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.services import scope_service

logger = logging.getLogger(__name__)

router = APIRouter()


class PipelineRef(BaseModel):
    pipeline_id: str


class PipelineCreate(BaseModel):
    name: str


class PipelineRename(BaseModel):
    pipeline_id: str
    name: str


class HypothesisUpdate(BaseModel):
    pipeline_id: str
    research_question: str | None = None
    hypothesis_statement: str | None = None
    evidence_for: list | None = None
    evidence_against: list | None = None


class ExtractToSubmodule(BaseModel):
    pipeline_id: str
    node_ids: list[str]
    name: str


class PasteNodes(BaseModel):
    pipeline_id: str
    source_pipeline_id: str
    node_ids: list[str]
    x: float | None = 0.0
    y: float | None = 0.0


class ImportPipeline(BaseModel):
    document: dict


class PortDirection(BaseModel):
    pipeline_id: str
    direction: str  # 'input' | 'output'
    var_type: str


class PipelinePlanQuery(BaseModel):
    pipeline_id: str
    target: str | None = ""


class PipelineRunRequest(BaseModel):
    pipeline_id: str
    mode: str | None = "all"  # all | until | endpoints
    target: str | None = ""  # step/fn name (mode="until")
    finalized: bool | None = None  # endpoint draft/record flag
    skip_computed: bool | None = True
    run_id: str | None = None


class UseCreate(BaseModel):
    parent_pipeline_id: str
    child_pipeline_id: str
    binding: dict | None = None
    x: float | None = 0.0
    y: float | None = 0.0


class BindingUpdate(BaseModel):
    use_id: str
    binding: dict


class UseRef(BaseModel):
    use_id: str


# --- the calls ---------------------------------------------------------------


def _list_pipelines() -> dict:
    return scope_service.list_pipelines()


def _create_pipeline(req: PipelineCreate) -> dict:
    return scope_service.create_pipeline(req.name)


def _rename_pipeline(req: PipelineRename) -> dict:
    return scope_service.rename_pipeline(req.pipeline_id, req.name)


def _delete_pipeline(req: PipelineRef) -> dict:
    """Hides the pipeline (never deletes data) — see scope_service.hide_pipeline."""
    return scope_service.hide_pipeline(req.pipeline_id)


def _unhide_pipeline(req: PipelineRef) -> dict:
    return scope_service.unhide_pipeline(req.pipeline_id)


def _get_hidden_pipelines() -> dict:
    return scope_service.list_hidden_pipelines()


def _list_hypotheses() -> dict:
    return scope_service.list_hypotheses()


def _create_hypothesis(req: PipelineCreate) -> dict:
    return scope_service.create_hypothesis(req.name)


def _update_hypothesis(req: HypothesisUpdate) -> dict:
    return scope_service.update_hypothesis(
        req.pipeline_id,
        research_question=req.research_question,
        hypothesis_statement=req.hypothesis_statement,
        evidence_for=req.evidence_for,
        evidence_against=req.evidence_against,
    )


def _delete_hypothesis(req: PipelineRef) -> dict:
    """Hides the hypothesis (never deletes data) — see scope_service.hide_hypothesis."""
    return scope_service.hide_hypothesis(req.pipeline_id)


def _get_pipeline_interface(req: PipelineRef) -> dict:
    return scope_service.pipeline_interface(req.pipeline_id)


def _get_hidden_ports(req: PipelineRef) -> dict:
    """This scope's hidden ports — the right-click context menu's Show/
    Hide label needs current state (to-do #9)."""
    return scope_service.get_hidden_ports(req.pipeline_id)


def _hide_port(req: PortDirection) -> dict:
    """Suppress ``var_type``'s exposed port on this scope's interface —
    toggled by right-clicking a variable node inside the subpipeline's
    own canvas (see domain.scope_filter.document_interface)."""
    return scope_service.hide_port(req.pipeline_id, req.direction, req.var_type)


def _unhide_port(req: PortDirection) -> dict:
    return scope_service.unhide_port(req.pipeline_id, req.direction, req.var_type)


def _extract_to_submodule(req: ExtractToSubmodule) -> dict:
    return scope_service.extract_to_submodule(req.pipeline_id, req.node_ids, req.name)


def _duplicate_pipeline(req: PipelineRename) -> dict:
    return scope_service.duplicate_pipeline(req.pipeline_id, req.name)


def _duplicate_hypothesis(req: PipelineRename) -> dict:
    return scope_service.duplicate_hypothesis(req.pipeline_id, req.name)


def _paste_nodes(req: PasteNodes) -> dict:
    """Copy/paste a node selection into this scope (to-do #5) — may be the
    same scope as ``source_pipeline_id`` (duplicate in place) or a
    different one (paste between hypotheses)."""
    return scope_service.paste_nodes(
        req.source_pipeline_id, req.node_ids, req.pipeline_id, req.x or 0.0, req.y or 0.0
    )


def _export_pipeline(req: PipelineRef) -> dict:
    """Portable document for the pipeline + everything it uses (to-do #7)
    — writes to exports/ and returns {"path", "document"}."""
    return scope_service.export_pipeline(req.pipeline_id)


def _export_pipeline_code(req: PipelineRef) -> dict:
    """Standalone Python/MATLAB script for the pipeline + everything it
    uses (to-do #6) — writes to exports/ and returns {"path", "language",
    "script", "warnings"}. 400 for a mixed-language closure (unsupported)."""
    return scope_service.export_pipeline_code(req.pipeline_id)


def _import_pipeline(req: ImportPipeline) -> dict:
    """Recreate an exported document with fresh ids (to-do #7)."""
    return scope_service.import_pipeline(req.document)


def _get_pipeline_plan(db, req: PipelinePlanQuery) -> list:
    """The plan-preview dialog's data (R2): compile the document scope to a
    backend pipeline and dry-run plan it — nothing executes."""
    from scistack_gui.services.execution_service import plan_pipeline

    return plan_pipeline(db, req.pipeline_id, req.target or "")


def _start_pipeline_run(req: PipelineRunRequest, *, transport: str) -> dict:
    """Execute the scope through the backend verbs in a background run
    thread (same relay/cancel machinery as per-node runs).

    The extension's dagPanel.ts is a privileged host that can itself
    generate + dispatch a MATLAB script to the MathWorks terminal (Stage 2);
    the browser has no such host and gets the standalone sidecar instead
    (Stage 3) — see start_pipeline_run's docstring. That is the one thing
    the transport decides.
    """
    from scistack_gui.api.run import start_pipeline_run

    return start_pipeline_run(
        req.pipeline_id,
        req.mode or "all",
        req.target or "",
        req.finalized,
        True if req.skip_computed is None else req.skip_computed,
        req.run_id,
        host_can_dispatch_matlab=(transport == "rpc"),
    )


def _add_pipeline_use(req: UseCreate) -> dict:
    return scope_service.add_pipeline_use(
        req.parent_pipeline_id, req.child_pipeline_id, req.binding, req.x or 0.0, req.y or 0.0
    )


def _update_use_binding(req: BindingUpdate) -> dict:
    return scope_service.update_use_binding(req.use_id, req.binding)


def _remove_pipeline_use(req: UseRef) -> dict:
    return scope_service.remove_pipeline_use(req.use_id)


_BAD_REQUEST = {ValueError: 400}
_NO_DB = {"needs_db": False}

SCOPE_HANDLERS: tuple[Handler, ...] = (
    Handler("list_pipelines", "/pipelines", None, _list_pipelines, http_method="GET", **_NO_DB),
    Handler("create_pipeline", "/pipelines", PipelineCreate, _create_pipeline, http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("rename_pipeline", "/pipelines/{pipeline_id}", PipelineRename, _rename_pipeline, http_method="PUT", http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("delete_pipeline", "/pipelines/{pipeline_id}", PipelineRef, _delete_pipeline, http_method="DELETE", http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("unhide_pipeline", "/pipelines/{pipeline_id}/unhide", PipelineRef, _unhide_pipeline, http_errors=_BAD_REQUEST, body=False, **_NO_DB),
    Handler("get_hidden_pipelines", "/pipelines/hidden", None, _get_hidden_pipelines, http_method="GET", **_NO_DB),
    Handler("list_hypotheses", "/hypotheses", None, _list_hypotheses, http_method="GET", **_NO_DB),
    Handler("create_hypothesis", "/hypotheses", PipelineCreate, _create_hypothesis, http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("update_hypothesis", "/hypotheses/{pipeline_id}", HypothesisUpdate, _update_hypothesis, http_method="PUT", http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("delete_hypothesis", "/hypotheses/{pipeline_id}", PipelineRef, _delete_hypothesis, http_method="DELETE", http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("get_pipeline_interface", "/pipelines/{pipeline_id}/interface", PipelineRef, _get_pipeline_interface, http_method="GET", **_NO_DB),
    Handler("get_hidden_ports", "/pipelines/{pipeline_id}/hidden-ports", PipelineRef, _get_hidden_ports, http_method="GET", **_NO_DB),
    Handler("hide_port", "/pipelines/{pipeline_id}/hide-port", PortDirection, _hide_port, **_NO_DB),
    Handler("unhide_port", "/pipelines/{pipeline_id}/unhide-port", PortDirection, _unhide_port, **_NO_DB),
    Handler("extract_to_submodule", "/pipelines/{pipeline_id}/extract", ExtractToSubmodule, _extract_to_submodule, http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("duplicate_pipeline", "/pipelines/{pipeline_id}/duplicate", PipelineRename, _duplicate_pipeline, http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("duplicate_hypothesis", "/hypotheses/{pipeline_id}/duplicate", PipelineRename, _duplicate_hypothesis, http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("paste_nodes", "/pipelines/{pipeline_id}/paste-nodes", PasteNodes, _paste_nodes, http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("export_pipeline", "/pipelines/{pipeline_id}/export", PipelineRef, _export_pipeline, http_method="GET", http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("export_pipeline_code", "/pipelines/{pipeline_id}/export-code", PipelineRef, _export_pipeline_code, http_method="GET", http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("import_pipeline", "/pipelines/import", ImportPipeline, _import_pipeline, http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("get_pipeline_plan", "/pipelines/{pipeline_id}/plan", PipelinePlanQuery, _get_pipeline_plan, http_method="GET", http_errors=_BAD_REQUEST),
    Handler("start_pipeline_run", "/pipelines/{pipeline_id}/run", PipelineRunRequest, _start_pipeline_run, http_errors=_BAD_REQUEST, wants_transport=True, **_NO_DB),
    Handler("add_pipeline_use", "/pipelines/{parent_pipeline_id}/uses", UseCreate, _add_pipeline_use, http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("update_use_binding", "/pipeline-uses/{use_id}/binding", BindingUpdate, _update_use_binding, http_method="PUT", http_errors=_BAD_REQUEST, **_NO_DB),
    Handler("remove_pipeline_use", "/pipeline-uses/{use_id}", UseRef, _remove_pipeline_use, http_method="DELETE", http_errors=_BAD_REQUEST, **_NO_DB),
)

install_routes(router, SCOPE_HANDLERS)
