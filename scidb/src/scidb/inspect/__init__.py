"""Read-side observability facade + CLI for scidb databases.

`Inspector` is the one read API the CLI, GUI, and MATLAB bridge consume; it
computes nothing new — it only shapes what provenance_query, state, and the
core tables already encode (see docs/claude/observability-api-design.md).

``LocationTree`` and friends are re-exported rather than defined here, for that
same reason: ``Inspector.locations`` returns them, so naming its return type
should not need a second import, but the rule stays in ``scidb.locations``
where the CLI, the GUI's picker and any library caller reach one definition.
"""

from ..locations import LocationNode, LocationState, LocationTree
from .api import (
    VERDICT_CURRENT,
    VERDICT_PARTIAL,
    VERDICT_SUPERSEDED,
    DbOverview,
    ExclusionRecord,
    Inspector,
    NodeStateSummary,
    PickCandidate,
    ProvenanceTree,
    RecordSummary,
    RunRecord,
    RunRef,
    SchemaNode,
    SchemaTree,
    SqlResult,
    TraceEdge,
    TraceInput,
    TraceNode,
    VariableDetail,
    VariableSummary,
    location_sample,
    variant_verdict,
)
from .graph import (
    FunctionNode,
    PipelineEdge,
    PipelineGraph,
    VariableNode,
    VariantSummary,
)
from .mutate import MutationResult, Mutator
from .render import ASCII_STYLE, DEFAULT_STYLE, RenderStyle

__all__ = [
    "Inspector",
    "DbOverview",
    "VariableSummary",
    "VariableDetail",
    "SchemaNode",
    "SchemaTree",
    "LocationTree",
    "LocationNode",
    "LocationState",
    "RecordSummary",
    "PipelineGraph",
    "FunctionNode",
    "VariableNode",
    "PipelineEdge",
    "VariantSummary",
    "RenderStyle",
    "DEFAULT_STYLE",
    "ASCII_STYLE",
    "RunRecord",
    "RunRef",
    "ProvenanceTree",
    "TraceNode",
    "TraceEdge",
    "TraceInput",
    "NodeStateSummary",
    "SqlResult",
    "ExclusionRecord",
    "Mutator",
    "MutationResult",
    "PickCandidate",
    # Shared readings of a VariantSummary — the terminal and the GUI import
    # these rather than each deciding what "SUPERSEDED" means or which
    # locations to show (CLAUDE.md NOTE 3).
    "variant_verdict",
    "location_sample",
    "VERDICT_CURRENT",
    "VERDICT_PARTIAL",
    "VERDICT_SUPERSEDED",
]
