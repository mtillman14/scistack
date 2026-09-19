"""
scistackplotdb — plot scidb variables.

Loads variables into the long format ``scistackplot`` consumes, adds the four
things a flat table never needed (shape classification, schema-depth joins,
variants as factors, and a transport budget), and generates pipeline endpoints
from a finished spec.

::

    from scidb import configure_database
    from scistackplot import PlotSpec, Role, PlotKind, render
    from scistackplotdb import ScidbSource

    db = configure_database("experiment.duckdb", ["subject", "session", "trial"])
    source = ScidbSource(db)

    table = source.get_table(["StepLength"])
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )
    figure = render(table, spec)

Everything about recording a figure — ``finalized``, artifact stamping,
``scidb report`` — belongs to scidb's existing ``plot_`` endpoint machinery and
is unchanged. See ``docs/claude/plotting-library-design.md``.
"""

from __future__ import annotations

from .endpoint import (
    EndpointCode,
    default_output_variable,
    default_path_template,
    generate_endpoint,
)
from .hierarchy import join_frames, join_kind, joinable, joined_levels
from .load import (
    LATEST_COLUMN,
    MISSING_VERSION_LEVEL,
    VERSION_FACTOR_PREFIX,
    VariableFrame,
    attach_variants,
    data_column_types_for,
    data_columns_for,
    is_container_type,
    load_variable,
    registered_variables,
    sample_value,
    schema_keys,
)
from .source import ScidbSource
from .variants import (
    branch_params_for,
    selection_for,
    variant_graph,
    variant_set,
)

__all__ = [
    "ScidbSource",
    "variant_set",
    "variant_graph",
    "selection_for",
    "branch_params_for",
    "VariableFrame",
    "VERSION_FACTOR_PREFIX",
    "MISSING_VERSION_LEVEL",
    "LATEST_COLUMN",
    "load_variable",
    "attach_variants",
    "registered_variables",
    "schema_keys",
    "data_columns_for",
    "data_column_types_for",
    "is_container_type",
    "sample_value",
    "join_kind",
    "joinable",
    "join_frames",
    "joined_levels",
    "generate_endpoint",
    "EndpointCode",
    "default_output_variable",
    "default_path_template",
]

__version__ = "0.1.0"
