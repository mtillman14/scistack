"""A function's ROLE, from its name — the one classifier for the whole stack.

A LEAF (stdlib only), because the role is asked for everywhere: the
endpoint policy in ``foreach``, ``Pipeline.endpoints()``, the inspector's
report, and the GUI's sidebar filter. Until 2026-09-20 it lived in
``scidb.discover``, which imports ``pipeline`` and ``variable`` at the top,
so every one of those callers imported it INSIDE a function to dodge the
cycle — a role lookup was the single most common reason for a lazy import
in the package. Stage 3 of ``.claude/plan-architecture-2026-09-20.md``.
"""

from __future__ import annotations

# The name prefixes that mark a function's role, longest-match irrelevant
# (they are disjoint). Checked on the *name*, which is what crosses the MATLAB
# bridge unchanged — so a role needs no decorator (Python-only) and no classdef
# (MATLAB-only).
#
# ``stat_`` is SINGULAR. ``stats_summary`` is an ordinary process function; the
# sidebar may say "Stats", the prefix may not.
_ROLE_PREFIXES: tuple[tuple[str, str], ...] = (
    ("plot_", "plot"),
    ("stat_", "stat"),
    ("glue_", "glue"),
)

FunctionRole = str  # Literal["process", "plot", "stat", "glue"]

# ``{role: prefix}`` for the prefixed roles. Exported for the one caller that
# cannot use :func:`function_role` — a SQL filter over ``_invocation`` names,
# which must not fetch every invocation just to classify it in Python.
ROLE_PREFIX: dict[str, str] = {role: prefix for prefix, role in _ROLE_PREFIXES}

# Every role, in the order a UI should present them. Exported so the GUI can
# build its filter without holding a second copy of the vocabulary.
FUNCTION_ROLES: tuple[str, ...] = ("process", "plot", "stat", "glue")


def function_role(name: str) -> FunctionRole:
    """Classify a function by its name prefix.

    ``"process"`` (no recognized prefix) is the default bucket — the ordinary
    pipeline step. ``"plot"``, ``"stat"`` and ``"glue"`` are the prefixed roles.

    This is the single classifier for the whole stack. The prefixes already
    drive real execution behaviour inside scidb (endpoint policy, draft/record
    mode, artifact stamping), so the GUI must not own a second copy of the
    strings — the same choke-point reasoning as the discovery consolidation.
    A fifth role later touches this function and nothing else.
    """
    for prefix, role in _ROLE_PREFIXES:
        if name.startswith(prefix):
            return role
    return "process"


def endpoint_kind(fn_name: str) -> str | None:
    """``"plot"`` | ``"stat"`` | ``None`` — the plot/stat subset of the roles,
    which is what "endpoint" means (a leaf that renders an artifact rather
    than producing a record). Side-effect free; the endpoint CONTRACT checks
    (a plot needs a PathOutput, a stat defaults to ``as_table``) are
    ``foreach._endpoint_policy``."""
    role = function_role(fn_name)
    return role if role in ("plot", "stat") else None
