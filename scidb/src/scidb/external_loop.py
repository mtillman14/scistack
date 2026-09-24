"""The seam for a caller that runs ``for_each``'s loop ITSELF.

``scidb.for_each`` is prepare -> loop -> save in one call. The MATLAB bridge
(``scimatlab.bridge``) cannot do that: MATLAB runs the user's function, so
Python does the phases on either side of MATLAB's loop in two RPCs. That
bridge used to import six private ``scidb.foreach`` functions directly
(cleanup-audit F2), so any internal rename or signature change in
``foreach.py`` could break MATLAB with nothing in scidb saying it was a
contract.

This module IS that contract: the names below are what an external loop may
use, and the only ones. The logic stays in ``scidb.foreach`` (one owner);
these are re-exports under public names, so changing one of them is visibly
changing a published seam. ``scimatlab/tests/test_bridge_private_imports.py``
fails if the bridge imports anything private from scidb again.

* :func:`prepare` — everything before the loop (binding, loading, combo
  expansion); returns the ``_ForEachState`` the save consumes.
* :func:`save_resolved` — everything after it, from the loop's result table.
* :func:`build_skip_hook` — the ``skip_computed`` pre-combo filter.
* :func:`resolve_for_columns` — ``for_columns()`` -> concrete column lists.
* :func:`endpoint_policy` — the plot_/stat_ endpoint rules.
* :func:`apply_introspect` — the ``introspect=`` columns on a result table.
"""

from __future__ import annotations

from .foreach import _apply_introspect as apply_introspect
from .foreach import _build_skip_hook as build_skip_hook
from .foreach import _endpoint_policy as endpoint_policy
from .foreach import _for_each_prepare as prepare
from .foreach import _for_each_save_resolved as save_resolved
from .foreach import _resolve_for_columns as resolve_for_columns

__all__ = [
    "apply_introspect",
    "build_skip_hook",
    "endpoint_policy",
    "prepare",
    "resolve_for_columns",
    "save_resolved",
]
