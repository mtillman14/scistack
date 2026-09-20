"""The two per-combo loading sentinels.

``for_each`` loads every input it can in bulk before the loop. An input whose
class has no bulk load — or a ``Merge`` with such a constituent — is wrapped
in one of these instead, and the loop resolves it per combination via
``cls.load(**combo)``. They are values that travel: ``glue`` tests for them
when it decides whether a chain can run in bulk, and the MATLAB bridge
rejects them (MATLAB has no per-combo loader). Neither reader should have
to import ``foreach`` for two empty classes, so they live in this leaf.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from scifor import Merge


class PerComboLoader:
    """Sentinel for inputs that need per-combo loading (class lacks bulk load support).

    ``spec`` can be:
    - A plain class (has .load())
    - A ``Fixed`` wrapping a plain class (load with overridden metadata)
    - A ``ColumnSelection`` wrapping a plain class (load, then select cols)
    - A ``Fixed`` wrapping a ``ColumnSelection`` (both overrides)

    ``for_each`` wraps fn so these are resolved per-combo via cls.load(**combo).
    """

    __slots__ = ("spec",)

    def __init__(self, spec: Any):
        self.spec = spec


class PerComboLoaderMerge:
    """Sentinel for Merge where some/all constituents lack bulk load support.

    Holds the original ``scidb.Merge`` spec; ``for_each`` wraps fn to
    resolve each constituent per-combo via cls.load(**combo_metadata).
    """

    __slots__ = ("merge_spec",)

    def __init__(self, merge_spec: "Merge"):
        self.merge_spec = merge_spec
