"""ForEachConfig — serializes for_each() computation config into version keys."""

import json
from typing import Any, Callable


class ForEachConfig:
    """Serializes for_each() computation config into version keys.

    Captures the parts of a for_each() call that affect the computation's
    identity but are not part of the schema metadata: the function, loadable
    inputs (which variable types / Fixed wrappers are used), where= filter,
    and other behavioral flags.

    These keys are merged into save_metadata so that changing the config
    (e.g. switching smoothing=0.2 to smoothing=0.3, or adding a where= filter)
    creates a new version_keys group rather than silently overwriting existing
    results.
    """

    def __init__(
        self,
        fn: Callable,
        inputs: dict[str, Any],
        where=None,
        distribute: bool = False,
        as_table=None,
        pass_metadata=None,
    ):
        self.fn = fn
        self.inputs = inputs
        self.where = where
        self.distribute = distribute
        self.as_table = as_table
        self.pass_metadata = pass_metadata

    def to_version_keys(self) -> dict:
        """Return dict of config keys to merge into save_metadata."""
        keys = {}
        keys["__fn"] = getattr(self.fn, "__name__", repr(self.fn))
        inputs_key = self._serialize_inputs()
        if inputs_key != "{}":
            keys["__inputs"] = inputs_key
        if self.where is not None:
            keys["__where"] = self.where.to_key()
        if self.distribute:
            keys["__distribute"] = True
        if self.as_table:
            if isinstance(self.as_table, list):
                keys["__as_table"] = sorted(self.as_table)
            elif self.as_table is True:
                keys["__as_table"] = True
        if self.pass_metadata is not None:
            keys["__pass_metadata"] = self.pass_metadata
        return keys

    def _serialize_inputs(self) -> str:
        """Serialize loadable inputs to a canonical JSON string.

        Only includes loadable inputs (variable types, Fixed, ColumnSelection,
        Merge) — constants are already included in save_metadata directly.
        """
        from .foreach import _is_loadable

        result = {}
        for name in sorted(self.inputs):
            spec = self.inputs[name]
            if _is_loadable(spec):
                if hasattr(spec, "to_key"):
                    result[name] = spec.to_key()
                elif isinstance(spec, type):
                    result[name] = spec.__name__
                else:
                    result[name] = repr(spec)
        return json.dumps(result, sort_keys=True)
