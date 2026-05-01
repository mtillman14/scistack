"""ForEachConfig — serializes for_each() computation config into version keys."""

import hashlib
import inspect
import json
from typing import Any, Callable


def _compute_fn_hash(fn: Callable) -> str:
    """SHA-256 of the function's source code, truncated to 16 hex chars.

    Falls back to hashing ``fn.__name__`` if source is unavailable (e.g.
    built-in or compiled functions).  The hash is used downstream by
    check_combo_state to detect whether the function body has changed since
    an output record was saved.
    """
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        src = getattr(fn, "__name__", repr(fn))
    return hashlib.sha256(src.encode()).hexdigest()[:16]


# The canonical for_each call-site identity is captured by exactly these
# version_keys fields (see ForEachConfig.to_version_keys()).  Anything else
# in a saved version_keys dict — direct constants unpacked as top-level
# keys, ``__upstream``, ``__output_num``, scihist's lineage extras — is
# per-record bookkeeping and must not affect the call_id.
#
# ``__fn_hash`` is intentionally excluded too, so cosmetic source edits to
# the function body don't fork the call site (see ForEachConfig.to_call_id
# docstring for rationale).
_CALL_ID_INCLUDED_KEYS = (
    "__fn",
    "__inputs",
    "__constants",
    "__where",
    "__distribute",
    "__as_table",
)


def call_id_from_version_keys(version_keys: dict) -> str:
    """Compute a 16-hex-char call_id from any version_keys dict.

    Used by both ``ForEachConfig.to_call_id()`` (forward path, before save)
    and ``list_pipeline_variants()`` (reverse path, reading from
    ``_record_metadata``) so the call_id of a freshly built config matches
    the call_id derived from records it eventually wrote.

    Uses a strict allow-list of canonical config keys, ignoring any
    per-record fields that scidb/scihist may have stored alongside.
    """
    keys = {k: version_keys[k] for k in _CALL_ID_INCLUDED_KEYS if k in version_keys}
    payload = json.dumps(keys, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


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
    ):
        self.fn = fn
        self.inputs = inputs
        self.where = where
        self.distribute = distribute
        self.as_table = as_table

    def to_version_keys(self) -> dict:
        """Return dict of config keys to merge into save_metadata."""
        keys = {}
        keys["__fn"] = getattr(self.fn, "__name__", repr(self.fn))
        keys["__fn_hash"] = _compute_fn_hash(self.fn)
        inputs_key = self._serialize_inputs()
        if inputs_key != "{}":
            keys["__inputs"] = inputs_key
        direct = self._get_direct_constants()
        if direct:
            keys["__constants"] = json.dumps(direct, sort_keys=True)
        if self.where is not None:
            keys["__where"] = self.where.to_key()
        if self.distribute:
            keys["__distribute"] = True
        if self.as_table:
            if isinstance(self.as_table, list):
                keys["__as_table"] = sorted(self.as_table)
            elif self.as_table is True:
                keys["__as_table"] = True
        return keys

    def to_call_id(self) -> str:
        """Stable identifier for this for_each() call site, 16 hex chars.

        Hashes the version keys minus ``__fn_hash`` (and other per-record
        fields) so that cosmetic edits to the function source do not fork
        the call site.  Two for_each() calls with the same loadable inputs,
        constants, where, distribute, and as_table settings produce the
        same call_id even if the function body was reformatted between runs.

        Used to disambiguate ``_for_each_expected`` rows when the same
        function is invoked from multiple call sites — without this,
        function_name alone collides and the second call's expected combos
        clobber the first's.
        """
        return call_id_from_version_keys(self.to_version_keys())

    def _get_direct_constants(self) -> dict:
        """Return scalar constant inputs (non-loadable values)."""
        from .foreach import _is_loadable
        return {k: v for k, v in self.inputs.items() if not _is_loadable(v)}

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
