"""Which schema keys a run iterates — THE owner of that decision.

Every run route asks here: the GUI's Python Run and compiled pipeline, the
generated MATLAB command and MATLAB pipeline script, and the settings panel
that shows the level a Run will use. Before 2026-09-23 only the Python routes
did; the MATLAB routes read an unset level as "every key", and the value
``[]`` meant three different things depending on the route
(docs/claude/cleanup-audit.md §4.2, F22–F27).

Two things live here:

* :class:`SchemaLevel` — the ONE spelling of a level, with three states. The
  GUI stores a node's level as JSON ``null`` / ``[]`` / ``["subject", ...]``;
  :meth:`SchemaLevel.from_stated` is the only reader of that shape, and
  :meth:`SchemaLevel.for_each_schema_keys` / :meth:`SchemaLevel.iterate_keys`
  the only writers of the two run spellings (``for_each(schema_keys=...)``
  reads ``None`` as one call and ``[]`` as every key — the opposite of the
  node's storage, which is exactly how the confusion arose).
* :func:`resolve_schema_level` — the precedence, pure: stated on the node >
  where THIS call site last ran > what its inputs imply > every key.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scifor import EachOf, PathInput

from . import provenance_query
from .log import Log

UNSET = "unset"
ONE_CALL = "one_call"
KEYS = "keys"

RULE_STATED = "stated on the node"
RULE_RECORDED = "the level it last ran at"
RULE_INPUTS = "the finest level its inputs carry"
RULE_NOTHING = "no history and no bound input to read a level from"


@dataclass(frozen=True)
class SchemaLevel:
    """A run's iteration level: :data:`UNSET` (nobody said), :data:`ONE_CALL`
    (iterate nothing: one call over the whole dataset) or :data:`KEYS`
    (iterate exactly ``keys``, in dataset order).

    ``KEYS`` is never empty: an empty key list IS one call, and normalizing
    it here is what stops ``[]`` from meaning anything else downstream.
    """

    state: str
    keys: tuple[str, ...] = ()

    @classmethod
    def unset(cls) -> "SchemaLevel":
        return cls(UNSET)

    @classmethod
    def one_call(cls) -> "SchemaLevel":
        return cls(ONE_CALL)

    @classmethod
    def of(cls, keys, schema_keys=None) -> "SchemaLevel":
        """A level iterating *keys* — in *schema_keys* order when given, which
        also drops (and warns about) any key the dataset does not have."""
        keys = [str(k) for k in (keys or []) if k]
        if schema_keys is not None:
            order = list(schema_keys)
            unknown = [k for k in keys if k not in order]
            if unknown:
                Log.warn(
                    f"[schema-level] ignoring key(s) {unknown} that are not "
                    f"dataset schema keys {order}"
                )
            keys = [k for k in order if k in set(keys)]
        return cls(KEYS, tuple(keys)) if keys else cls(ONE_CALL)

    @classmethod
    def from_stated(cls, raw: Any, schema_keys=None) -> "SchemaLevel":
        """Read a node's stored level: ``None`` → unset, ``[]`` → one call,
        ``[k, ...]`` → those keys. A ``SchemaLevel`` passes through."""
        if isinstance(raw, SchemaLevel):
            return raw
        if raw is None:
            return cls.unset()
        if isinstance(raw, str):
            raw = [raw]
        return cls.of(list(raw), schema_keys)

    @property
    def is_unset(self) -> bool:
        return self.state == UNSET

    @property
    def is_one_call(self) -> bool:
        return self.state == ONE_CALL

    def to_stated(self) -> "list[str] | None":
        """The node-storage spelling: ``None`` / ``[]`` / ``[k, ...]``."""
        if self.is_unset:
            return None
        return list(self.keys)

    def _require_resolved(self, what: str) -> None:
        if self.is_unset:
            raise ValueError(
                f"{what} needs a resolved schema level; this one is unset — "
                f"call resolve_schema_level first"
            )

    def for_each_schema_keys(self) -> "list[str] | None":
        """The ``for_each(schema_keys=...)`` argument: ``None`` for one call,
        the key list otherwise. Never ``[]`` (which for_each reads as EVERY
        key)."""
        self._require_resolved("for_each(schema_keys=)")
        return list(self.keys) if self.keys else None

    def iterate_keys(self) -> list[str]:
        """The keys to iterate as a plain list, ``[]`` for one call — the
        MATLAB generator's spelling (it emits no schema kwargs for ``[]``)."""
        self._require_resolved("iterate_keys()")
        return list(self.keys)

    def describe(self) -> str:
        if self.is_unset:
            return "unset"
        if self.is_one_call:
            return "nothing: one call over the whole dataset"
        return "[" + ", ".join(self.keys) + "]"

    def to_json(self, rule: str | None = None) -> dict:
        """Wire shape for the settings panel."""
        out = {"state": self.state, "keys": list(self.keys)}
        if rule is not None:
            out["rule"] = rule
        return out


# ---------------------------------------------------------------------------
# Input levels
# ---------------------------------------------------------------------------


def path_input_level(pi: Any, schema_keys) -> "list[str] | None":
    """The schema keys a PathInput's template names, in dataset order.

    An ``EachOf`` of PathInputs (alternate templates) contributes the UNION of
    its alternatives' placeholders (user decision 2026-09-23); alternatives
    that disagree are warned about, because they would resolve different
    files at different levels. Anything else returns ``None`` — no level to
    read — rather than being silently skipped by a caller's ``hasattr``.
    """
    order = list(schema_keys or [])
    if isinstance(pi, PathInput):
        names = set(pi.placeholder_keys())
        return [k for k in order if k in names]
    if isinstance(pi, EachOf) and pi.alternatives and all(
        isinstance(a, PathInput) for a in pi.alternatives
    ):
        per_alt = [
            [k for k in order if k in set(a.placeholder_keys())]
            for a in pi.alternatives
        ]
        union = {k for level in per_alt for k in level}
        if any(set(level) != union for level in per_alt):
            Log.warn(
                f"[schema-level] alternate PathInput templates name different "
                f"schema keys {per_alt}; iterating their union"
            )
        return [k for k in order if k in union]
    return None


def input_levels(duck, variable_types, path_inputs, schema_keys) -> list[list[str]]:
    """Each bound input's level: a variable's is the keys its records
    populate (``provenance_query.variable_schema_keys``; a type with no
    records has none), a PathInput's the keys its template names. An EMPTY
    level is a real answer (dataset-level / no placeholder)."""
    levels: list[list[str]] = []
    for pi in path_inputs or []:
        level = path_input_level(pi, schema_keys)
        if level is not None:
            levels.append(level)
    names = sorted({t for t in (variable_types or []) if t})
    if names:
        levels.extend(
            provenance_query.variable_schema_keys(duck, names, schema_keys).values()
        )
    return levels


# ---------------------------------------------------------------------------
# The precedence
# ---------------------------------------------------------------------------


def resolve_schema_level(
    schema_keys,
    stated: Any = None,
    recorded: "list[str] | None" = None,
    levels: "list[list[str]] | None" = None,
) -> "tuple[SchemaLevel, str]":
    """``(level, rule)`` — pure, so every route and the panel agree.

    1. **stated** on the node (anything but unset — ``[]`` included) wins;
    2. **recorded**: the keys THIS call site iterated on its last run
       (``None`` = no history under its current wiring; ``[]`` = it ran as
       one call);
    3. **inputs**: the union of every bound input's level; a coarser input
       broadcasts (docs/claude/coarse-level-inputs.md). An empty union is
       one call;
    4. every dataset key, only with nothing to go on.
    """
    schema_keys = list(schema_keys or [])
    level = SchemaLevel.from_stated(stated, schema_keys)
    if not level.is_unset:
        return level, RULE_STATED
    if recorded is not None:
        return SchemaLevel.of(recorded, schema_keys), RULE_RECORDED
    if levels:
        inferred = provenance_query.finest_schema_keys(levels, schema_keys)
        return SchemaLevel.of(inferred, schema_keys), RULE_INPUTS
    return SchemaLevel.of(schema_keys, schema_keys), RULE_NOTHING
