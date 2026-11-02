"""One ``scidb.for_each`` call as a value: its options in three bundles.

``scidb.for_each`` keeps its keyword signature for users (28 keywords, the
ones :data:`SCIDB_FOR_EACH_OPTIONS` declares). Internally the keywords are
grouped once, at entry, into a :class:`ForEachCall`:

* :class:`~scidb.run.foreach_config.RunOptions` (``run``) — the identity-bearing
  execution modes (``as_table``, ``distribute``);
* :class:`RunSelection` (``selection``) — which locations and rows the call
  reads (``where``, ``locations``, ``schema_filter``, ``schema_keys``, the
  metadata iterables);
* :class:`RunContext` (``context``) — facts about the call that steer or are
  recorded but are never identity (``step``, ``parameter_names``,
  ``finalized``, ``pipeline``, the private hooks);

and the rest stay plain fields. Which bundle an option belongs to is declared
once, on its :class:`scifor.OptionSpec`; this module only reads the
declaration. A recursion (``step=``, EachOf alternatives) or a deferred
pipeline step re-runs the SAME value with one field replaced, so no hand-written
kwarg list can drop an option (``glue=`` and ``locations=`` were each lost
that way once).
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from scifor import FOR_EACH_OPTIONS, UNSET, OptionSpec

from scidb.run.foreach_config import RunOptions

#: scidb's private hooks, beyond the user options scifor declares. Not part
#: of the drop-in contract (a leading underscore), all in the ``context``.
SCIDB_HOOK_OPTIONS: "tuple[OptionSpec, ...]" = (
    OptionSpec("_inject_combo_metadata", False, "bool", "context", db_only=True),
    OptionSpec("_pre_combo_hook", None, "object", "context", db_only=True),
    OptionSpec("_progress_fn", None, "object", "context", db_only=True),
    OptionSpec("_cancel_check", None, "object", "context", db_only=True),
)

#: Every keyword ``scidb.for_each`` takes besides ``fn``/``inputs``/``outputs``
#: and the metadata iterables, in signature order.
SCIDB_FOR_EACH_OPTIONS: "tuple[OptionSpec, ...]" = FOR_EACH_OPTIONS + SCIDB_HOOK_OPTIONS


@dataclass(frozen=True)
class RunSelection:
    """Which locations and rows a call reads.

    ``schema_filter`` and ``schema_keys`` are resolved into
    ``metadata_iterables`` / ``where`` once, at entry; afterwards both are
    None, so a recursion never resolves them twice.
    """

    where: Any = None
    locations: Any = None
    schema_filter: "Mapping[str, list] | None" = None
    schema_keys: "list[str] | None" = None
    metadata_iterables: "dict[str, Any]" = field(default_factory=dict)


@dataclass(frozen=True)
class RunContext:
    """Facts about a call that steer it or are recorded, never identity."""

    finalized: bool = False
    parameter_names: "dict[str, str] | None" = None
    step: "str | None" = None
    #: A Pipeline to register into, None (run eagerly), or UNSET (use the
    #: active pipeline, if any).
    pipeline: Any = UNSET
    #: Pass the combination's schema-key values to the function as keyword
    #: arguments it accepts (generates_file and endpoint functions).
    inject_combo_metadata: bool = False
    #: ``combo -> bool``; True skips the combination before anything loads
    #: (``skip_computed`` installs one).
    pre_combo_hook: "Callable[[dict], bool] | None" = None
    progress_fn: "Callable[[dict], None] | None" = None
    cancel_check: "Callable[[], bool] | None" = None

    def cancelled(self) -> bool:
        """Has the caller asked the run to stop?"""
        return self.cancel_check is not None and bool(self.cancel_check())


@dataclass(frozen=True)
class ForEachCall:
    """One ``scidb.for_each`` call, its keywords bundled (module docstring)."""

    fn: Callable
    inputs: "dict[str, Any]"
    outputs: "list[Any]"
    run: RunOptions = field(default_factory=RunOptions)
    selection: RunSelection = field(default_factory=RunSelection)
    context: RunContext = field(default_factory=RunContext)
    dry_run: bool = False
    save: bool = True
    db: Any = None
    introspect: bool = False
    skip_computed: bool = False
    schema_key_types: "dict[str, str] | None" = None
    on_error: str = "continue"
    on_duplicate: str = "error"
    share_limits: "dict[str, list[str]] | None" = None
    project_root: Any = None
    glue: "dict[str, Any] | None" = None

    @classmethod
    def from_options(
        cls,
        fn: Callable,
        inputs: "dict[str, Any]",
        outputs: "list[Any]",
        metadata_iterables: "Mapping[str, Any]",
        **options: Any,
    ) -> "ForEachCall":
        """Bundle the keywords of one call. ``options`` must name every
        option in :data:`SCIDB_FOR_EACH_OPTIONS`, so an option added to the
        declaration but not passed through here fails loudly."""
        declared = {spec.name for spec in SCIDB_FOR_EACH_OPTIONS}
        if set(options) != declared:
            raise TypeError(
                f"ForEachCall.from_options: missing {sorted(declared - set(options))}, "
                f"unexpected {sorted(set(options) - declared)}"
            )
        bundles: "dict[str, dict[str, Any]]" = {
            "run": {},
            "selection": {"metadata_iterables": dict(metadata_iterables)},
            "context": {},
        }
        loose: "dict[str, Any]" = {}
        for spec in SCIDB_FOR_EACH_OPTIONS:
            target = bundles[spec.bundle] if spec.bundle is not None else loose
            target[spec.attribute] = options[spec.name]
        return cls(
            fn=fn,
            inputs=inputs,
            outputs=outputs,
            run=RunOptions(**bundles["run"]),
            selection=RunSelection(**bundles["selection"]),
            context=RunContext(**bundles["context"]),
            **loose,
        )

    def option_values(self) -> "dict[str, Any]":
        """The call's keywords, as ``scidb.for_each`` takes them (minus
        ``pipeline``, so a replay runs instead of registering again)."""
        values: "dict[str, Any]" = {}
        for spec in SCIDB_FOR_EACH_OPTIONS:
            if spec.name == "pipeline":
                continue
            holder = getattr(self, spec.bundle) if spec.bundle is not None else self
            values[spec.name] = getattr(holder, spec.attribute)
        return values

    @property
    def metadata_iterables(self) -> "dict[str, Any]":
        return self.selection.metadata_iterables

    def with_selection(self, **changes: Any) -> "ForEachCall":
        return replace(self, selection=replace(self.selection, **changes))

    def with_context(self, **changes: Any) -> "ForEachCall":
        return replace(self, context=replace(self.context, **changes))

    def with_run(self, **changes: Any) -> "ForEachCall":
        return replace(self, run=replace(self.run, **changes))
