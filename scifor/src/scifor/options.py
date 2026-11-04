"""The ONE declaration of ``for_each``'s options.

``scifor.for_each`` and ``scidb.for_each`` are drop-in replacements for one
another by keyword. Until this declaration existed the option list lived in
four places (scidb's signature, scifor's, MATLAB's ``split_options`` and the
GUI's MATLAB command generation) and drifted: a kwarg one side did not know
landed in ``**metadata_iterables`` as a bogus iteration axis.

:data:`FOR_EACH_OPTIONS` lists every user option ``scidb.for_each`` takes
(scifor cannot import scidb, so the declaration lives here and scidb extends
it with its private hooks). Everything else is derived:

* :data:`DB_ONLY_KWARGS` — the options only a database gives meaning to;
  ``scifor.for_each`` accepts and discards them;
* ``scidb/tests/test_foreach_options.py`` holds both signatures to this list
  (names and defaults);
* scidb groups the options into bundles by :attr:`OptionSpec.bundle`;
* MATLAB's ``split_options`` asks :func:`classify_for_each_args` which of a
  call's name-value pairs are options (MATLAB-only options are declared
  here too, in :data:`MATLAB_ONLY_OPTIONS`);
* the GUI's MATLAB command generation emits options by iterating
  :data:`FOR_EACH_OPTIONS`.

``fn``, ``inputs`` and ``outputs`` are positional arguments, not options, and
``**metadata_iterables`` are iteration axes; none of them appear here.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from scistacklog import Log

#: Where scidb keeps an option once ``for_each`` has bundled it:
#: ``"run"`` — ``scidb.run.foreach_config.RunOptions`` (identity-bearing modes);
#: ``"selection"`` — which locations and rows the call reads;
#: ``"context"`` — facts about the call that are recorded or steer it but are
#: never identity; ``None`` — read directly by ``for_each``.
Bundle = Literal["run", "selection", "context"]

#: The shape of an option's value, for code that writes a ``for_each`` call
#: in another language (MATLAB ``split_options``, GUI command generation).
Kind = Literal["bool", "str", "names", "mapping", "filter", "path", "object"]


class _Unset:
    """The default of an option whose omission means something different
    from ``None`` (``pipeline``: omitted = use the active pipeline)."""

    _instance: "_Unset | None" = None

    def __new__(cls) -> "_Unset":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNSET"


UNSET: Any = _Unset()


@dataclass(frozen=True)
class OptionSpec:
    """One ``for_each`` keyword option.

    ``name`` is the keyword; ``default`` its default in both signatures;
    ``kind`` the shape of its value; ``bundle`` where scidb groups it;
    ``db_only`` whether only ``scidb.for_each`` acts on it (scifor discards
    it with one INFO line).
    """

    name: str
    default: Any
    kind: Kind
    bundle: "Bundle | None" = None
    db_only: bool = False

    @property
    def attribute(self) -> str:
        """The field that holds this option on its bundle: the name without
        a leading underscore (``_progress_fn`` -> ``progress_fn``)."""
        return self.name.lstrip("_")


FOR_EACH_OPTIONS: "tuple[OptionSpec, ...]" = (
    OptionSpec("dry_run", False, "bool"),
    OptionSpec("save", True, "bool", db_only=True),
    OptionSpec("as_table", None, "names", "run"),
    OptionSpec("db", None, "object", db_only=True),
    OptionSpec("distribute", False, "bool", "run"),
    OptionSpec("where", None, "filter", "selection"),
    OptionSpec("introspect", False, "bool", db_only=True),
    OptionSpec("skip_computed", False, "bool", db_only=True),
    OptionSpec("schema_filter", None, "mapping", "selection", db_only=True),
    OptionSpec("schema_keys", None, "names", "selection"),
    OptionSpec("schema_key_types", None, "mapping"),
    OptionSpec("on_error", "continue", "str"),
    OptionSpec("locations", None, "mapping", "selection"),
    OptionSpec("share_limits", None, "mapping"),
    OptionSpec("project_root", None, "path"),
    OptionSpec("glue", None, "mapping", db_only=True),
    OptionSpec("parameter_names", None, "mapping", "context", db_only=True),
    OptionSpec("step", None, "str", "context", db_only=True),
    OptionSpec("pipeline", UNSET, "object", "context", db_only=True),
)

# Former options. for_each takes unknown keywords as iteration keys, so without
# this a stale call would quietly iterate over (and record) the old option.
REMOVED_OPTIONS: "dict[str, str]" = {
    "finalized": (
        "plot_/stat_ functions now always record, like every other function. "
        "Delete the argument; pass save=False for a preview that records nothing."
    ),
}


def reject_removed_options(metadata_iterables: "dict[str, Any]") -> None:
    """Raise ``TypeError`` if a call still passes a removed for_each option."""
    for name in metadata_iterables:
        if name in REMOVED_OPTIONS:
            raise TypeError(
                f"for_each() no longer takes {name}=. {REMOVED_OPTIONS[name]}"
            )


DB_ONLY_KWARGS: "frozenset[str]" = frozenset(o.name for o in FOR_EACH_OPTIONS if o.db_only)


def option(name: str) -> OptionSpec:
    """The declaration of option *name* (KeyError if there is none)."""
    for spec in FOR_EACH_OPTIONS:
        if spec.name == name:
            return spec
    raise KeyError(name)


# ---------------------------------------------------------------------------
# MATLAB: which name-value pairs of a for_each call are options
# ---------------------------------------------------------------------------

MatlabLayer = Literal["scidb", "scifor"]
ArgRole = Literal["option", "discard", "internal", "axis"]

#: Options only MATLAB's ``for_each`` takes, and the MATLAB layers that act
#: on each. ``categorical`` turns text columns of the returned MATLAB table
#: into categoricals; ``outputs`` is MATLAB scifor's named form of the
#: positional OUTPUTS argument (MATLAB scidb moves ``outputs=`` into that slot
#: before parsing); ``preload`` and ``parallel`` are accepted by MATLAB scidb
#: and ignored (MATLAB tests and user scripts still pass them).
MATLAB_ONLY_OPTIONS: "dict[str, tuple[MatlabLayer, ...]]" = {
    "categorical": ("scidb", "scifor"),
    "outputs": ("scifor",),
    "preload": ("scidb",),
    "parallel": ("scidb",),
}


def matlab_accepted_options(layer: MatlabLayer) -> "frozenset[str]":
    """Public option names MATLAB's *layer* ``for_each`` acts on."""
    python = {o.name for o in FOR_EACH_OPTIONS if layer == "scidb" or not o.db_only}
    matlab = {n for n, layers in MATLAB_ONLY_OPTIONS.items() if layer in layers}
    return frozenset(python | matlab)


def matlab_discarded_options(layer: MatlabLayer) -> "frozenset[str]":
    """Option names *layer* accepts and ignores. MATLAB scifor discards every
    MATLAB scidb option it cannot act on, so a scidb call runs unchanged."""
    if layer == "scidb":
        return frozenset()
    return matlab_accepted_options("scidb") - matlab_accepted_options("scifor")


def _squash(name: str) -> str:
    return name.lower().replace("_", "")


def classify_for_each_args(names: "Sequence[str]", layer: str) -> "list[ArgRole]":
    """One role per name of a MATLAB ``for_each`` call's name-value pairs.

    MATLAB's ``split_options`` (``+scidb/for_each.m``, ``+scifor/for_each.m``,
    through ``+scifor/option_pairs.m``) sends the names and keeps only what is
    MATLAB's own: turning each option's VALUE into the MATLAB type its loop
    reads. Names match case-insensitively:

    * ``"option"`` — a public option *layer* acts on;
    * ``"discard"`` — a scidb option MATLAB scifor accepts and ignores;
    * ``"internal"`` — a leading-underscore name: the private handshake
      between MATLAB scidb and MATLAB scifor (``_all_combos``, ...), which
      MATLAB's switch owns;
    * ``"axis"`` — anything else: a metadata iteration axis.

    An axis whose name differs from an option only by case or underscores
    (``skipComputed``) is logged at WARNING: it is almost always a misspelled
    option, and it becomes a phantom iteration axis.
    """
    if layer not in ("scidb", "scifor"):
        raise ValueError(f"layer must be 'scidb' or 'scifor', got {layer!r}")
    accepted = matlab_accepted_options(layer)  # type: ignore[arg-type]
    discarded = matlab_discarded_options(layer)  # type: ignore[arg-type]
    squashed = {_squash(n): n for n in accepted | discarded}
    roles: "list[ArgRole]" = []
    for raw in names:
        name = str(raw)
        key = name.lower()
        if key in accepted:
            roles.append("option")
        elif key in discarded:
            roles.append("discard")
        elif key.startswith("_"):
            roles.append("internal")
        else:
            roles.append("axis")
            near = squashed.get(_squash(name))
            if near is not None:
                Log.warn(
                    f'Metadata key "{name}" looks like the for_each option '
                    f'"{near}" but was not recognized; it is being treated as '
                    "an iteration axis. Check its spelling.",
                    layer="scifor",
                )
    Log.debug(
        f"classify_for_each_args ({layer}): {roles.count('option')} option(s), "
        f"{roles.count('discard')} discarded, {roles.count('internal')} "
        f"internal, {roles.count('axis')} iteration axis name(s)",
        layer="scifor",
    )
    return roles
