"""
``PlotSpec`` — the serializable description of a plot.

This is the load-bearing object of the whole package. The interactive GUI does
not produce pictures; it produces a ``PlotSpec``, and everything downstream
(resolution, rendering, code generation, and the pipeline ``plot_`` endpoint)
is a pure function of one. Keeping it small, serializable, and diffable is what
lets an inherently visual tool participate in a reproducible pipeline.

See ``docs/claude/plotting-library-design.md`` for the reasoning.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field, replace
from enum import Enum
from typing import Any


class Role(str, Enum):
    """
    What a factor column does in a plot.

    Every factor carries exactly **one** role. That invariant is the whole
    reason this enum exists: the R/Shiny proof of concept spread the same
    information across four independent widgets and spent most of its length
    keeping them consistent with each other via ``setdiff``.

    Four roles, in two panes (docs/claude/grouping-and-collapse.md):

    * **Grouping pane** — ``GROUP``: one *mark* per level combination, where a
      mark is whatever the kind draws (a bar, a box, a line). The grouped
      factors are ORDERED by :attr:`PlotSpec.groups`, innermost first, and one
      of them may be tagged as the colour (:attr:`PlotSpec.color`). Colour is
      not a role: it never split data, it labelled a split that a grouping
      layer had already made.
    * **Factors pane** — ``FACET`` (one subplot per level), ``ITERATE`` (one
      figure per level) and ``COLLAPSE`` (averaged away). The outermost
      collapsed key is the *sample* the kind's statistic is computed over.

    The old ``X`` / ``COLOR`` / ``AGGREGATE`` / ``FREE`` vocabulary is gone
    outright (2026-09-17): ``FREE`` meant "let the kind decide", which is the
    ambiguity the two-pane model removes, and a saved spec carrying it is
    refused by :meth:`PlotSpec.from_dict` rather than guessed at.
    """

    GROUP = "group"              # one mark per level; ordered by PlotSpec.groups
    FACET = "facet"              # one subplot per level
    ITERATE = "iterate"          # separate FIGURE per level (fan-out)
    COLLAPSE = "collapse"        # averaged away; outermost collapsed key = sample

    def __str__(self) -> str:
        return self.value


#: Role strings this version no longer reads. Listed so ``from_dict`` can
#: refuse them by name instead of failing with a bare ``ValueError`` from the
#: enum — and so the message can say what replaced them.
LEGACY_ROLE_VALUES = ("x", "color", "aggregate", "free")

#: How many LABELLED tick layers a categorical x axis may carry — grouping
#: layers minus the coloured one, which is labelled by the legend instead.
#: Three is not arbitrary: a fourth level of nesting cannot be read off an
#: axis, and the label stack below the plot grows taller than the plot.
MAX_X_LAYERS = 3


class PlotKind(str, Enum):
    """The visual form of the plot."""

    SCATTER = "scatter"      # one marker per row
    STRIP = "strip"          # scatter with categorical jitter
    SPAGHETTI = "spaghetti"  # markers + one polyline per replicate (scalar, categorical x)
    LINE = "line"            # one polyline per series (1-D measures)
    BOX = "box"              # distribution per x position
    VIOLIN = "violin"        # distribution per x position, density
    BAR = "bar"              # statistic per x position, with error bar
    BAND = "band"            # statistic line + shaded error region (1-D)
    HEATMAP = "heatmap"      # 2-D matrix

    def __str__(self) -> str:
        return self.value


#: Kinds whose y position is ONE value per row.
#:
#: Membership is what makes a kind imply a collapse: selected on a 1-D measure,
#: any of these means "reduce each vector to one value first"
#: (:mod:`scistackplot.cell`). LINE and BAND are the kinds that read the
#: samples, and HEATMAP wants a matrix, so none of the three appear here.
SCALAR_KINDS = (
    PlotKind.SCATTER,
    PlotKind.STRIP,
    PlotKind.SPAGHETTI,
    PlotKind.BOX,
    PlotKind.VIOLIN,
    PlotKind.BAR,
)

#: Kinds that can carry a "Show sample" overlay (:attr:`PlotSpec.show_sample`):
#: the categorical kinds — a bar, box or violin summarises its sample, scatter
#: / strip draw one point per sample row, and a spaghetti draws one point per
#: line per tick (since 2026-09-21: the trials and cycles behind each
#: subject's point, placed on that subject's line) — all of which leave room
#: beside each mark for the underlying points. LINE / BAND have a numeric x
#: (no slot to place a point in) and HEATMAP has no marks.
OVERLAY_KINDS = (
    PlotKind.BAR,
    PlotKind.BOX,
    PlotKind.VIOLIN,
    PlotKind.SCATTER,
    PlotKind.STRIP,
    PlotKind.SPAGHETTI,
)


class Statistic(str, Enum):
    MEAN = "mean"
    MEDIAN = "median"

    def __str__(self) -> str:
        return self.value


class ErrorBand(str, Enum):
    NONE = "none"
    SD = "sd"
    SEM = "sem"
    CI95 = "ci95"
    IQR = "iqr"

    def __str__(self) -> str:
        return self.value


class MatchOp(str, Enum):
    """How a facet-layout rule tests a panel's label."""

    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    EQUALS = "equals"
    REGEX = "regex"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class Matcher:
    """
    One row (or column) of a facet grid, defined by what its panels look like.

    Naming a grid by *rules* rather than by position is what makes a layout
    reusable: "left column = names starting with L" holds for any muscle set,
    any subject, any variable, so the same arrangement can later be saved as a
    preset and applied to a different plot.
    """

    op: MatchOp = MatchOp.CONTAINS
    value: str = ""
    #: Shown as the row/column header. Defaults to the value.
    label: str | None = None

    @property
    def display(self) -> str:
        """Row/column header. An unset slot has no header, not the op's name."""
        if self.is_blank:
            return self.label or ""
        return self.label or self.value

    @property
    def is_blank(self) -> bool:
        """
        An unset slot. The grid has a fixed number of row/column slots, so most
        of them are empty most of the time, and an empty slot must mean
        "whatever is left over, in order" — never "everything".
        """
        return not self.value

    def matches(self, text: str) -> bool:
        if self.is_blank:
            # Without this, CONTAINS "" matches every panel ('' in text) and the
            # first blank slot swallows the whole grid; NOT_CONTAINS "" is the
            # same bug inverted.
            return False
        text = "" if text is None else str(text)
        needle = self.value
        if self.op is MatchOp.STARTS_WITH:
            return text.startswith(needle)
        if self.op is MatchOp.ENDS_WITH:
            return text.endswith(needle)
        if self.op is MatchOp.CONTAINS:
            return needle in text
        if self.op is MatchOp.NOT_CONTAINS:
            return needle not in text
        if self.op is MatchOp.EQUALS:
            return text == needle
        if self.op is MatchOp.REGEX:
            import re

            try:
                return re.search(needle, text) is not None
            except re.error:
                # An invalid pattern must not take the whole figure down while
                # the user is still typing it.
                return False
        return False


@dataclass(frozen=True)
class Filter:
    """A row filter applied before anything else."""

    column: str
    #: Keep rows whose value is in this list (categorical include).
    include: list[Any] | None = None
    #: Drop rows whose value is in this list.
    exclude: list[Any] | None = None
    #: Numeric range, inclusive; either bound may be None.
    minimum: float | None = None
    maximum: float | None = None


#: A number with no leading zeros whose value is integral: ``1``, ``-3``,
#: ``1.0``, ``1.00``. NOT ``01`` (leading zero) and NOT ``1.5``.
_INTEGRAL = re.compile(r"-?(?:0|[1-9]\d*)(?:\.0+)?")


def value_spellings(value: Any) -> tuple[str, ...]:
    """Every text form *value* may legitimately arrive as, sorted.

    One value crosses three layers and can pick up a different spelling in
    each: a schema key that round-tripped through DuckDB as a float reaches
    this frame as ``1.0``, while the picker -- whose tree comes from scidb --
    sends ``1``. Compared as raw text those select nothing, in silence, which
    is the worst available failure. So an integral number matches both
    spellings.

    A zero-padded string does NOT: ``"01"`` and ``"1"`` can be two genuinely
    distinct trials, and which one is identity is scidb's decision
    (docs/claude/schema-key-types.md), never a shortcut taken here.

    Sorted so :mod:`scistackplot.codegen` can bake the result into generated
    source and get byte-identical output for one spec.

    ``scifor.locations.value_spellings`` is the same function for combos; the
    shared cases in docs/claude/location-filter-cases.json pin them together.
    """
    text = str(value)
    if _INTEGRAL.fullmatch(text):
        whole = int(float(text))
        return tuple(sorted({text, str(whole), f"{whole}.0"}))
    return (text,)


@dataclass(frozen=True)
class LocationFilter:
    """Which schema locations to draw, as a set of hierarchy **prefixes**.

    The difference from :class:`Filter` is the whole reason this exists.
    ``Filter`` holds one include-list per column, so a set of them can only ever
    express a **Cartesian product**: ``subject ∈ {01,02} × trial ∈ {3,7}``. That
    is the right shape for "narrow this factor", and the wrong shape for a
    hierarchical location picker, where the user ticks boxes on a tree and means
    something ragged — *all* of subject 01, plus only trials 1–3 of subject 02.
    Real datasets are ragged (subject 01 ran eight trials, subject 02 ran five),
    so the Cartesian form is not merely less expressive, it names combinations
    that never existed.

    Worse, in a tree UI the per-column form is actively misleading: unticking
    ``S01 / trial 3`` would silently drop trial 3 from *every* subject, because
    there is only one ``trial`` include-list to edit.

    ``include`` is a list of prefixes, each a list of ``[key, value]`` pairs
    outermost first: ``[["subject", "01"]]`` is all of subject 01,
    ``[["subject", "02"], ["trial", "3"]]`` is one trial. Storing the **minimal
    covering set** (collapse a fully-ticked parent to its own short prefix) is
    what keeps a later-added trial inside an already-selected subject instead of
    silently outside it.

    **A prefix names its keys rather than relying on position**, and that is not
    verbosity. A dataset schema is a linear hierarchy but a *saved location* need
    not fill it: a cross-cutting result saved at ``subject`` + ``speed`` with
    ``timepoint`` NULL is a supported, documented shape
    (docs/claude/schema-hierarchy-contiguity.md), and its location is
    ``[["subject","01"], ["speed","SSV"]]``. Read positionally against the
    schema, that second value would be matched against ``timepoint`` — selecting
    nothing, in silence. Naming the key makes the hole impossible to
    misinterpret, and it means the picker sends a node's own path back verbatim
    with no translation step to get wrong.

    An empty filter -- no ``include`` prefixes and no ``exclude_levels`` -- is
    **inert**: it constrains nothing, matching the rule an unfilled variant row
    follows (docs/claude/plot-variant-rows.md §3). Clicking into a picker is
    not a statement about the data.

    Values are compared as text, like every other selection that crosses JSON,
    with one documented exception: :func:`value_spellings`. The whole rule,
    including the cases both sides are tested against, is
    docs/claude/location-filter-semantics.md.
    """

    #: Schema keys, outermost first. Display order only — matching reads the
    #: keys named inside each prefix, never this list's positions.
    keys: list[str] = field(default_factory=list)
    #: Prefixes to keep. Lists rather than tuples so JSON and TOML round-trip
    #: with no conversion; :meth:`prefixes` is the tuple view for matching.
    include: list[list[list[str]]] = field(default_factory=list)
    #: ``{key: [levels]}`` dropped wherever they appear — a standing RULE, not
    #: a set of places, and the half ``include`` cannot express.
    #:
    #: Unticking one trial of one subject is ragged and belongs in ``include``.
    #: Unticking a *session* means "that session is out, everywhere, including
    #: in data that does not exist yet": exploding it into prefixes would
    #: enumerate today's subjects and freeze them, so a subject added next
    #: month would vanish entirely rather than merely lose that session. Both
    #: halves are needed, and a location survives only if ``include`` covers it
    #: AND no level rule names one of its values.
    exclude_levels: dict[str, list[str]] = field(default_factory=dict)

    def excluded(self) -> dict[str, tuple[str, ...]]:
        """``exclude_levels`` as text, empty lists dropped.

        An empty list means "nothing excluded" and must be INERT rather than a
        rule matching nothing, or :meth:`is_empty` would report a filter where
        there is none and every fast path would be skipped for no reason.
        """
        out: dict[str, tuple[str, ...]] = {}
        for key, values in (self.exclude_levels or {}).items():
            levels = tuple(dict.fromkeys(str(value) for value in values))
            if levels:
                out[str(key)] = levels
        return out

    def prefixes(self) -> list[tuple[tuple[str, str], ...]]:
        """``include`` as tuples of ``(key, value)`` text pairs, empties dropped."""
        out = []
        for entry in self.include:
            prefix = tuple(
                (str(pair[0]), str(pair[1])) for pair in entry if len(pair) == 2
            )
            if prefix:
                out.append(prefix)
        return out

    def is_empty(self) -> bool:
        return not self.prefixes() and not self.excluded()

    def to_dict(self) -> dict:
        return {
            "keys": list(self.keys),
            "include": [
                [[str(pair[0]), str(pair[1])] for pair in entry if len(pair) == 2]
                for entry in self.include
            ],
            "exclude_levels": {
                key: list(values) for key, values in self.excluded().items()
            },
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "LocationFilter":
        return cls(
            keys=list(raw.get("keys") or []),
            include=[
                [[str(pair[0]), str(pair[1])] for pair in entry if len(pair) == 2]
                for entry in (raw.get("include") or [])
            ],
            exclude_levels={
                str(key): [str(value) for value in values]
                for key, values in (raw.get("exclude_levels") or {}).items()
            },
        )


@dataclass(frozen=True)
class LevelGroup:
    """A factor derived by bucketing another factor's levels.

    For a ``session`` key whose values are ``pre, post1, post2, post3``, this is
    how "baseline vs post" becomes a factor you can colour or facet by without
    editing any data. A key whose levels *already are* the groups needs none of
    this — it is a factor with a role today.

    Kept as a spec field rather than a column somewhere so it travels with the
    figure: the same bucketing is part of what the plot means, and a reader
    re-opening the spec sees the definition rather than an unexplained column.
    """

    #: The new factor's name, e.g. ``"Phase"``.
    name: str
    #: The existing factor whose levels are being bucketed, e.g. ``"session"``.
    source: str
    #: ``{level: group label}``. Levels are matched as text.
    mapping: dict[str, str] = field(default_factory=dict)
    #: What happens to levels the mapping does not name. ``None`` DROPS those
    #: rows — "just these two groups, ignore the rest" — and any other value is
    #: the bucket they land in. There is no third option where they stay
    #: unlabelled: a NaN group silently becomes its own series.
    unmatched: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "source": self.source,
            "mapping": dict(self.mapping),
            "unmatched": self.unmatched,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "LevelGroup":
        return cls(
            name=raw["name"],
            source=raw["source"],
            mapping=dict(raw.get("mapping") or {}),
            unmatched=raw.get("unmatched"),
        )


@dataclass(frozen=True)
class FactorVariable:
    """A variable joined in as a FACTOR rather than plotted, or one column of it.

    ``column`` is what makes a wide table usable: a Demographics variable loaded
    from a spreadsheet stores one column per field (``Age``, ``Sex``,
    ``InterventionGroup``), so the variable as a whole has no single value to
    group by, and naming the column is the only way to say which question is
    being asked. ``column=None`` means the variable itself, which is the
    single-data-column case (a per-subject ``Condition``).

    Frozen because :meth:`scistackplot.sources.base.BaseSource.get_table`
    memoizes on the tuple of these.
    """

    variable: str
    column: str | None = None
    #: Which VARIANT of the grouping variable supplies the labels, as a
    #: selection keyed by frame column — the same plain-data shape
    #: :class:`VariantSet.selection` uses, and for the same two hard reasons: it
    #: round-trips through JSON-RPC and through a generated docstring, and
    #: ``scistackplot`` must keep working with no scidb installed.
    #:
    #: Stored as a tuple of pairs rather than a dict because this class is
    #: frozen AND hashable — ``BaseSource.get_table`` memoizes on a tuple of
    #: these, so a dict field would make the memo key unhashable. Construct it
    #: with a dict (``FactorVariable("Demographics", "Sex", {"CodeIsLatest":
    #: True})``); read it back through :attr:`selection`.
    #:
    #: Empty means **nothing said**, not "latest": every variant of the variable
    #: contributes, and where that leaves two labels for one schema location the
    #: join warns and takes the first. The picker always writes an explicit pin
    #: (defaulting to latest), so the empty case is a hand-written spec or one
    #: predating this field.
    variant: tuple[tuple[str, Any], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "variant", _hashable_selection(self.variant))

    @property
    def selection(self) -> dict[str, Any]:
        """:attr:`variant` as the dict every selection consumer expects."""
        return {key: value for key, value in self.variant}

    @property
    def factor_name(self) -> str:
        """The column this factor occupies in the long table.

        The **column's own name**, not ``Variable.column``: a plot endpoint
        receives the same column under that name from ``as_table``
        (``scifor.foreach._prepare_input`` keeps schema keys plus the selected
        column), so the interactive path and the generated code agree with no
        rename on either side. A name that collides with something already in
        the frame is refused where the join happens — never silently merged
        over.
        """
        return self.column or self.variable

    @property
    def label(self) -> str:
        """How the GUI names it — qualified, because a bare column name does not
        say which variable it came from once two sheets are in play."""
        return f"{self.variable}.{self.column}" if self.column else self.variable

    def to_dict(self) -> dict:
        return {
            "variable": self.variable,
            "column": self.column,
            # A dict on the wire, a tuple in memory. JSON has no tuples, and the
            # GUI edits this as an object like every other selection.
            "variant": self.selection,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "FactorVariable":
        return cls(
            variable=raw["variable"],
            column=raw.get("column"),
            variant=raw.get("variant") or (),
        )


def _hashable_selection(raw: Any) -> tuple[tuple[str, Any], ...]:
    """A variant selection as a hashable, order-stable tuple of pairs.

    Accepts the dict form callers naturally write and the tuple form a
    round-trip hands back. **List values become tuples**: a selection may name
    several levels (``{"bandpass.low_hz": ["20", "50"]}`` — membership, the same
    semantics ``scidb.database._match_branch_param`` gives a list-valued
    ``Variant`` kwarg), and a list inside a frozen dataclass makes the whole
    thing unhashable, which would break ``get_table``'s memo the moment anyone
    pinned a subcube.

    Sorted by key so two selections written in different orders are the same
    object — and therefore the same cache key.
    """
    if not raw:
        return ()
    items = raw.items() if isinstance(raw, dict) else raw
    normalised = []
    for key, value in items:
        if isinstance(value, (list, tuple, set, frozenset)):
            # Sorted for the same reason the keys are: {"20", "50"} and
            # {"50", "20"} select identically and must not be two cache keys.
            # Tuples included, so normalising is idempotent — this runs again on
            # every round-trip through `from_dict`.
            value = tuple(sorted(value, key=str))
        normalised.append((str(key), value))
    return tuple(sorted(normalised, key=lambda pair: pair[0]))


def _factor_variable_from_raw(raw: Any) -> FactorVariable:
    """Read one ``factor_variables`` entry, refusing the old flat form loudly.

    Grouping variables used to be bare names. A spec written before columns
    existed still parses everywhere else, so the failure would otherwise be a
    ``TypeError: string indices must be integers`` from three frames down —
    say what it is and how to fix it instead.
    """
    if isinstance(raw, str):
        raise ValueError(
            f"factor_variables entry {raw!r} is a bare variable name, which "
            f"this version no longer reads. Write it as "
            f'{{"variable": "{raw}"}} (or {{"variable": "{raw}", "column": '
            f'"SomeColumn"}} for one column of a wide table), or re-tick the '
            f"grouping in Plot Studio's Grouping section."
        )
    return FactorVariable.from_dict(raw)


@dataclass(frozen=True)
class Aggregation:
    """Centre and spread over the SAMPLE — the outermost collapsed key's
    levels, after every inner collapse (docs/claude/grouping-and-collapse.md).

    ``pooled`` switches the collapse chain off: every collapsed key is dropped
    in ONE groupby and the sample is every pooled row, so a subject with more
    trials weighs more ("weight by N"). Off by default because the nested,
    unweighted mean is what a methods section means by "mean across
    subjects"; on is the deliberate alternative, and the checkbox says which.
    A property of the whole chain, not of one key, which is why it lives here
    and not on a role.
    """

    statistic: Statistic = Statistic.MEAN
    error: ErrorBand = ErrorBand.SD
    pooled: bool = False


@dataclass(frozen=True)
class FacetOptions:
    """
    How faceted panels are arranged: a grid of a known size, plus one ordering
    rule per row and per column slot.

    ``n_rows``/``n_cols`` size the grid; setting either one computes the other
    from the panel count (:func:`grid_shape_for`), so the user names a width and
    the height follows. Each slot may then carry a :class:`Matcher` that claims
    the panels whose label it matches, which is what lets an EMG figure read as
    left/right x muscle group instead of an arbitrary 4-wide flow. Blank slots
    take whatever is left over, in order.
    """

    #: Grid size. None means "compute me from the other one and the panel count".
    n_rows: int | None = None
    n_cols: int | None = None
    #: One entry per row / column slot; blank entries are unset (see Matcher.is_blank).
    rows: list[Matcher] = field(default_factory=list)
    cols: list[Matcher] = field(default_factory=list)
    share_x: bool = True
    # NOTE: there is no `share_y`. It moved to `PlotSpec.y_axis` (:class:`YAxis`)
    # and became a question with more than two answers — "which factors separate
    # the limits" rather than "do the facets share them". `share_y=True` is
    # `YAxis(scope=[])` and `share_y=False` is a scope naming every panel
    # factor; keeping both would be two controls deciding one thing.

    @property
    def row_rules(self) -> list[Matcher]:
        """Row slots that actually claim panels."""
        return [m for m in self.rows if not m.is_blank]

    @property
    def col_rules(self) -> list[Matcher]:
        return [m for m in self.cols if not m.is_blank]

    @property
    def has_rules(self) -> bool:
        return bool(self.row_rules or self.col_rules)


@dataclass(frozen=True)
class YAxis:
    """What the y axis spans — and, more importantly, **what separates spans**.

    The old control was ``FacetOptions.share_y``: one boolean, all the facets of
    a figure share limits or they do not, with figures always scaled to
    themselves. That cannot express the two things a reader actually needs —
    "every plot in this study on one scale, so I can compare them" and
    "autoscale this panel, so I can see its shape" — let alone anything between.

    So limits are **split by a set of factors**:

    ================================ ==========================================
    ``scope``                        one limit per…
    ================================ ==========================================
    ``[]``                           the whole dataset — every panel, every
                                     figure, one range
    ``["subject"]``                  subject; all that subject's facets share it
    ``["subject", "ColName"]``       subject AND facet — true per-panel
                                     autoscale
    ================================ ==========================================

    Only factors that **separate panels** may appear: ITERATE (a factor per
    figure) and FACET (a factor per subplot). A GROUP or COLLAPSE factor lives
    *within* a panel, so splitting on it would ask one axis to have two ranges;
    :func:`~scistackplot.ylimits.eligible_scope` drops those and says so rather
    than failing or silently obeying.

    ``minimum``/``maximum`` override whatever ``scope`` computed, independently:
    a floor of 0 with a computed ceiling is a normal thing to want. They are
    spelled out rather than ``min``/``max`` because those shadow builtins in
    every comprehension that touches them.
    """

    #: Panel factors that separate limits. Empty means one range for everything.
    scope: list[str] = field(default_factory=list)
    minimum: float | None = None
    maximum: float | None = None

    @property
    def is_manual(self) -> bool:
        """Whether BOTH ends are pinned — the case that needs no data at all."""
        return self.minimum is not None and self.maximum is not None

    def to_dict(self) -> dict:
        return {
            "scope": list(self.scope),
            "minimum": self.minimum,
            "maximum": self.maximum,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "YAxis":
        return cls(
            scope=list(raw.get("scope") or []),
            minimum=_as_float(raw.get("minimum")),
            maximum=_as_float(raw.get("maximum")),
        )


def _as_float(value: Any) -> float | None:
    """A y-limit bound, or None for "compute it".

    An empty box in the GUI arrives as ``""``, and ``float("")`` raises — which
    would make clearing a limit an error rather than the way you ask for the
    computed one.
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class VariantSet:
    """One named variant: a label, and the variant coordinates it selects.

    This is the unit the Plot Studio's Variants section edits — one row, one
    :class:`VariantSet` — and the unit a scientist writes by hand as
    ``scistackplotdb.variant_set("baseline", Variant(X, code_version="v1"))``.

    ``selection`` is keyed by **frame column** (``"Code:bandpass"``,
    ``"bandpass.low_hz"``) rather than by a ``scidb.Variant`` object, and that
    is deliberate: a spec has to survive JSON-RPC and the docstring round trip
    (``codegen.extract_spec``), and this package must keep working with no scidb
    installed at all — the CSV source depends on that. Translating a
    ``scidb.Variant`` into these keys is scistackplotdb's job, in the layer that
    knows how scidb namespaces branch params.

    A value may be:

    * a level (``"20"``) or a list of them (``["20", "50"]`` — "any of these",
      the same subcube rule the popup's checkboxes produce);
    * ``"latest"`` on a code axis, resolved against the data rather than
      hard-coded to an ordinal — see :func:`~scistackplot.variants.resolve_selection`.

    ``name`` may be None, meaning "call me whatever my selection says". The GUI
    keeps it None until the user types over the auto label, so a label stays
    honest while the selection is still being edited.
    """

    name: str | None = None
    selection: dict[str, Any] = field(default_factory=dict)
    #: Which variable this row draws from. ``None`` means the plot's primary
    #: measure (``PlotSpec.measures[0]``).
    #:
    #: This is what makes "plot Raw against Filtered" the same feature as "plot
    #: v1 against v2": a row is one series, and a series is a variable plus a
    #: region of that variable's variant space. Rows over different variables
    #: stack into the same ``Variant`` factor, so they take a colour or a facet
    #: like any other level, and the export already had the shape for it — one
    #: ``for_each`` input per row (see ``codegen.variant_params``).
    #:
    #: It also removes an ambiguity that would otherwise be a silent wrong
    #: figure. ``resolve_selection`` drops selection keys naming a column the
    #: frame lacks — right for a stale spec, but with two variables in one frame
    #: a row pinning ``Code:filterEMG=v1`` names nothing about ``Force`` and
    #: would claim every ``Force`` row too, drawing it once per variant. Naming
    #: the variable makes "not applicable here" and "stale" distinguishable.
    variable: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "selection": dict(self.selection),
            "variable": self.variable,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "VariantSet":
        return cls(
            name=raw.get("name"),
            selection=dict(raw.get("selection") or {}),
            variable=raw.get("variable"),
        )


@dataclass(frozen=True)
class StyleOptions:
    palette: str | None = None
    #: Inches — the SAVED figure and the generated code. The interactive preview
    #: fills its pane and does not read these (see docs/claude/figure-size.md).
    width: float = 8.0
    height: float = 6.0
    #: Points, matplotlib's ``font.size``; every other text size (axis labels,
    #: ticks, legend, title) is relative to it, so one number scales them all.
    #: 14 rather than matplotlib's 10: at 8 x 6 in the default was unreadable
    #: once the figure sat in a slide or a two-column page (user, 2026-09-16).
    #: The plotly preview uses the same number as px, so the setting is visible
    #: before anything is saved.
    font_size: float = 14.0
    log_x: bool = False
    log_y: bool = False
    title: str | None = None
    x_label: str | None = None
    y_label: str | None = None
    marker_size: float = 36.0
    alpha: float = 0.85
    #: Hide the labels of an x layer that is also the colour layer while the
    #: legend lists the same levels in the same colours (tick labels or a
    #: bracket row). Opt-in (user, 2026-09-23): the ticks and brackets stay.
    hide_legend_ticks: bool = False
    #: The x tick labels' settings, fixed by the user; None = fitted
    #: (``ticklabels.fit_labels``). Rotation in degrees (0 / 45 / 90), font in
    #: points, ``tick_every`` = show every k-th label (1 = all). A fixed value
    #: is kept even where it overlaps; the fit then reports that it does.
    tick_rotation: int | None = None
    tick_font_size: float | None = None
    tick_every: int | None = None


@dataclass(frozen=True)
class PlotSpec:
    """
    A complete, serializable plot description.

    ``measures[0]`` is the y measure. Plotting several variables together is
    :attr:`variant_sets`' job — one row per series — not a longer ``measures``
    list: rows stack into the ``Variant`` factor, and a factor can take a role.
    """

    measures: list[str]
    #: Variable supplying the x axis of a relational (x–y) plot, or None.
    #:
    #: A field of its own rather than ``measures[1]``, because it is a different
    #: operation from an overlaid series and the positional form hid that. An x
    #: measure is a **wide join** — one x value per row of y — while overlaid
    #: variables **stack long** into one value column. Conflating them meant
    #: "the second measure" silently meant one or the other depending on
    #: context. When set, grouping layers are series, never ticks.
    x_measure: str | None = None
    roles: dict[str, Role] = field(default_factory=dict)
    #: Order of the ``GROUP`` factors, **innermost first**: the first entry is
    #: the mark's own identity, each later one wraps around it. What that
    #: means per kind — nested x ticks, a series id, the spaghetti lines — is
    #: ``roles.tick_layers`` / ``roles.series_layers``' business.
    #:
    #: Membership is the roles dict (who holds ``Role.GROUP``); this is only
    #: the order, so assigning a role can never produce an invalid spec — a
    #: name here that no longer holds GROUP is ignored, and a GROUP-holder
    #: missing from here is placed by the data's own nesting.
    #: :meth:`ordered_groups` is the one place those two are reconciled.
    groups: list[str] = field(default_factory=list)
    #: The grouping layer labelled by a legend instead of by tick / series
    #: label — any entry of :attr:`groups`, or None. ``validate`` refuses a
    #: name that is not a grouping layer.
    color: str | None = None
    kind: PlotKind = PlotKind.SCATTER
    aggregate: Aggregation = field(default_factory=Aggregation)
    #: How a 1-D measure's CELLS are reduced to one value each when a scalar
    #: kind is selected for it — the "measure of centre" a violin of per-trial
    #: vectors is drawn from. See :mod:`scistackplot.cell`.
    #:
    #: There is deliberately no "on/off" field: the KIND decides
    #: (:data:`SCALAR_KINDS`), so ``cell_statistic=median`` with ``kind=line``
    #: — a state a checkbox would allow and then have to adjudicate — simply
    #: means nothing and does nothing.
    #:
    #: Named for the cell, not "collapse", because *Collapse* is the role that
    #: averages a factor's LEVELS away (``Role.COLLAPSE``) and the two are
    #: different questions: median within each trial, mean across trials is a
    #: perfectly ordinary thing to ask for. Separate from
    #: ``aggregate.statistic`` for the same reason.
    cell_statistic: Statistic = Statistic.MEAN
    index_column: str | None = None
    #: "Show sample": the collapsed keys whose data is overlaid as points on a
    #: categorical kind (bar, box, violin, scatter, strip — which draw the
    #: sample, so showing the sample key itself repeats the marks; a deeper key
    #: adds information). The collapse chain (``roles.collapse_order``, deepest
    #: first) is cut BEFORE the deepest key named here and what is left is
    #: drawn as one point per row inside its mark — ``["trial"]`` shows every
    #: trial of every subject with cycles averaged within it; ``["cycle"]``
    #: shows the raw data. A deeper key implies the shallower collapsed keys
    #: (they are its identity), so ``["trial"]`` and ``["subject", "trial"]``
    #: draw the same overlay. ``roles.overlay_steps`` is the one reader.
    #:
    #: Inert, like :attr:`cell_statistic`, on a kind that cannot carry an
    #: overlay (line, band, heatmap, spaghetti) or when nothing is collapsed:
    #: the names are kept, the capability report says why nothing is drawn.
    show_sample: list[str] = field(default_factory=list)
    #: Whether the overlay points sharing every shown key's value are joined
    #: across the x positions of a panel. ``None`` (the default) is
    #: AUTOMATIC: joined when the deepest shown key sits above the grouping
    #: layers in the hierarchy — a subject recurs at every session, so its
    #: points are repeated measures; a trial belongs to one session, so they
    #: are not. ``roles.overlay_join`` is the one statement of that rule;
    #: ``True`` / ``False`` override it.
    join_sample: bool | None = None
    #: The SHOWN key whose levels colour the overlay points (and the lines
    #: joining them), independently of the marks' colour: bars coloured by
    #: intervention group, one colour per subject on top. ``None`` (the
    #: default) paints a point in its mark's colour. Inert, like a
    #: :attr:`show_sample` entry, when the key is not shown right now;
    #: ``roles.overlay_color`` is the one reader, ``validate`` refuses a name
    #: that is no factor at all.
    sample_color: str | None = None
    #: Whether the overlay's own colour levels (:attr:`sample_color`) are
    #: listed in the legend. Off, the legend reads as though nothing were
    #: shown — the points keep their colours. ``roles.overlay_in_legend`` is
    #: the one reader.
    sample_in_legend: bool = True
    facet: FacetOptions = field(default_factory=FacetOptions)
    #: What the y axis spans, and what separates spans. See :class:`YAxis`.
    y_axis: YAxis = field(default_factory=YAxis)
    style: StyleOptions = field(default_factory=StyleOptions)
    filters: list[Filter] = field(default_factory=list)
    #: Which schema locations to draw. Written by the schema location picker,
    #: which REPLACED the flat per-key pickers — see :class:`LocationFilter` for
    #: why a set of ``Filter``s cannot express the same selection.
    location_filter: LocationFilter = field(default_factory=LocationFilter)
    #: Variables joined in as FACTORS rather than plotted — a subject-level
    #: ``Condition`` holding stim/sham, say, or one column of a wide
    #: demographics table. They classify as CATEGORICAL and so are rightly
    #: refused as measures; as factors they take a role like any other and give
    #: you the grouping the data already records.
    factor_variables: list[FactorVariable] = field(default_factory=list)
    #: Factors derived by bucketing another factor's levels.
    level_groups: list[LevelGroup] = field(default_factory=list)
    #: Named variants to plot — one entry per row of the GUI's Variants section.
    #:
    #: One entry is a pin: the figure shows that variant and nothing else.
    #: Several entries are a **comparison**: a synthetic ``Variant`` factor
    #: appears with one level per entry, and it takes a role like any other
    #: factor (colour, facet, separate figures). That is the difference between
    #: "show me the current results" and "show me v1 against v3", expressed by
    #: adding a row rather than by a different control.
    #:
    #: Empty means no selection at all — every variant in the data, each variant
    #: factor still needing a role of its own (``validate`` refuses to pool
    #: them silently).
    variant_sets: list[VariantSet] = field(default_factory=list)

    # ---- convenience accessors ------------------------------------------

    @property
    def y_measure(self) -> str:
        return self.measures[0]

    def variant_variables(self) -> list[str]:
        """Every variable this spec plots, primary first, in row order.

        The union a source has to load and stack. A row without a ``variable``
        draws from the primary measure, so the primary is always present.
        """
        names = list(self.measures[:1])
        for variant in self.variant_sets:
            if variant.variable and variant.variable not in names:
                names.append(variant.variable)
        return names

    def ordered_groups(
        self,
        roles: dict[str, Role] | None = None,
        depths: dict[str, int] | None = None,
    ) -> list[str]:
        """The grouping layers, innermost first.

        Reconciles two sources that are edited independently — which factors
        hold ``Role.GROUP`` (moving a factor between the panes) and what order
        they nest in (a list the user reorders). Names that no longer hold
        GROUP are dropped, and GROUP-holders the order never mentioned are
        placed after the ones it did, so neither widget can put the spec in a
        state the other rejects.

        ``roles`` defaults to the spec's own; pass completed roles when the
        table may have defaulted some.

        ``depths`` (``LongTable.factor_depths``) places the holders this spec
        never ordered by the data's own nesting: **deeper keys inside
        shallower**, which in an innermost-first list means deeper first — a
        subject-level ``InterventionGroup`` wraps the sessions rather than the
        other way round. A factor with no depth (a variant axis, a derived
        bucket) is not in the hierarchy at all; it goes innermost, which keeps
        the derived thing inside the recorded ones — the readable arrangement
        for "v1 against v2 inside each session".

        An explicit :attr:`groups` always wins: depth is where to *start*, not
        an order the user cannot override.
        """
        holders = [
            name
            for name, role in (roles if roles is not None else self.roles).items()
            if role is Role.GROUP
        ]

        def depth_rank(depth: int | None) -> tuple[int, int]:
            """Innermost-first rank. ``None`` (not in the hierarchy) sorts
            first — innermost; then deeper before shallower. Mirrored by
            `depthRank` in `scistack-gui/frontend/.../xLayers.ts`."""
            return (0, 0) if depth is None else (1, -depth)

        ordered = [name for name in self.groups if name in holders]
        rest = [name for name in holders if name not in ordered]
        if depths:
            # Stable: equal depths (a schema key and a factor variable recorded
            # at the same level) keep declaration order rather than swapping on
            # a dict rebuild.
            rest.sort(key=lambda name: depth_rank(depths.get(name)))
        ordered.extend(rest)
        return ordered

    def factors_with_role(self, role: Role) -> list[str]:
        """Factors carrying ``role``, in the spec's declared order."""
        return [name for name, r in self.roles.items() if r == role]

    def first_with_role(self, role: Role) -> str | None:
        found = self.factors_with_role(role)
        return found[0] if found else None

    @property
    def iterate_factors(self) -> list[str]:
        return self.factors_with_role(Role.ITERATE)

    @property
    def collapse_factors(self) -> list[str]:
        """Factors averaged away, in the spec's declared order — NOT the
        collapse order, which is ``roles.collapse_order``'s to decide."""
        return self.factors_with_role(Role.COLLAPSE)

    def with_roles(self, **roles: Role) -> "PlotSpec":
        """Return a copy with role assignments merged in (for tests/GUI edits)."""
        merged = dict(self.roles)
        merged.update(roles)
        return replace(self, roles=merged)

    # ---- serialization ---------------------------------------------------

    def to_dict(self) -> dict:
        raw = asdict(self)
        raw["roles"] = {k: str(v) for k, v in self.roles.items()}
        raw["kind"] = str(self.kind)
        raw["cell_statistic"] = str(self.cell_statistic)
        raw["aggregate"] = {
            "statistic": str(self.aggregate.statistic),
            "error": str(self.aggregate.error),
            "pooled": self.aggregate.pooled,
        }
        raw["facet"] = {
            "n_rows": self.facet.n_rows,
            "n_cols": self.facet.n_cols,
            "share_x": self.facet.share_x,
            "rows": [_matcher_to_dict(m) for m in self.facet.rows],
            "cols": [_matcher_to_dict(m) for m in self.facet.cols],
        }
        raw["y_axis"] = self.y_axis.to_dict()
        raw["location_filter"] = self.location_filter.to_dict()
        raw["variant_sets"] = [s.to_dict() for s in self.variant_sets]
        raw["level_groups"] = [g.to_dict() for g in self.level_groups]
        raw["factor_variables"] = [f.to_dict() for f in self.factor_variables]
        raw["groups"] = list(self.groups)
        raw["show_sample"] = list(self.show_sample)
        # TOML has no null; drop empty optionals so a round trip is stable.
        return _drop_nulls(raw)

    @classmethod
    def from_dict(cls, raw: dict) -> "PlotSpec":
        _refuse_legacy_spec(raw)
        agg = raw.get("aggregate") or {}
        return cls(
            measures=list(raw["measures"]),
            x_measure=raw.get("x_measure"),
            roles={k: Role(v) for k, v in (raw.get("roles") or {}).items()},
            groups=list(raw.get("groups") or []),
            color=raw.get("color"),
            kind=PlotKind(raw.get("kind", PlotKind.SCATTER)),
            cell_statistic=Statistic(raw.get("cell_statistic", Statistic.MEAN)),
            aggregate=Aggregation(
                statistic=Statistic(agg.get("statistic", Statistic.MEAN)),
                error=ErrorBand(agg.get("error", ErrorBand.SD)),
                pooled=bool(agg.get("pooled", False)),
            ),
            index_column=raw.get("index_column"),
            show_sample=[str(n) for n in (raw.get("show_sample") or [])],
            join_sample=_optional_bool(raw.get("join_sample")),
            sample_color=raw.get("sample_color"),
            sample_in_legend=bool(raw.get("sample_in_legend", True)),
            facet=_facet_from_dict(raw.get("facet") or {}),
            y_axis=YAxis.from_dict(raw.get("y_axis") or {}),
            style=StyleOptions(**(raw.get("style") or {})),
            filters=[Filter(**f) for f in (raw.get("filters") or [])],
            location_filter=LocationFilter.from_dict(raw.get("location_filter") or {}),
            factor_variables=[
                _factor_variable_from_raw(f)
                for f in (raw.get("factor_variables") or [])
            ],
            level_groups=[
                LevelGroup.from_dict(g) for g in (raw.get("level_groups") or [])
            ],
            variant_sets=[
                VariantSet.from_dict(s) for s in (raw.get("variant_sets") or [])
            ],
        )

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> "PlotSpec":
        return cls.from_dict(json.loads(text))

    def to_toml(self) -> str:
        try:
            import tomli_w
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise ImportError(
                "Writing a PlotSpec as TOML needs 'tomli-w' "
                "(pip install scistackplot[dev]). PlotSpec.to_json() is "
                "always available."
            ) from exc
        return tomli_w.dumps(self.to_dict())

    @classmethod
    def from_toml(cls, text: str) -> "PlotSpec":
        try:
            import tomllib
        except ImportError:  # pragma: no cover - Python 3.10
            try:
                import tomli as tomllib  # type: ignore[no-redef]
            except ImportError as exc:
                raise ImportError(
                    "Reading a PlotSpec from TOML needs Python 3.11+ "
                    "(tomllib) or the 'tomli' package."
                ) from exc
        return cls.from_dict(tomllib.loads(text))


class LegacySpecError(ValueError):
    """A saved spec written in the pre-2026-09-17 role vocabulary.

    Refused rather than translated, on purpose. ``free`` on a bar meant "the
    sample" — which is now ``collapse`` — and ``x_layers`` ran outermost-first
    where ``groups`` runs innermost-first, so any mapping would be wrong in
    exactly the common cases. The message names the doc that says how to
    re-express the figure (feedback_beta_no_deprecation: renames are clean
    breaks).
    """


def _refuse_legacy_spec(raw: dict) -> None:
    legacy_keys = [key for key in ("x_layers", "collapse_statistic") if key in raw]
    legacy_roles = {
        name: value
        for name, value in (raw.get("roles") or {}).items()
        if value in LEGACY_ROLE_VALUES
    }
    if not legacy_keys and not legacy_roles:
        return
    parts = []
    if legacy_roles:
        parts.append(
            "role(s) " + ", ".join(f"{n}={v!r}" for n, v in legacy_roles.items())
        )
    if legacy_keys:
        parts.append("key(s) " + ", ".join(repr(k) for k in legacy_keys))
    raise LegacySpecError(
        f"This plot spec uses the old role vocabulary ({'; '.join(parts)}), "
        f"which this version no longer reads. Roles are now 'group' (ordered "
        f"by 'groups', innermost first, one of them optionally 'color'), "
        f"'facet', 'iterate' and 'collapse'; 'collapse_statistic' is "
        f"'cell_statistic'. Re-express the figure in Plot Studio or edit the "
        f"spec by hand — see docs/claude/grouping-and-collapse.md."
    )


def grid_shape_for(
    n_panels: int, n_rows: int | None = None, n_cols: int | None = None
) -> tuple[int, int]:
    """
    The subplot grid for ``n_panels`` panels, given whatever the user pinned.

    One function, called from ``reduce`` and reported back to the GUI, so that
    "I said 2 columns, where did 3 rows come from?" has exactly one answer.
    Naming one dimension computes the other; naming neither falls back to a
    roughly square grid at most 4 wide, and a handful of panels stay in a single
    horizontal row.

    Placement may still grow the result (see ``reduce._assign_grid``) rather than
    let two panels share a cell.
    """
    count = max(1, int(n_panels))
    rows = int(n_rows) if n_rows and n_rows > 0 else None
    cols = int(n_cols) if n_cols and n_cols > 0 else None

    if rows and cols:
        return rows, cols
    if cols:
        return math.ceil(count / cols), cols
    if rows:
        return rows, math.ceil(count / rows)

    # Neither pinned: a 13-field struct wants a grid, 3 muscles want one row.
    auto_cols = min(4, math.ceil(math.sqrt(count))) if count > 3 else count
    auto_cols = max(1, auto_cols)
    return math.ceil(count / auto_cols), auto_cols


def _matcher_to_dict(matcher: Matcher) -> dict:
    return {"op": str(matcher.op), "value": matcher.value, "label": matcher.label}


def _facet_from_dict(raw: dict) -> FacetOptions:
    return FacetOptions(
        n_rows=raw.get("n_rows"),
        n_cols=raw.get("n_cols"),
        rows=[_matcher_from_dict(m) for m in (raw.get("rows") or [])],
        cols=[_matcher_from_dict(m) for m in (raw.get("cols") or [])],
        share_x=raw.get("share_x", True),
    )


def _matcher_from_dict(raw: dict) -> Matcher:
    return Matcher(
        op=MatchOp(raw.get("op", MatchOp.CONTAINS)),
        value=raw.get("value", ""),
        label=raw.get("label"),
    )


def _drop_nulls(obj: Any) -> Any:
    """Recursively drop None values so JSON and TOML round-trip identically."""
    if isinstance(obj, dict):
        return {k: _drop_nulls(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_drop_nulls(v) for v in obj]
    return obj


def _optional_bool(value: Any) -> bool | None:
    """``join_sample`` as saved: absent (TOML has no null, so ``to_dict``
    drops the automatic setting) is None; anything else is its truth."""
    return None if value is None else bool(value)
