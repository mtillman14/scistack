"""StatsSpec: everything a StatStudio table is, as data (S10, S16, S18).

- ``design`` is a plot-layer :class:`~scistackplot.PlotSpec`, the design
  carrier (S11). Only its design fields (``scistackplot.design``) mean
  anything here. The stats layer reads rows at a fixed analysis kind
  (``tables.ANALYSIS_KIND``), so the carrier's display fields are ignored.
- ``dvs``: the dependent variables, one row block each (S16). An empty list
  means the design's own plotted variable. ``DV.columns`` picks columns of a
  multi-column Variable, each column its own DV.
- ``descriptives``: which statistics the Descriptives tab shows.
- ``comparisons``: the analyses on the Comparisons tab.
- ``linked_plot``: the saved plot this spec syncs with. The link lives here
  only (S11).

``to_dict`` / ``from_dict`` are the wire format. ``from_dict`` is strict: an
unknown key or a bad value raises :class:`StatsSpecError`, naming it. Salvage
for opening old saves is a stage 9 concern.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, replace
from typing import Any

from scistackplot import PlotSpec

FORMAT = 1

FAMILIES = ("parametric", "nonparametric")
TAILS = ("two-sided", "greater", "less")
#: Correction family scopes (P5). ``stratum`` = one DV × one stratum, the
#: default (S13); ``context`` = one combination of the outer grouping layers.
FAMILY_SCOPES = ("none", "context", "stratum", "dv", "table")
#: How a comparison's difference is spread (S14).
DIFFERENCE_SPREADS = ("ci", "se", "sd_diff")
FACTOR_KINDS = ("within", "between")


class StatsSpecError(ValueError):
    """A stored or built StatsSpec is not valid; the message names the field."""


def _check(value: Any, allowed: tuple, name: str) -> None:
    if value not in allowed:
        raise StatsSpecError(f"{name} must be one of {list(allowed)}, got {value!r}")


@dataclass(frozen=True)
class DV:
    """One dependent variable: a Variable, or chosen columns of one."""

    variable: str
    #: ``ColName`` columns of a multi-column Variable; None = the variable as
    #: it is (a single value, or every field as the design treats it).
    columns: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if not self.variable:
            raise StatsSpecError("a DV needs a variable name")
        if self.columns is not None:
            object.__setattr__(self, "columns", tuple(str(c) for c in self.columns))
            if not self.columns:
                raise StatsSpecError(f"DV {self.variable!r}: columns=[] selects nothing (use None for all)")

    @property
    def key(self) -> str:
        return self.variable if self.columns is None else f"{self.variable}[{', '.join(self.columns)}]"

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"variable": self.variable}
        if self.columns is not None:
            out["columns"] = list(self.columns)
        return out

    @classmethod
    def from_dict(cls, raw: dict) -> "DV":
        _only(raw, {"variable", "columns"}, "dv")
        columns = raw.get("columns")
        return cls(variable=str(raw.get("variable", "")), columns=tuple(columns) if columns is not None else None)


#: Cell layouts of the Descriptives table: the statistics one cell shows.
CELLS = ("mean_sd", "mean_sem", "mean_ci95", "median_iqr", "none")


@dataclass(frozen=True)
class DescriptiveSpec:
    """What the Descriptives tab shows (S18). Numbers come from csv-stats."""

    #: The combined cell, e.g. ``mean_sd`` = "mean (SD)"; ``none`` = no
    #: combined cell, only the ``statistics`` rows.
    cell: str = "mean_sd"
    #: Extra rows per DV, by catalog id (``describe.STATISTICS``).
    statistics: tuple[str, ...] = ("n",)

    def __post_init__(self) -> None:
        _check(self.cell, CELLS, "descriptives.cell")
        object.__setattr__(self, "statistics", tuple(self.statistics))

    def to_dict(self) -> dict[str, Any]:
        return {"cell": self.cell, "statistics": list(self.statistics)}

    @classmethod
    def from_dict(cls, raw: dict) -> "DescriptiveSpec":
        _only(raw, {"cell", "statistics"}, "descriptives")
        return cls(cell=raw.get("cell", "mean_sd"), statistics=tuple(raw.get("statistics", ("n",))))


@dataclass(frozen=True)
class FormatSpec:
    """How numbers read in every table and export (one owner: ``format``)."""

    #: Decimals for values, spreads and differences. None = automatic per
    #: DV: enough to show two significant digits of its smallest spread
    #: (``format.auto_decimals``), so a table of metres and milliseconds
    #: both read well.
    decimals: int | None = None
    #: Decimals of p-values (APA: 3, with "< .001" below).
    p_decimals: int = 3
    #: Decimals of effect sizes.
    effect_decimals: int = 2
    #: Append * / ** / *** (p < .05 / .01 / .001) to adjusted p-values.
    stars: bool = False

    def __post_init__(self) -> None:
        if self.decimals is not None and not (0 <= self.decimals <= 8):
            raise StatsSpecError(f"formatting.decimals must be 0-8 or None, got {self.decimals!r}")
        if not (1 <= self.p_decimals <= 6) or not (0 <= self.effect_decimals <= 6):
            raise StatsSpecError("formatting: p_decimals must be 1-6 and effect_decimals 0-6")

    def to_dict(self) -> dict[str, Any]:
        out = {f.name: getattr(self, f.name) for f in fields(self)}
        return {k: v for k, v in out.items() if v is not None}

    @classmethod
    def from_dict(cls, raw: dict) -> "FormatSpec":
        _only(raw, {f.name for f in fields(cls)}, "formatting")
        return cls(**raw)


@dataclass(frozen=True)
class Analysis:
    """One analysis on the Comparisons tab.

    The tests are chosen from the design (``catalog.select_tests``) unless an
    override names one: within factors get paired tests, between factors
    independent ones, ``family`` picks parametric or rank-based. The choice and
    its reason are recorded in the report, never applied silently.
    """

    id: str = "a1"
    family: str = "parametric"
    #: Main effects and interactions over every grouping layer (secondary).
    omnibus: bool = True
    #: Pairwise comparisons of the innermost layer's levels within each
    #: combination of the outer layers (primary, S13).
    pairwise: bool = True
    omnibus_test: str | None = None
    pairwise_test: str | None = None
    #: Student instead of Welch for independent pairwise t-tests.
    equal_var: bool = False
    alpha: float = 0.05
    ci_level: float = 0.95
    tails: str = "two-sided"
    #: p-value correction over each family (csv-stats' methods).
    correction: str = "holm"
    family_scope: str = "stratum"
    #: Effect-size id to show (``effects.EFFECT_SIZES``); None = the design's default.
    effect_size: str | None = None
    difference_spread: str = "ci"
    #: ``{factor: "within"|"between"}`` over the derived classification (P4).
    overrides: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.id:
            raise StatsSpecError("an analysis needs an id")
        _check(self.family, FAMILIES, f"analysis {self.id}: family")
        _check(self.tails, TAILS, f"analysis {self.id}: tails")
        _check(self.family_scope, FAMILY_SCOPES, f"analysis {self.id}: family_scope")
        _check(self.difference_spread, DIFFERENCE_SPREADS, f"analysis {self.id}: difference_spread")
        for name, kind in self.overrides.items():
            _check(kind, FACTOR_KINDS, f"analysis {self.id}: override for {name!r}")
        if not (0 < self.alpha < 1) or not (0 < self.ci_level < 1):
            raise StatsSpecError(f"analysis {self.id}: alpha and ci_level must be between 0 and 1")
        if not (self.omnibus or self.pairwise):
            raise StatsSpecError(f"analysis {self.id}: turn on omnibus, pairwise or both")

    def to_dict(self) -> dict[str, Any]:
        out = {f.name: getattr(self, f.name) for f in fields(self)}
        out["overrides"] = dict(self.overrides)
        return {k: v for k, v in out.items() if v is not None}

    @classmethod
    def from_dict(cls, raw: dict) -> "Analysis":
        names = {f.name for f in fields(cls)}
        _only(raw, names, "analysis")
        return cls(**{k: (dict(v) if k == "overrides" else v) for k, v in raw.items()})


@dataclass(frozen=True)
class StatsSpec:
    design: PlotSpec
    dvs: tuple[DV, ...] = ()
    linked_plot: str | None = None
    descriptives: DescriptiveSpec = field(default_factory=DescriptiveSpec)
    comparisons: tuple[Analysis, ...] = ()
    variant_mode: str = "pinned"
    formatting: FormatSpec = field(default_factory=FormatSpec)

    def __post_init__(self) -> None:
        object.__setattr__(self, "dvs", tuple(self.dvs))
        object.__setattr__(self, "comparisons", tuple(self.comparisons))
        _check(self.variant_mode, ("pinned",), "variant_mode")  # sweep: stage 13
        ids = [a.id for a in self.comparisons]
        if len(ids) != len(set(ids)):
            raise StatsSpecError(f"analysis ids must be unique, got {ids}")
        keys = [d.key for d in self.dvs]
        if len(keys) != len(set(keys)):
            raise StatsSpecError(f"each DV may appear once, got {keys}")

    @property
    def targets(self) -> tuple[DV, ...]:
        """The DVs to analyse: ``dvs``, or the design's plotted variable."""
        return self.dvs or (DV(self.design.y_measure),)

    def with_design(self, design: PlotSpec) -> "StatsSpec":
        return replace(self, design=design)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "format": FORMAT,
            "design": self.design.to_dict(),
            "dvs": [d.to_dict() for d in self.dvs],
            "descriptives": self.descriptives.to_dict(),
            "comparisons": [a.to_dict() for a in self.comparisons],
            "variant_mode": self.variant_mode,
            "formatting": self.formatting.to_dict(),
        }
        if self.linked_plot is not None:
            out["linked_plot"] = self.linked_plot
        return out

    @classmethod
    def from_dict(cls, raw: dict) -> "StatsSpec":
        if not isinstance(raw, dict):
            raise StatsSpecError(f"a stats spec must be a table, got {type(raw).__name__}")
        _only(raw, {"format", "design", "dvs", "descriptives", "comparisons", "variant_mode", "linked_plot", "formatting"}, "stats spec")
        if raw.get("format") != FORMAT:
            raise StatsSpecError(f"stats spec format {raw.get('format')!r} is not {FORMAT}")
        if "design" not in raw:
            raise StatsSpecError("a stats spec needs a design")
        return cls(
            design=PlotSpec.from_dict(raw["design"]),
            dvs=tuple(DV.from_dict(d) for d in raw.get("dvs", [])),
            linked_plot=raw.get("linked_plot"),
            descriptives=DescriptiveSpec.from_dict(raw.get("descriptives", {})),
            comparisons=tuple(Analysis.from_dict(a) for a in raw.get("comparisons", [])),
            variant_mode=raw.get("variant_mode", "pinned"),
            formatting=FormatSpec.from_dict(raw.get("formatting", {})),
        )


def _only(raw: Any, allowed: set[str], what: str) -> None:
    if not isinstance(raw, dict):
        raise StatsSpecError(f"{what} must be a table, got {type(raw).__name__}")
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise StatsSpecError(f"{what}: unknown setting(s) {unknown}")
