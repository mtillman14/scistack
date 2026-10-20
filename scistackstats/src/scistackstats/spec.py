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

#: ``spm``: statistical parametric mapping on 1-D curves (S4), via spm1d.
#: "auto" (the default) runs the recommendation (catalog.recommend): the
#: data decide, with the evidence shown; the others pin a family.
FAMILIES = ("auto", "parametric", "nonparametric", "mixed_model", "spm")
#: How an analysis treats units missing some within level (user decision
#: 2026-10-19). "use_all": every observation used where the method allows it
#: (a mixed model); "complete_cases": incomplete units are left out of every
#: test of the analysis, named. Either way it is recorded and shown.
MISSING_POLICIES = ("use_all", "complete_cases")
#: What the GUI shows for each family and missing-data policy.
FAMILY_LABELS = {
    "auto": "Recommended from the data",
    "parametric": "Parametric (t-tests, ANOVA)",
    "nonparametric": "Rank-based (Wilcoxon, Mann–Whitney, Friedman, Kruskal–Wallis)",
    "mixed_model": "Linear mixed model",
    "spm": "SPM (1-D curves)",
}
MISSING_LABELS = {
    "use_all": "Use every observation (mixed model where needed)",
    "complete_cases": "Complete cases only (drop incomplete units, named)",
}
TAILS = ("two-sided", "greater", "less")
#: Correction family scopes (P5). ``stratum`` = one DV × one stratum, the
#: default (S13); ``context`` = one combination of the outer grouping layers.
#: ``variants``: the same comparison across every variant of a sweep (S7:
#: opt-in, never the default).
FAMILY_SCOPES = ("none", "context", "stratum", "dv", "table", "variants")
#: ``pinned``: the design's own variant selection (S3 default). ``sweep``: every
#: variant of every variant axis, each analysed separately (S3, S7).
VARIANT_MODES = ("pinned", "sweep")
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
    family: str = "auto"
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
    #: ``MISSING_POLICIES``; recorded in the report and every caption.
    missing: str = "use_all"

    def __post_init__(self) -> None:
        if not self.id:
            raise StatsSpecError("an analysis needs an id")
        _check(self.family, FAMILIES, f"analysis {self.id}: family")
        _check(self.missing, MISSING_POLICIES, f"analysis {self.id}: missing")
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


BAR_PAIRS = ("significant", "all", "adjacent")
BAR_LABELS = ("stars", "p")


@dataclass(frozen=True)
class OverlaySpec:
    """Brackets drawn back on the linked figure (S20, P6).

    The rule is stored, not the bars: bars are derived from the latest
    results at resolve time (``overlays.bars_for``). A bar the user hides is
    a hidden comparison id, never deleted.
    """

    show: bool = False
    #: Which analysis feeds the bars; None = the first.
    analysis: str | None = None
    #: ``significant`` (adjusted p < alpha), ``all``, or ``adjacent`` (levels
    #: next to each other in level order).
    pairs: str = "significant"
    #: ``stars`` (* / ** / ***) or ``p`` (the adjusted p, APA).
    label: str = "stars"
    hidden: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _check(self.pairs, BAR_PAIRS, "overlay.pairs")
        _check(self.label, BAR_LABELS, "overlay.label")
        object.__setattr__(self, "hidden", tuple(str(h) for h in self.hidden))

    def to_dict(self) -> dict[str, Any]:
        out = {f.name: getattr(self, f.name) for f in fields(self)}
        out["hidden"] = list(self.hidden)
        return {k: v for k, v in out.items() if v is not None}

    @classmethod
    def from_dict(cls, raw: dict) -> "OverlaySpec":
        _only(raw, {f.name for f in fields(cls)}, "overlay")
        return cls(**{k: (tuple(v) if k == "hidden" else v) for k, v in raw.items()})


@dataclass(frozen=True)
class StatsSpec:
    design: PlotSpec
    dvs: tuple[DV, ...] = ()
    linked_plot: str | None = None
    descriptives: DescriptiveSpec = field(default_factory=DescriptiveSpec)
    comparisons: tuple[Analysis, ...] = ()
    variant_mode: str = "pinned"
    formatting: FormatSpec = field(default_factory=FormatSpec)
    overlay: OverlaySpec = field(default_factory=OverlaySpec)

    def __post_init__(self) -> None:
        object.__setattr__(self, "dvs", tuple(self.dvs))
        object.__setattr__(self, "comparisons", tuple(self.comparisons))
        _check(self.variant_mode, VARIANT_MODES, "variant_mode")
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
            "overlay": self.overlay.to_dict(),
        }
        if self.linked_plot is not None:
            out["linked_plot"] = self.linked_plot
        return out

    @classmethod
    def from_dict(cls, raw: dict) -> "StatsSpec":
        if not isinstance(raw, dict):
            raise StatsSpecError(f"a stats spec must be a table, got {type(raw).__name__}")
        _only(raw, {"format", "design", "dvs", "descriptives", "comparisons", "variant_mode", "linked_plot", "formatting", "overlay"}, "stats spec")
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
            overlay=OverlaySpec.from_dict(raw.get("overlay", {})),
        )


def _only(raw: Any, allowed: set[str], what: str) -> None:
    if not isinstance(raw, dict):
        raise StatsSpecError(f"{what} must be a table, got {type(raw).__name__}")
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise StatsSpecError(f"{what}: unknown setting(s) {unknown}")
