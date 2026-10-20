"""The mixed model as an lme4 formula: ONE owner of what the text means
against a design (.claude/plan-statstudio-model-formula.md).

The user's text is the source of truth for the model (``Analysis.formula``,
the right side only; the left side is each DV). csv-stats owns the grammar
(``engine.parse_formula``); this module owns what the names in it mean
here:

- a **fixed** name is a factor of the design (a grouping layer, or one the
  formula adds) or a picked **covariate** (``Analysis.covariates``, by
  ``FactorVariable.factor_name``). The fixed part is one crossing
  (``a * b * x``): csv-stats fits the full factorial.
- a **random group** is a column of the analysis rows: the experimental unit
  (by its sample key, ``subject``, which is the composite ``UNIT`` column),
  a schema key composing the unit (``site``), or a fixed factor, in an
  interaction (``subject:time``).
- a **slope** is a fixed factor or a covariate of the model.

The design stays the one owner of the factors (shared by Descriptives, every
analysis and the plot link). :func:`check` with ``rebase=True`` (a run, a
display) rewrites the fixed factors from the design, keeping covariates and
random terms. With ``rebase=False`` (the popup's live check) a difference is
reported as ``add``/``drop``, and :func:`apply_to_design` writes it: a new
factor gets role Group, a left-out one Collapse (rows average over it, as the
figure would). The changed design then raises the plot-divergence banner.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any, Sequence

from scistacklog import Log

from scistackplot import FactorVariable, PlotSpec, Role

from .engine import EngineRefusal, FormulaSyntaxError, parse_formula, render_formula
from .tables import UNIT, AnalysisFrame

LAYER = "scistackstats"


@dataclass
class ModelCheck:
    """What a formula means here. ``errors`` (``{"message", "position",
    "length"}``, the span when known) make the model unusable; the tests
    are then refused with the first one."""

    text: str
    is_default: bool
    fixed_factors: list[str] = field(default_factory=list)
    covariates: list[str] = field(default_factory=list)
    #: Random terms in the formula's own names (``{"group", "terms", "correlated"}``).
    random: list[dict[str, Any]] = field(default_factory=list)
    add: list[str] = field(default_factory=list)
    drop: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    #: One entry per name, for the GUI's resolved-terms list.
    terms: list[dict[str, str]] = field(default_factory=list)
    unit: str | None = None

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def needs_mixed_model(self) -> bool:
        """Covariates or a random part other than ``(1 | unit)``: only a mixed
        model fits it, so the analysis' tests are lmm / lmm_pairwise."""
        return bool(self.covariates) or self.random != _default_random(self.unit)

    def engine_options(self) -> dict[str, Any]:
        """``covariates`` and ``random`` for csv-stats, the unit's name mapped
        to the composite ``UNIT`` column."""
        def col(name: str) -> str:
            return UNIT if name == self.unit else name

        return {
            "covariates": list(self.covariates),
            "random": [{"group": [col(g) for g in r["group"]], "terms": list(r["terms"]),
                        "correlated": r["correlated"]} for r in self.random],
        }

    def columns(self) -> list[str]:
        """The row columns the model reads beyond the DV and the factors."""
        out = list(self.covariates)
        for r in self.random:
            for name in (*r["group"], *r["terms"]):
                c = UNIT if name == self.unit else name
                if name != "(Intercept)" and c not in out:
                    out.append(c)
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text, "is_default": self.is_default, "fixed_factors": list(self.fixed_factors),
            "covariates": list(self.covariates), "random": [dict(r) for r in self.random],
            "add": list(self.add), "drop": list(self.drop), "errors": list(self.errors),
            "warnings": list(self.warnings), "terms": list(self.terms), "unit": self.unit,
            "needs_mixed_model": self.needs_mixed_model if self.ok else False,
        }


def _default_random(unit: str | None) -> list[dict[str, Any]]:
    return [{"group": [unit], "terms": ["(Intercept)"], "correlated": True}] if unit else []


def default_formula(factors: Sequence[str], unit: str | None) -> str:
    """The model StatStudio fits when the user wrote none: the factors
    crossed (outer first) + ``(1 | unit)``."""
    return render_formula(list(factors), _default_random(unit))


def _span(text: str, name: str) -> tuple[int, int]:
    """Where ``name`` is written in ``text`` (first occurrence, bare or in
    backticks), for an underline; ``(0, len(text))`` if not found."""
    for spelled in (f"`{name}`", name):
        m = re.search(rf"(?<![A-Za-z0-9_.`]){re.escape(spelled)}(?![A-Za-z0-9_.`])", text)
        if m:
            return m.start(), m.end() - m.start()
    return 0, max(1, len(text))


def check(
    text: str | None,
    *,
    factors: Sequence[str],
    available: Sequence[str],
    unit: str | None,
    unit_keys: Sequence[str],
    row_columns: Sequence[str],
    covariates: Sequence[FactorVariable] = (),
    strata: Sequence[str] = (),
    rebase: bool = True,
) -> ModelCheck:
    """What ``text`` (None = the default) means against the design.

    ``factors``: the design's grouping layers, outer first. ``available``:
    every factor the table offers (what the formula may add). ``row_columns``:
    the analysis rows' columns (random groups must be among them).
    ``covariates``: the analysis' picked covariates.
    """
    factors = list(factors)
    default = default_formula(factors, unit)
    source = text if text is not None else default
    try:
        parsed = parse_formula(source)
    except FormulaSyntaxError as exc:
        return ModelCheck(text=source, is_default=False, unit=unit,
                          errors=[{"message": exc.reason, "position": exc.position, "length": exc.length}])
    except EngineRefusal as exc:
        return ModelCheck(text=source, is_default=False, unit=unit, errors=[{"message": exc.reason}])

    out = ModelCheck(text=source, is_default=text is None, unit=unit)
    cov_names = [c.factor_name for c in covariates]
    errors, terms = out.errors, out.terms

    def error(message: str, name: str | None = None) -> None:
        pos, length = _span(source, name) if name else (0, max(1, len(source)))
        errors.append({"message": message, "position": pos, "length": length})

    if parsed["response"] is not None:
        error("write the right side only: the left side is each measure")
    named_factors, named_covs = [], []
    for name in parsed["fixed"]:
        if name in cov_names:
            named_covs.append(name)
            terms.append({"name": name, "role": "covariate",
                          "detail": next(c.label for c in covariates if c.factor_name == name)})
        elif name == unit:
            # Only the unit itself. A schema key ABOVE it (group in
            # group / subject) is constant within each unit: a between
            # factor, not the unit (scidb.log 2026-10-20 flagged group).
            error(f"{name} identifies the experimental unit; it belongs in a random term like (1 | {unit})", name)
        elif name in strata:
            error(f"{name} separates the analyses (each figure/panel is analysed on its own); "
                  f"it cannot be a model factor too", name)
        elif name in factors or name in available:
            named_factors.append(name)
        else:
            error(f"{name!r} is neither a factor of this design nor a picked covariate "
                  f"(pick it with Add covariate…)", name)

    if rebase:
        # The design owns the factors: a stored formula follows it.
        out.fixed_factors = factors
        if set(named_factors) != set(factors) and text is not None:
            Log.info("[stats model] formula factors %s rebased onto the design's %s", named_factors, factors,
                     layer=LAYER)
    else:
        out.fixed_factors = named_factors
        out.add = [f for f in named_factors if f not in factors]
        out.drop = [f for f in factors if f not in named_factors]
    out.covariates = named_covs
    for name in out.fixed_factors:
        terms.append({"name": name, "role": "factor",
                      "detail": "added to the design (role Group)" if name in out.add else "grouping layer"})
    for name in out.drop:
        terms.append({"name": name, "role": "dropped", "detail": "left out: rows average over it (role Collapse)"})
    unused = [c for c in cov_names if c not in named_covs]
    if unused:
        out.warnings.append(f"picked but not in the formula (dropped on Apply): {', '.join(unused)}")

    model_names = set(out.fixed_factors) | set(out.covariates)
    rows = set(row_columns)
    for r in parsed["random"]:
        for g in r["group"]:
            if g == unit or g in unit_keys:
                continue
            if g in out.fixed_factors and (g in rows or g in out.add):
                continue
            if g in strata:
                error(f"{g} separates the analyses: within one, it has a single level", g)
            elif g in available and g not in rows:
                error(f"{g} is averaged away in these rows (role Collapse); a random-effects group must be a "
                      f"column of the rows: the unit ({unit}), a key above it, or a model factor", g)
            else:
                error(f"{g!r} cannot group random effects here: use the unit ({unit or 'none: nothing is collapsed'}), "
                      f"a schema key above it, or a model factor (e.g. {unit or 'subject'}:{g})", g)
        for t in r["terms"]:
            if t == "(Intercept)":
                continue
            if t not in model_names:
                error(f"the random slope {t!r} is not a fixed factor or covariate of this model", t)
            elif not r["correlated"] and t in out.fixed_factors:
                error(f"uncorrelated (||) random slopes of a factor ({t}) are not supported; use |", t)
        group_text = ":".join(r["group"])
        terms.append({"name": group_text, "role": "group",
                      "detail": " + ".join("intercept" if t == "(Intercept)" else f"slope {t}" for t in r["terms"])
                      + ("" if r["correlated"] else ", uncorrelated")})
    if not parsed["random"]:
        error("a mixed model needs at least one random term, e.g. "
              f"(1 | {unit})" if unit else "a mixed model needs a random term, and these rows have no "
              "experimental unit (nothing is collapsed): collapse to a unit first")
    out.random = [dict(r) for r in parsed["random"]]

    # The canonical text: the fixed factors as the design has them (rebase)
    # or as written, then the covariates, then the random terms.
    try:
        out.text = render_formula([*out.fixed_factors, *out.covariates], out.random)
    except EngineRefusal:
        pass
    out.is_default = out.text == default
    Log.info("[stats model] %s -> %s; factors %s covariates %s random %d term(s)%s%s", source, out.text,
             out.fixed_factors, out.covariates, len(out.random),
             f"; add {out.add}" if out.add else "", f"; drop {out.drop}" if out.drop else "", layer=LAYER)
    if out.errors:
        Log.info("[stats model] %d problem(s): %s", len(out.errors), [e["message"] for e in out.errors], layer=LAYER)
    return out


def for_frame(formula: str | None, covariates: Sequence[FactorVariable], frame: "AnalysisFrame", *,
              available: Sequence[str] = (), rebase: bool = True) -> ModelCheck:
    """:func:`check` against one DV's analysis rows (``tables.AnalysisFrame``).
    ``available``: factors the formula may add beyond the rows' own (the
    table's factor names; the popup passes them)."""
    return check(
        formula, factors=frame.outer_first, available=[*available, *frame.factors],
        unit=frame.unit, unit_keys=[k for k in frame.unit_keys if k != frame.unit],
        row_columns=list(frame.rows.columns), covariates=covariates, strata=frame.strata, rebase=rebase,
    )


def apply_to_design(design: PlotSpec, add: Sequence[str], drop: Sequence[str]) -> PlotSpec:
    """The design with the formula's factor changes: ``add`` become Group
    (outermost), ``drop`` become Collapse (the rows average over them, as the
    figure's own Collapse role does)."""
    if not add and not drop:
        return design
    roles = dict(design.roles)
    groups = [g for g in design.groups if g not in drop]
    for name in drop:
        roles[name] = Role.COLLAPSE
    for name in add:
        roles[name] = Role.GROUP
        if name not in groups:
            groups.append(name)
    Log.info("[stats model] design from the formula: add %s, drop %s -> groups %s", list(add), list(drop), groups,
             layer=LAYER)
    return replace(design, roles=roles, groups=groups)


def bindings_in(formula_covariates: Sequence[str], picked: Sequence[FactorVariable]) -> tuple[FactorVariable, ...]:
    """The picked covariates the text references: the text is the truth, so
    a binding it no longer names is dropped on Apply."""
    keep = set(formula_covariates)
    return tuple(c for c in picked if c.factor_name in keep)
