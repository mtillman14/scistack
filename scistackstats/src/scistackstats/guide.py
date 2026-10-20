"""The facts StatStudio's step-by-step wizard explains (plan-statstudio-guided).

:func:`guide_facts` reads the spec's rows and designs (and, after a run, its
report) into plain facts: who the unit is, which factors are compared, which
cells are empty, what was recommended and why, how many comparisons ran.
Every fact comes from its owner (``tables``, ``design``, ``catalog``,
``run``, ``effects``); this module only gathers them. :mod:`.explain` turns
them into the lay text, and :func:`guide` returns both for the GUI.

``report=None`` is the preview: no test runs (no csv-stats call), so the
first steps stay fast while the user edits the design.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from scistacklog import Log

from .catalog import available_tests, needed_action
from .model import for_frame
from .design import Design, cell_counts, derive_design
from .effects import default_effect_size
from .run import StatsReport
from .spec import GUIDE_STEPS, Analysis, StatsSpec
from .tables import AnalysisFrameError, analysis_frame

LAYER = "scistackstats"


@dataclass
class FactorFacts:
    """One factor of the table, as the wizard's "What are you comparing?"
    and "Same people at every level?" steps show it."""

    name: str
    levels: list[str]
    #: Effective role: "group" | "facet" | "iterate" | "collapse".
    role: str | None
    is_schema: bool
    #: Within/between of the first stratum (None unless compared).
    kind: str | None = None
    derived: str | None = None
    overridden: bool = False
    units_multi: int = 0
    units_single: int = 0
    basis: str = "data"
    #: A variant axis or a multi-column variable's columns: set in Advanced.
    advanced_only: bool = False


@dataclass
class DVFacts:
    dv: str
    variable: str
    error: str | None = None
    n_rows: int = 0
    n_values: int = 0
    unit: str | None = None
    unit_keys: list[str] = field(default_factory=list)
    averaged: list[str] = field(default_factory=list)
    #: Compared factors, innermost (the primary) first.
    compared: list[str] = field(default_factory=list)
    strata: list[str] = field(default_factory=list)
    n_strata: int = 0
    n_units: int | None = None
    #: Units missing some within level in ANY stratum.
    incomplete_units: list[str] = field(default_factory=list)
    #: ``{"stratum": text, "cells": [{factor: level}]}`` per stratum with empty cells.
    empty_cells: list[dict[str, Any]] = field(default_factory=list)
    #: The first stratum's ``design.cell_counts``.
    cells: list[dict[str, Any]] = field(default_factory=list)
    #: Pairs of the primary per stratum, and the outer combinations they sit in.
    n_pairs: int = 0
    n_contexts: int = 0
    #: ``catalog.needed_action`` per factor needing a decision (deduplicated).
    actions: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[dict[str, str]] = field(default_factory=list)
    #: ``catalog.available_tests`` on the first stratum (the shape reasons).
    available_tests: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ResultFacts:
    """The first analysis' run, as the test / correction / effect-size /
    results steps need it."""

    pairwise_test: str | None
    omnibus_test: str | None
    pairwise_reason: str | None
    omnibus_reason: str | None
    recommended: dict[str, Any] | None
    n_comparisons: int
    n_refused: int
    refusal_reasons: list[str]
    n_significant: int
    n_families: int
    largest_family: int
    family_statement: str
    effect_size: str | None
    omnibus_refused: list[str]
    any_within: bool


@dataclass
class GuideFacts:
    spec: StatsSpec
    analysis: Analysis | None
    dvs: list[DVFacts]
    factors: list[FactorFacts]
    result: ResultFacts | None = None

    @property
    def available_tests(self) -> list[dict[str, Any]]:
        first = next((d for d in self.dvs if d.error is None), None)
        return first.available_tests if first is not None else []


def _stratum_text(stratum: dict[str, Any]) -> str:
    return ", ".join(f"{k}={v}" for k, v in stratum.items())


def _dv_facts(spec: StatsSpec, dv, table, overrides: dict[str, str]) -> tuple[DVFacts, Design | None, dict]:
    facts = DVFacts(dv=dv.key, variable=dv.variable)
    try:
        frame = analysis_frame(spec, dv, table)
    except (AnalysisFrameError, KeyError) as exc:
        facts.error = str(exc)
        return facts, None, {}
    rows = frame.rows
    facts.n_rows = int(len(rows))
    facts.n_values = int(rows[frame.y].notna().sum()) if frame.y in rows.columns else 0
    facts.unit, facts.unit_keys, facts.averaged = frame.unit, list(frame.unit_keys), list(frame.averaged)
    facts.compared, facts.strata = list(frame.factors), list(frame.strata)
    groups = frame.stratum_groups()
    facts.n_strata = len(groups)
    first: Design | None = None
    incomplete: list[str] = []
    seen_actions: set[str] = set()
    for stratum, part in groups:
        design = derive_design(frame, stratum, part, overrides)
        if first is None:
            first = design
            facts.n_units = design.n_units
            facts.cells = cell_counts(design, part)
            facts.warnings = list(design.warnings)
            if design.factors:
                primary = design.factors[0]
                facts.n_pairs = len(primary.levels) * (len(primary.levels) - 1) // 2
                facts.n_contexts = 1
                for f in design.factors[1:]:
                    facts.n_contexts *= max(1, len(f.levels))
        incomplete += [u for u in design.incomplete_units if u not in incomplete]
        if design.empty_cells:
            facts.empty_cells.append({"stratum": _stratum_text(stratum), "cells": design.empty_cells})
        action = needed_action(design)
        if action and action["factor"] not in seen_actions:
            seen_actions.add(action["factor"])
            facts.actions.append(action)
    facts.incomplete_units = incomplete
    if first is not None:
        analysis = spec.comparisons[0] if spec.comparisons else None
        mixed = False
        if analysis is not None and analysis.custom_model:
            model = for_frame(analysis.formula, analysis.covariates, frame, available=list(table.factor_names))
            mixed = model.ok and model.needs_mixed_model
        facts.available_tests = available_tests(first, mixed_formula=mixed)
    return facts, first, frame.roles


def _factor_facts(spec: StatsSpec, table, roles: dict[str, str], design: Design | None,
                  dv_columns: bool) -> list[FactorFacts]:
    out = []
    variant = {f.name for f in table.variant_factors}
    fields = {f.name for f in table.field_factors}
    for name in table.factor_names:
        info = table.factor(name)
        facts = FactorFacts(
            name=name, levels=[str(v) for v in info.levels], role=roles.get(name),
            is_schema=table.is_schema_key(name),
            advanced_only=name in variant or (name in fields and dv_columns),
        )
        if design is not None and any(f.name == name for f in design.factors):
            f = design.factor(name)
            facts.kind, facts.derived, facts.overridden = f.kind, f.derived, f.overridden
            facts.units_multi, facts.units_single, facts.basis = f.units_multi, f.units_single, f.basis
        out.append(facts)
    return out


def _significant(c, alpha: float) -> bool:
    p = c.p_adjusted if c.p_adjusted is not None else c.p
    return p is not None and p < alpha


def _result_facts(report: StatsReport, analysis: Analysis) -> ResultFacts | None:
    reports = [a for a in report.analyses if a.analysis.get("id") == analysis.id]
    if not reports:
        return None
    ar = reports[0]
    ran = [s for s in ar.strata if s.refused is None] or ar.strata
    selection = ran[0].selection if ran else {}
    comparisons = ar.comparisons()
    refused = [c for c in comparisons if c.refused]
    reasons: list[str] = []
    for c in refused:
        if c.refused not in reasons:
            reasons.append(c.refused)
    for s in ar.strata:
        if s.refused and s.refused not in reasons:
            reasons.append(s.refused)
    omnibus_refused: list[str] = []
    for s in ar.strata:
        if s.omnibus is not None and s.omnibus.refused and s.omnibus.refused not in omnibus_refused:
            omnibus_refused.append(s.omnibus.refused)
    any_within = any(f.get("kind") == "within" for s in ar.strata for f in s.design.get("factors", []))
    pairwise = selection.get("pairwise")
    effect = analysis.effect_size or (default_effect_size(pairwise, any_within=any_within) if pairwise else None)
    return ResultFacts(
        pairwise_test=pairwise, omnibus_test=selection.get("omnibus"),
        pairwise_reason=selection.get("pairwise_reason"), omnibus_reason=selection.get("omnibus_reason"),
        recommended=selection.get("recommended"),
        n_comparisons=len(comparisons), n_refused=len(refused), refusal_reasons=reasons,
        n_significant=sum(1 for c in comparisons if c.refused is None and _significant(c, analysis.alpha)),
        n_families=len(ar.families), largest_family=max((len(f.members) for f in ar.families), default=0),
        family_statement=ar.family_statement, effect_size=effect, omnibus_refused=omnibus_refused,
        any_within=any_within,
    )


def guide_facts(spec: StatsSpec, tables: dict, report: StatsReport | None = None) -> GuideFacts:
    """The facts for every step (module docstring)."""
    analysis = spec.comparisons[0] if spec.comparisons else None
    overrides = dict(analysis.overrides) if analysis else {}
    dvs: list[DVFacts] = []
    factors: list[FactorFacts] = []
    for i, dv in enumerate(spec.targets):
        table = tables.get(dv.variable)
        if table is None:
            dvs.append(DVFacts(dv=dv.key, variable=dv.variable, error=f"{dv.variable} could not be loaded"))
            continue
        facts, design, roles = _dv_facts(spec, dv, table, overrides)
        dvs.append(facts)
        if i == 0:
            factors = _factor_facts(spec, table, roles, design, dv.columns is not None)
    result = _result_facts(report, analysis) if report is not None and analysis is not None else None
    return GuideFacts(spec=spec, analysis=analysis, dvs=dvs, factors=factors, result=result)


def guide(spec: StatsSpec, tables: dict, report: StatsReport | None = None) -> dict[str, Any]:
    """The wizard's payload: one entry per step (``spec.GUIDE_STEPS`` order)
    and the concepts they name, as JSON."""
    from .explain import CONCEPTS, build_steps

    t0 = time.perf_counter()
    facts = guide_facts(spec, tables, report)
    steps = build_steps(facts)
    # The steps' concepts and every choice's (shown when the pointer is on it).
    used = sorted({c for s in steps for c in s.concepts}
                  | {ch.concept for s in steps for card in s.cards for ch in card.choices if ch.concept})
    blocked = [s.id for s in steps if s.blocked]
    Log.info("[stats guide] %d step(s), blocked %s, results %s, %.3fs", len(steps), blocked or "none",
             "yes" if report is not None else "no (preview)", time.perf_counter() - t0, layer=LAYER)
    if [s.id for s in steps] != list(GUIDE_STEPS):
        raise RuntimeError(f"explain.build_steps returned {[s.id for s in steps]}, not spec.GUIDE_STEPS")
    return {"steps": [s.to_dict() for s in steps], "concepts": {c: CONCEPTS[c].to_dict() for c in used},
            "has_results": report is not None}


__all__ = ["DVFacts", "FactorFacts", "GuideFacts", "ResultFacts", "guide", "guide_facts"]
