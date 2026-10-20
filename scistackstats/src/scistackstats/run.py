"""Run a StatsSpec's comparisons: DVs × strata × analyses → csv-stats → report.

For each analysis, DV and stratum:

1. derive the design from the rows (``design.derive_design``);
2. choose the tests from it (``catalog.select_tests``), and say why;
3. **secondary**: the omnibus model over every grouping layer, if asked;
4. **primary** (S13): every pair of the innermost layer's levels, inside each
   combination of the outer layers. A paired test when that layer is within
   the unit, an independent one when it is between. Always run, whatever the
   interaction shows, with the interaction p beside each comparison;
5. correct the primary p-values in families (P5; default: one DV × one
   stratum). Omnibus terms are not adjusted, and the statement says so (S7).

Nothing here raises for a data problem: a refusal (no unit for a within
factor, csv-stats gap, too few values, …) is recorded where it happened, with
its reason. A configuration error (an unknown correction) raises
``StatsSpecError`` before anything runs.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

import pandas as pd
from scistacklog import Log

from scistackplot import LongTable
from scistackplot.design import design_fingerprint

from .catalog import TESTS, needed_action, recommend, select_tests
from .design import WITHIN, Design, comparison_tiers, derive_design
from .effects import EFFECT_SIZES, chosen_effect_size, difference_spec
from .engine import EngineRefusal, get_engine
from .exclusions import Exclusions, from_record
from .model import ModelCheck, for_frame
from .spec import Analysis, StatsSpec, StatsSpecError
from .tables import UNIT, AnalysisFrame, AnalysisFrameError, CovariateValues, analysis_frame, join_covariates

LAYER = "scistackstats"

CORRECTION_LABELS = {
    "none": "no correction",
    "bonferroni": "Bonferroni",
    "holm": "Holm",
    "fdr_bh": "Benjamini–Hochberg (FDR)",
    "fdr_by": "Benjamini–Yekutieli (FDR)",
}
SCOPE_LABELS = {
    "context": "combination of the outer grouping layers",
    "stratum": "DV and figure/panel",
    "dv": "DV",
    "table": "table",
    "variants": "comparison across the variants",
}


def _text(mapping: dict[str, Any]) -> dict[str, str]:
    return {str(k): str(v) for k, v in mapping.items()}


def _key_text(mapping: dict[str, Any]) -> str:
    return ", ".join(f"{k}={v}" for k, v in mapping.items())


def _shown_size(effect: dict[str, Any], size_id: str | None) -> dict[str, Any] | None:
    if size_id is None:
        return None
    sizes = effect.get("effect_sizes") or {}
    raw = sizes.get(size_id)
    if raw is None:
        return {"id": size_id, "name": EFFECT_SIZES[size_id].name, "value": None, "ci": None,
                "note": "not computed by csv-stats for this test"}
    return {"id": size_id, "name": EFFECT_SIZES[size_id].name, "value": raw.get("value"), "ci": raw.get("ci")}


@dataclass
class Comparison:
    id: str
    dv: str
    stratum: dict[str, str]
    context: dict[str, str]
    term: str
    a: str
    b: str
    test: str
    #: The variant-axis part of ``stratum`` (a sweep's coordinates).
    variant: dict[str, str] = field(default_factory=dict)
    statistic_name: str | None = None
    statistic: float | None = None
    df: list[float | None] = field(default_factory=list)
    #: "satterthwaite", "wald", "residual", ... as csv-stats reports it.
    df_method: str | None = None
    p: float | None = None
    p_adjusted: float | None = None
    family: str | None = None
    estimate: float | None = None
    #: The spread of the difference the analysis asked for (S14): a [low,
    #: high] pair for "ci", one number for "se" / "sd_diff".
    spread: Any = None
    effect_size: dict[str, Any] | None = None
    n: dict[str, Any] | None = None
    #: ``{interaction term: p}`` for every omnibus interaction containing
    #: this comparison's factor (S13: shown beside it).
    interaction_p: dict[str, float | None] = field(default_factory=dict)
    warnings: list[dict[str, str]] = field(default_factory=list)
    #: SPM only: supra-threshold clusters ``{start, end, p, extent}`` (nodes)
    #: and the critical t*. None for a scalar test.
    clusters: list[dict[str, Any]] | None = None
    threshold: float | None = None
    refused: str | None = None
    refusal_kind: str | None = None
    #: ``exclusions.Exclusions.to_dict()`` when the test left anything out.
    excluded: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class OmnibusResult:
    test: str
    effects: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[dict[str, str]] = field(default_factory=list)
    n: dict[str, Any] | None = None
    #: ``exclusions.Exclusions.to_dict()`` when the test left anything out.
    excluded: dict[str, Any] | None = None
    sphericity: list[dict[str, Any]] = field(default_factory=list)
    refused: str | None = None
    refusal_kind: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class StratumResult:
    dv: str
    stratum: dict[str, str]
    design: dict[str, Any]
    selection: dict[str, Any]
    difference: dict[str, Any] | None
    effect_size_note: str | None
    omnibus: OmnibusResult | None
    primary: list[Comparison]
    refused: str | None = None
    #: Units the "complete_cases" policy left out of every test here.
    excluded: dict[str, Any] | None = None
    #: Stage F: a decision the user must make before tests can run
    #: (``{"factor", "message", "choices"}``), from a design refusal.
    action: dict[str, Any] | None = None
    #: The model formula this stratum ran (``dv ~ …``), when the analysis
    #: has one (``model.py``); None for StatStudio's default model.
    model: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "dv": self.dv, "stratum": self.stratum, "design": self.design, "selection": self.selection,
            "difference": self.difference, "effect_size_note": self.effect_size_note,
            "omnibus": self.omnibus.to_dict() if self.omnibus else None,
            "primary": [c.to_dict() for c in self.primary], "refused": self.refused,
            "excluded": self.excluded, "action": self.action, "model": self.model,
        }


@dataclass
class Family:
    id: str
    scope: str
    method: str
    members: list[str]

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class AnalysisReport:
    analysis: dict[str, Any]
    strata: list[StratumResult]
    families: list[Family]
    family_statement: str
    refusals: list[dict[str, str]] = field(default_factory=list)

    def comparisons(self) -> list[Comparison]:
        return [c for s in self.strata for c in s.primary]

    def to_dict(self) -> dict[str, Any]:
        return {
            "analysis": self.analysis, "strata": [s.to_dict() for s in self.strata],
            "families": [f.to_dict() for f in self.families], "family_statement": self.family_statement,
            "refusals": self.refusals,
        }


@dataclass
class StatsReport:
    analyses: list[AnalysisReport]
    engine: dict[str, str]
    #: ``scistackplot.design.design_fingerprint`` of the design the report
    #: ran on, so a GUI can tell when the results are stale against the spec.
    design_digest: str
    #: ``AnalysisFrame.summary()`` per DV that had rows (for captions).
    dvs: list[dict[str, Any]] = field(default_factory=list)
    #: DVs whose rows could not be built: ``{dv, reason}``.
    refusals: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"analyses": [a.to_dict() for a in self.analyses], "engine": self.engine,
                "design_digest": self.design_digest, "dvs": self.dvs, "refusals": self.refusals}


# ---------------------------------------------------------------------------


def _levels_for(design: Design) -> dict[str, list[Any]]:
    return {f.name: list(f.levels) for f in design.factors}


def _model_inputs(test: str, model: ModelCheck | None, base: list[str]) -> tuple[list[str], dict[str, Any]]:
    """The extra row columns and engine options a model formula adds to a
    mixed-model call (covariates, random groups and slopes). Other tests,
    and StatStudio's default model, are called exactly as before."""
    if model is None or not model.needs_mixed_model or test not in ("lmm", "lmm_pairwise"):
        return base, {}
    return [*base, *(c for c in model.columns() if c not in base)], model.engine_options()


def _omnibus(engine, test: str, frame: AnalysisFrame, rows: pd.DataFrame, design: Design, analysis: Analysis,
             model: ModelCheck | None = None) -> OmnibusResult:
    columns = [frame.y, *[f.name for f in design.factors], *([UNIT] if UNIT in rows.columns else [])]
    columns, model_options = _model_inputs(test, model, columns)
    try:
        record = engine.run(
            test, rows[columns], frame.y,
            between=design.between, within=design.within,
            subject=UNIT if design.within or model_options else None, levels=_levels_for(design),
            options={"alpha": analysis.alpha, "ci_level": analysis.ci_level, **model_options},
        )
    except EngineRefusal as exc:
        return OmnibusResult(test=test, refused=exc.reason, refusal_kind=exc.kind)
    size_id, _ = chosen_effect_size(analysis, test, any_within=bool(design.within))
    effects = []
    for effect in record.get("effects", []):
        effects.append({
            "term": effect.get("term"), "statistic_name": effect.get("statistic_name"),
            "statistic": effect.get("statistic"), "df": effect.get("df"), "p": effect.get("p"),
            "df_method": effect.get("df_method"),
            "sphericity_correction": effect.get("sphericity_correction"),
            "effect_size": _shown_size(effect, size_id),
        })
    excluded = _exclusions(record, rows, bool(design.within), f"{frame.dv.key} {_key_text(design.stratum)} omnibus {test}")
    return OmnibusResult(test=test, effects=effects, warnings=list(record.get("warnings", [])), n=record.get("n"),
                         excluded=excluded)


def _exclusions(record: dict[str, Any], rows: pd.DataFrame, by_unit: bool, where: str) -> dict[str, Any] | None:
    """``exclusions.from_record`` for one engine call, logged at WARN: what a
    test left out is never silent (user rule 2026-10-19)."""
    if by_unit and UNIT in rows.columns:
        offered, counts = int(rows[UNIT].nunique()), "subjects"
    else:
        offered, counts = int(len(rows)), "rows"
    found = from_record(record.get("n"), offered, counts)
    if found is None:
        return None
    Log.warn("[stats] %s: %s", where.strip(), found.sentence(), layer=LAYER)
    return found.to_dict()


def _pairwise(engine, test, frame, rows, design, tiers, analysis, stratum_text, omnibus, size_id, diff,
              model: ModelCheck | None = None) -> list[Comparison]:
    primary = tiers.primary
    within = design.factor(primary).kind == WITHIN
    interactions: dict[str, float | None] = {}
    if omnibus is not None and omnibus.refused is None:
        for effect in omnibus.effects:
            parts = str(effect["term"]).split(":")
            if len(parts) > 1 and primary in parts:
                interactions[effect["term"]] = effect["p"]
    if test == "lmm_pairwise":
        return _pairwise_full_model(engine, test, frame, rows, design, tiers, analysis, stratum_text,
                                    interactions, size_id, diff, model)
    out = []
    for context in tiers.contexts:
        part = rows
        for name, level in context.items():
            part = part[part[name] == level]
        for a, b in tiers.pairs:
            pair_rows = part[part[primary].isin([a, b])]
            comparison = Comparison(
                id=f"{analysis.id}|{frame.dv.key}|{stratum_text}|{_key_text(context)}|{a}|{b}",
                dv=frame.dv.key, stratum=_text(dict(design.stratum)), context=_text(context),
                term=primary, a=str(a), b=str(b), test=test, interaction_p=dict(interactions),
                variant={k: str(v) for k, v in design.stratum.items() if k in frame.variant_axes},
            )
            absent = [lv for lv in (a, b) if not (pair_rows[primary] == lv).any()]
            if absent:
                # An empty cell (design.empty_cells): say which, instead of
                # csv-stats' "levels [...] are not in column ..." (scidb.log
                # 2026-10-20).
                where = f" at {_key_text(context)}" if context else ""
                comparison.refused = (f"no rows for {primary} = {', '.join(str(lv) for lv in absent)}{where}: "
                                      f"there is nothing to compare")
                comparison.refusal_kind = "invalid"
                Log.info("[stats] %s %s: %s vs %s not run: %s", frame.dv.key, stratum_text or "(one stratum)",
                         a, b, comparison.refused, layer=LAYER)
                out.append(comparison)
                continue
            columns = [frame.y, primary, *([UNIT] if UNIT in pair_rows.columns else [])]
            try:
                record = engine.run(
                    test, pair_rows[columns], frame.y,
                    between=[] if within else [primary], within=[primary] if within else [],
                    subject=UNIT if within else None, levels={primary: [a, b]},
                    options={"alpha": analysis.alpha, "ci_level": analysis.ci_level, "tails": analysis.tails},
                )
            except EngineRefusal as exc:
                comparison.refused, comparison.refusal_kind = exc.reason, exc.kind
                out.append(comparison)
                continue
            (effect,) = record["effects"]
            estimate = effect.get("estimate") or {}
            spread_key = {"ci": "ci", "se": "se", "sd_diff": "sd_of_differences"}[diff.spread]
            comparison.statistic_name = effect.get("statistic_name")
            comparison.statistic = effect.get("statistic")
            comparison.df = list(effect.get("df") or [])
            comparison.df_method = effect.get("df_method")
            comparison.p = effect.get("p")
            comparison.estimate = estimate.get("value")
            comparison.spread = estimate.get(spread_key)
            comparison.effect_size = _shown_size(effect, size_id)
            comparison.n = record.get("n")
            comparison.warnings = list(record.get("warnings", []))
            comparison.excluded = _exclusions(
                record, pair_rows, within, f"{frame.dv.key} {stratum_text} {_key_text(context)} {a} vs {b}",
            )
            if "clusters" in effect:
                comparison.clusters = list(effect.get("clusters") or [])
                comparison.threshold = effect.get("threshold")
            out.append(comparison)
    return out


def _pairwise_full_model(engine, test, frame, rows, design, tiers, analysis, stratum_text,
                         interactions, size_id, diff, model: ModelCheck | None = None) -> list[Comparison]:
    """Mixed-model contrasts: ONE engine call on the whole stratum.

    csv-stats fits the full model once and returns every pair of the primary
    factor within each combination of the outer factors (emmeans-style; the
    user's decision of 2026-10-19). Fitting a model per pair on just the
    pair's rows would use less of the data for the variance estimates. The
    contrasts are then laid out as the same Comparison records the other
    pairwise tests produce.
    """
    primary = tiers.primary
    columns = [frame.y, *[f.name for f in design.factors], *([UNIT] if UNIT in rows.columns else [])]

    def comparison(context, a, b) -> Comparison:
        return Comparison(
            id=f"{analysis.id}|{frame.dv.key}|{stratum_text}|{_key_text(context)}|{a}|{b}",
            dv=frame.dv.key, stratum=_text(dict(design.stratum)), context=_text(context),
            term=primary, a=str(a), b=str(b), test=test, interaction_p=dict(interactions),
            variant={k: str(v) for k, v in design.stratum.items() if k in frame.variant_axes},
        )

    columns, model_options = _model_inputs(test, model, columns)
    try:
        record = engine.run(
            test, rows[columns], frame.y, between=design.between, within=design.within,
            subject=UNIT, levels=_levels_for(design),
            options={"alpha": analysis.alpha, "ci_level": analysis.ci_level, "tails": analysis.tails,
                     "factor": primary, **model_options},
        )
    except EngineRefusal as exc:
        out = []
        for context in tiers.contexts:
            for a, b in tiers.pairs:
                c = comparison(context, a, b)
                c.refused, c.refusal_kind = exc.reason, exc.kind
                out.append(c)
        return out
    by_key = {(tuple(sorted(e.get("context", {}).items())), e["a"], e["b"]): e for e in record["effects"]}
    spread_key = {"ci": "ci", "se": "se", "sd_diff": "sd_of_differences"}[diff.spread]
    out = []
    for context in tiers.contexts:
        key_context = tuple(sorted((k, str(v)) for k, v in context.items()))
        for a, b in tiers.pairs:
            c = comparison(context, a, b)
            effect = by_key.get((key_context, str(a), str(b)))
            if effect is None:
                c.refused, c.refusal_kind = "the mixed model returned no contrast for this pair", "invalid"
                Log.warn("[stats] %s %s: no lmm contrast for %s - %s in %s", frame.dv.key, stratum_text, a, b,
                         _key_text(context), layer=LAYER)
                out.append(c)
                continue
            estimate = effect.get("estimate") or {}
            c.statistic_name = effect.get("statistic_name")
            c.statistic = effect.get("statistic")
            c.df = list(effect.get("df") or [])
            c.df_method = effect.get("df_method")
            c.p = effect.get("p")
            c.estimate = estimate.get("value")
            c.spread = estimate.get(spread_key)
            c.effect_size = _shown_size(effect, size_id)
            c.n = record.get("n")
            c.warnings = list(record.get("warnings", []))
            out.append(c)
    Log.info("[stats] %s %s: %d mixed-model contrast(s) of %s from one full model", frame.dv.key,
             stratum_text or "(one stratum)", len(out), primary, layer=LAYER)
    return out


def _family_key(scope: str, c: Comparison) -> tuple | None:
    if scope == "none":
        return None
    if scope == "context":
        return (c.dv, _key_text(c.stratum), _key_text(c.context))
    if scope == "stratum":
        return (c.dv, _key_text(c.stratum))
    if scope == "dv":
        return (c.dv,)
    if scope == "variants":
        rest = {k: v for k, v in c.stratum.items() if k not in c.variant}
        return (c.dv, _key_text(rest), _key_text(c.context), c.a, c.b)
    return ()


def apply_families(engine, analysis: Analysis, comparisons: list[Comparison]) -> list[Family]:
    """Adjust primary p-values within each family (P5); returns the families."""
    groups: dict[tuple, list[Comparison]] = {}
    for c in comparisons:
        if c.refused is not None or c.p is None:
            continue
        key = _family_key(analysis.family_scope, c)
        if key is None:
            c.p_adjusted, c.family = c.p, None
            continue
        groups.setdefault(key, []).append(c)
    families = []
    for i, (key, members) in enumerate(groups.items(), start=1):
        adjusted = engine.correct([m.p for m in members], analysis.correction)
        fid = f"{analysis.id}:f{i}"
        for member, p_adj in zip(members, adjusted):
            member.p_adjusted, member.family = p_adj, fid
        families.append(Family(fid, analysis.family_scope, analysis.correction, [m.id for m in members]))
    return families


def family_statement(analysis: Analysis, families: list[Family], n_comparisons: int) -> str:
    """The S7 sentence: which comparisons were corrected, how, in what families."""
    if not families and n_comparisons == 0:
        primary = "Primary comparisons: none ran (see the refusals)."
    elif analysis.family_scope == "none" or analysis.correction == "none" or not families:
        primary = f"Primary comparisons ({n_comparisons}): p-values not adjusted for multiple comparisons."
    else:
        sizes = sorted({len(f.members) for f in families})
        size_text = f"{sizes[0]}" if len(sizes) == 1 else f"{sizes[0]}–{sizes[-1]}"
        primary = (
            f"Primary comparisons ({n_comparisons}): p-values adjusted with "
            f"{CORRECTION_LABELS.get(analysis.correction, analysis.correction)} within each "
            f"{SCOPE_LABELS[analysis.family_scope]} ({len(families)} famil{'y' if len(families) == 1 else 'ies'} "
            f"of {size_text})."
        )
    if analysis.omnibus:
        primary += " Omnibus effects are not adjusted."
    return primary


def _model_for(analysis: Analysis, frame: AnalysisFrame, covariates: dict[str, CovariateValues],
               available: list[str]) -> tuple[AnalysisFrame, ModelCheck | None]:
    """The analysis' model formula against one DV's rows, with its
    covariates joined on; ``(frame, None)`` for StatStudio's default model."""
    if not analysis.custom_model:
        return frame, None
    problems: list[str] = []
    if analysis.covariates:
        rows, problems = join_covariates(frame.rows, [c.factor_name for c in analysis.covariates], covariates)
        frame = replace(frame, rows=rows)
    model = for_frame(analysis.formula, analysis.covariates, frame, available=available)
    model.errors.extend({"message": p} for p in problems)
    if model.errors:
        Log.warn("[stats] %s: model formula %r unusable: %s", frame.dv.key, model.text,
                 "; ".join(e["message"] for e in model.errors), layer=LAYER)
    return frame, model


def run_analysis(analysis: Analysis, frames: list[AnalysisFrame], refusals: list[dict[str, str]],
                 covariates: dict[str, CovariateValues] | None = None,
                 available: dict[str, list[str]] | None = None) -> AnalysisReport:
    engine = get_engine()
    strata: list[StratumResult] = []
    for frame in frames:
        # available: the table's factors, so a stored formula naming one the
        # design has since dropped is rebased, not "unknown".
        frame, model = _model_for(analysis, frame, covariates or {}, (available or {}).get(frame.dv.key, []))
        model_text = f"{frame.dv.key} ~ {model.text}" if model is not None else None
        for stratum, rows in frame.stratum_groups():
            stratum_text = _key_text(stratum)
            design = derive_design(frame, stratum, rows, analysis.overrides)
            if model is not None and not model.ok:
                reason = "model formula: " + model.errors[0]["message"]
                strata.append(StratumResult(
                    dv=frame.dv.key, stratum=_text(stratum), design=design.to_dict(),
                    selection={"omnibus": None, "omnibus_reason": reason, "pairwise": None,
                               "pairwise_reason": reason, "why": reason, "recommended": None},
                    difference=None, effect_size_note=None,
                    omnibus=OmnibusResult(test="", refused=reason, refusal_kind="design") if analysis.omnibus else None,
                    primary=[], refused=reason if analysis.pairwise else None, model=model_text,
                ))
                continue
            stratum_excluded = None
            if analysis.missing == "complete_cases" and design.incomplete_units:
                # The user's recorded choice: every test of this analysis sees
                # the same complete units; the others are named, never silent.
                dropped = list(design.incomplete_units)
                offered = int(rows[UNIT].nunique())
                rows = rows[~rows[UNIT].astype(str).isin(dropped)]
                stratum_excluded = Exclusions(
                    offered=offered, used=offered - len(dropped), counts="subjects",
                    units=[{"unit": u, "reason": "missing a within-subject level (complete cases chosen)"}
                           for u in dropped],
                ).to_dict()
                Log.warn("[stats] %s %s: complete cases: %s", frame.dv.key, stratum_text or "(one stratum)",
                         Exclusions(**stratum_excluded).sentence(), layer=LAYER)
                design = derive_design(frame, stratum, rows, analysis.overrides)
            rec = recommend(analysis, design, rows, frame.y, engine, UNIT)
            selection = select_tests(analysis, design, rec,
                                     mixed_formula=model is not None and model.needs_mixed_model)
            Log.info("[stats] %s %s: missing=%s; %s%s", frame.dv.key, stratum_text or "(one stratum)",
                     analysis.missing, selection.why,
                     f"; recommended omnibus={rec.omnibus} pairwise={rec.pairwise}" if rec else "", layer=LAYER)
            tiers = comparison_tiers(design, rows)
            omnibus = None
            if analysis.omnibus and selection.omnibus:
                if selection.omnibus_reason:
                    omnibus = OmnibusResult(test=selection.omnibus, refused=selection.omnibus_reason, refusal_kind="design")
                else:
                    omnibus = _omnibus(engine, selection.omnibus, frame, rows, design, analysis, model)
            elif analysis.omnibus:
                omnibus = OmnibusResult(test="", refused=selection.omnibus_reason, refusal_kind="design")
            if omnibus is not None and omnibus.refused:
                Log.warn("[stats] %s %s: omnibus %s not run: %s", frame.dv.key,
                         stratum_text or "(one stratum)", omnibus.test or "(none)",
                         omnibus.refused, layer=LAYER)
            primary: list[Comparison] = []
            diff = None
            size_note = None
            stratum_refusal = None
            if analysis.pairwise:
                test = selection.pairwise
                if test is None or selection.pairwise_reason:
                    stratum_refusal = selection.pairwise_reason
                    # The reason, not just a count: scidb.log 2026-10-19 said
                    # "3 refused ... (see the refusals)" and nothing else, so
                    # an empty comparison table had no explanation in the log.
                    Log.warn("[stats] %s %s: comparisons not run: %s", frame.dv.key,
                             stratum_text or "(one stratum)", stratum_refusal, layer=LAYER)
                else:
                    size_id, size_note = chosen_effect_size(analysis, test, any_within=bool(design.within))
                    diff = difference_spec(test, analysis.difference_spread, analysis.ci_level)
                    primary = _pairwise(engine, test, frame, rows, design, tiers, analysis,
                                        stratum_text, omnibus, size_id, diff, model)
                    Log.info("[stats] %s %s: %s; %s", frame.dv.key, stratum_text or "(one stratum)",
                             selection.why, diff.label, layer=LAYER)
            strata.append(StratumResult(
                dv=frame.dv.key, stratum=_text(stratum), design=design.to_dict(),
                selection=selection.to_dict(), difference=diff.to_dict() if diff else None,
                effect_size_note=size_note, omnibus=omnibus, primary=primary, refused=stratum_refusal,
                excluded=stratum_excluded, action=needed_action(design), model=model_text,
            ))
    comparisons = [c for s in strata for c in s.primary]
    families = apply_families(engine, analysis, comparisons)
    statement = family_statement(analysis, families, sum(1 for c in comparisons if c.refused is None))
    refused = sum(1 for c in comparisons if c.refused) + sum(1 for s in strata if s.refused)
    Log.info(
        "[stats] analysis %s: %d stratum(s), %d comparison(s), %d refused, %d famil(ies). %s",
        analysis.id, len(strata), len(comparisons), refused, len(families), statement, layer=LAYER,
    )
    return AnalysisReport(analysis=analysis.to_dict(), strata=strata, families=families,
                          family_statement=statement, refusals=list(refusals))


def run(stats_spec: StatsSpec, tables: dict[str, LongTable],
        covariates: dict[str, CovariateValues] | None = None) -> StatsReport:
    """Every analysis of ``stats_spec`` over every DV (module docstring).
    ``covariates``: ``tables.load_covariates``, for analyses with a model
    formula that names covariates."""
    engine = get_engine()
    allowed = set(engine.corrections())
    for analysis in stats_spec.comparisons:
        if analysis.correction not in allowed:
            raise StatsSpecError(f"analysis {analysis.id}: correction must be one of {sorted(allowed)}, got {analysis.correction!r}")
        for name in (analysis.omnibus_test, analysis.pairwise_test):
            if name is not None and name not in TESTS:
                raise StatsSpecError(f"analysis {analysis.id}: unknown test {name!r}; choose from {list(TESTS)}")
    with Log.timer("stats_run", layer=LAYER, extra=f"{len(stats_spec.comparisons)} analysis(es)") as timing:
        by_kind: dict[bool, list[AnalysisFrame]] = {}
        refusals_by: dict[bool, list[dict[str, str]]] = {}
        with timing.phase("rows"):
            # Scalar rows always (Descriptives and every scalar test); 1-D
            # curves only when an SPM analysis asks for them.
            for curves in sorted({False} | {a.family == "spm" for a in stats_spec.comparisons}):
                by_kind[curves], refusals_by[curves] = [], []
                for dv in stats_spec.targets:
                    try:
                        by_kind[curves].append(analysis_frame(stats_spec, dv, tables[dv.variable], curves=curves))
                    except AnalysisFrameError as exc:
                        refusals_by[curves].append({"dv": dv.key, "reason": str(exc)})
                    except KeyError:
                        refusals_by[curves].append({"dv": dv.key, "reason": f"no table loaded for {dv.variable}"})
        refusals = refusals_by[False]
        available = {dv.key: list(tables[dv.variable].factor_names)
                     for dv in stats_spec.targets if dv.variable in tables}
        reports = []
        for analysis in stats_spec.comparisons:
            curves = analysis.family == "spm"
            with timing.phase(f"analysis {analysis.id}"):
                reports.append(run_analysis(analysis, by_kind[curves], refusals_by[curves], covariates, available))
        frames = by_kind[False]
    for refusal in refusals:
        Log.warn("[stats] %s refused: %s", refusal["dv"], refusal["reason"], layer=LAYER)
    return StatsReport(
        analyses=reports, engine=engine.versions(), design_digest=design_fingerprint(stats_spec.design),
        dvs=[f.summary() for f in frames], refusals=refusals,
    )
