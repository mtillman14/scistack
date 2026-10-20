"""Which test runs where, and why not — the one refusal owner.

:func:`why_unavailable` answers "can test T run on this design?" with ``None``
or a reason. The GUI greys a menu entry with that reason, and the runner
records it as a refusal. :func:`select_tests` picks the tests for an analysis
from the design, and always says why (S12: never switch silently).

This module knows the *statistical* requirements: a paired test needs a
within factor and a unit, a two-sample test needs two levels, and so on.
What csv-stats does not support *yet* is csv-stats' to say: those come back
from the engine at run time as ``not yet in csv-stats: …`` refusals. Copying
csv-stats' support matrix here would give it a second owner.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .design import BETWEEN, MIXED, WITHIN, Design, cells_text
from .spec import Analysis


@dataclass(frozen=True)
class StatTest:
    id: str
    label: str
    #: "pairwise" (two levels of one factor) or "omnibus" (every factor).
    role: str
    family: str
    #: For pairwise tests: the factor kind it needs.
    kind: str | None = None


TESTS: dict[str, StatTest] = {
    t.id: t
    for t in (
        StatTest("ttest_ind_welch", "Welch's t-test", "pairwise", "parametric", BETWEEN),
        StatTest("ttest_ind_student", "Student's t-test", "pairwise", "parametric", BETWEEN),
        StatTest("ttest_paired", "Paired t-test", "pairwise", "parametric", WITHIN),
        StatTest("mann_whitney", "Mann–Whitney U", "pairwise", "nonparametric", BETWEEN),
        StatTest("wilcoxon", "Wilcoxon signed-rank", "pairwise", "nonparametric", WITHIN),
        StatTest("spm_ttest2", "SPM two-sample t (1-D curves)", "pairwise", "spm", BETWEEN),
        StatTest("spm_ttest_paired", "SPM paired t (1-D curves)", "pairwise", "spm", WITHIN),
        StatTest("lmm_pairwise", "Mixed-model contrast (Satterthwaite t)", "pairwise", "mixed_model", WITHIN),
        StatTest("anova", "ANOVA", "omnibus", "parametric"),
        StatTest("kruskal", "Kruskal–Wallis", "omnibus", "nonparametric"),
        StatTest("friedman", "Friedman", "omnibus", "nonparametric"),
        StatTest("lmm", "Linear mixed model (Satterthwaite F)", "omnibus", "mixed_model"),
    )
}

#: Tests that need every unit at every within level they compare. With
#: missing data they still RUN (never unavailable for it, user decision
#: 2026-10-19) and name whom they left out (``exclusions``).
COMPLETE_CASES_TESTS = frozenset({"ttest_paired", "wilcoxon", "spm_ttest_paired", "anova", "friedman"})


def _design_problem(design: Design) -> str | None:
    if not design.factors:
        return "nothing is grouped: add a grouping layer to compare its levels"
    if design.mixed:
        names = ", ".join(design.mixed)
        return f"{names}: some units at one level, some at several; set within or between first"
    if design.within and design.unit is None:
        return "a within-subject factor needs an experimental unit (collapse to one, without weight by N)"
    return None


def needed_action(design: Design) -> dict[str, Any] | None:
    """The decision the user must make before any test can run here, or None.

    Stage F: a refusal the user can fix with one choice is not a table of
    "not run" rows; the GUI shows it as a banner with these choices, applied
    as the analysis override. Today: a factor whose units sit at one level
    AND at several (only possible for non-schema factors since Stage E).
    """
    for f in design.factors:
        if f.kind == MIXED:
            return {
                "factor": f.name,
                "message": (f"Comparisons need one decision: {f.name} has {f.units_multi} unit(s) at several "
                            f"levels and {f.units_single} at one. Is it within-subject (the same units "
                            f"measured repeatedly) or between-subject (separate groups)?"),
                "choices": ["within", "between"],
            }
    return None


#: Why a non-mixed test cannot run when the analysis' model formula has
#: covariates or custom random effects (plan-statstudio-model-formula).
MIXED_FORMULA_REASON = "the model formula has covariates or random terms that only a mixed model fits"


def why_unavailable(test_id: str, design: Design, *, mixed_formula: bool = False) -> str | None:
    """``None`` if ``test_id`` can run on ``design``, else the reason.

    ``mixed_formula``: the analysis' model formula needs a mixed model, so
    only lmm / lmm_pairwise can run. Their usual "needs a within factor"
    rule is then csv-stats' to judge: a covariate or a crossed random group
    can make a mixed model sensible without one, and csv-stats refuses an
    unidentifiable one with lme4's reason.
    """
    info = TESTS.get(test_id)
    if info is None:
        return f"unknown test {test_id!r}"
    problem = _design_problem(design)
    if problem:
        return problem
    if mixed_formula:
        if info.family != "mixed_model":
            return MIXED_FORMULA_REASON
        if info.role == "pairwise" and len(design.factors[0].levels) < 2:
            return f"{design.factors[0].name} has fewer than 2 levels here"
        thin = [f.name for f in design.factors if len(f.levels) < 2]
        return f"{', '.join(thin)} has fewer than 2 levels here" if thin and info.role == "omnibus" else None
    if info.role == "pairwise":
        primary = design.factors[0]
        if len(primary.levels) < 2:
            return f"{primary.name} has fewer than 2 levels here"
        if primary.kind != info.kind:
            return f"{info.label} needs a {info.kind}-subject factor; {primary.name} is {primary.kind}"
        return None
    # Omnibus
    if test_id == "kruskal":
        if len(design.factors) != 1 or design.factors[0].kind != BETWEEN:
            return "Kruskal–Wallis takes one between-subject factor"
        if len(design.factors[0].levels) < 2:
            return f"{design.factors[0].name} has fewer than 2 levels here"
    if test_id == "friedman":
        if len(design.factors) != 1 or design.factors[0].kind != WITHIN:
            return "Friedman takes one within-subject factor"
        if len(design.factors[0].levels) < 3:
            return "Friedman needs 3+ levels; with 2, the Wilcoxon test is the test"
    if test_id in ("anova", "lmm"):
        thin = [f.name for f in design.factors if len(f.levels) < 2]
        if thin:
            return f"{', '.join(thin)} has fewer than 2 levels here"
    if test_id == "lmm" and not design.within:
        return ("a mixed model needs repeated observations per unit: at least one within-subject factor "
                "(with only between factors each unit has one row)")
    return None


@dataclass(frozen=True)
class Selection:
    omnibus: str | None
    omnibus_reason: str | None
    pairwise: str | None
    pairwise_reason: str | None
    #: Why these tests (shown in the GUI and the report).
    why: str
    #: :class:`Recommendation` ``to_dict()``, whatever was selected, so the
    #: GUI can show "Recommended: …" beside a test the user chose instead.
    recommended: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"omnibus": self.omnibus, "omnibus_reason": self.omnibus_reason,
                "pairwise": self.pairwise, "pairwise_reason": self.pairwise_reason, "why": self.why,
                "recommended": self.recommended}


@dataclass(frozen=True)
class Recommendation:
    """The tests the DATA point to, with the evidence (Stage D, 2026-10-19).

    A recommendation, never a silent switch: it pre-selects the tests when
    the family is "auto", the GUI shows it with its evidence, and the user
    can pick any test that runs on the design's shape instead.
    """

    omnibus: str | None
    pairwise: str | None
    #: Human-readable lines: "residuals not normal (Shapiro–Wilk W = 0.91,
    #: p = .003)", "4 subjects lack some sessions: a mixed model uses all ...".
    evidence: tuple[str, ...]
    #: csv-stats' raw assumption records.
    checks: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"omnibus": self.omnibus, "pairwise": self.pairwise,
                "evidence": list(self.evidence), "checks": list(self.checks)}


def _check_text(check: dict[str, Any]) -> str:
    stat = check.get("statistic")
    p = check.get("p")
    if p is None:
        return f"{check.get('test')}: too few values to check {check.get('scope')}"
    name = {"normality": "normal", "homogeneity_of_variance": "equal variances"}.get(check.get("name"), check.get("name"))
    verdict = f"not {name}" if check.get("violated") else name
    label = {"shapiro_wilk": "Shapiro–Wilk W", "levene": "Levene F"}.get(check.get("test"), check.get("test"))
    return f"{check.get('scope')}: {verdict} ({label} = {stat:.2f}, p = {p:.3g})"


def recommend(analysis: Analysis, design: Design, rows: Any, y: str, engine: Any, unit_column: str) -> Recommendation | None:
    """The tests the data point to, or None when the design can't be tested
    (no grouping layer, an unresolved mixed factor, curves).

    - Within units incomplete and ``missing == "use_all"``: the linear mixed
      model (omnibus) and mixed-model contrasts (pairwise, within primary),
      because they use every observation.
    - Otherwise, residual normality (csv-stats' Shapiro–Wilk) decides
      parametric vs rank-based. Variances are reported, and Welch is used
      unless the analysis assumes equal variances.
    """
    if _design_problem(design) or analysis.family == "spm":
        return None
    primary = design.factors[0]
    names = [f.name for f in design.factors]
    columns = [y, *names, *([unit_column] if design.within and unit_column in rows.columns else [])]
    checks = tuple(engine.assumptions(
        rows[columns], y, between=design.between, within=design.within,
        subject=unit_column if design.within else None,
        levels={f.name: list(f.levels) for f in design.factors}, alpha=analysis.alpha,
    ))
    evidence = [_check_text(c) for c in checks]
    normal = not any(c.get("violated") for c in checks if c.get("name") == "normality")
    welch = "ttest_ind_student" if analysis.equal_var else "ttest_ind_welch"
    between_pair = welch if normal else "mann_whitney"
    if design.empty_cells:
        # Never recommend a model csv-stats will refuse (scidb.log
        # 2026-10-20: a full-interaction mixed model on data with empty
        # cells was refused, and every contrast that hung on it with it).
        # The pairwise tests run inside each combination of the other
        # factors, so the cells that have data are still compared.
        evidence.insert(0, f"{len(design.empty_cells)} combination(s) of levels have no rows "
                           f"({cells_text(design.empty_cells)}): a model with every interaction can't be "
                           f"estimated, so no omnibus test is recommended; analyse a factor separately, "
                           f"leave out the empty level, or write a model formula without that interaction")
        if primary.kind == WITHIN:
            pairwise = "ttest_paired" if normal else "wilcoxon"
        else:
            pairwise = between_pair
        return Recommendation(None, pairwise, tuple(evidence), checks)
    if design.incomplete_units and analysis.missing == "use_all":
        n = len(design.incomplete_units)
        evidence.insert(0, f"{n} {design.unit or 'unit'}(s) lack some within levels "
                           f"({', '.join(design.incomplete_units)}): a mixed model uses all their observations")
        pairwise = "lmm_pairwise" if primary.kind == WITHIN else between_pair
        return Recommendation("lmm", pairwise, tuple(evidence), checks)
    if normal:
        pairwise = "ttest_paired" if primary.kind == WITHIN else between_pair
        return Recommendation("anova", pairwise, tuple(evidence), checks)
    pairwise = "wilcoxon" if primary.kind == WITHIN else "mann_whitney"
    if len(design.factors) == 1:
        omnibus = "friedman" if primary.kind == WITHIN and len(primary.levels) >= 3 else (
            "kruskal" if primary.kind == BETWEEN else None)
    else:
        omnibus = None
        evidence.append("no rank-based test for a factorial design: choose ANOVA or a mixed model for the omnibus")
    return Recommendation(omnibus, pairwise, tuple(evidence), checks)


def _pairwise_default(analysis: Analysis, design: Design,
                      rec: Recommendation | None = None) -> tuple[str | None, str]:
    primary = design.factors[0] if design.factors else None
    if primary is None:
        return None, "no grouping layer"
    within = primary.kind == WITHIN
    if analysis.family == "auto" and rec is not None and rec.pairwise:
        return rec.pairwise, "recommended: " + "; ".join(rec.evidence)
    if analysis.family == "mixed_model" and within:
        return "lmm_pairwise", f"{primary.name} is within-subject; mixed model chosen"
    if analysis.family == "spm":
        test = "spm_ttest_paired" if within else "spm_ttest2"
    elif analysis.family == "nonparametric":
        test = "wilcoxon" if within else "mann_whitney"
    elif within:
        test = "ttest_paired"
    else:
        test = "ttest_ind_student" if analysis.equal_var else "ttest_ind_welch"
    because = f"{primary.name} is {primary.kind}-subject" + (" (overridden)" if primary.overridden else "")
    if test.startswith("ttest_ind"):
        because += ", equal variances assumed" if analysis.equal_var else ", equal variances not assumed"
    return test, because


def _omnibus_default(analysis: Analysis, design: Design,
                     rec: Recommendation | None = None) -> tuple[str | None, str]:
    if not design.factors:
        return None, "no grouping layer"
    if analysis.family == "spm":
        return None, "SPM on curves runs the pairwise tests; an SPM ANOVA is not yet in csv-stats"
    if analysis.family == "auto" and rec is not None:
        if rec.omnibus:
            return rec.omnibus, "recommended: " + "; ".join(rec.evidence)
        return None, "; ".join(rec.evidence) or "no omnibus test recommended"
    if analysis.family == "mixed_model":
        return "lmm", "mixed model chosen"
    if analysis.family in ("parametric", "auto"):
        # "auto" without rows to check (e.g. the GUI's menu before a run).
        return "anova", "parametric family" if analysis.family == "parametric" else "auto: no data checked yet"
    if len(design.factors) == 1:
        f = design.factors[0]
        if f.kind == WITHIN:
            return "friedman", f"{f.name} is within-subject"
        return "kruskal", f"{f.name} is between-subject"
    return None, "no rank-based test for a factorial design; the pairwise rank tests still run"


def select_tests(analysis: Analysis, design: Design, rec: Recommendation | None = None, *,
                 mixed_formula: bool = False) -> Selection:
    """The omnibus and pairwise tests for ``analysis`` on ``design``.

    An override (``omnibus_test`` / ``pairwise_test``) wins, and is still
    checked by :func:`why_unavailable`. With family "auto", ``rec`` (from
    :func:`recommend`) supplies the defaults; without it, parametric.
    ``mixed_formula`` (the model formula needs a mixed model): the defaults
    are lmm / lmm_pairwise, "chosen: model formula", and an override of
    another test is refused with :data:`MIXED_FORMULA_REASON`.
    """
    omnibus = omnibus_reason = pairwise = pairwise_reason = None
    whys: list[str] = []
    formula_because = "chosen: model formula"
    if analysis.omnibus:
        if analysis.omnibus_test:
            omnibus, because = analysis.omnibus_test, "chosen"
            if TESTS.get(omnibus) and TESTS[omnibus].role != "omnibus":
                omnibus_reason = f"{TESTS[omnibus].label} is not an omnibus test"
        elif mixed_formula:
            omnibus, because = "lmm", formula_because
        else:
            omnibus, because = _omnibus_default(analysis, design, rec)
        if omnibus is None:
            omnibus_reason = because
        elif omnibus_reason is None:
            omnibus_reason = why_unavailable(omnibus, design, mixed_formula=mixed_formula)
        if omnibus:
            whys.append(f"omnibus: {TESTS[omnibus].label if omnibus in TESTS else omnibus} ({because})")
    if analysis.pairwise:
        if analysis.pairwise_test:
            pairwise, because = analysis.pairwise_test, "chosen"
            if TESTS.get(pairwise) and TESTS[pairwise].role != "pairwise":
                pairwise_reason = f"{TESTS[pairwise].label} is not a pairwise test"
        elif mixed_formula:
            pairwise, because = "lmm_pairwise", formula_because
        else:
            pairwise, because = _pairwise_default(analysis, design, rec)
        if pairwise is None:
            pairwise_reason = because
        elif pairwise_reason is None:
            pairwise_reason = why_unavailable(pairwise, design, mixed_formula=mixed_formula)
        if pairwise:
            whys.append(f"pairwise: {TESTS[pairwise].label if pairwise in TESTS else pairwise} ({because})")
    return Selection(omnibus, omnibus_reason, pairwise, pairwise_reason, "; ".join(whys) or "nothing selected",
                     rec.to_dict() if rec is not None else None)


def available_tests(design: Design, *, mixed_formula: bool = False) -> list[dict[str, Any]]:
    """Every test with its availability, for the GUI's "+ Add test" menu."""
    return [
        {"id": t.id, "label": t.label, "role": t.role, "family": t.family,
         "reason": why_unavailable(t.id, design, mixed_formula=mixed_formula)}
        for t in TESTS.values()
    ]
