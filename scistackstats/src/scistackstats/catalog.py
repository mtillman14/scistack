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

from .design import BETWEEN, MIXED, WITHIN, Design
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
        StatTest("anova", "ANOVA", "omnibus", "parametric"),
        StatTest("kruskal", "Kruskal–Wallis", "omnibus", "nonparametric"),
        StatTest("friedman", "Friedman", "omnibus", "nonparametric"),
    )
}


def _design_problem(design: Design) -> str | None:
    if not design.factors:
        return "nothing is grouped: add a grouping layer to compare its levels"
    if design.mixed:
        names = ", ".join(design.mixed)
        return f"{names}: some units at one level, some at several; set within or between first"
    if design.within and design.unit is None:
        return "a within-subject factor needs an experimental unit (collapse to one, without weight by N)"
    return None


def why_unavailable(test_id: str, design: Design) -> str | None:
    """``None`` if ``test_id`` can run on ``design``, else the reason."""
    info = TESTS.get(test_id)
    if info is None:
        return f"unknown test {test_id!r}"
    problem = _design_problem(design)
    if problem:
        return problem
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
    if test_id == "anova":
        thin = [f.name for f in design.factors if len(f.levels) < 2]
        if thin:
            return f"{', '.join(thin)} has fewer than 2 levels here"
    return None


@dataclass(frozen=True)
class Selection:
    omnibus: str | None
    omnibus_reason: str | None
    pairwise: str | None
    pairwise_reason: str | None
    #: Why these tests (shown in the GUI and the report).
    why: str

    def to_dict(self) -> dict[str, Any]:
        return {"omnibus": self.omnibus, "omnibus_reason": self.omnibus_reason,
                "pairwise": self.pairwise, "pairwise_reason": self.pairwise_reason, "why": self.why}


def _pairwise_default(analysis: Analysis, design: Design) -> tuple[str | None, str]:
    primary = design.factors[0] if design.factors else None
    if primary is None:
        return None, "no grouping layer"
    within = primary.kind == WITHIN
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


def _omnibus_default(analysis: Analysis, design: Design) -> tuple[str | None, str]:
    if not design.factors:
        return None, "no grouping layer"
    if analysis.family == "spm":
        return None, "SPM on curves runs the pairwise tests; an SPM ANOVA is not yet in csv-stats"
    if analysis.family == "parametric":
        return "anova", "parametric family"
    if len(design.factors) == 1:
        f = design.factors[0]
        if f.kind == WITHIN:
            return "friedman", f"{f.name} is within-subject"
        return "kruskal", f"{f.name} is between-subject"
    return None, "no rank-based test for a factorial design; the pairwise rank tests still run"


def select_tests(analysis: Analysis, design: Design) -> Selection:
    """The omnibus and pairwise tests for ``analysis`` on ``design``.

    An override (``omnibus_test`` / ``pairwise_test``) wins, and is still
    checked by :func:`why_unavailable`.
    """
    omnibus = omnibus_reason = pairwise = pairwise_reason = None
    whys: list[str] = []
    if analysis.omnibus:
        if analysis.omnibus_test:
            omnibus, because = analysis.omnibus_test, "chosen"
            if TESTS.get(omnibus) and TESTS[omnibus].role != "omnibus":
                omnibus_reason = f"{TESTS[omnibus].label} is not an omnibus test"
        else:
            omnibus, because = _omnibus_default(analysis, design)
        if omnibus is None:
            omnibus_reason = because
        elif omnibus_reason is None:
            omnibus_reason = why_unavailable(omnibus, design)
        if omnibus:
            whys.append(f"omnibus: {TESTS[omnibus].label if omnibus in TESTS else omnibus} ({because})")
    if analysis.pairwise:
        if analysis.pairwise_test:
            pairwise, because = analysis.pairwise_test, "chosen"
            if TESTS.get(pairwise) and TESTS[pairwise].role != "pairwise":
                pairwise_reason = f"{TESTS[pairwise].label} is not a pairwise test"
        else:
            pairwise, because = _pairwise_default(analysis, design)
        if pairwise is None:
            pairwise_reason = because
        elif pairwise_reason is None:
            pairwise_reason = why_unavailable(pairwise, design)
        if pairwise:
            whys.append(f"pairwise: {TESTS[pairwise].label if pairwise in TESTS else pairwise} ({because})")
    return Selection(omnibus, omnibus_reason, pairwise, pairwise_reason, "; ".join(whys) or "nothing selected")


def available_tests(design: Design) -> list[dict[str, Any]]:
    """Every test with its availability, for the GUI's "+ Add test" menu."""
    return [
        {"id": t.id, "label": t.label, "role": t.role, "family": t.family, "reason": why_unavailable(t.id, design)}
        for t in TESTS.values()
    ]
