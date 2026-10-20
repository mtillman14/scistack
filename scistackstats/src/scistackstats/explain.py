"""Every statistics concept and wizard step, in lay terms (plan-statstudio-guided).

The ONE owner of StatStudio's plain-language wording. The webview shows
these strings and decides nothing (it holds no statistics vocabulary, a
lint in the frontend tests checks it). Facts come from :mod:`.guide`
(which reads them from their owners); this module only words them.

- :data:`CONCEPTS`: one entry per idea the wizard names, with its lay title,
  the technical name the results tables use, a short explanation and a
  longer one. Every test, correction and effect size in the catalogs has
  one (``tests/test_explain.py`` checks the coverage).
- :func:`build_steps`: one :class:`StepGuide` per ``spec.GUIDE_STEPS`` id,
  with the question, the facts about THIS data, choice cards (each with the
  spec edit it makes, as an ``action``), the recommendation and whether the
  step blocks going on.

Actions are a small vocabulary the webview applies through its spec
helpers (``statsSpec.ts``): ``set_role``, ``set_primary``, ``set_override``,
``set_analysis`` (``patch``; a None value clears the field back to its
default) and ``exclude_level``.

Wording status: drafted 2026-10-20; the user reviews it in a separate pass
before it ships (plan Q3).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from .catalog import TESTS
from .design import BETWEEN, MIXED, WITHIN, cells_text
from .effects import EFFECT_SIZES, default_effect_size
from .run import CORRECTION_LABELS

if TYPE_CHECKING:
    from .guide import DVFacts, FactorFacts, GuideFacts


# ---------------------------------------------------------------------------
# Concepts
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Concept:
    id: str
    #: The lay name, shown first.
    title: str
    #: The name in the results tables and papers, shown beside the lay name.
    technical: str
    #: One to three plain sentences.
    lay: str
    #: The longer explanation behind "Learn more".
    more: str = ""

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def _c(id: str, title: str, technical: str, lay: str, more: str = "") -> Concept:
    return Concept(id, title, technical, lay, more)


_CONCEPT_LIST = (
    # --- what is measured, and on whom ------------------------------------
    _c("measure", "What you measured", "dependent variable (DV)",
       "The number you recorded and want to compare, such as walking distance or heart rate. "
       "Each measure is analysed on its own.",
       "If you pick several measures, each gets its own rows in the tables and its own tests."),
    _c("unit", "One independent thing", "experimental unit",
       "The thing that was sampled independently, often one participant. Measurements from the same "
       "participant are related, so the analysis counts each participant once, not each measurement.",
       "Getting this right matters more than the choice of test. If one person contributes many rows and "
       "each row is treated as independent, the test believes it has far more evidence than it really has, "
       "and finds 'significant' differences too easily. This mistake is called pseudoreplication."),
    _c("pseudoreplication", "Counting the same person many times", "pseudoreplication",
       "Treating several measurements of the same participant as if they came from different participants. "
       "It makes results look more certain than they are."),
    _c("averaged_first", "Averaged first", "aggregation to the unit",
       "Repeated recordings of the same condition, such as several trials, are averaged into one value per "
       "participant before any test, so each participant counts once."),
    # --- what each factor does --------------------------------------------
    _c("role.compare", "Compare its levels", "factor (grouping layer)",
       "The groups or conditions you want to compare, such as treatment vs control, or before vs after."),
    _c("role.separate", "Analyse separately for each", "stratum",
       "Runs the whole analysis once for each level, as if each were its own small study. Use it for things "
       "you don't want to compare with each other.",
       "Levels analysed separately are never compared with each other, and each gets its own table section."),
    _c("role.average", "Average over", "collapsed factor",
       "Averages its levels together into one value, so they are not compared with each other. Use it for "
       "repeated recordings of the same condition, such as several trials.",
       "The participant is averaged over too: each participant's recordings become one value per condition, "
       "so each participant counts once. Of everything you average over, the largest (usually the "
       "participant) is what counts as one independent unit."),
    # --- repeated measures --------------------------------------------------
    _c("within", "Same participants at every level", "within-subject factor (repeated measures)",
       "Every participant was measured at each level, for example before and after training. Each "
       "participant is compared with themselves, which removes person-to-person differences and makes the "
       "test more sensitive."),
    _c("between", "Different participants at each level", "between-subject factor",
       "Each participant belongs to only one level, for example a treatment group or a control group. "
       "The comparison is between different people."),
    _c("mixed_membership", "Some at one level, some at several", "mixed membership",
       "Some participants appear at one level only and others at several. StatStudio can't tell whether this "
       "is a grouping (different people) or repeated measures with some missing data, so you decide."),
    # --- missing data ---------------------------------------------------------
    _c("incomplete", "Missing some measurements", "incomplete units",
       "Some participants weren't measured at every level, for example because of a missed visit."),
    _c("missing.use_all", "Use every measurement", "all available data (mixed model)",
       "Keeps everyone, including people with missed visits, by using a model that copes with gaps "
       "(a linear mixed model). Usually the better choice.",
       "Dropping people loses information, and if the people who missed visits differ from those who "
       "didn't, dropping them can bias the results."),
    _c("missing.complete_cases", "Only people with every measurement", "complete-case analysis",
       "Leaves out anyone missing a measurement, so every test compares the same people. Simpler, but it "
       "throws data away. Everyone left out is named in the table notes."),
    _c("empty_cells", "Combinations nobody was measured in", "empty cells",
       "A combination of levels with no data at all, for example one group that was never measured at one "
       "visit. A test of every combination at once can't be calculated, because there is nothing to "
       "calculate for that combination.",
       "This is different from a few missed visits: nobody at all has that combination. The pairs that do "
       "have data are still compared. To get an overall test, analyse one of the factors separately, or leave "
       "out the level that has the gap."),
    # --- the question -----------------------------------------------------------
    _c("primary", "Main comparison", "primary comparisons (simple effects)",
       "The pairs you most want to compare. StatStudio compares every pair of its levels, separately "
       "within each level of the other compared factors."),
    _c("pairwise", "Comparing two at a time", "pairwise comparisons",
       "Compares two levels at a time and reports how big the difference is and how certain it is."),
    _c("omnibus", "Overall test", "omnibus test",
       "One test per factor that asks 'is there any difference at all?', and one for how the factors "
       "combine, before looking at specific pairs."),
    _c("interaction", "Does one factor change the other's effect?", "interaction",
       "Whether the difference between the levels of one factor depends on another factor, for example the "
       "groups differ after training but not before."),
    # --- assumptions and families of tests ---------------------------------------
    _c("normality", "Bell-shaped values", "normality (Shapiro–Wilk test)",
       "Many tests assume the leftover variation follows a bell curve. When it clearly doesn't (skewed "
       "values, outliers), tests based on ranks are safer.",
       "StatStudio checks this on the residuals (each value minus its group's mean). Checking first and then "
       "choosing is debated among statisticians, so the check is shown as a recommendation, not a rule."),
    _c("equal_variances", "Similar spread in each group", "homogeneity of variance (Levene's test)",
       "Whether the groups vary by similar amounts. When they don't, Welch's t-test is used, which doesn't "
       "need equal spread."),
    _c("parametric", "Tests on the values themselves", "parametric tests",
       "t-tests and ANOVA compare averages, and assume roughly bell-shaped data."),
    _c("rank_based", "Tests on ranks", "nonparametric (rank-based) tests",
       "These replace each value by its rank (1st, 2nd, ...), so outliers and skew matter less. Slightly less "
       "sensitive than tests on the values when the data are bell-shaped."),
    _c("mixed_model", "Mixed model", "linear mixed model",
       "A model that gives each participant their own starting level, so participants with missing "
       "measurements still count."),
    # --- the tests (one per catalog.TESTS id) -------------------------------------
    _c("test.ttest_ind_welch", "Compare the averages of two separate groups", "Welch's t-test",
       "Compares the averages of two groups of different people, without assuming they vary by the same "
       "amount."),
    _c("test.ttest_ind_student", "Compare the averages of two separate groups (equal spread)",
       "Student's t-test",
       "Compares the averages of two groups of different people, assuming they vary by the same amount."),
    _c("test.ttest_paired", "Compare the same people twice", "paired t-test",
       "Compares two measurements of the same people by looking at each person's own difference."),
    _c("test.mann_whitney", "Compare two separate groups by rank", "Mann–Whitney U test",
       "Compares two groups of different people using ranks instead of the values: does one group tend to "
       "have higher values?"),
    _c("test.wilcoxon", "Compare the same people twice, by rank", "Wilcoxon signed-rank test",
       "Compares two measurements of the same people using the ranks of their differences."),
    _c("test.spm_ttest2", "Compare whole curves of two separate groups", "SPM two-sample t-test",
       "Compares whole curves (such as a gait cycle) point by point between two groups, and shows where "
       "along the curve they differ."),
    _c("test.spm_ttest_paired", "Compare whole curves of the same people", "SPM paired t-test",
       "Compares whole curves of the same people under two conditions, point by point, and shows where they "
       "differ."),
    _c("test.lmm_pairwise", "Compare pairs using the mixed model", "mixed-model contrasts",
       "Compares two levels using the mixed model, so participants with missing measurements still count."),
    _c("test.anova", "Overall test on the values", "ANOVA",
       "Tests whether the averages differ across the levels of each factor, and whether the factors "
       "combine."),
    _c("test.kruskal", "Overall rank test, separate groups", "Kruskal–Wallis test",
       "A rank-based overall test for one factor whose levels are different people."),
    _c("test.friedman", "Overall rank test, same people", "Friedman test",
       "A rank-based overall test for one factor measured repeatedly in the same people."),
    _c("test.lmm", "Overall test from the mixed model", "linear mixed model (F tests)",
       "Asks whether each factor, and their combination, matters, using every participant's data."),
    # --- many comparisons -----------------------------------------------------------
    _c("multiple_comparisons", "Many comparisons, more false alarms", "multiple comparisons",
       "Each test has a small chance (usually 5%) of a false alarm: calling a difference real when it is "
       "just chance. Run 20 tests and you can expect about one false alarm. A correction raises the bar for "
       "each test so false alarms stay rare."),
    _c("family", "Which tests share a correction", "correction family",
       "The group of tests that a correction is spread across. By default that is one measure in one table "
       "section."),
    _c("correction.none", "No correction", "unadjusted p-values",
       "Each test is judged at 5% on its own. Only defensible for one comparison you planned in advance."),
    _c("correction.bonferroni", "Strict and simple", "Bonferroni correction",
       "Splits the 5% evenly across the tests. Simple and strict, so it can miss real differences."),
    _c("correction.holm", "Strict, step by step (recommended)", "Holm correction",
       "A step-by-step version of Bonferroni. It keeps false alarms just as rare but misses fewer real "
       "differences. A good default."),
    _c("correction.fdr_bh", "Allow a few false alarms among many findings", "Benjamini–Hochberg (FDR)",
       "Keeps the share of 'significant' findings that are false alarms low, instead of the chance of any "
       "false alarm. Finds more, at the cost of a few false ones. Common for many exploratory tests."),
    _c("correction.fdr_by", "Like Benjamini–Hochberg, for related tests", "Benjamini–Yekutieli (FDR)",
       "Like Benjamini–Hochberg, but still valid when the tests are strongly related. Stricter."),
    # --- size of an effect ---------------------------------------------------------------
    _c("effect_size", "How big the difference is", "effect size",
       "A p-value says whether a difference is likely to be real, not whether it matters. An effect size "
       "puts the difference on a standard scale, so it can be compared across measures and studies."),
    _c("difference", "The difference, in your units", "estimated difference",
       "How much higher one level is than the other, in the measure's own units (such as metres)."),
    _c("confidence_interval", "Range of plausible values", "confidence interval (CI)",
       "The range of differences that fit the data. If it doesn't include 0, the difference is significant. "
       "A wide range means the estimate is uncertain."),
    _c("spread.ci", "Range of plausible values", "95% CI of the difference",
       "Shows the range the true difference probably lies in. The usual choice for papers."),
    _c("spread.se", "Typical error of the difference", "standard error (SE) of the difference",
       "How far the estimated difference would typically move in a new sample."),
    _c("spread.sd_diff", "How much people's changes vary", "SD of the differences",
       "For the same people measured twice: how much each person's change varies around the average change."),
    _c("effect.d_s", "Difference in standard deviations", "Cohen's d_s",
       "The difference between two groups divided by their pooled spread. Rough guide: 0.2 small, 0.5 medium, "
       "0.8 large."),
    _c("effect.g_s", "Difference in standard deviations (small-sample corrected)", "Hedges' g_s",
       "Like Cohen's d, corrected so it isn't too large in small samples. Rough guide: 0.2 small, 0.5 medium, "
       "0.8 large."),
    _c("effect.glass_delta", "Difference in the control group's standard deviations", "Glass's Δ",
       "The difference divided by the spread of one group (usually the control). Useful when the groups vary "
       "by very different amounts."),
    _c("effect.d_z", "Average change in units of how much changes vary", "Cohen's d_z",
       "For the same people measured twice: the average change divided by how much the changes vary between "
       "people."),
    _c("effect.d_av", "Average change in units of the typical spread", "Cohen's d_av",
       "For the same people measured twice: the average change divided by the average of the two spreads. "
       "Comparable with group-difference sizes."),
    _c("effect.d_rm", "Average change, corrected for the link between measurements", "Cohen's d_rm",
       "For the same people measured twice: a d that allows for how strongly the two measurements are "
       "related."),
    _c("effect.rank_biserial", "How often one beats the other", "rank-biserial correlation",
       "From −1 to 1: how much more often values in one level are higher than values in the other. 0 means "
       "no tendency."),
    _c("effect.eta2_p", "Share of variation explained (this factor alone)", "partial η²",
       "The share of the variation explained by a factor, leaving the other factors out of the total."),
    _c("effect.eta2_g", "Share of variation explained (comparable across designs)", "generalized η²",
       "The share of the variation explained by a factor, counted so that repeated-measures and "
       "separate-groups studies can be compared."),
    _c("effect.omega2_p", "Share of variation explained (less biased)", "partial ω²",
       "Like partial η², but corrected so it isn't too large in small samples."),
    _c("effect.epsilon2", "Share of variation explained, by rank", "ε² (rank)",
       "The rank-based share of variation explained, for the Kruskal–Wallis test."),
    _c("effect.kendall_w", "How consistently people rank the levels", "Kendall's W",
       "From 0 to 1: how consistently participants put the levels in the same order, for the Friedman "
       "test."),
    # --- reading results ----------------------------------------------------------------------
    _c("p_value", "How surprising the data would be by chance", "p-value",
       "If there were truly no difference, how surprising would data like these be? A small p (below 0.05) "
       "means the difference is unlikely to be chance alone. It is not the chance that the result is wrong, "
       "and it says nothing about how big the difference is."),
    _c("adjusted_p", "p after the correction", "adjusted p-value",
       "The p-value after the many-comparisons correction. Use this one to decide what is significant."),
    _c("alpha", "Significance threshold", "α (alpha)",
       "The p-value below which a result is called significant, usually 0.05."),
    _c("statistic", "The test's own number", "test statistic and df",
       "The number the test calculates (t, U, W or F), with its degrees of freedom (df). Bigger means a "
       "bigger difference relative to the noise. Papers report it beside p."),
)

CONCEPTS: dict[str, Concept] = {c.id: c for c in _CONCEPT_LIST}


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@dataclass
class Choice:
    id: str
    label: str
    #: One plain sentence under the label, or "".
    lay: str = ""
    recommended: bool = False
    selected: bool = False
    #: The spec edit (module docstring), or None for "keep as it is".
    action: dict[str, Any] | None = None
    #: Why it can't be chosen here, or None.
    disabled: str | None = None
    concept: str | None = None


@dataclass
class Card:
    id: str
    title: str
    lay: str = ""
    choices: list[Choice] = field(default_factory=list)


@dataclass
class StepGuide:
    id: str
    #: The step's name in the step list.
    title: str
    #: The question the step asks, as its headline.
    question: str
    #: Concept ids, in the order the explanation panel shows them.
    concepts: list[str] = field(default_factory=list)
    #: Sentences about THIS data.
    facts: list[str] = field(default_factory=list)
    cards: list[Card] = field(default_factory=list)
    #: What StatStudio recommends here and why, in one or two sentences.
    recommendation: str | None = None
    #: Why the user can't go on past this step yet, or None.
    blocked: str | None = None
    warnings: list[str] = field(default_factory=list)
    #: The step reads a run's results (tests, corrections, effect sizes).
    needs_results: bool = False
    #: A small table: ``{"columns": [...], "rows": [[...]]}``.
    table: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _n(count: int, word: str, plural: str | None = None) -> str:
    return f"{count} {word if count == 1 else (plural or word + 's')}"


def _and(names: list[str]) -> str:
    names = [str(n) for n in names]
    if len(names) <= 1:
        return "".join(names)
    return ", ".join(names[:-1]) + " and " + names[-1]


def _first_sentence(text: str) -> str:
    first = text.split(". ")[0]
    return first if first.endswith((".", "?", "!")) else first + "."


def _levels(levels: list[str], limit: int = 5) -> str:
    shown = ", ".join(levels[:limit])
    return shown + (f" (+{len(levels) - limit} more)" if len(levels) > limit else "")


def _unit_word(dv: "DVFacts | None") -> str:
    return dv.unit if dv is not None and dv.unit else "participant"


def _first_dv(facts: "GuideFacts") -> "DVFacts | None":
    return next((d for d in facts.dvs if d.error is None), None)


def _compared(facts: "GuideFacts") -> list["FactorFacts"]:
    dv = _first_dv(facts)
    names = dv.compared if dv else []
    by_name = {f.name: f for f in facts.factors}
    return [by_name[n] for n in names if n in by_name]


def _set_analysis(**patch: Any) -> dict[str, Any]:
    return {"op": "set_analysis", "patch": patch}


# --- measure ------------------------------------------------------------------


def _step_measure(facts: "GuideFacts") -> StepGuide:
    step = StepGuide("measure", "Measure", "What did you measure?", concepts=["measure"])
    for dv in facts.dvs:
        if dv.error:
            step.warnings.append(f"{dv.dv}: {dv.error}")
            continue
        who = f" from {_n(dv.n_units, dv.unit)}" if dv.unit and dv.n_units is not None else ""
        step.facts.append(f"{dv.dv}: {_n(dv.n_values, 'value')} to analyse{who}.")
    if not facts.dvs:
        step.blocked = "Add a measure to analyse."
    elif all(dv.error for dv in facts.dvs):
        step.blocked = "None of the measures could be read; see the messages above."
    return step


# --- unit ---------------------------------------------------------------------


def _step_unit(facts: "GuideFacts") -> StepGuide:
    dv = _first_dv(facts)
    step = StepGuide("unit", "Independent unit", "What is one independent thing you measured?",
                     concepts=["unit", "averaged_first", "pseudoreplication"])
    if dv is None:
        step.blocked = "Choose a measure first."
        return step
    if dv.unit:
        step.facts.append(f"Each {dv.unit} counts once: {_n(dv.n_units or 0, dv.unit)} in total.")
        above = [k for k in dv.unit_keys if k != dv.unit]
        if above:
            step.facts.append(
                f"A {dv.unit} is identified within its {_and(above)}: {dv.unit} \"01\" in two different "
                f"{above[-1]}s is two different {dv.unit}s.")
        if dv.averaged:
            step.facts.append(f"Several {_and(dv.averaged)} per {dv.unit} are averaged into one value first.")
        step.recommendation = (f"Counting each {dv.unit} once is right for most studies. To change it, set what "
                               f"is averaged over in the next step.")
        return step
    step.warnings.append(
        "Nothing is averaged over, so every row is treated as an independent measurement. If several rows "
        "come from the same participant, that overstates the evidence.")
    card = Card("unit", "Which is the independent unit?",
                "Choosing one averages everything inside it, so it counts once.")
    for f in facts.factors:
        if f.is_schema and not f.advanced_only and f.role != "group":
            card.choices.append(Choice(f"unit:{f.name}", f"Each {f.name}",
                                       f"Average over {f.name}, so each {f.name} counts once.",
                                       action={"op": "set_role", "factor": f.name, "role": "collapse"},
                                       concept="role.average"))
    if card.choices:
        step.cards.append(card)
    return step


# --- factors --------------------------------------------------------------------


_ROLE_CHOICES = (
    ("group", "Compare", "role.compare"),
    ("separate", "Analyse separately", "role.separate"),
    ("collapse", "Average over", "role.average"),
)


def _step_factors(facts: "GuideFacts") -> StepGuide:
    dv = _first_dv(facts)
    step = StepGuide("factors", "What to compare", "What are you comparing?",
                     concepts=["role.compare", "role.separate", "role.average"])
    if dv is None:
        step.blocked = "Choose a measure first."
        return step
    by_name = {f.name: f for f in facts.factors}
    compared = [n for n in reversed(dv.compared)]
    if compared:
        step.facts.append(f"Comparing: {' × '.join(compared)}.")
    if dv.strata:
        step.facts.append(f"Analysed separately for each: {_and(dv.strata)}.")
    averaged = [f.name for f in facts.factors if f.role == "collapse"]
    if averaged:
        step.facts.append(f"Averaged over: {_and(averaged)}.")
    n_cells = 1
    for name in compared:
        n_cells *= max(1, len(by_name[name].levels)) if name in by_name else 1
    if len(compared) >= 3 and dv.n_units:
        step.warnings.append(
            f"That is {n_cells} combinations for {_n(dv.n_units, dv.unit or 'participant')}. With this many, some "
            f"combinations may have few or no {dv.unit or 'participant'}s; consider analysing one factor "
            f"separately.")
    hidden = [f.name for f in facts.factors if f.advanced_only]
    if hidden:
        step.facts.append(f"Set in the Advanced view: {_and(hidden)}.")
    for f in facts.factors:
        if f.advanced_only:
            continue
        current = "separate" if f.role in ("facet", "iterate") else f.role
        is_unit = f.name == dv.unit
        card = Card(f"role:{f.name}", f.name, f"Levels: {_levels(f.levels)}" if f.levels else "")
        if is_unit:
            card.lay = f"Your independent unit (previous step). {card.lay}".strip()
        for role, label, concept in _ROLE_CHOICES:
            target = ("iterate" if f.role not in ("facet", "iterate") else f.role) if role == "separate" else role
            card.choices.append(Choice(
                f"{f.name}:{role}", label, _first_sentence(CONCEPTS[concept].lay), selected=current == role,
                action={"op": "set_role", "factor": f.name, "role": target}, concept=concept,
                disabled=(f"{f.name} is the independent unit; it is averaged over"
                          if is_unit and role != "collapse" else None),
            ))
        step.cards.append(card)
    if not compared:
        step.blocked = "Choose at least one thing to compare."
    return step


# --- within / between --------------------------------------------------------------


def _kind_lay(f: "FactorFacts", unit: str) -> str:
    if f.kind == MIXED:
        return (f"{_n(f.units_multi, unit)} were measured at more than one {f.name} level and "
                f"{_n(f.units_single, unit)} at only one.")
    if f.basis == "schema":
        return (f"{f.name} is recorded inside each {unit} in your dataset, so it is a repeated measurement. "
                f"{_n(f.units_single, unit)} have only one {f.name} level: missing data, not a separate group.")
    if f.derived == WITHIN:
        return f"Each of the {_n(f.units_multi, unit)} was measured at more than one {f.name} level."
    if f.derived == BETWEEN:
        return f"Each {unit} appears at only one {f.name} level."
    return ""


def _step_within_between(facts: "GuideFacts") -> StepGuide:
    dv = _first_dv(facts)
    unit = _unit_word(dv)
    step = StepGuide("within_between", "Same or different people",
                     f"Was each {unit} measured at every level?", concepts=["within", "between"])
    compared = _compared(facts)
    if dv is None or not compared:
        step.facts.append("Nothing is compared yet.")
        return step
    if not dv.unit:
        step.facts.append("Without an independent unit, every level is treated as different participants.")
        return step
    analysis_id = facts.analysis.id if facts.analysis else None
    for f in compared:
        lay = _kind_lay(f, unit)
        if f.overridden:
            lay += f" You set it to {'repeated measures' if f.kind == WITHIN else 'separate groups'}."
        card = Card(f"kind:{f.name}", f.name, lay)
        for kind, label, concept in ((WITHIN, f"Same {unit}s, measured repeatedly", "within"),
                                     (BETWEEN, f"Different {unit}s in each level", "between")):
            card.choices.append(Choice(
                f"{f.name}:{kind}", label, _first_sentence(CONCEPTS[concept].lay),
                recommended=f.derived == kind, selected=f.kind == kind,
                action={"op": "set_override", "analysis": analysis_id, "factor": f.name,
                        "kind": None if f.derived == kind else kind},
                concept=concept,
            ))
        step.cards.append(card)
    if any(f.kind == MIXED for f in compared):
        step.concepts.append("mixed_membership")
    if dv.actions:
        step.blocked = " ".join(a["message"] for a in dv.actions)
    return step


# --- data check ------------------------------------------------------------------------


def _step_data_check(facts: "GuideFacts") -> StepGuide:
    dv = _first_dv(facts)
    unit = _unit_word(dv)
    step = StepGuide("data_check", "Missing data", "Is anything missing?", concepts=["incomplete"])
    if dv is None:
        step.blocked = "Choose a measure first."
        return step
    analysis = facts.analysis
    if dv.n_units is not None:
        step.facts.append(f"{_n(dv.n_units, unit)} in total.")
    if dv.cells:
        names = list(dv.cells[0]["cell"])
        counted = "units" if dv.cells[0]["units"] is not None else "rows"
        step.table = {
            "columns": [*names, f"{unit}s" if counted == "units" else "values"],
            "rows": [[*c["cell"].values(), c[counted]] for c in dv.cells],
            "empty_rows": [i for i, c in enumerate(dv.cells) if c["rows"] == 0],
        }
    if dv.incomplete_units:
        k = len(dv.incomplete_units)
        shown = _and(dv.incomplete_units[:6]) + (f" and {k - 6} more" if k > 6 else "")
        step.facts.append(f"{k} of the {_n(dv.n_units or k, unit)} weren't measured at every level: {shown}.")
        current = analysis.missing if analysis else "use_all"
        card = Card("missing", "What should happen to them?")
        for policy in ("use_all", "complete_cases"):
            concept = CONCEPTS[f"missing.{policy}"]
            card.choices.append(Choice(
                f"missing:{policy}", concept.title, _first_sentence(concept.lay),
                recommended=policy == "use_all", selected=current == policy,
                action=_set_analysis(missing=policy), concept=f"missing.{policy}",
            ))
        step.cards.append(card)
        step.concepts += ["missing.use_all", "missing.complete_cases"]
        step.recommendation = ("Use every measurement: a mixed model keeps everyone, and nobody's data is thrown "
                               "away.")
    else:
        step.facts.append(f"Every {unit} was measured at every level.")
    if dv.empty_cells:
        step.concepts.append("empty_cells")
        first = dv.empty_cells[0]
        where = f" (in {_n(len(dv.empty_cells), 'section')} of {dv.n_strata})" if dv.n_strata > 1 else ""
        step.facts.append(f"Nobody was measured in: {cells_text(first['cells'])}{where}.")
        if dv.incomplete_units:
            step.facts.append(
                f"{unit.capitalize()}s in a group that was never measured at a level count as 'not measured at "
                f"every level' above, even though nobody could have been.")
        step.warnings.append("An overall test of every combination can't be calculated with an empty "
                             "combination. Pairs that have data are still compared.")
        primary = dv.compared[0] if dv.compared else None
        card = Card("empty", "What should happen to the empty combinations?")
        card.choices.append(Choice(
            "empty:keep", "Keep everything", "Compare the pairs that have data, and skip the overall test.",
            recommended=True, selected=True, action=None,
        ))
        others = sorted({k for e in dv.empty_cells for c in e["cells"] for k in c if k != primary})
        for name in others:
            card.choices.append(Choice(
                f"empty:separate:{name}", f"Analyse each {name} separately",
                f"Runs the analysis once per {name}, so the gap only affects its own section.",
                action={"op": "set_role", "factor": name, "role": "iterate"}, concept="role.separate",
            ))
        levels = []
        for e in dv.empty_cells:
            for c in e["cells"]:
                for name, level in c.items():
                    if name != primary and (name, level) not in levels:
                        levels.append((name, level))
        for name, level in levels[:6]:
            card.choices.append(Choice(
                f"empty:exclude:{name}:{level}", f"Leave out {name} = {level}",
                f"Removes {name} {level} from this analysis and from the linked figure.",
                action={"op": "exclude_level", "factor": name, "level": level},
            ))
        step.cards.append(card)
    return step


# --- the question ------------------------------------------------------------------------


def _step_question(facts: "GuideFacts") -> StepGuide:
    dv = _first_dv(facts)
    step = StepGuide("question", "Main question", "What is your main question?",
                     concepts=["primary", "pairwise", "omnibus"])
    compared = _compared(facts)
    analysis = facts.analysis
    if dv is None or not compared or analysis is None:
        step.blocked = "Choose at least one thing to compare first." if analysis is not None else (
            "This table has no analysis; add one in the Advanced view.")
        return step
    primary, outer = compared[0], compared[1:]
    where = f", within each {' × '.join(f.name for f in reversed(outer))}" if outer else ""
    step.facts.append(f"Main comparison: every pair of {primary.name} levels ({_levels(primary.levels)}){where}.")
    total = dv.n_pairs * max(1, dv.n_contexts)
    sections = f" in each of {dv.n_strata} sections" if dv.n_strata > 1 else ""
    step.facts.append(f"That is {_n(total, 'comparison')}{sections}.")
    if len(compared) > 1:
        step.concepts.append("interaction")
        card = Card("primary", "Which comparison matters most?",
                    "Its pairs are compared directly; the other factors set where they are compared.")
        for f in compared:
            card.choices.append(Choice(
                f"primary:{f.name}", f"Compare the {f.name} levels", f"Levels: {_levels(f.levels)}.",
                selected=f.name == primary.name, action={"op": "set_primary", "factor": f.name},
                concept="primary",
            ))
        step.cards.append(card)
    omnibus_on = analysis.omnibus
    recommend_omnibus = len(compared) > 1 or len(primary.levels) > 2
    card = Card("omnibus", "Run an overall test first?")
    card.choices.append(Choice(
        "omnibus:on", "Yes, an overall test and then the pairs", _first_sentence(CONCEPTS["omnibus"].lay),
        recommended=recommend_omnibus, selected=omnibus_on,
        action=_set_analysis(omnibus=True, pairwise=True), concept="omnibus"))
    card.choices.append(Choice(
        "omnibus:off", "No, only the pairs", "Skips the overall test; the pairs are still corrected for being "
        "many.", recommended=not recommend_omnibus, selected=not omnibus_on and analysis.pairwise,
        action=_set_analysis(omnibus=False, pairwise=True), concept="pairwise"))
    step.cards.append(card)
    if dv.empty_cells and omnibus_on:
        step.warnings.append("Some combinations have no data, so the overall test can't be calculated "
                             "(see 'Is anything missing?').")
    return step


# --- the test ------------------------------------------------------------------------


def _check_lay(check: dict[str, Any]) -> str | None:
    name, violated, p = check.get("name"), check.get("violated"), check.get("p")
    scope = check.get("scope")
    where = f" ({scope})" if scope and scope not in ("residuals", "groups") else ""
    if p is None:
        return f"There are too few values to check the shape of the data{where}."
    if name == "normality":
        return ("The values are not bell-shaped" + where + ", so a test based on ranks is safer." if violated
                else "The values are roughly bell-shaped" + where + ", so tests on the values themselves are fine.")
    if name == "homogeneity_of_variance":
        return ("The groups vary by clearly different amounts" + where + "; Welch's t-test allows for that."
                if violated else "The groups vary by similar amounts" + where + ".")
    return None


def _test_label(test_id: str | None) -> str:
    if not test_id:
        return "none"
    concept = CONCEPTS.get(f"test.{test_id}")
    return concept.technical if concept else TESTS[test_id].label if test_id in TESTS else test_id


def _test_card(card_id: str, title: str, role: str, chosen: str | None, recommended: str | None,
               available: list[dict[str, Any]], field_name: str) -> Card:
    card = Card(card_id, title)
    card.choices.append(Choice(
        f"{card_id}:recommended", f"Recommended: {_test_label(recommended)}",
        CONCEPTS[f"test.{recommended}"].lay if recommended and f"test.{recommended}" in CONCEPTS
        else "Nothing is recommended here.",
        recommended=True, selected=chosen is None,
        action=_set_analysis(**{field_name: None, "family": "auto"}),
        concept=f"test.{recommended}" if recommended else None,
    ))
    for t in available:
        if t["role"] != role or t["id"] == recommended:
            continue
        concept = CONCEPTS.get(f"test.{t['id']}")
        card.choices.append(Choice(
            f"{card_id}:{t['id']}", concept.technical if concept else t["label"], concept.lay if concept else "",
            selected=chosen == t["id"], action=_set_analysis(**{field_name: t["id"], "family": "auto"}),
            disabled=t.get("reason"), concept=f"test.{t['id']}",
        ))
    return card


def _step_test(facts: "GuideFacts") -> StepGuide:
    step = StepGuide("test", "Test", "Which test?", concepts=["normality", "equal_variances"],
                     needs_results=True)
    result, analysis, dv = facts.result, facts.analysis, _first_dv(facts)
    if analysis is None:
        step.blocked = "This table has no analysis; add one in the Advanced view."
        return step
    if result is None:
        step.facts.append("Checking your data…")
        return step
    rec = result.recommended or {}
    reasons: list[str] = []
    if dv is not None and dv.empty_cells:
        reasons.append("Some combinations have no data, so no overall test is recommended.")
    elif rec.get("omnibus") == "lmm" and dv is not None and dv.incomplete_units:
        reasons.append(f"{_n(len(dv.incomplete_units), dv.unit or 'participant')} missed some measurements; "
                       f"a mixed model still uses their data.")
        step.concepts.append("mixed_model")
    for check in rec.get("checks", []):
        text = _check_lay(check)
        if text and text not in reasons:
            reasons.append(text)
    step.facts += reasons
    parts = [f"{_test_label(rec.get('pairwise'))} for the pairs"] if rec.get("pairwise") else []
    if analysis.omnibus:
        parts.append(f"{_test_label(rec.get('omnibus'))} for the overall test" if rec.get("omnibus")
                     else "no overall test")
    if parts:
        step.recommendation = "Recommended: " + _and(parts) + "."
    available = facts.available_tests
    if analysis.pairwise:
        step.cards.append(_test_card("pairwise", "Comparing pairs", "pairwise", analysis.pairwise_test,
                                     rec.get("pairwise"), available, "pairwise_test"))
    if analysis.omnibus:
        step.cards.append(_test_card("omnibus_test", "Overall test", "omnibus", analysis.omnibus_test,
                                     rec.get("omnibus"), available, "omnibus_test"))
    for test in (result.pairwise_test, result.omnibus_test):
        if test and f"test.{test}" in CONCEPTS and f"test.{test}" not in step.concepts:
            step.concepts.append(f"test.{test}")
    pair_blocked = not analysis.pairwise or result.pairwise_reason
    omni_blocked = not analysis.omnibus or result.omnibus_reason or not result.omnibus_test
    if pair_blocked and omni_blocked:
        step.blocked = ("Nothing can run with these settings: "
                        f"{result.pairwise_reason or result.omnibus_reason or 'no test applies'}.")
    return step


# --- many comparisons ------------------------------------------------------------------


def _step_multiple_comparisons(facts: "GuideFacts") -> StepGuide:
    result, analysis = facts.result, facts.analysis
    count = result.n_comparisons - result.n_refused if result else 0
    question = (f"You are making {count} comparisons. How should false alarms be kept rare?" if result
                else "How should false alarms be kept rare?")
    step = StepGuide("multiple_comparisons", "Many comparisons", question,
                     concepts=["multiple_comparisons", "family"], needs_results=True)
    if analysis is None or result is None:
        if analysis is not None:
            step.facts.append("Checking your data…")
        return step
    largest = result.largest_family
    if largest > 1:
        expected = largest * analysis.alpha
        step.facts.append(
            f"The largest group of comparisons that share a correction has {largest}. If nothing were really "
            f"different, about {expected:.1f} of them would still look significant by chance without a "
            f"correction.")
    elif count <= 1:
        step.facts.append("Only one comparison: a correction changes nothing.")
    step.facts.append(f"In the report: {result.family_statement}")
    card = Card("correction", "Correction")
    for method in ("holm", "bonferroni", "fdr_bh", "fdr_by", "none"):
        if method not in CORRECTION_LABELS:
            continue
        concept = CONCEPTS[f"correction.{method}"]
        card.choices.append(Choice(
            f"correction:{method}", concept.technical, concept.lay, recommended=method == "holm",
            selected=analysis.correction == method, action=_set_analysis(correction=method),
            concept=f"correction.{method}",
        ))
        step.concepts.append(f"correction.{method}")
    step.cards.append(card)
    step.recommendation = "Holm: it keeps false alarms rare and misses fewer real differences than Bonferroni."
    return step


# --- effect size --------------------------------------------------------------------------


def _step_effect_size(facts: "GuideFacts") -> StepGuide:
    step = StepGuide("effect_size", "Size of the difference", "How should the size of each difference be reported?",
                     concepts=["effect_size", "difference", "confidence_interval"], needs_results=True)
    result, analysis, dv = facts.result, facts.analysis, _first_dv(facts)
    if analysis is None or result is None:
        if analysis is not None:
            step.facts.append("Checking your data…")
        return step
    test = result.pairwise_test
    units = f" in {dv.variable}'s own units" if dv else ""
    step.facts.append(f"Each comparison reports the difference{units} with its range of plausible values.")
    default = default_effect_size(test, any_within=result.any_within) if test else None
    if result.effect_size and result.effect_size in EFFECT_SIZES:
        step.facts.append(f"And a standard size: {EFFECT_SIZES[result.effect_size].name}.")
        step.concepts.append(f"effect.{result.effect_size}")
    sizes = [e for e in EFFECT_SIZES.values() if test in e.tests]
    if sizes:
        card = Card("effect", "Standard size")
        for e in sizes:
            concept = CONCEPTS[f"effect.{e.id}"]
            card.choices.append(Choice(
                f"effect:{e.id}", concept.technical, concept.lay, recommended=e.id == default,
                selected=(analysis.effect_size or default) == e.id,
                action=_set_analysis(effect_size=None if e.id == default else e.id), concept=f"effect.{e.id}",
            ))
        step.cards.append(card)
        if default:
            step.recommendation = f"{EFFECT_SIZES[default].name}, the usual choice for this test."
    elif test:
        step.facts.append("This test has no standard effect size in StatStudio; the difference itself is reported.")
    spreads = ["ci", "se"] + (["sd_diff"] if result.any_within else [])
    card = Card("spread", "Uncertainty of the difference")
    for spread in spreads:
        concept = CONCEPTS[f"spread.{spread}"]
        card.choices.append(Choice(
            f"spread:{spread}", concept.technical, concept.lay, recommended=spread == "ci",
            selected=analysis.difference_spread == spread, action=_set_analysis(difference_spread=spread),
            concept=f"spread.{spread}",
        ))
    step.cards.append(card)
    return step


# --- results --------------------------------------------------------------------------------


def _step_results(facts: "GuideFacts") -> StepGuide:
    step = StepGuide("results", "Results", "Your results",
                     concepts=["difference", "confidence_interval", "p_value", "adjusted_p", "statistic",
                               "effect_size", "alpha"], needs_results=True)
    result, analysis = facts.result, facts.analysis
    if analysis is None:
        step.facts.append("This table has descriptives only; add an analysis in the Advanced view to compare.")
        return step
    if result is None:
        step.facts.append("Calculating…")
        return step
    ran = result.n_comparisons - result.n_refused
    step.facts.append(f"{ran} of {_n(result.n_comparisons, 'comparison')} ran; {result.n_significant} "
                      f"significant at α = {analysis.alpha:g} after the correction.")
    if result.n_refused:
        step.warnings.append(f"{_n(result.n_refused, 'comparison')} could not run: "
                             f"{'; '.join(result.refusal_reasons[:3])}.")
    for reason in result.omnibus_refused[:2]:
        step.warnings.append(f"The overall test did not run: {reason}.")
    step.facts.append("Each row compares two levels: the difference, its range of plausible values, the test's "
                      "own number, the p-value before and after the correction, and the standard size.")
    return step


_BUILDERS = {
    "measure": _step_measure,
    "unit": _step_unit,
    "factors": _step_factors,
    "within_between": _step_within_between,
    "data_check": _step_data_check,
    "question": _step_question,
    "test": _step_test,
    "multiple_comparisons": _step_multiple_comparisons,
    "effect_size": _step_effect_size,
    "results": _step_results,
}


def build_steps(facts: "GuideFacts") -> list[StepGuide]:
    """One :class:`StepGuide` per ``spec.GUIDE_STEPS`` id, in order."""
    from .spec import GUIDE_STEPS

    return [_BUILDERS[step_id](facts) for step_id in GUIDE_STEPS]
