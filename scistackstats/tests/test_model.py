"""The model formula (scistackstats.model, plan-statstudio-model-formula):
what the text means against a design, covariates joined per row, the
design written from the formula, the tests it forces, and the run."""

from __future__ import annotations

import pandas as pd
import pytest

from conftest import SUBJECTS, bar_spec, popcorn_table
from scistackplot import FactorVariable, Role
from scistackplot.design import design_diff
from scistackstats import (
    Analysis, CovariateValues, FakeEngine, StatsSpec, analysis_frame, apply_to_design, available_tests,
    default_formula, for_frame, run, select_tests, use_engine, why_not_exportable,
)
from scistackstats.catalog import MIXED_FORMULA_REASON
from scistackstats.tables import UNIT, join_covariates

AGE = FactorVariable("Demographics", "Age")
ROLES_2 = {"brand": Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE}


def _frame(table=None, **design):
    spec = StatsSpec(design=bar_spec(**design))
    return analysis_frame(spec, spec.targets[0], table or popcorn_table())


def _ages(keys=("subject",)) -> CovariateValues:
    frame = pd.DataFrame({"subject": SUBJECTS, "Age": [30.0, 41.0, 52.0, 35.0, 47.0, 60.0]})
    if "trial" in keys:
        frame = frame.merge(pd.DataFrame({"trial": ["1", "2"]}), how="cross")
    return CovariateValues(name="Age", label=AGE.label, keys=list(keys), frame=frame)


# --- the text against the design ---------------------------------------------

def test_the_default_is_the_factors_crossed_plus_a_unit_intercept():
    frame = _frame(table=popcorn_table(sessions=True), roles=ROLES_2, groups=["brand", "session"])
    assert default_formula(frame.outer_first, frame.unit) == "session * brand + (1 | subject)"
    m = for_frame(None, (), frame)
    assert m.ok and m.is_default and not m.needs_mixed_model


def test_a_random_slope_needs_a_mixed_model_and_maps_the_unit_to_its_column():
    m = for_frame("brand + (1 + brand | subject)", (), _frame())
    assert m.ok and m.needs_mixed_model
    assert m.engine_options()["random"] == [
        {"group": [UNIT], "terms": ["(Intercept)", "brand"], "correlated": True}]
    assert UNIT in m.columns()


def test_a_covariate_is_named_by_its_column_and_crossed_in():
    m = for_frame("brand * Age + (1 | subject)", (AGE,), _frame())
    assert m.ok and m.covariates == ["Age"] and m.needs_mixed_model
    assert m.text == "brand * Age + (1 | subject)"
    assert {"name": "Age", "role": "covariate", "detail": "Demographics.Age"} in m.terms


@pytest.mark.parametrize("text, message", [
    ("brand * height + (1 | subject)", "neither a factor of this design nor a picked covariate"),
    ("brand * subject + (1 | subject)", "identifies the experimental unit"),
    ("brand + (1 | trial)", "averaged away"),
    ("brand + (1 | rater)", "cannot group random effects"),
    ("brand + (1 + Age | subject)", "not a fixed factor or covariate"),
    ("brand", "at least one random term"),
    ("y ~ brand + (1 | subject)", "right side only"),
    ("brand + (1 + brand || subject)", "uncorrelated (||) random slopes of a factor"),
])
def test_problems_are_named_and_located(text, message):
    m = for_frame(text, (AGE,), _frame(), available=["trial"])
    assert not m.ok
    assert any(message in e["message"] for e in m.errors), m.errors
    assert all("position" in e for e in m.errors)


def test_a_syntax_error_carries_its_span():
    m = for_frame("brand + (1 | subject", (), _frame())
    (e,) = m.errors
    assert "not closed" in e["message"] and e["position"] == 8


# --- the design owns the factors -----------------------------------------------

def test_the_popup_check_reports_factor_changes_and_apply_writes_the_design():
    """trial is averaged over (Collapse) in the figure; a formula naming it
    ADDS it to the design."""
    plot = bar_spec()
    m = for_frame("trial * brand + (1 | subject)", (), _frame(), available=["trial"], rebase=False)
    assert m.ok and m.add == ["trial"] and m.drop == []
    design = apply_to_design(plot, m.add, m.drop)
    assert design.roles["trial"] is Role.GROUP and design.groups == ["brand", "trial"]
    assert design_diff(plot, design), "the plot-divergence banner has something to show"
    dropped = apply_to_design(design, [], ["brand"])
    assert dropped.roles["brand"] is Role.COLLAPSE and dropped.groups == ["trial"]


def test_a_stored_formula_follows_a_design_change():
    """Rebase: the design no longer groups by trial since the formula was
    written; the fixed part follows the design, the random part stays."""
    m = for_frame("trial * brand + (1 + brand | subject)", (), _frame(), available=["trial"])
    assert m.ok and m.fixed_factors == ["brand"]
    assert m.text == "brand + (1 + brand | subject)"


# --- covariates joined per row ---------------------------------------------------

def test_a_subject_covariate_joins_onto_every_row_of_that_subject():
    frame = _frame()
    rows, problems = join_covariates(frame.rows, ["Age"], {"Age": _ages()})
    assert problems == [] and len(rows) == len(frame.rows)
    assert rows.groupby("subject")["Age"].nunique().max() == 1
    assert rows.loc[rows["subject"] == "S03", "Age"].iloc[0] == 52.0


def test_a_covariate_recorded_below_the_rows_is_refused():
    frame = _frame()  # trials averaged within subject
    _, problems = join_covariates(frame.rows, ["Age"], {"Age": _ages(("subject", "trial"))})
    assert problems and "recorded at or above the rows' level" in problems[0]


def test_a_text_covariate_is_refused_and_a_missing_value_is_kept_as_nan():
    frame = _frame()
    text = CovariateValues(name="Age", label="Demographics.Age", keys=["subject"],
                           frame=pd.DataFrame({"subject": SUBJECTS, "Age": ["young"] * 6}))
    _, problems = join_covariates(frame.rows, ["Age"], {"Age": text})
    assert "not numeric" in problems[0]
    part = _ages()
    part.frame = part.frame[part.frame["subject"] != "S01"]
    rows, problems = join_covariates(frame.rows, ["Age"], {"Age": part})
    assert problems == [] and rows.loc[rows["subject"] == "S01", "Age"].isna().all()


# --- tests the formula forces ------------------------------------------------------

def test_a_mixed_formula_selects_the_mixed_model_and_greys_the_rest():
    from scistackstats.design import derive_design

    frame = _frame()
    design = derive_design(frame, {}, frame.rows, {})
    sel = select_tests(Analysis(), design, mixed_formula=True)
    assert (sel.omnibus, sel.pairwise) == ("lmm", "lmm_pairwise") and "model formula" in sel.why
    forced = select_tests(Analysis(pairwise_test="ttest_paired"), design, mixed_formula=True)
    assert forced.pairwise_reason == MIXED_FORMULA_REASON
    menu = {t["id"]: t["reason"] for t in available_tests(design, mixed_formula=True)}
    assert menu["anova"] == MIXED_FORMULA_REASON and menu["lmm"] is None


# --- the run ---------------------------------------------------------------------------

def _run(analysis, covariates=None, engine=None, **design):
    spec = StatsSpec(design=bar_spec(**design), comparisons=(analysis,))
    with use_engine(engine or FakeEngine()):
        return run(spec, {"Ratings": popcorn_table()}, covariates).analyses[0]


def test_the_run_passes_covariates_and_random_terms_to_the_mixed_model():
    fake = FakeEngine()
    report = _run(Analysis(formula="brand * Age + (1 | subject)", covariates=(AGE,)), {"Age": _ages()}, fake)
    calls = [c for c in fake.calls if c["test"] in ("lmm", "lmm_pairwise")]
    assert {c["test"] for c in calls} == {"lmm", "lmm_pairwise"}
    for c in calls:
        assert c["covariates"] == ["Age"] and "Age" in c["columns"] and UNIT in c["columns"]
        assert c["random"] == [{"group": [UNIT], "terms": ["(Intercept)"], "correlated": True}]
    (stratum,) = report.strata
    assert stratum.model == "Ratings ~ brand * Age + (1 | subject)"
    assert "model formula" in stratum.selection["why"]


def test_the_default_model_calls_the_engine_exactly_as_before():
    fake = FakeEngine()
    _run(Analysis(family="mixed_model"), engine=fake)
    for c in fake.calls:
        assert c["covariates"] == [] and c["random"] == []


def test_an_unusable_formula_refuses_the_tests_with_its_reason():
    report = _run(Analysis(formula="brand * height + (1 | subject)"))
    (stratum,) = report.strata
    assert stratum.refused.startswith("model formula:") and "height" in stratum.refused
    assert stratum.omnibus.refused == stratum.refused


def test_a_covariate_that_did_not_load_is_a_refusal_not_a_crash():
    report = _run(Analysis(formula="brand * Age + (1 | subject)", covariates=(AGE,)), {})
    (stratum,) = report.strata
    assert "covariate Age" in stratum.refused


def test_too_many_random_effects_for_the_rows_is_refused_by_csv_stats():
    """Decision 3: the rows are one mean per subject x brand, so a brand
    slope per subject has as many effects as observations; csv-stats refuses
    it with lme4's reason before fitting (real engine)."""
    from scistackstats import CsvStatsEngine

    report = _run(Analysis(formula="brand + (1 + brand | subject)"), engine=CsvStatsEngine())
    reasons = [c.refused for c in report.comparisons()]
    assert reasons and all("number of observations" in (r or "") for r in reasons), reasons


def test_the_caption_states_the_model_and_the_spec_round_trips():
    from scistackstats import labels_for
    from scistackstats.format import comparisons_caption

    analysis = Analysis(formula="brand * Age + (1 | subject)", covariates=(AGE,))
    spec = StatsSpec(design=bar_spec(), comparisons=(analysis,))
    assert StatsSpec.from_dict(spec.to_dict()) == spec
    assert "formula" not in Analysis().to_dict() and "covariates" not in Analysis().to_dict()
    with use_engine(FakeEngine()):
        report = run(spec, {"Ratings": popcorn_table()}, {"Age": _ages()})
    caption = comparisons_caption(report, report.analyses[0], spec, labels_for(spec, {"Ratings": popcorn_table()}))
    assert "Model: Ratings ~ brand * Age + (1 | subject)." in caption
    assert "covariates" in why_not_exportable(spec, {"Ratings": popcorn_table()})
