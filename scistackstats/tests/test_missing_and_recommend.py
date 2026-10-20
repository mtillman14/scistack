"""Missing data, recommendations and factor kinds from the schema (2026-10-19).

User decisions: data is never dropped silently; the user chooses whether
incomplete units are dropped (``Analysis.missing``, recorded and stated); the
GUI recommends a test from the data but any test that fits the data's shape
can be chosen; csv-stats offers every family, including mixed models.

The shape under test is scidb.log 2026-10-19's: ``session`` nested below
``subject``, and one subject (S05) missing a session because it hasn't been
recorded yet.
"""

from __future__ import annotations

import logging

import pytest

from conftest import bar_spec, popcorn_frame
from scistackplot import LongTable, Role
from scistackstats import Analysis, DV, FakeEngine, StatsSpec, run, use_engine
from scistackstats.catalog import needed_action, select_tests
from scistackstats.design import WITHIN, MIXED, derive_design
from scistackstats.format import comparisons_grid, omnibus_grid
from scistackstats.spec import StatsSpecError
from scistackstats.tables import analysis_frame

ROLES = {"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE}


def _incomplete_table(*, session_in_schema: bool) -> LongTable:
    frame = popcorn_frame(sessions=True)
    frame = frame[frame.brand == "Acme"].drop(columns="brand")
    frame = frame[~((frame.subject == "S05") & (frame.session == "post"))]
    schema = ["subject", "session", "trial"] if session_in_schema else ["subject", "trial"]
    return LongTable.from_frame(
        frame, factors=["subject", "session", "trial"], measures=["Ratings"], name="Ratings",
        schema_levels=schema, level_order={"session": ["pre", "post"]},
    )


def _spec(analysis: Analysis) -> StatsSpec:
    return StatsSpec(design=bar_spec(roles=ROLES, groups=["session"]), comparisons=(analysis,))


def _design(table: LongTable, overrides=None):
    frame = analysis_frame(_spec(Analysis()), DV("Ratings"), table)
    stratum, rows = frame.stratum_groups()[0]
    return derive_design(frame, stratum, rows, overrides), rows


def _run(analysis: Analysis, table: LongTable, engine=None):
    with use_engine(engine or FakeEngine()):
        return run(_spec(analysis), {"Ratings": table})


# --- Stage E: the schema decides within vs between --------------------------------


def test_a_schema_key_below_the_unit_is_within_and_names_who_is_incomplete():
    design, _ = _design(_incomplete_table(session_in_schema=True))
    session = design.factor("session")
    assert (session.kind, session.basis) == (WITHIN, "schema")
    assert design.incomplete_units == ["S05"]
    assert any(w["code"] == "incomplete_within" for w in design.warnings)
    assert needed_action(design) is None, "nothing to decide: the schema answered"


def test_a_non_schema_factor_still_needs_a_decision():
    design, _ = _design(_incomplete_table(session_in_schema=False))
    assert design.factor("session").kind == MIXED
    action = needed_action(design)
    assert action["factor"] == "session" and action["choices"] == ["within", "between"]
    assert "one decision" in action["message"]


def test_the_report_carries_the_action_for_the_banner():
    report = _run(Analysis(), _incomplete_table(session_in_schema=False))
    (stratum,) = report.analyses[0].strata
    assert stratum.action and stratum.action["factor"] == "session"
    assert stratum.to_dict()["action"] == stratum.action


def test_an_override_wins_over_the_schema():
    design, _ = _design(_incomplete_table(session_in_schema=True), overrides={"session": "between"})
    assert design.factor("session").kind == "between"


# --- Stage D: recommendation from the data; any shape-valid test may be chosen ---------


def test_use_all_with_incomplete_units_recommends_the_mixed_model():
    report = _run(Analysis(), _incomplete_table(session_in_schema=True))
    (stratum,) = report.analyses[0].strata
    assert (stratum.selection["omnibus"], stratum.selection["pairwise"]) == ("lmm", "lmm_pairwise")
    rec = stratum.selection["recommended"]
    assert "S05" in rec["evidence"][0] and "mixed model" in rec["evidence"][0]


def test_complete_normal_data_recommends_the_parametric_tests():
    table = LongTable.from_frame(
        popcorn_frame(sessions=True).query("brand == 'Acme'").drop(columns="brand"), factors=["subject", "session", "trial"],
        measures=["Ratings"], name="Ratings", schema_levels=["subject", "session", "trial"],
        level_order={"session": ["pre", "post"]},
    )
    report = _run(Analysis(), table)
    (stratum,) = report.analyses[0].strata
    assert (stratum.selection["omnibus"], stratum.selection["pairwise"]) == ("anova", "ttest_paired")
    assert "residuals: normal" in stratum.selection["recommended"]["evidence"][0]


def test_non_normal_residuals_recommend_the_rank_tests():
    table = LongTable.from_frame(
        popcorn_frame(sessions=True).query("brand == 'Acme'").drop(columns="brand"), factors=["subject", "session", "trial"],
        measures=["Ratings"], name="Ratings", schema_levels=["subject", "session", "trial"],
        level_order={"session": ["pre", "post"]},
    )
    report = _run(Analysis(), table, engine=FakeEngine(violated=("normality",)))
    (stratum,) = report.analyses[0].strata
    assert stratum.selection["pairwise"] == "wilcoxon"
    assert "not normal" in stratum.selection["recommended"]["evidence"][0]


def test_a_chosen_test_runs_and_the_recommendation_is_still_shown():
    """Any test that fits the shape can be chosen, missing data or not; a
    complete-cases test then names whom it left out (Stage A)."""
    fake = FakeEngine(exclude={"ttest_paired": [("S05", "missing a within-subject level")]})
    report = _run(Analysis(pairwise_test="ttest_paired"), _incomplete_table(session_in_schema=True), engine=fake)
    (stratum,) = report.analyses[0].strata
    assert stratum.selection["pairwise"] == "ttest_paired"
    assert stratum.selection["pairwise_reason"] is None, "missing data never makes a test unavailable"
    assert stratum.selection["recommended"]["pairwise"] == "lmm_pairwise"
    assert all(c.excluded and c.excluded["units"][0]["unit"] == "S05" for c in stratum.primary)


def test_the_mixed_model_needs_a_within_factor():
    from scistackstats.catalog import why_unavailable
    from conftest import popcorn_table
    from scistackplot.spec import Aggregation

    frame = analysis_frame(StatsSpec(design=bar_spec(aggregate=Aggregation(pooled=True))), DV("Ratings"), popcorn_table())
    stratum, rows = frame.stratum_groups()[0]
    design = derive_design(frame, stratum, rows)
    assert "within-subject factor" in why_unavailable("lmm", design)


def test_select_tests_without_rows_falls_back_to_parametric():
    design, _ = _design(_incomplete_table(session_in_schema=True))
    s = select_tests(Analysis(), design)
    assert s.omnibus == "anova" and "auto: no data checked yet" in s.why


# --- Stage B: the missing-data policy is the user's, recorded and stated -------------


def test_complete_cases_drops_named_units_from_every_test_and_logs_it(caplog):
    with caplog.at_level(logging.WARNING):
        report = _run(Analysis(missing="complete_cases"), _incomplete_table(session_in_schema=True))
    analysis = report.analyses[0]
    (stratum,) = analysis.strata
    assert stratum.excluded["units"] == [
        {"unit": "S05", "reason": "missing a within-subject level (complete cases chosen)"}]
    assert stratum.design["incomplete_units"] == [], "the design is re-derived on the complete units"
    assert stratum.selection["omnibus"] == "anova", "complete data now: no mixed model needed"
    assert any("complete cases" in r.getMessage() and "S05" in r.getMessage() for r in caplog.records)
    spec = _spec(Analysis(missing="complete_cases"))
    grid = comparisons_grid(report, analysis, spec)
    assert "Missing data: complete cases only (chosen); 1 incomplete unit(s) excluded" in grid.caption
    assert any("S05" in n for n in grid.notes)


def test_use_all_is_stated_with_the_mixed_model_and_satterthwaite():
    report = _run(Analysis(), _incomplete_table(session_in_schema=True))
    analysis = report.analyses[0]
    spec = _spec(Analysis())
    caption = comparisons_grid(report, analysis, spec).caption
    assert ("Missing data: every observation used where the test allows it "
            "(linear mixed model; Satterthwaite degrees of freedom)") in caption
    assert "Missing data:" in omnibus_grid(report, analysis, spec).caption


def test_a_wald_fallback_is_stated_in_the_caption():
    """If csv-stats could not compute a Satterthwaite df, the caption must
    not claim Satterthwaite for everything."""

    class WaldFallback(FakeEngine):
        def run(self, test, data, dv, **kw):
            record = super().run(test, data, dv, **kw)
            for e in record["effects"]:
                e["df_method"] = "wald"
            return record

    report = _run(Analysis(), _incomplete_table(session_in_schema=True), engine=WaldFallback())
    caption = comparisons_grid(report, report.analyses[0], _spec(Analysis())).caption
    assert "some tests fell back to Wald" in caption


def test_mixed_model_contrasts_come_from_one_call_on_the_whole_stratum():
    """Stage 5 (full-model contrasts, user decision 2026-10-19): one engine
    call with every row and the primary factor named, not one call per pair
    on the pair's rows."""
    fake = FakeEngine()
    report = _run(Analysis(), _incomplete_table(session_in_schema=True), engine=fake)
    calls = [c for c in fake.calls if c["test"] == "lmm_pairwise"]
    assert len(calls) == 1
    (stratum,) = report.analyses[0].strata
    assert calls[0]["rows"] == stratum.design["n_rows"]
    assert [(c.a, c.b) for c in stratum.primary] == [("pre", "post")]
    assert all(c.refused is None and c.estimate is not None for c in stratum.primary)
    # A model-estimated difference, not a paired or pooled-variance one.
    assert stratum.difference["label"].startswith("Δ = a − b, mixed-model estimated difference [95% CI")


def test_the_policy_is_validated_and_round_trips():
    with pytest.raises(StatsSpecError):
        Analysis(missing="impute")
    a = Analysis()
    assert (a.family, a.missing) == ("auto", "use_all")
    raw = Analysis(missing="complete_cases").to_dict()
    assert Analysis.from_dict(raw).missing == "complete_cases"
