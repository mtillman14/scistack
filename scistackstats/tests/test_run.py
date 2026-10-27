"""The runner: families, refusals, tiers, determinism (FakeEngine) and one
real csv-stats run end to end (Stage 7)."""

from __future__ import annotations

import json

import pytest

from conftest import bar_spec, popcorn_frame, popcorn_table
from scistackplot import Role
from scistackstats import Analysis, FakeEngine, StatsSpec, StatsSpecError, run, use_engine

ROLES_2 = {"brand": Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE}
ROLES_ITER = {"brand": Role.GROUP, "session": Role.ITERATE, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE}


def _run(analysis: Analysis, *, engine=None, table=None, **design):
    spec = StatsSpec(design=bar_spec(**design), comparisons=(analysis,))
    with use_engine(engine or FakeEngine()):
        return run(spec, {"Ratings": table or popcorn_table()}).analyses[0]


def test_primary_pairs_are_one_family_by_default():
    fake = FakeEngine(p_values={("ttest_paired", "Acme", "Butter"): 0.01, ("ttest_paired", "Acme", "Corn"): 0.04,
                                ("ttest_paired", "Butter", "Corn"): 0.03})
    report = _run(Analysis(), engine=fake)
    comps = report.comparisons()
    assert [(c.a, c.b) for c in comps] == [("Acme", "Butter"), ("Acme", "Corn"), ("Butter", "Corn")]
    assert [c.test for c in comps] == ["ttest_paired"] * 3
    assert [c.p_adjusted for c in comps] == pytest.approx([0.03, 0.06, 0.06])
    assert len(report.families) == 1 and len(report.families[0].members) == 3
    assert "Holm" in report.family_statement and "Omnibus effects are not adjusted" in report.family_statement


@pytest.mark.parametrize("scope, n_families, size", [("stratum", 2, 3), ("dv", 1, 6), ("table", 1, 6)])
def test_family_scopes(scope, n_families, size):
    report = _run(Analysis(family_scope=scope), table=popcorn_table(sessions=True), roles=ROLES_ITER)
    assert len(report.strata) == 2
    assert len(report.families) == n_families
    assert {len(f.members) for f in report.families} == {size}


def test_context_scope_splits_by_outer_level():
    report = _run(Analysis(family_scope="context"), table=popcorn_table(sessions=True),
                  roles=ROLES_2, groups=["brand", "session"])
    assert len(report.families) == 2, "one family per session (the outer layer)"


def test_no_correction():
    report = _run(Analysis(family_scope="none"))
    assert all(c.p_adjusted == c.p for c in report.comparisons())
    assert "not adjusted" in report.family_statement


def test_primary_comparisons_are_reported_when_the_interaction_is_not_significant():
    fake = FakeEngine(p_values={("anova", "session:brand"): 0.9, ("anova", "brand:session"): 0.9})
    report = _run(Analysis(), engine=fake, table=popcorn_table(sessions=True), roles=ROLES_2,
                  groups=["brand", "session"])
    comps = report.comparisons()
    assert len(comps) == 6, "3 brand pairs inside each of 2 sessions"
    for c in comps:
        assert c.interaction_p and list(c.interaction_p.values()) == [0.9]
    omnibus_terms = [e["term"] for e in report.strata[0].omnibus.effects]
    assert len(omnibus_terms) == 3


def test_a_refusal_is_recorded_not_raised():
    fake = FakeEngine(refuse={"ttest_paired": ("not yet in csv-stats: something", "gap")})
    report = _run(Analysis(), engine=fake)
    assert all(c.refused.startswith("not yet in csv-stats") for c in report.comparisons())
    assert all(c.refusal_kind == "gap" for c in report.comparisons())
    assert report.families == []


def test_design_refusal_stops_the_pairwise_tests_with_its_reason():
    from conftest import BRANDS
    from scistackplot import LongTable

    frame = popcorn_frame()
    frame = frame[~((frame.subject == "S01") & (frame.brand != "Acme"))]
    table = LongTable.from_frame(frame, factors=["subject", "brand", "trial"], measures=["Ratings"],
                                 name="Ratings", schema_levels=["subject", "trial"], level_order={"brand": BRANDS})
    report = _run(Analysis(), table=table)
    (stratum,) = report.strata
    assert "set within or between" in stratum.refused
    assert stratum.primary == []


def test_a_design_refusal_logs_its_reason(caplog):
    """scistack.log 2026-10-19 said "3 refused ... (see the refusals)" and never
    WHY, so an empty comparison table had no explanation in the log."""
    import logging

    from conftest import BRANDS
    from scistackplot import LongTable

    frame = popcorn_frame()
    frame = frame[~((frame.subject == "S01") & (frame.brand != "Acme"))]
    table = LongTable.from_frame(frame, factors=["subject", "brand", "trial"], measures=["Ratings"],
                                 name="Ratings", schema_levels=["subject", "trial"], level_order={"brand": BRANDS})
    with caplog.at_level(logging.WARNING):
        _run(Analysis(), table=table)
    lines = [r.getMessage() for r in caplog.records if "comparisons not run" in r.getMessage()]
    assert len(lines) == 1
    assert "Ratings" in lines[0] and "set within or between" in lines[0]


def test_report_is_deterministic_json():
    one = json.dumps(_run(Analysis()).to_dict(), sort_keys=True)
    two = json.dumps(_run(Analysis()).to_dict(), sort_keys=True)
    assert one == two


def test_unknown_correction_raises_before_running():
    with pytest.raises(StatsSpecError, match="correction"):
        _run(Analysis(correction="tukey"))


def test_real_csvstats_end_to_end():
    """Better Popcorn with csv-stats: paired t-tests of the brands, RM ANOVA."""
    # Pinned: this checks the t-test / RM-ANOVA path; "auto" picks Wilcoxon here
    # (the residuals are not normal, see the next test).
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(family="parametric"),))
    report = run(spec, {"Ratings": popcorn_table()}).analyses[0]
    (stratum,) = report.strata
    assert stratum.selection["pairwise"] == "ttest_paired"
    assert stratum.omnibus.test == "anova" and stratum.omnibus.refused is None
    (term,) = stratum.omnibus.effects
    assert term["term"] == "brand" and term["effect_size"]["name"] == "Generalized η²"
    comps = report.comparisons()
    assert len(comps) == 3 and all(c.refused is None for c in comps)
    per_subject = popcorn_frame().groupby(["subject", "brand"])["Ratings"].mean().unstack()
    acme_butter = comps[0]
    assert acme_butter.estimate == pytest.approx((per_subject["Acme"] - per_subject["Butter"]).mean())
    assert acme_butter.effect_size["name"] == "Cohen's d_z"
    assert isinstance(acme_butter.spread, list) and len(acme_butter.spread) == 2
    assert all(c.p_adjusted >= c.p for c in comps)
    assert stratum.difference["label"].startswith("Δ = a − b, mean [95% CI of the paired differences]")


def test_real_csvstats_recommendation_carries_its_evidence():
    """The default ("auto") on real csv-stats: the Popcorn residuals fail
    Shapiro–Wilk, so the rank tests are recommended and the evidence says why."""
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(),))
    report = run(spec, {"Ratings": popcorn_table()}).analyses[0]
    (stratum,) = report.strata
    rec = stratum.selection["recommended"]
    check = rec["checks"][0]
    assert check["name"] == "normality" and check["test"] == "shapiro_wilk"
    expected = "wilcoxon" if check["violated"] else "ttest_paired"
    assert rec["pairwise"] == stratum.selection["pairwise"] == expected
    assert rec["evidence"][0].startswith("residuals: ")
    assert "pairwise: " in stratum.selection["why"] and "(recommended: residuals: " in stratum.selection["why"]


def test_statement_when_every_comparison_was_refused():
    """Regression: an empty family list crashed the statement (IndexError)."""
    fake = FakeEngine(refuse={"ttest_paired": ("x", "invalid")})
    report = _run(Analysis(), engine=fake)
    assert "none ran" in report.family_statement
