"""Within/between derivation, tiers, test selection and effect-size naming (Stage 6)."""

from __future__ import annotations

import pytest

from conftest import BRANDS, bar_spec, popcorn_frame, popcorn_table
from scistackplot import LongTable, Role
from scistackplot.spec import Aggregation
from scistackstats import (
    DV,
    Analysis,
    StatsSpec,
    analysis_frame,
    comparison_tiers,
    default_effect_size,
    derive_design,
    difference_spec,
    select_tests,
    why_unavailable,
)
from scistackstats.effects import EFFECT_SIZES, chosen_effect_size

ROLES_2 = {"brand": Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE}


def _design(spec_kwargs=None, table=None, overrides=None):
    table = table or popcorn_table()
    frame = analysis_frame(StatsSpec(design=bar_spec(**(spec_kwargs or {}))), DV("Ratings"), table)
    stratum, rows = frame.stratum_groups()[0]
    return derive_design(frame, stratum, rows, overrides), rows


def test_every_subject_rates_every_brand_so_brand_is_within():
    design, _ = _design()
    (brand,) = design.factors
    assert brand.kind == "within" and brand.units_multi == 6 and brand.units_single == 0
    assert design.unit == "subject" and design.n_units == 6


def test_a_subject_level_factor_is_between():
    design, _ = _design(
        {"roles": {"group": Role.GROUP, "brand": Role.ITERATE, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
         "groups": ["group"]},
        table=popcorn_table(group=True),
    )
    # One stratum per brand; the first is enough.
    assert design.factor("group").kind == "between"


def _mixed_table() -> LongTable:
    frame = popcorn_frame()
    frame = frame[~((frame.subject == "S01") & (frame.brand != "Acme"))]
    return LongTable.from_frame(frame, factors=["subject", "brand", "trial"], measures=["Ratings"],
                                name="Ratings", schema_levels=["subject", "trial"], level_order={"brand": BRANDS})


def test_mixed_membership_is_flagged_and_overridable():
    design, _ = _design(table=_mixed_table())
    assert design.factor("brand").kind == "mixed_membership"
    assert any(w["code"] == "mixed_membership" for w in design.warnings)
    reason = why_unavailable("ttest_paired", design)
    assert "set within or between" in reason
    overridden, _ = _design(table=_mixed_table(), overrides={"brand": "within"})
    assert overridden.factor("brand").kind == "within" and overridden.factor("brand").overridden
    assert why_unavailable("ttest_paired", overridden) is None


def test_no_unit_makes_every_factor_between_with_a_warning():
    design, _ = _design({"aggregate": Aggregation(pooled=True)})
    assert design.factors[0].kind == "between"
    assert any(w["code"] == "no_unit" for w in design.warnings)


def test_tiers_one_layer():
    design, rows = _design()
    tiers = comparison_tiers(design, rows)
    assert tiers.primary == "brand"
    assert tiers.contexts == [{}]
    assert tiers.pairs == [("Acme", "Butter"), ("Acme", "Corn"), ("Butter", "Corn")]
    assert tiers.secondary == ["brand"]


def test_tiers_two_layers():
    design, rows = _design({"roles": ROLES_2, "groups": ["brand", "session"]}, table=popcorn_table(sessions=True))
    tiers = comparison_tiers(design, rows)
    assert tiers.primary == "brand", "the innermost layer is the primary comparison"
    assert sorted(c["session"] for c in tiers.contexts) == ["post", "pre"]
    assert len(tiers.pairs) == 3
    assert tiers.secondary == ["session", "brand", "session:brand"]


def test_select_tests_follows_the_design():
    within, _ = _design()
    s = select_tests(Analysis(), within)
    assert (s.omnibus, s.pairwise) == ("anova", "ttest_paired")
    assert s.omnibus_reason is None and s.pairwise_reason is None
    assert "brand is within-subject" in s.why
    s = select_tests(Analysis(family="nonparametric"), within)
    assert (s.omnibus, s.pairwise) == ("friedman", "wilcoxon")
    between, _ = _design({"aggregate": Aggregation(pooled=True)})
    s = select_tests(Analysis(), between)
    assert s.pairwise == "ttest_ind_welch" and "not assumed" in s.why
    assert select_tests(Analysis(equal_var=True), between).pairwise == "ttest_ind_student"
    assert select_tests(Analysis(family="nonparametric"), between).omnibus == "kruskal"


def test_a_wrong_override_is_refused_with_the_reason():
    within, _ = _design()
    s = select_tests(Analysis(pairwise_test="mann_whitney"), within)
    assert "between-subject factor" in s.pairwise_reason
    s = select_tests(Analysis(omnibus_test="ttest_paired"), within)
    assert "not an omnibus test" in s.omnibus_reason


def test_factorial_rank_omnibus_is_unavailable_but_pairwise_runs():
    design, _ = _design({"roles": ROLES_2, "groups": ["brand", "session"]}, table=popcorn_table(sessions=True))
    s = select_tests(Analysis(family="nonparametric"), design)
    assert s.omnibus is None and "factorial" in s.omnibus_reason
    assert s.pairwise == "wilcoxon" and s.pairwise_reason is None


def test_effect_size_defaults_name_the_variant():
    assert default_effect_size("ttest_ind_welch", any_within=False) == "g_s"
    assert default_effect_size("ttest_paired", any_within=True) == "d_z"
    assert default_effect_size("anova", any_within=True) == "eta2_g"
    assert default_effect_size("anova", any_within=False) == "eta2_p"
    assert EFFECT_SIZES["eta2_g"].name == "Generalized η²"
    assert "Cohen" not in EFFECT_SIZES["g_s"].name
    size, note = chosen_effect_size(Analysis(effect_size="d_av"), "ttest_ind_welch", any_within=False)
    assert size == "g_s" and "does not apply" in note


def test_difference_spec_says_what_the_spread_is():
    assert difference_spec("ttest_paired", "ci", 0.95).label == "Δ = a − b, mean [95% CI of the paired differences]"
    assert "Welch" in difference_spec("ttest_ind_welch", "ci", 0.95).label
    sd = difference_spec("ttest_ind_welch", "sd_diff", 0.95)
    assert sd.spread == "ci" and "needs paired" in sd.note
    assert "Hodges–Lehmann" in difference_spec("wilcoxon", "se", 0.95).label
