"""Effect sizes by exact name, and what a comparison's difference is (S14, S15).

No bare "Cohen's d" or "η²": every effect size has an id that names the
variant, a display name, a formula and a citation, and those go into headers,
captions and exports. csv-stats computes the values (``effect_sizes`` on each
effect of a core record); this module only names them and picks a default
per design.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .spec import Analysis


@dataclass(frozen=True)
class EffectSizeInfo:
    id: str
    name: str
    formula: str
    citation: str
    #: Test ids it is computed for.
    tests: tuple[str, ...]


_TWO_IND = ("ttest_ind_welch", "ttest_ind_student")

EFFECT_SIZES: dict[str, EffectSizeInfo] = {
    e.id: e
    for e in (
        EffectSizeInfo("d_s", "Cohen's d_s", "(M_a − M_b) / pooled SD", "Cohen (1988); Lakens (2013)", _TWO_IND),
        EffectSizeInfo("g_s", "Hedges' g_s", "d_s × J(df), exact small-sample correction", "Hedges (1981); Lakens (2013)", _TWO_IND),
        EffectSizeInfo("glass_delta", "Glass's Δ", "(M_a − M_b) / SD of b", "Glass (1976)", _TWO_IND),
        EffectSizeInfo("d_z", "Cohen's d_z", "mean difference / SD of the differences", "Cohen (1988); Lakens (2013)", ("ttest_paired",)),
        EffectSizeInfo("d_av", "Cohen's d_av", "mean difference / mean of the two SDs", "Cumming (2012); Lakens (2013)", ("ttest_paired",)),
        EffectSizeInfo("d_rm", "Cohen's d_rm", "d corrected for the correlation of the measures", "Morris & DeShon (2002); Lakens (2013)", ("ttest_paired",)),
        EffectSizeInfo("rank_biserial", "Rank-biserial r", "P(a > b) − P(a < b)", "Kerby (2014)", ("mann_whitney", "wilcoxon")),
        EffectSizeInfo("eta2_p", "Partial η²", "SS_effect / (SS_effect + SS_error)", "Cohen (1973)", ("anova",)),
        EffectSizeInfo("eta2_g", "Generalized η²", "SS_effect / (SS_effect + every subject and error SS)", "Olejnik & Algina (2003); Bakeman (2005)", ("anova",)),
        EffectSizeInfo("omega2_p", "Partial ω²", "df_effect (F − 1) / (df_effect (F − 1) + N)", "Olejnik & Algina (2003)", ("anova",)),
        EffectSizeInfo("epsilon2", "ε² (rank)", "H (n + 1) / (n² − 1)", "Tomczak & Tomczak (2014)", ("kruskal",)),
        EffectSizeInfo("kendall_w", "Kendall's W", "χ² / (n (k − 1))", "Kendall & Babington Smith (1939)", ("friedman",)),
    )
}


def default_effect_size(test_id: str, *, any_within: bool) -> str | None:
    """The shown effect size for a test, by design (S15)."""
    if test_id in _TWO_IND:
        return "g_s"
    if test_id == "ttest_paired":
        return "d_z"
    if test_id in ("mann_whitney", "wilcoxon"):
        return "rank_biserial"
    if test_id == "anova":
        return "eta2_g" if any_within else "eta2_p"
    if test_id == "kruskal":
        return "epsilon2"
    if test_id == "friedman":
        return "kendall_w"
    return None


def chosen_effect_size(analysis: Analysis, test_id: str, *, any_within: bool) -> tuple[str | None, str | None]:
    """``(effect size id, note)``: the analysis' choice if it applies to this
    test, else the default with a note saying why."""
    default = default_effect_size(test_id, any_within=any_within)
    wanted = analysis.effect_size
    if wanted is None:
        return default, None
    info = EFFECT_SIZES.get(wanted)
    if info is None:
        return default, f"unknown effect size {wanted!r}; showing {default}"
    if test_id not in info.tests:
        return default, f"{info.name} does not apply to {test_id}; showing {EFFECT_SIZES[default].name if default else 'none'}"
    return wanted, None


@dataclass(frozen=True)
class DifferenceSpec:
    #: Which spread of the difference is shown: "ci", "se" or "sd_diff".
    spread: str
    #: Column header / caption text, e.g. "Δ = a − b, mean [95% CI of the paired differences]".
    label: str
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"spread": self.spread, "label": self.label, "note": self.note}


def difference_spec(test_id: str, spread: str, ci_level: float) -> DifferenceSpec:
    """What a comparison's difference and its spread are, for this test (S14)."""
    pct = f"{ci_level * 100:g}%"
    note = None
    if test_id.startswith("spm_"):
        return DifferenceSpec("ci", "SPM{t}: supra-threshold clusters (nodes) with their p-values; a − b")
    if test_id in ("mann_whitney", "wilcoxon"):
        if spread != "ci":
            note = f"{spread} is not defined for a Hodges–Lehmann shift; showing its {pct} CI"
        return DifferenceSpec("ci", f"Δ = a − b, Hodges–Lehmann shift [{pct} CI, distribution-free]", note)
    if test_id == "ttest_paired":
        what = {"ci": f"{pct} CI of the paired differences", "se": "SE of the mean difference",
                "sd_diff": "SD of the paired differences"}[spread]
        return DifferenceSpec(spread, f"Δ = a − b, mean ({what})" if spread != "ci" else f"Δ = a − b, mean [{what}]")
    method = "Welch" if test_id == "ttest_ind_welch" else "pooled-variance"
    if spread == "sd_diff":
        note = "SD of differences needs paired data; showing the CI of the difference"
        spread = "ci"
    what = {"ci": f"{pct} CI of the difference, {method}", "se": f"SE of the difference, {method}"}[spread]
    return DifferenceSpec(spread, f"Δ = a − b, mean [{what}]" if spread == "ci" else f"Δ = a − b, mean ({what})", note)
