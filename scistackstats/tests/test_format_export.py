"""Stage 8: the formatting owner, captions, grids and every export format."""

from __future__ import annotations

import csv
import io
import json

import pytest

from conftest import BRANDS, bar_spec, popcorn_frame, popcorn_table
from scistackplot import Role
from scistackplot.spec import Alias
from scistackstats import Analysis, DescriptiveSpec, FakeEngine, StatsSpec, describe_table, run, use_engine
from scistackstats.export import FORMATS, export_comparisons, export_descriptives, latex_escape
from scistackstats.format import (
    apa_sentence,
    auto_decimals,
    comparisons_grid,
    descriptives_grids,
    labels_for,
    number,
    omnibus_grid,
    p_value,
    statistic,
    stars,
)
from scistackstats.spec import FormatSpec

ROLES_2 = {"brand": Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE}


# --- one number ---------------------------------------------------------------


def test_number_uses_unicode_minus_and_never_negative_zero():
    assert number(-1.5, 1) == "−1.5"
    assert number(-0.001, 2) == "0.00"
    assert number(None, 2) == "—"
    assert number(float("nan"), 2) == "—"


def test_apa_p_values():
    assert p_value(0.0004) == "< .001"
    assert p_value(0.0412) == ".041"
    assert p_value(0.99995) == "> .999"
    assert p_value(0.5, 2) == ".50"
    assert p_value(None) == "—"
    assert stars(0.0004) == "***" and stars(0.03) == "*" and stars(0.2) == ""


def test_statistic_strings():
    assert statistic("t", -2.137, [5.0]) == "t(5) = −2.14"
    assert statistic("F", 4.1, [1.6, 8.25]) == "F(1.6, 8.2) = 4.10"
    assert statistic("chi2", 6.0, [2.0]) == "χ²(2) = 6.00"
    assert statistic("U", 12.0, []) == "U = 12.00"


def test_auto_decimals_show_two_significant_digits_of_the_spread():
    assert auto_decimals([0.42, 1.3]) == 2
    assert auto_decimals([0.0123]) == 3
    assert auto_decimals([12.0]) == 0
    assert auto_decimals([]) == 2


# --- descriptives grid + caption ------------------------------------------------


def _desc(spec: StatsSpec, table=None):
    tables = {"Ratings": table or popcorn_table()}
    return describe_table(spec, tables), tables


def test_descriptives_grid_mean_sd_per_brand():
    spec = StatsSpec(design=bar_spec(), descriptives=DescriptiveSpec(cell="mean_sd", statistics=("n",)))
    table, tables = _desc(spec)
    (grid,) = descriptives_grids(table, spec, labels_for(spec, tables))
    assert grid.flat_header() == ["Measure", "Statistic", *BRANDS]
    first = grid.rows[0].cells
    per_subject = popcorn_frame().groupby(["brand", "subject"])["Ratings"].mean()
    acme = per_subject["Acme"]
    d = auto_decimals([per_subject[b].std(ddof=1) for b in BRANDS])
    assert first[:2] == ["Ratings", "Mean (SD)"]
    assert first[2] == f"{acme.mean():.{d}f} ({acme.std(ddof=1):.{d}f})"
    assert grid.rows[1].cells[1:] == ["n", "6", "6", "6"]
    assert "one value per subject (trial averaged within subject first)" in grid.caption
    assert "SD uses n − 1." in grid.caption
    assert "Variants: no selection" in grid.caption


def test_nested_headers_span_the_inner_layer():
    spec = StatsSpec(design=bar_spec(roles=ROLES_2, groups=["brand", "session"]))
    table, tables = _desc(spec, popcorn_table(sessions=True))
    (grid,) = descriptives_grids(table, spec, labels_for(spec, tables))
    top, bottom = grid.header_rows
    assert [c.span for c in top[2:]] == [3, 3]
    assert [c.text for c in bottom[2:]] == BRANDS * 2
    assert grid.width == 8


def test_aliases_come_from_the_plot_alias_owner():
    design = bar_spec(aliases={"brand": Alias(name="Popcorn brand", levels={"Acme": "ACME"}),
                               "Ratings": Alias(name="Taste rating")})
    spec = StatsSpec(design=design)
    table, tables = _desc(spec)
    (grid,) = descriptives_grids(table, spec, labels_for(spec, tables))
    assert grid.flat_header()[2] == "ACME"
    assert grid.rows[0].cells[0] == "Taste rating"
    assert grid.title == "Taste rating by Popcorn brand"


def test_fixed_decimals_override_auto():
    spec = StatsSpec(design=bar_spec(), formatting=FormatSpec(decimals=1))
    table, tables = _desc(spec)
    (grid,) = descriptives_grids(table, spec)
    mean, sd = grid.rows[0].cells[2].split(" (")
    assert len(mean.split(".")[1]) == 1


def _report(spec: StatsSpec, table=None, engine=None):
    with use_engine(engine or FakeEngine(p_values={("anova", "brand"): 0.0004})):
        return run(spec, {"Ratings": table or popcorn_table()})


def test_p_column_shows_the_omnibus_main_effect():
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(),))
    table, tables = _desc(spec)
    (grid,) = descriptives_grids(table, spec, labels_for(spec, tables), _report(spec))
    assert grid.flat_header()[-1] == "p"
    assert grid.rows[0].cells[-1] == "< .001"
    assert grid.rows[1].cells[-1] == ""
    assert any("main effect of brand; not adjusted" in n for n in grid.notes)


# --- comparisons grid -----------------------------------------------------------


def test_comparisons_grid_and_caption():
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(),))
    report = _report(spec, engine=FakeEngine(p_values={"ttest_paired": 0.01}))
    analysis = report.analyses[0]
    grid = comparisons_grid(report, analysis, spec)
    assert grid.flat_header()[3] == "Δ [95% CI]"
    assert [r.cells[2] for r in grid.rows] == ["Acme − Butter", "Acme − Corn", "Butter − Corn"]
    assert grid.rows[0].cells[6] == ".030", "Holm over 3"
    assert analysis.family_statement in grid.notes
    assert grid.caption.startswith("Pairwise comparisons of the brand levels.")
    assert "Δ = a − b, mean [95% CI of the paired differences]." in grid.caption
    assert "Test: Paired t-test." in grid.caption
    assert "Unit of analysis: subject." in grid.caption
    omni = omnibus_grid(report, analysis, spec)
    assert omni is not None and omni.rows[0].cells[2] == "brand"


def test_refused_comparisons_say_why():
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(),))
    report = _report(spec, engine=FakeEngine(refuse={"ttest_paired": ("not yet in csv-stats: x", "gap")}))
    grid = comparisons_grid(report, report.analyses[0], spec)
    assert all(r.cells[4].startswith("not run: not yet in csv-stats") for r in grid.rows)


def test_apa_sentence():
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(),))
    report = _report(spec, engine=FakeEngine(p_values={"ttest_paired": 0.01}))
    analysis = report.analyses[0]
    sentence = apa_sentence(analysis.comparisons()[0], analysis, spec.formatting)
    assert sentence.startswith("Ratings: Acme − Butter, Δ = ")
    assert "95% CI [" in sentence and "t(" in sentence
    assert "p = .010 (Holm-adjusted .030)" in sentence
    assert sentence.endswith(".")


def test_real_engine_comparisons_export_end_to_end():
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(),))
    tables = {"Ratings": popcorn_table()}
    report = run(spec, tables)
    text = export_comparisons(report, spec, "md", tables=tables)
    assert "Cohen's d_z" in text and "Generalized η²" in text
    assert "Greenhouse–Geisser" in text


# --- every format -----------------------------------------------------------------


@pytest.mark.parametrize("fmt", FORMATS)
def test_every_format_carries_caption_and_notes(fmt):
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(),))
    tables = {"Ratings": popcorn_table()}
    report = _report(spec)
    text = export_comparisons(report, spec, fmt, tables=tables)
    statement = report.analyses[0].family_statement
    if fmt == "json":
        payload = json.loads(text)
        assert payload["grids"][0]["caption"]
        assert statement in payload["grids"][0]["notes"]
        assert payload["data"]["analyses"]
    elif fmt == "latex":
        assert r"\toprule" in text and r"\bottomrule" in text and "Note." in text
        assert "Holm" in text
    elif fmt == "html":
        assert "<caption" in text and "<tfoot>" in text and "Holm" in text
    else:
        assert "Pairwise comparisons of the brand levels" in text
        assert "Holm" in text


def test_csv_parses_with_one_header_row():
    spec = StatsSpec(design=bar_spec(), descriptives=DescriptiveSpec(cell="mean_ci95"))
    table, tables = _desc(spec)
    text = export_descriptives(table, spec, "csv", tables=tables)
    rows = list(csv.reader(io.StringIO(text)))
    assert rows[0] == ["Measure", "Statistic", *BRANDS]
    assert rows[1][1] == "Mean [95% CI]"
    assert any(r and r[0].startswith("Note.") for r in rows)


def test_latex_and_html_escape_level_names():
    assert latex_escape("Step_Length 5% & more") == r"Step\_Length 5\% \& more"
    assert latex_escape("p < .001, Δ = −1") == r"p $<$ .001, $\Delta$ = $-$1"
    spec = StatsSpec(design=bar_spec(aliases={"brand": Alias(levels={"Acme": "A<b>"})}))
    table, tables = _desc(spec)
    html_text = export_descriptives(table, spec, "html", tables=tables)
    assert "A&lt;b&gt;" in html_text and "A<b>" not in html_text
