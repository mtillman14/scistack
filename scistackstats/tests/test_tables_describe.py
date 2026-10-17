"""The rows a statistic sees, and the Descriptives table built on them (Stages 4–5)."""

from __future__ import annotations

import pandas as pd
import pytest

from conftest import BRANDS, SUBJECTS, bar_spec, popcorn_frame, popcorn_table
from csvstats.descriptive import summarize  # test-side reference only
from scistackplot import LongTable, Role, resolve
from scistackplot.resolved import X, Y_HIGH, Y_LOW
from scistackplot.spec import Aggregation, ErrorBand
from scistackstats import DV, AnalysisFrameError, DescriptiveSpec, StatsSpec, analysis_frame, describe_table
from scistackstats.tables import UNIT


def _spec(**changes) -> StatsSpec:
    return StatsSpec(design=bar_spec(**changes))


def test_rows_are_the_plot_sample(table):
    frame = analysis_frame(_spec(), DV("Ratings"), table)
    assert len(frame.rows) == len(SUBJECTS) * len(BRANDS), "trials averaged within subject × brand"
    assert frame.factors == ["brand"]
    assert frame.unit == "subject" and frame.unit_keys == ["subject"]
    assert frame.levels["brand"] == BRANDS
    raw = popcorn_frame()
    expected = raw.groupby(["subject", "brand"])["Ratings"].mean()
    got = frame.rows.set_index(["subject", "brand"])["Ratings"]
    assert got.sort_index().to_numpy() == pytest.approx(expected.sort_index().to_numpy())


def test_figure_kind_never_changes_the_rows(table):
    from scistackplot import PlotKind

    bar = analysis_frame(_spec(), DV("Ratings"), table).rows
    box = analysis_frame(_spec(kind=PlotKind.BOX), DV("Ratings"), table).rows
    pd.testing.assert_frame_equal(bar.reset_index(drop=True), box.reset_index(drop=True))


def test_pooled_has_no_unit(table):
    frame = analysis_frame(_spec(aggregate=Aggregation(pooled=True)), DV("Ratings"), table)
    assert frame.unit is None
    assert any("no experimental unit" in n for n in frame.notes)


def test_unit_id_includes_the_keys_above_it():
    """Subject "01" at two sites is two people."""
    rows = []
    for site in ("A", "B"):
        for subject in ("01", "02"):
            for brand in ("x", "y"):
                rows.append({"site": site, "subject": subject, "brand": brand, "Ratings": 1.0 + (brand == "y")})
    frame = pd.DataFrame(rows)
    table = LongTable.from_frame(frame, factors=["site", "subject", "brand"], measures=["Ratings"],
                                 name="Ratings", schema_levels=["site", "subject"])
    spec = StatsSpec(design=bar_spec(roles={"brand": Role.GROUP, "subject": Role.COLLAPSE, "site": Role.ITERATE}))
    result = analysis_frame(spec, DV("Ratings"), table)
    assert result.unit_keys == ["site", "subject"]
    assert result.rows[UNIT].nunique() == 4
    assert result.strata == ["site"]


def _emg_table() -> LongTable:
    rows = []
    for s in SUBJECTS:
        for brand in BRANDS:
            for col, base in (("TA", 1.0), ("SOL", 2.0), ("GM", 3.0)):
                rows.append({"subject": s, "brand": brand, "ColName": col, "EMG": base + BRANDS.index(brand)})
    return LongTable.from_frame(pd.DataFrame(rows), factors=["subject", "brand", "ColName"], measures=["EMG"],
                                name="EMG", schema_levels=["subject"], field_factors=["ColName"])


def test_colname_columns_are_separate_dvs():
    spec = StatsSpec(design=bar_spec(measures=["EMG"], roles={"brand": Role.GROUP, "subject": Role.COLLAPSE}),
                     dvs=(DV("EMG", columns=("TA", "SOL")),))
    frame = analysis_frame(spec, spec.dvs[0], _emg_table())
    assert "ColName" in frame.strata
    assert set(frame.rows["ColName"]) == {"TA", "SOL"}


def test_unknown_colname_column_is_refused_by_name():
    spec = StatsSpec(design=bar_spec(measures=["EMG"]), dvs=(DV("EMG", columns=("XX",)),))
    with pytest.raises(AnalysisFrameError, match="XX"):
        analysis_frame(spec, spec.dvs[0], _emg_table())


# --- Descriptives -----------------------------------------------------------


def test_mean_sd_per_brand_across_people(table):
    """The Better Popcorn example: mean (SD) of each brand across the 6 people."""
    out = describe_table(_spec(), {"Ratings": table})
    (block,) = out.blocks
    assert block.layers == ["brand"] and block.unit == "subject"
    assert [c["brand"] for c in block.columns] == BRANDS
    (section,) = block.sections
    per_subject = popcorn_frame().groupby(["brand", "subject"])["Ratings"].mean()
    for cell in section.cells:
        expected = summarize(per_subject[cell.column["brand"]].to_numpy())
        assert cell.stats["n"] == 6
        assert cell.stats["mean"] == pytest.approx(expected["mean"])
        assert cell.stats["sd"] == pytest.approx(expected["sd"])
    assert out.to_dict()["cell_header"] == "Mean (SD)"


def test_nested_layers_read_outermost_first():
    table = popcorn_table(sessions=True)
    spec = _spec(roles={"brand": Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
                 groups=["brand", "session"])
    (block,) = describe_table(spec, {"Ratings": table}).blocks
    assert block.layers == ["session", "brand"]
    assert len(block.columns) == 6
    assert [c["session"] for c in block.columns[:3]] == [block.columns[0]["session"]] * 3, "session-major"


def test_strata_are_sections():
    table = popcorn_table(sessions=True)
    spec = _spec(roles={"brand": Role.GROUP, "session": Role.ITERATE, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE})
    (block,) = describe_table(spec, {"Ratings": table}).blocks
    assert sorted(s.stratum["session"] for s in block.sections) == ["post", "pre"]


def test_table_ci95_equals_the_plotted_error_bars(table):
    """One owner: the tabulated CI and the drawn bar are the same numbers."""
    plot = bar_spec(aggregate=Aggregation(error=ErrorBand.CI95))
    spec = StatsSpec(design=plot, descriptives=DescriptiveSpec(cell="mean_ci95"))
    (section,) = describe_table(spec, {"Ratings": table}).blocks[0].sections
    (preview,) = resolve(plot, table)
    drawn = preview.panels[0].frame.set_index(X)
    for cell in section.cells:
        brand = cell.column["brand"]
        assert cell.stats["ci_low"] == pytest.approx(drawn.loc[brand, Y_LOW])
        assert cell.stats["ci_high"] == pytest.approx(drawn.loc[brand, Y_HIGH])


def test_a_dv_without_a_table_is_a_refused_block_not_an_error(table):
    spec = StatsSpec(design=bar_spec(), dvs=(DV("Ratings"), DV("Missing")))
    out = describe_table(spec, {"Ratings": table})
    assert out.blocks[0].refused is None
    assert "Missing" in out.blocks[1].refused


def test_unknown_statistic_raises():
    with pytest.raises(ValueError, match="nope"):
        describe_table(StatsSpec(design=bar_spec(), descriptives=DescriptiveSpec(statistics=("nope",))), {})
