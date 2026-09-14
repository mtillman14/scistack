"""
Stage 1: the resolve path must not redo work it has already done.

Three independent changes, tested here together because they share one
property — the figures must come out identical no matter how much was reused:

* ``BaseSource.get_table`` memoizes built tables;
* ``reduce._plan`` memoizes everything decided before per-figure work;
* the 1-D explode happens per FIGURE rather than once over the whole fan-out.

The measured problem (2026-09-11 log): four concurrent resolves, each
re-melting, re-stacking and re-exploding 17.1M samples, with ``resolve_one``
climbing 3.3s -> 27.7s as they piled up.
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from scistackplot import DataFrameSource, PlotSpec, Role, resolve, resolve_one
from scistackplot import reduce as reduce_mod
from scistackplot.spec import FacetOptions, PlotKind


@pytest.fixture(autouse=True)
def _clear_plan_cache():
    reduce_mod.clear_plan_cache()
    yield
    reduce_mod.clear_plan_cache()


@pytest.fixture
def two_measure_source(series_frame) -> DataFrameSource:
    """A source with two stackable measures.

    Most memo assertions need this rather than a single-measure source: a
    one-measure request on a flat source returns the source's own stored
    `LongTable`, so it looks cached whether or not the memo exists.
    """
    frame = series_frame.copy()
    frame["Other"] = frame["Signal"]
    return DataFrameSource(
        frame,
        factors=["subject", "session", "trial"],
        measures=["Signal", "Other"],
        name="Signal",
    )


# --- the built-table memo (1a) ---------------------------------------------


def _count_builds(monkeypatch) -> list:
    """Record every table BUILD. Returns the list the spy appends to.

    Patches ``BaseSource`` — the class that defines the method — rather than
    the instance's own class, which merely inherits it. Patching the subclass
    would leave a redundant copy of the method on it after teardown.
    """
    from scistackplot.sources.base import BaseSource

    builds: list = []
    original = BaseSource._build_table

    def spy(self, measures, **kwargs):
        builds.append(tuple(measures))
        return original(self, measures, **kwargs)

    monkeypatch.setattr(BaseSource, "_build_table", spy)
    return builds


def test_the_same_request_builds_one_table(two_measure_source, monkeypatch):
    """A stacked request, deliberately: a single-measure one on a flat source
    returns the source's own stored table either way, so it would pass with the
    memo removed and prove nothing."""
    source = two_measure_source
    builds = _count_builds(monkeypatch)

    first = source.get_table(["Signal", "Other"])
    second = source.get_table(["Signal", "Other"])

    assert first is second
    assert len(builds) == 1


def test_a_different_request_builds_its_own_table(two_measure_source):
    source = two_measure_source

    one = source.get_table(["Signal"])
    stacked = source.get_table(["Signal", "Other"])

    assert one is not stacked
    # Measure ORDER decides which is primary, so it is part of the identity.
    assert source.get_table(["Other", "Signal"]) is not stacked


def test_stacking_two_measures_produces_one_value_column(two_measure_source):
    """Regression: `_stacked` passed the primary measure's name as pandas'
    `value_name` while that same column was being melted, which
    `DataFrame.melt` refuses — so this raised ValueError for EVERY input and
    stacking on a CSV or DataFrame source had never once worked. The scidb path
    has its own `_stacked_table`, which is why only the standalone sources were
    affected and nothing noticed. Found 2026-09-11."""
    from scistackplot.variants import VARIABLE_COLUMN

    stacked = two_measure_source.get_table(["Signal", "Other"])
    frame = stacked.frame

    # One value column, named for the primary measure, with a Variable column
    # saying which measure each row came from.
    assert stacked.measure_names == ["Signal"]
    assert VARIABLE_COLUMN in frame.columns
    assert set(frame[VARIABLE_COLUMN]) == {"Signal", "Other"}
    # Both measures' rows survive, stacked rather than joined.
    assert len(frame) == 2 * len(two_measure_source._table().frame)
    # The placeholder used during the melt must not leak into the result.
    assert not [c for c in frame.columns if c.startswith("__")]


def test_the_table_memo_is_bounded(series_frame):
    """The entries are whole frames, so an unbounded memo is a leak."""
    from scistackplot.sources.base import TABLE_CACHE_ENTRIES

    frame = series_frame.copy()
    for extra in range(6):
        frame[f"M{extra}"] = frame["Signal"]
    source = DataFrameSource(
        frame,
        factors=["subject", "session", "trial"],
        measures=["Signal", *[f"M{i}" for i in range(6)]],
        name="Signal",
    )

    for extra in range(6):
        source.get_table([f"M{extra}"])

    assert len(source._table_cache()) == TABLE_CACHE_ENTRIES


def test_a_cached_table_is_not_mutated_by_use(series_frame):
    """The memo hands the SAME table to every caller, so a resolve that wrote
    to it would corrupt the next one. Nothing in either package writes in
    place; this is the guard that keeps it true."""
    source = DataFrameSource(
        series_frame,
        factors=["subject", "session", "trial"],
        measures=["Signal"],
        name="Signal",
    )
    table = source.get_table(["Signal"])
    before = table.frame.copy(deep=True)

    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.COLOR, "session": Role.AGGREGATE},
        kind=PlotKind.LINE,
    )
    resolve(spec, table)

    pd.testing.assert_frame_equal(table.frame, before)
    assert source.get_table(["Signal"]) is table


def test_invalidate_drops_built_tables(two_measure_source, monkeypatch):
    """Asserted by counting BUILDS, not by object identity.

    A single-measure request on a flat source hands back the source's own
    stored `LongTable` — the same object every time, memo or no memo — so
    identity cannot tell a rebuild from a cache hit there. What
    `invalidate_tables` actually promises is that the next request does the
    work again.
    """
    source = two_measure_source
    builds = _count_builds(monkeypatch)

    source.get_table(["Signal", "Other"])
    source.get_table(["Signal", "Other"])
    assert len(builds) == 1, "the second request should have hit the memo"

    source.invalidate_tables()
    source.get_table(["Signal", "Other"])

    assert len(builds) == 2


# --- the plan memo (1b) -----------------------------------------------------


def _count_plans(monkeypatch) -> list:
    """Record every plan BUILD. Returns the list the spy appends to."""
    builds: list = []
    original = reduce_mod._build_plan

    def spy(spec, table):
        builds.append(1)
        return original(spec, table)

    monkeypatch.setattr(reduce_mod, "_build_plan", spy)
    return builds


def _spec(**overrides) -> PlotSpec:
    base = dict(
        measures=["Signal"],
        roles={"subject": Role.COLOR},
        kind=PlotKind.LINE,
    )
    base.update(overrides)
    return PlotSpec(**base)


def test_an_identical_spec_reuses_the_plan(series_table, monkeypatch):
    calls = _count_plans(monkeypatch)

    resolve_one(_spec(), series_table, 0)
    resolve_one(_spec(), series_table, 0)

    assert len(calls) == 1


def test_changing_only_the_plot_kind_reuses_the_plan(series_table, monkeypatch):
    """The point of the cache. Kind, facet grid and style change how a figure
    looks, not which rows go into it."""
    calls = _count_plans(monkeypatch)

    resolve_one(_spec(kind=PlotKind.LINE), series_table, 0)
    resolve_one(
        _spec(kind=PlotKind.BAND, roles={"subject": Role.FREE}), series_table, 0
    )
    resolve_one(
        _spec(kind=PlotKind.LINE, facet=FacetOptions(n_cols=2)), series_table, 0
    )

    # Two distinct role assignments -> two plans. The facet change reuses one.
    assert len(calls) == 2


def test_a_cached_plan_still_renders_the_NEW_kind(series_table):
    """The trap this cache sets for itself: `_build_figure` reads `plan.spec`,
    and the kind is deliberately not in the key. Without `_with_presentation`
    the second call would silently redraw the first kind."""
    lines = resolve_one(_spec(kind=PlotKind.LINE), series_table, 0)[0]
    scatter = resolve_one(_spec(kind=PlotKind.SCATTER), series_table, 0)[0]

    assert lines.kind is PlotKind.LINE
    assert scatter.kind is PlotKind.SCATTER


def test_a_cached_plan_still_uses_the_NEW_facet_grid(struct_table):
    def grid(n_cols):
        return resolve_one(
            PlotSpec(
                measures=["RawEMG"],
                roles={"ColName": Role.FACET},
                kind=PlotKind.LINE,
                facet=FacetOptions(n_cols=n_cols),
            ),
            struct_table,
            0,
        )[0]

    assert grid(3).grid_cols == 3
    assert grid(1).grid_cols == 1


def test_changing_a_filter_rebuilds_the_plan(series_table, monkeypatch):
    from scistackplot.spec import Filter

    calls = _count_plans(monkeypatch)

    resolve_one(_spec(), series_table, 0)
    resolve_one(
        _spec(filters=[Filter(column="subject", include=["01"])]), series_table, 0
    )

    assert len(calls) == 2


def test_a_different_table_is_a_different_plan(series_frame, monkeypatch):
    """Two tables built from equal data are still two questions — the cache
    keys on the table's identity, not on its contents.

    The two tables are built DIRECTLY rather than via a source: asking a flat
    source for one measure twice hands back its own stored `LongTable` both
    times, so `one is two` and there would be nothing here to test.
    """
    from scistackplot import LongTable

    def build() -> LongTable:
        return LongTable.from_frame(
            series_frame,
            factors=["subject", "session", "trial"],
            measures=["Signal"],
            name="Signal",
            schema_levels=["subject", "session", "trial"],
        )

    one, two = build(), build()
    assert one is not two

    calls = _count_plans(monkeypatch)

    resolve_one(_spec(), one, 0)
    resolve_one(_spec(), two, 0)

    assert len(calls) == 2


def test_the_plan_cache_is_bounded(series_table):
    from scistackplot.spec import Filter

    for subject in ["01", "02", "03"]:
        resolve_one(
            _spec(filters=[Filter(column="subject", include=[subject])]),
            series_table,
            0,
        )

    assert len(reduce_mod._plan_cache) <= reduce_mod._PLAN_CACHE_ENTRIES


def test_an_invalid_spec_still_raises_on_a_warm_cache(series_table):
    from scistackplot import RoleError

    resolve_one(_spec(), series_table, 0)

    with pytest.raises(RoleError):
        # Two factors on COLOR: a role conflict, and one the cache must not
        # paper over by serving the valid plan beside it.
        resolve_one(
            _spec(roles={"subject": Role.COLOR, "session": Role.COLOR}),
            series_table,
            0,
        )


# --- explode per figure (1c) ------------------------------------------------


def test_only_the_requested_figure_is_exploded(series_table, monkeypatch):
    """A fan-out used to explode every figure's rows and throw all but one
    away. The 2026-09-11 log caught it: 17.1M samples built, 8.9M downsampled."""
    exploded = []
    original = reduce_mod._explode_1d

    def spy(frame, measure, index_column):
        exploded.append(len(frame))
        return original(frame, measure, index_column)

    monkeypatch.setattr(reduce_mod, "_explode_1d", spy)

    spec = _spec(roles={"subject": Role.ITERATE})
    figure, labels, _ = resolve_one(spec, series_table, 0)

    assert len(labels) == 3, "three subjects should fan out to three figures"
    assert len(exploded) == 1, "only the figure being shown should explode"
    # 3 subjects x 2 sessions x 4 trials = 24 rows; one subject's share is 8.
    assert exploded[0] == 8


def test_resolving_every_figure_still_explodes_every_group(series_table, monkeypatch):
    exploded = []
    original = reduce_mod._explode_1d

    def spy(frame, measure, index_column):
        exploded.append(len(frame))
        return original(frame, measure, index_column)

    monkeypatch.setattr(reduce_mod, "_explode_1d", spy)

    figures = resolve(_spec(roles={"subject": Role.ITERATE}), series_table)

    assert len(figures) == 3
    assert exploded == [8, 8, 8]


def test_one_figure_matches_the_same_figure_from_the_whole_fanout(series_table):
    """The property the reordering must not break: grouping before the explode
    has to produce exactly what exploding before the grouping did."""
    spec = _spec(roles={"subject": Role.ITERATE})

    every = resolve(spec, series_table)
    for index, expected in enumerate(every):
        one, _, position = resolve_one(spec, series_table, index)
        assert position == index
        assert one.figure_key == expected.figure_key
        assert one.row_count == expected.row_count
        assert len(one.panels) == len(expected.panels)
        for got, want in zip(one.panels, expected.panels, strict=True):
            pd.testing.assert_frame_equal(got.frame, want.frame)


def test_aggregating_still_averages_sample_by_sample(series_table):
    """AGGREGATE on a 1-D measure has to run AFTER the explode — it averages
    each sample position across the collapsed factor's levels, which cannot
    happen while the rows are still nested arrays."""
    spec = _spec(roles={"subject": Role.COLOR, "trial": Role.AGGREGATE})

    figure = resolve_one(spec, series_table, 0)[0]

    # 3 subjects x 2 sessions x 10 samples, with the 4 trials averaged away.
    assert figure.row_count == 60


def test_a_scalar_measure_is_never_exploded(scalar_table, monkeypatch):
    called = []

    def refuse(*args, **kwargs):
        called.append(1)
        raise AssertionError("a scalar measure must never be exploded")

    monkeypatch.setattr(reduce_mod, "_explode_1d", refuse)

    resolve_one(
        PlotSpec(
            measures=["StepLength"],
            roles={"subject": Role.X, "session": Role.COLOR},
            kind=PlotKind.SCATTER,
        ),
        scalar_table,
        0,
    )

    assert called == []


# --- why a plan cache lookup missed ----------------------------------------
#
# Added 2026-09-13. The log showed four consecutive resolves of the SAME figure
# each rebuilding the plan — identical roles, identical output, four misses —
# and there was no way to tell a GUI firing four times from a spec field
# churning between four otherwise-identical requests. A hit logged at DEBUG; a
# miss logged nothing but a `build_plan` timing line that never said it was a
# miss. These pin the line that answers it.


def _cache_lines(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("plan cache:")
    ]


def test_a_hit_says_so_at_info(series_table, caplog):
    with caplog.at_level(logging.INFO, logger="scistackplot"):
        resolve_one(_spec(), series_table, 0)
        # `caplog` collects for the whole test, so the warm-up resolve's own
        # MISS is in there too until this clears it. Every one of these tests
        # asks about ONE lookup.
        caplog.clear()
        resolve_one(_spec(), series_table, 0)

    (line,) = _cache_lines(caplog)
    assert "HIT" in line, line


def test_the_first_resolve_reports_an_empty_cache(series_table, caplog):
    with caplog.at_level(logging.INFO, logger="scistackplot"):
        resolve_one(_spec(), series_table, 0)

    (line,) = _cache_lines(caplog)
    assert "MISS" in line and "cache empty" in line


def test_a_miss_names_the_spec_field_that_changed(series_table, caplog):
    """The diagnosis the 19:25 log could not give: WHICH field churned."""
    from scistackplot.spec import Filter

    with caplog.at_level(logging.INFO, logger="scistackplot"):
        resolve_one(_spec(), series_table, 0)
        caplog.clear()
        resolve_one(
            _spec(filters=[Filter(column="subject", include=["01"])]), series_table, 0
        )

    (line,) = _cache_lines(caplog)
    assert "MISS" in line
    assert "spec differs in 1 field(s)" in line, line
    assert "filters:" in line, line


def test_a_miss_names_every_differing_field_up_to_a_cap(series_table, caplog):
    """Bounded: a wholly different spec must not print the whole spec."""
    from scistackplot.spec import Aggregation, ErrorBand, Filter, Statistic, YAxis

    with caplog.at_level(logging.INFO, logger="scistackplot"):
        resolve_one(_spec(), series_table, 0)
        caplog.clear()
        resolve_one(
            _spec(
                roles={"subject": Role.FREE},
                filters=[Filter(column="subject", include=["01"])],
                aggregate=Aggregation(statistic=Statistic.MEDIAN, error=ErrorBand.SEM),
                y_axis=YAxis(minimum=0.0),
            ),
            series_table,
            0,
        )

    (line,) = _cache_lines(caplog)
    assert "MISS" in line
    assert "more)" in line, f"the field list must be capped: {line}"
    assert len(line) < 400, f"one log line, not a spec dump: {len(line)} chars"


def test_a_rebuilt_table_is_reported_as_the_table_changing(series_frame, caplog):
    """Same spec, new table object — a different diagnosis from a spec change,
    and the one that points at the source cache rather than at the panel."""
    from scistackplot import LongTable

    first = LongTable.from_frame(series_frame, measures=["Signal"])
    second = LongTable.from_frame(series_frame, measures=["Signal"])

    with caplog.at_level(logging.INFO, logger="scistackplot"):
        resolve_one(_spec(), first, 0)
        caplog.clear()
        resolve_one(_spec(), second, 0)

    (line,) = _cache_lines(caplog)
    assert "MISS" in line and "table object changed" in line, line


def test_the_reason_survives_an_unparseable_neighbour(series_table, caplog):
    """The helper must never be the thing that breaks a resolve."""
    reduce_mod._plan_cache[(id(series_table), "not json")] = (series_table, None)
    with caplog.at_level(logging.INFO, logger="scistackplot"):
        figure, _labels, _index = resolve_one(_spec(), series_table, 0)

    assert figure.panels
    assert _cache_lines(caplog)
