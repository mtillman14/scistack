"""The engine seam: csv-stats behind one module, refusals instead of fallbacks."""

from __future__ import annotations

import pandas as pd
import pytest

from scistackstats import CsvStatsEngine, EngineRefusal, FakeEngine, get_engine, use_engine


def _paired() -> pd.DataFrame:
    rows = []
    for s, (a, b) in enumerate([(5.0, 6.1), (4.2, 4.9), (6.3, 6.8), (5.5, 6.9)]):
        rows += [{"u": f"S{s}", "time": "pre", "y": a}, {"u": f"S{s}", "time": "post", "y": b}]
    return pd.DataFrame(rows)


def test_csvstats_engine_returns_the_core_record():
    engine = CsvStatsEngine()
    record = engine.run("ttest_paired", _paired(), "y", within=["time"], subject="u",
                        levels={"time": ["post", "pre"]})
    (effect,) = record["effects"]
    assert record["test"] == "ttest_paired"
    assert (effect["a"], effect["b"]) == ("post", "pre")
    assert effect["estimate"]["value"] > 0
    assert "d_z" in effect["effect_sizes"]
    assert engine.versions()["csvstats"]


def test_a_csvstats_gap_is_a_refusal_not_a_fallback():
    data = _paired().assign(g="x", h="y")
    with pytest.raises(EngineRefusal) as caught:
        CsvStatsEngine().run("anova", data, "y", between=["g", "h"], within=["time"], subject="u")
    assert caught.value.kind == "gap"
    assert caught.value.reason.startswith("not yet in csv-stats")


def test_invalid_data_is_a_refusal_with_csvstats_message():
    data = pd.concat([_paired(), _paired().assign(time="mid")])
    with pytest.raises(EngineRefusal) as caught:
        CsvStatsEngine().run("ttest_ind_welch", data, "y", between=["time"])
    assert caught.value.kind == "invalid"
    assert "exactly 2 levels" in caught.value.reason


def test_use_engine_swaps_and_restores():
    fake = FakeEngine()
    before = get_engine()
    with use_engine(fake):
        assert get_engine() is fake
    assert get_engine() is before


def test_fake_engine_keeps_real_descriptives_and_corrections():
    fake = FakeEngine()
    assert fake.summarize([1.0, 2.0, 3.0])["mean"] == pytest.approx(2.0)
    assert fake.correct([0.01, 0.04, 0.03], "holm") == pytest.approx([0.03, 0.06, 0.06])
