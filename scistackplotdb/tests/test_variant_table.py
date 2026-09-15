"""A metadata-only table: same variant structure, none of the payload.

`ScidbSource.variant_table` exists because `scistackplot.default_selection` reads
`default_pin`, `latest_column` and the variant factors' levels — and nothing else.
Answering that through `get_table` loaded every record's every field's every
sample: measured 2026-09-13 as 174 million samples / ~5.2 GB for one variable,
which is why the schema-location picker timed out while `location_states` itself
took 9.5 s (.claude/plot-at-scale-plan.md §7).

The contract worth pinning is not "it is faster" — it is that the ANSWER is the
same one the expensive path gives, and that no data column is read to get it.
"""

from __future__ import annotations

import pandas as pd
import pytest
from scistackplot import default_selection

from scistackplotdb import ScidbSource


@pytest.fixture
def source(seeded):
    return ScidbSource(seeded)


class TestVariantTableCarriesNoPayload:
    def test_no_measures(self, source):
        table = source.variant_table("Emg")
        assert table.measures == []

    def test_no_data_column_is_present(self, source):
        """The muscle columns of a dict variable must not be in the frame."""
        table = source.variant_table("Emg")
        for muscle in ("RHAM", "RTA", "LMG"):
            assert muscle not in table.frame.columns

    def test_no_data_column_is_even_queried(self, source, monkeypatch):
        """Assert on the SQL, not on wall time.

        A timing assertion would pass on a small fixture whatever the query did;
        the whole point is that the payload columns never enter the SELECT.
        """
        seen: list[str] = []
        # Both fetch doors: the load query goes through `_fetchdf` since the
        # payload fetch stopped boxing (test_load_fetch.py), and a payload
        # column reaching either one is the regression this guards.
        for name in ("_fetchall", "_fetchdf"):
            original = getattr(source._db._duck, name)

            def spy(sql, params=None, _original=original):
                seen.append(sql)
                return _original(sql, params)

            monkeypatch.setattr(source._db._duck, name, spy)
        source.variant_table("Emg")

        selects = [s for s in seen if "SELECT t.record_id" in s]
        assert selects, "the variable load query never ran"
        for sql in selects:
            for muscle in ("RHAM", "RTA", "LMG"):
                assert f'"{muscle}"' not in sql, f"payload column in SQL: {sql}"

    def test_schema_keys_and_record_id_survive(self, source):
        table = source.variant_table("Emg")
        assert "record_id" in table.frame.columns
        for key in ("subject", "session", "trial"):
            assert key in table.frame.columns
        assert len(table.frame) > 0


class TestAnswersMatchTheExpensivePath:
    def test_default_selection_agrees_with_the_full_table(self, source):
        """The reason this is safe to substitute."""
        cheap = default_selection(source.variant_table("Emg"))
        expensive = default_selection(source.get_table(["Emg"]))
        assert cheap == expensive

    def test_agrees_for_a_scalar_variable_too(self, source):
        cheap = default_selection(source.variant_table("StepLength"))
        expensive = default_selection(source.get_table(["StepLength"]))
        assert cheap == expensive

    def test_variant_columns_and_levels_match(self, source):
        cheap = source.variant_table("Emg")
        expensive = source.get_table(["Emg"])
        cheap_variants = {f.name: f.levels for f in cheap.factors if f.is_variant}
        dear_variants = {f.name: f.levels for f in expensive.factors if f.is_variant}
        assert cheap_variants == dear_variants

    def test_latest_column_and_pin_match(self, source):
        cheap = source.variant_table("Emg")
        expensive = source.get_table(["Emg"])
        assert cheap.latest_column == expensive.latest_column
        assert cheap.default_pin == expensive.default_pin


class TestCaching:
    def test_second_call_is_a_cache_hit(self, source, monkeypatch):
        source.variant_table("Emg")

        calls: list[int] = []
        import scistackplotdb.source as source_mod

        original = source_mod.load_variable

        def spy(*args, **kwargs):
            calls.append(1)
            return original(*args, **kwargs)

        monkeypatch.setattr(source_mod, "load_variable", spy)
        source.variant_table("Emg")
        assert calls == [], "variant_table reloaded instead of using its cache"

    def test_does_not_collide_with_the_plottable_table(self, source):
        """Distinct cache keys: one must not be served for the other."""
        metadata = source.variant_table("Emg")
        plottable = source.get_table(["Emg"])
        assert metadata.measures == []
        assert plottable.measures != []
        # And re-fetching the metadata one still gets the metadata one.
        assert source.variant_table("Emg").measures == []


class TestMetadataLoadIsOptIn:
    def test_load_variable_still_includes_data_by_default(self, seeded):
        from scistackplotdb.load import load_variable

        frame = load_variable(seeded, "Emg")
        assert frame.data_columns
        assert isinstance(frame.frame, pd.DataFrame)
        assert "RHAM" in frame.frame.columns

    def test_include_data_false_reports_no_data_columns(self, seeded):
        from scistackplotdb.load import load_variable

        frame = load_variable(seeded, "Emg", include_data=False)
        assert frame.data_columns == []
        assert "RHAM" not in frame.frame.columns
        # Still a real frame of records — this is a narrower load, not an empty one.
        assert len(frame.frame) > 0


# --- the frame behind it: provenance without payload -----------------------
#
# `variant_table` and `variant_graph` ask the same three questions of a
# variable — record ids, variant columns, variant axes — and neither touches a
# measure. `_variant_frame` is what answers them, and the cache it uses has to
# stay apart from the full-frame one or a later plot gets a frame with no data
# in it.


class TestTheProvenanceFrame:
    def test_variant_graph_does_not_load_the_data(self, source):
        """The case that matters: a variable nobody has plotted. Before this,
        opening a picker over `Emg` read every muscle's every sample to answer
        "which versions of its loader exist"."""
        assert "Emg" not in source._frames

        source.variant_graph("Emg")

        assert "Emg" not in source._frames, "the full frame was loaded"
        assert "Emg" in source._variant_frames

    def test_the_provenance_frame_carries_no_data_columns(self, source, seeded):
        """Checked against the TABLE's real columns, not against the frame's own
        `data_columns` — `include_data=False` reports that as empty, so reading
        it back would assert nothing at all."""
        from scistackplotdb.load import data_columns_for

        loaded = source._variant_frame("Emg")
        stored = data_columns_for(seeded, "Emg")

        assert stored, "precondition: Emg has data columns to leave out"
        assert loaded.data_columns == []
        for column in stored:
            assert column not in loaded.frame.columns, (
                f"{column!r} is a measure column and should not have been read"
            )

    def test_a_loaded_variable_is_reused_rather_than_re_read(self, source):
        """When the panel has already loaded it, the full frame answers these
        questions too — paying for a second read would be the mirror of the
        bug this fixes."""
        source.get_table(["Emg"])
        assert "Emg" in source._frames

        assert source._variant_frame("Emg") is source._frames["Emg"]
        assert "Emg" not in source._variant_frames

    def test_the_two_caches_are_separate(self, source):
        """A data-less frame in `_frames` would be handed to a later plot as
        though it held the measure — an empty figure with no error anywhere."""
        source.variant_graph("Emg")

        table = source.get_table(["Emg"])

        assert len(table.measures) == 1
        assert not table.frame.empty

    def test_invalidate_drops_the_provenance_frame_too(self, source):
        """Provenance goes stale with the data: a re-run writes new records
        under a new function version, which is what this cache answers."""
        source.variant_graph("Emg")
        assert "Emg" in source._variant_frames

        source.invalidate("Emg")

        assert "Emg" not in source._variant_frames

    def test_the_graph_is_the_same_either_way(self, source):
        """The contract this shares with `variant_table`: the cheap path's
        ANSWER is the expensive path's answer. If they ever diverge, a picker
        would offer versions the figure does not have."""
        cheap = source.variant_graph("Emg")
        source.get_table(["Emg"])
        source._variant_frames.clear()

        assert source.variant_graph("Emg") == cheap
