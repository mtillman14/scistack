"""The input-binding round trip, guarded at both ends.

A column selection that goes into a run must come back out of history
unchanged. ``docs/claude/input-binding-round-trip.md`` §5 explains why one
check cannot cover both ends:

* the WRITE side (``provenance_save.check_selector_round_trip``) catches a
  selection that never reached an edge;
* the READ side (``provenance_query.check_recorded_selectors``) catches a
  selection that history recorded and the next run fails to carry.

A read-side check alone is blind to the first case, because there is nothing
recorded to disagree with. That blindness is exactly the live bug, which is
why both exist.
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each
from scidb.provenance_query import check_recorded_selectors
from scidb.provenance_save import check_selector_round_trip


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "selectors.duckdb", ["subject", "trial"])
    yield db
    _scifor.set_schema([])
    db.close()


# ---------------------------------------------------------------------------
# Write side
# ---------------------------------------------------------------------------


class TestWriteSide:
    """Constructed pairs, NOT reached through the live bug.

    Deliberate: Stage 3 fixes the ``for_columns`` reassembly path, and a test
    whose only trigger was that bug would go quietly vacuous the moment it
    was fixed.
    """

    def test_a_selection_that_reached_no_edge_is_reported(self, caplog):
        with caplog.at_level(logging.WARNING):
            lost = check_selector_round_trip(
                "scale_joint",
                {"value": '{"columns": ["ankle"]}'},
                {"value": None},
            )
        assert lost == ["value"]
        assert "[selector-lost]" in caplog.text
        assert "scale_joint" in caplog.text
        assert "value" in caplog.text

    def test_a_selection_that_reached_its_edge_is_silent(self, caplog):
        with caplog.at_level(logging.WARNING):
            lost = check_selector_round_trip(
                "scale_joint",
                {"value": '{"columns": ["ankle"]}'},
                {"value": '{"columns": ["ankle"]}'},
            )
        assert lost == []
        assert "[selector-lost]" not in caplog.text

    def test_a_whole_variable_input_is_not_a_loss(self, caplog):
        with caplog.at_level(logging.WARNING):
            assert check_selector_round_trip("f", {"value": None}, {"value": None}) == []
        assert caplog.text == ""

    def test_the_context_names_the_save_path(self, caplog):
        with caplog.at_level(logging.WARNING):
            check_selector_round_trip(
                "scale_joint",
                {"value": '{"columns": ["ankle"]}'},
                {},
                context="3/3 saved row(s) had no __rid_* columns",
            )
        assert "no __rid_* columns" in caplog.text


class TestWriteSideOnTheRealPath:
    def test_a_for_columns_run_records_every_selector(self, db, caplog):
        """The regression guard for the live bug: a per-column run must not
        report a lost selector. RED until the reassembly save path carries
        the selection (plan Stage 3); green and permanent thereafter."""

        class Wide(BaseVariable):
            pass

        class Doubled(BaseVariable):
            pass

        Wide.save(pd.DataFrame({"a": [1.0], "b": [2.0]}), subject="01", trial="1")

        def double(value):
            return float(pd.Series(value).iloc[0]) * 2

        with caplog.at_level(logging.WARNING):
            for_each(
                double, {"value": Wide.for_columns()}, [Doubled], subject=[], trial=[]
            )
        assert "[selector-lost]" not in caplog.text, caplog.text

    def test_a_plain_column_selection_records_its_selector(self, db, caplog):
        class Wide2(BaseVariable):
            pass

        class Scaled(BaseVariable):
            pass

        Wide2.save(pd.DataFrame({"a": [1.0], "b": [2.0]}), subject="01", trial="1")

        def scale(value):
            return float(pd.Series(value).iloc[0]) * 10

        with caplog.at_level(logging.WARNING):
            for_each(scale, {"value": Wide2["a"]}, [Scaled], subject=[], trial=[])
        assert "[selector-lost]" not in caplog.text, caplog.text


# ---------------------------------------------------------------------------
# Read side
# ---------------------------------------------------------------------------


class TestReadSide:
    """What history recorded vs what the next run binds.

    These call the checker directly with a synthetic ``asked`` map, because
    the question is about the COMPARISON, not about any one caller.
    """

    @pytest.fixture
    def recorded(self, db):
        """A run of ``pick`` recorded with ``value`` restricted to "a"."""

        class Table(BaseVariable):
            pass

        class Picked(BaseVariable):
            pass

        Table.save(pd.DataFrame({"a": [1.0], "b": [2.0]}), subject="01", trial="1")

        def pick(value):
            return float(pd.Series(value).iloc[0])

        for_each(pick, {"value": Table["a"]}, [Picked], subject=[], trial=[])
        return db

    def test_a_dropped_selection_warns(self, recorded, caplog):
        with caplog.at_level(logging.WARNING):
            lost = check_recorded_selectors(recorded._duck, "pick", {"value": None})
        assert lost == ["value"]
        assert "[selector-dropped]" in caplog.text

    def test_the_same_selection_is_silent(self, recorded, caplog):
        with caplog.at_level(logging.INFO):
            lost = check_recorded_selectors(
                recorded._duck, "pick", {"value": '{"columns": ["a"]}'}
            )
        assert lost == []
        assert "[selector-dropped]" not in caplog.text
        assert "[selector-changed]" not in caplog.text

    def test_a_changed_selection_is_info_not_warn(self, recorded, caplog):
        with caplog.at_level(logging.INFO):
            lost = check_recorded_selectors(
                recorded._duck, "pick", {"value": '{"columns": ["b"]}'}
            )
        assert lost == []
        assert "[selector-changed]" in caplog.text
        assert "[selector-dropped]" not in caplog.text

    def test_a_param_history_never_saw_is_not_its_business(self, recorded, caplog):
        with caplog.at_level(logging.WARNING):
            assert check_recorded_selectors(recorded._duck, "pick", {"other": None}) == []
        assert "[selector-dropped]" not in caplog.text

    def test_an_unknown_function_says_nothing(self, db, caplog):
        with caplog.at_level(logging.WARNING):
            assert check_recorded_selectors(db._duck, "never_ran", {"value": None}) == []
        assert caplog.text == ""
