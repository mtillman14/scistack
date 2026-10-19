"""Custom serialization (``to_db``/``from_db``) on every save path.

Regression for 2026-10-18: ``DatabaseManager.save`` hashed ``variable.data``
BEFORE calling ``to_db`` (so an object value raised "Unserializable data
type"), and ``save_batch`` -- every ``for_each`` save -- never called
``to_db`` at all. Both now store a custom-serialized variable the same way:
``to_db`` on every item first, then the normal batch (hash of the stored
frame, one bulk insert into the columnar ``"custom": True`` table), and loads
hand each record's frame to ``from_db``.
"""

import json
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "codec.duckdb", SCHEMA)
    Trajectory_Calls.reset()
    yield db
    _scifor.set_schema([])
    db.close()


@dataclass
class Trajectory:
    t: np.ndarray
    label: str

    def __eq__(self, other):
        return (
            isinstance(other, Trajectory)
            and self.label == other.label
            and np.array_equal(self.t, other.t)
        )


class Trajectory_Calls:
    """Order of codec calls and bulk inserts, to observe batching."""

    events: list = []

    @classmethod
    def reset(cls):
        cls.events = []


class MarkerTraj(BaseVariable):
    def to_db(self) -> pd.DataFrame:
        Trajectory_Calls.events.append("to_db")
        return pd.DataFrame({"t": self.data.t, "label": self.data.label})

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> Trajectory:
        Trajectory_Calls.events.append("from_db")
        return Trajectory(t=df["t"].to_numpy(), label=str(df["label"].iloc[0]))


class GaitTable(BaseVariable):
    """DataFrame pass-through codec (like test_where.GaitData)."""

    def to_db(self) -> pd.DataFrame:
        return self.data.copy()

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> pd.DataFrame:
        Trajectory_Calls.events.append("from_db")
        return df.copy()


class NotAFrame(BaseVariable):
    def to_db(self):
        return [1, 2, 3]

    @classmethod
    def from_db(cls, df):
        return df


class RawSamples(BaseVariable):
    pass


class StepLen(BaseVariable):
    pass


class TrajLength(BaseVariable):
    pass


def _traj(i: int) -> Trajectory:
    return Trajectory(t=np.arange(3, dtype=float) + i, label=f"m{i}")


def _dtype(db, name: str) -> dict:
    rows = db._duck._fetchall(
        "SELECT dtype FROM _variables WHERE variable_name = ?", [name]
    )
    return json.loads(rows[0][0])


def _stored_rows(db, name: str) -> int:
    return db._duck._fetchall(f'SELECT COUNT(*) FROM "{name}_data"')[0][0]


class TestDirectSave:
    def test_object_value_round_trips(self, db):
        MarkerTraj.save(_traj(0), subject="S01", trial="1")
        loaded = MarkerTraj.load(subject="S01", trial="1")
        assert loaded.data == _traj(0)
        assert _dtype(db, "MarkerTraj") == {"custom": True}

    def test_to_db_must_return_dataframe(self, db):
        with pytest.raises(TypeError, match=r"NotAFrame\.to_db\(\) must return"):
            NotAFrame.save(1, subject="S01", trial="1")


class TestSaveBatch:
    def test_object_values_round_trip(self, db):
        items = [(_traj(i), {"subject": "S01", "trial": str(i)}) for i in range(4)]
        rids = db.save_batch(MarkerTraj, items)
        assert all(isinstance(r, str) for r in rids)
        assert _dtype(db, "MarkerTraj") == {"custom": True}
        assert _stored_rows(db, "MarkerTraj") == 4 * 3
        for i in range(4):
            assert MarkerTraj.load(subject="S01", trial=str(i)).data == _traj(i)

    def test_converts_every_item_then_one_insert(self, db, monkeypatch):
        real = db._insert_columnar_frames

        def spy(table_name, frames):
            Trajectory_Calls.events.append(f"insert:{len(frames)}")
            return real(table_name, frames)

        monkeypatch.setattr(db, "_insert_columnar_frames", spy)
        items = [(_traj(i), {"subject": "S01", "trial": str(i)}) for i in range(5)]
        db.save_batch(MarkerTraj, items)
        assert Trajectory_Calls.events == ["to_db"] * 5 + ["insert:5"]

    def test_same_record_id_as_direct_save(self, db):
        rid_direct = MarkerTraj.save(_traj(7), subject="S01", trial="1")
        (rid_batch,) = db.save_batch(
            MarkerTraj, [(_traj(7), {"subject": "S01", "trial": "1"})]
        )
        assert rid_batch == rid_direct
        assert _stored_rows(db, "MarkerTraj") == 3  # stored once

    def test_resave_and_in_batch_repeat_store_once(self, db):
        item = (_traj(1), {"subject": "S01", "trial": "1"})
        db.save_batch(MarkerTraj, [item, item])
        db.save_batch(MarkerTraj, [item])
        assert _stored_rows(db, "MarkerTraj") == 3

    def test_mismatched_columns_skipped_rest_saved(self, db):
        class Varying(BaseVariable):
            def to_db(self):
                return pd.DataFrame(self.data)

            @classmethod
            def from_db(cls, df):
                return df

        rids = db.save_batch(
            Varying,
            [
                ({"a": [1.0]}, {"subject": "S01", "trial": "1"}),
                ({"b": [2.0]}, {"subject": "S01", "trial": "2"}),
                ({"a": [3.0]}, {"subject": "S01", "trial": "3"}),
            ],
        )
        assert rids[1] is None
        assert isinstance(rids[0], str) and isinstance(rids[2], str)

    def test_to_db_must_return_dataframe(self, db):
        with pytest.raises(TypeError, match=r"NotAFrame\.to_db\(\) must return"):
            db.save_batch(NotAFrame, [(1, {"subject": "S01", "trial": "1"})])


class TestForEach:
    def test_object_output_feeds_next_step(self, db):
        for trial in ["1", "2"]:
            RawSamples.save(np.array([0.0, 1.0, 2.0]), subject="S01", trial=trial)

        def make_traj(raw):
            return Trajectory(t=np.asarray(raw, dtype=float), label="heel")

        def traj_length(traj):
            assert isinstance(traj, Trajectory), type(traj)
            return float(len(traj.t))

        for_each(make_traj, {"raw": RawSamples}, [MarkerTraj],
                 subject=["S01"], trial=["1", "2"])
        assert _dtype(db, "MarkerTraj") == {"custom": True}
        assert MarkerTraj.load(subject="S01", trial="2").data == Trajectory(
            t=np.array([0.0, 1.0, 2.0]), label="heel"
        )

        for_each(traj_length, {"traj": MarkerTraj}, [TrajLength],
                 subject=["S01"], trial=["1", "2"])
        assert TrajLength.load(subject="S01", trial="1").data == 3.0

    def test_dataframe_codec_output_loads_through_from_db(self, db):
        """A pass-through DataFrame codec saved by for_each used to land in
        dataframe mode (no from_db on load) while save() wrote "custom"."""
        for trial in ["1", "2"]:
            RawSamples.save(np.array([1.0, 2.0]), subject="S01", trial=trial)

        def to_table(raw):
            return pd.DataFrame({"Side": ["L"], "Speed": [float(raw[0])]})

        for_each(to_table, {"raw": RawSamples}, [GaitTable],
                 subject=["S01"], trial=["1", "2"])
        assert _dtype(db, "GaitTable") == {"custom": True}
        Trajectory_Calls.reset()
        GaitTable.load(subject="S01", trial="1")
        assert Trajectory_Calls.events == ["from_db"]


class TestWhereOnBatchSavedColumns:
    def test_column_filter_on_save_batch_rows(self, db):
        db.save_batch(
            GaitTable,
            [
                (pd.DataFrame({"Side": ["L"], "Speed": [1.2]}), {"subject": "1", "trial": "1"}),
                (pd.DataFrame({"Side": ["R"], "Speed": [1.5]}), {"subject": "1", "trial": "2"}),
                (pd.DataFrame({"Side": ["L"], "Speed": [1.1]}), {"subject": "1", "trial": "3"}),
            ],
        )
        StepLen.save(0.65, subject="1", trial="1")
        StepLen.save(0.72, subject="1", trial="2")
        StepLen.save(0.68, subject="1", trial="3")
        results = StepLen.load(where=GaitTable["Side"] == "L")
        assert sorted(v.data for v in results) == [0.65, 0.68]


class BaselineTable(BaseVariable):
    """A plain DataFrame variable: one record, several stored rows."""


class TestFixedTableInputRecordsItsEdge:
    """A Fixed pin on a table-valued record matched its ROWS: one 2-row
    record was reported as "matches 2 records, not one" and its input edge
    was dropped (scidb.foreach rid tracking counts records now)."""

    def test_fixed_pin_on_multi_row_record_records_an_edge(self, db):
        from scifor import Fixed

        BaselineTable.save(
            pd.DataFrame({"t": [0.0, 1.0], "v": [5.0, 6.0]}), subject="S01", trial="0"
        )
        RawSamples.save(np.array([1.0, 2.0]), subject="S01", trial="1")

        def use_baseline(raw, baseline):
            return float(len(baseline))

        for_each(
            use_baseline,
            {"raw": RawSamples, "baseline": Fixed(BaselineTable, trial="0")},
            [StepLen],
            subject=["S01"],
            trial=["1"],
        )
        assert StepLen.load(subject="S01", trial="1").data == 2.0
        params = {
            p
            for (p,) in db._duck._fetchall(
                "SELECT DISTINCT ii.param_name FROM _invocation_input ii "
                "JOIN _invocation inv ON inv.invocation_id = ii.invocation_id "
                "WHERE inv.function_name = ?",
                ["use_baseline"],
            )
        }
        assert params == {"raw", "baseline"}, params
