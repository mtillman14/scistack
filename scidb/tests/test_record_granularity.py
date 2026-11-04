"""One record per schema combination, unless the data says otherwise.

Found by reading examples/vo2max/scidb.model.log: wiring
``PathInput -> pandas.read_csv -> cpet_data_raw`` over 4 CSV files saved
**1364** records — 322 + 372 + 304 + 366, i.e. one record per CSV ROW, each
holding a 1x2 DataFrame. All 322 records from one file shared the same
(subject, session) address and were distinguishable only by their contents,
which is a variant explosion: any downstream load of that variable sees 322
variants for one combo.

The rule now: a returned DataFrame's rows spread into separate records only
when the DataFrame supplies address information (a schema key) the call did
not already pin. See scifor.foreach._spread_decision.
"""

import numpy as np
import pandas as pd
import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each


class CpetRaw(BaseVariable):
    schema_version = 1


class SessionSummary(BaseVariable):
    schema_version = 1


class Signal(BaseVariable):
    schema_version = 1


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "granularity.duckdb", ["subject", "session"])
    yield database
    _scifor.set_schema([])
    database.close()


def _n_records(db, var_name: str) -> int:
    return db._duck.fetchall(
        "SELECT COUNT(*) FROM _record WHERE type = ?", [var_name]
    )[0][0]


class TestWholeTablePerCombo:
    def test_multirow_table_is_one_record_per_combo(self, db):
        """The vo2max shape: a 300-row table per combo is ONE record."""
        rows = 300

        def read_table():
            return pd.DataFrame(
                {"time": np.arange(rows, dtype=float), "vo2": np.ones(rows)}
            )

        for_each(
            read_table,
            {},
            [CpetRaw],
            subject=["SS01", "SS02"],
            session=["01"],
        )

        assert _n_records(db, "CpetRaw") == 2, (
            "expected one record per (subject, session), not one per data row"
        )

    def test_saved_table_round_trips_whole(self, db):
        def read_table():
            return pd.DataFrame({"time": [0.0, 1.0, 2.0], "vo2": [4.0, 5.0, 6.0]})

        for_each(read_table, {}, [CpetRaw], subject=["SS01"], session=["01"])

        # load() returns the BaseVariable wrapper; .data is the stored value.
        loaded = CpetRaw.load(subject="SS01", session="01")
        assert not isinstance(loaded, list), (
            f"expected ONE record for this combo, got {len(loaded)}"
        )
        assert isinstance(loaded.data, pd.DataFrame)
        assert len(loaded.data) == 3
        assert list(loaded.data["vo2"]) == [4.0, 5.0, 6.0]

    def test_one_invocation_per_combo_not_per_row(self, db):
        """A 300-row table must not produce 300 output edges for one combo."""

        def read_table():
            return pd.DataFrame({"v": np.arange(300, dtype=float)})

        for_each(read_table, {}, [CpetRaw], subject=["SS01"], session=["01"])

        n_edges = db._duck.fetchall("SELECT COUNT(*) FROM _invocation_output")[0][0]
        assert n_edges == 1, f"expected 1 output edge, got {n_edges}"


class TestUnpinnedSchemaKeySpreads:
    def test_output_carrying_unpinned_key_saves_per_key_records(self, db):
        """Called at subject level, returning rows that name their session:
        each row genuinely addresses its own (subject, session)."""

        def split_sessions():
            return pd.DataFrame({"session": ["01", "02", "03"], "score": [1.0, 2.0, 3.0]})

        for_each(split_sessions, {}, [SessionSummary], subject=["SS01"])

        assert _n_records(db, "SessionSummary") == 3
        for session, expected in [("01", 1.0), ("02", 2.0), ("03", 3.0)]:
            # Each row was filed at its OWN session — that is the whole point.
            loaded = SessionSummary.load(subject="SS01", session=session)
            assert not isinstance(loaded, list), (
                f"session={session} should hold exactly one record"
            )
            data = loaded.data
            score = data["score"].iloc[0] if isinstance(data, pd.DataFrame) else data
            assert float(score) == expected


class TestDistributeUnchanged:
    def test_distribute_still_fans_out(self, db):
        """distribute=True splits before collection and files each piece one
        level below the deepest iterated key — untouched by the new rule."""

        def make_rows():
            return pd.DataFrame({"v": [10.0, 20.0, 30.0]})

        for_each(make_rows, {}, [Signal], distribute=True, subject=["SS01"])

        assert _n_records(db, "Signal") == 3

    def test_distribute_at_deepest_key_raises(self, db):
        def make_rows():
            return pd.DataFrame({"v": [1.0, 2.0]})

        with pytest.raises(ValueError, match="no lower level to distribute to"):
            for_each(
                make_rows,
                {},
                [Signal],
                distribute=True,
                subject=["SS01"],
                session=["01"],
            )


class TestDistributeBelowCarriedKeys:
    """The CERNER CSV (scistack.log 2026-11-04): 34 rows for 33 subjects, schema
    [subject, bout, date], `read_csv` with distribute=True."""

    @pytest.fixture
    def bout_db(self, tmp_path):
        db = configure_database(tmp_path / "bouts.duckdb", ["subject", "bout", "date"])
        yield db
        db.close()

    def test_every_row_gets_a_bout_within_its_subject(self, bout_db):
        def load():
            return pd.DataFrame(
                {"subject": ["A", "B", "B"], "admit": ["d1", "d2", "d3"], "los": [1.0, 2.0, 3.0]}
            )

        for_each(load, {}, [Signal], distribute=True, db=bout_db)

        def row(**keys):
            rec = Signal.load(db=bout_db, **keys)
            assert not isinstance(rec, list), f"{keys}: {len(rec)} records"
            return rec.data

        assert row(subject="A", bout=1)["admit"].iloc[0] == "d1"
        assert row(subject="B", bout=1)["admit"].iloc[0] == "d2"
        assert row(subject="B", bout=2)["admit"].iloc[0] == "d3"
        # subject is an address, and no cell is a list.
        assert list(row(subject="B", bout=2).columns) == ["admit", "los"]
        assert len(Signal.load(db=bout_db, subject="B")) == 2

    def test_a_rerun_files_the_same_addresses(self, bout_db):
        def load():
            return pd.DataFrame({"subject": ["A", "A"], "los": [1.0, 2.0]})

        for_each(load, {}, [Signal], distribute=True, db=bout_db)
        for_each(load, {}, [Signal], distribute=True, db=bout_db)
        assert len(Signal.load(db=bout_db, subject="A")) == 2
