"""The spread rule must reach the MATLAB path.

MATLAB's loop (+scidb/for_each.m -> +scifor/for_each.m with
_nest_table_outputs=true) hands ``for_each_save`` one row per combination
with each output's whole return value nested in a cell. Python's loop, by
contrast, decides in ``scifor._spread_decision`` whether a returned table's
ROWS are separately addressed (it carries a schema key the combination did
not pin). Until 2026-09-15 nothing re-applied that decision on the bridge, so
a ``loadFunctionalOutcomes`` run that returned a 73x27 table labelled by
``subject``/``session`` with every schema level deselected saved ONE
dataset-level record on the MATLAB path and 73 on the Python path
(scidb.log 2026-09-15 19:16, ``result_tbl shape=(1, 1)``).

Runs entirely in Python without MATLAB: prepare -> the nested frame MATLAB
would build -> for_each_save, mirroring test_bridge_endpoints.py.
"""

import logging
import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import pandas as pd
import pytest
from scidb.database import configure_database
from scimatlab.bridge import (
    for_each_prepare,
    for_each_save,
    register_matlab_variable,
)

SCHEMA = ["subject", "session", "speed"]


@pytest.fixture
def db(tmp_path):
    import scifor as _scifor

    _scifor.set_schema([])
    db = configure_database(tmp_path / "spread.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


@pytest.fixture
def literal_file(tmp_path):
    """A static PathInput target: no {key} placeholders, so nothing iterates
    and the run is one dataset-level call — the loadFunctionalOutcomes shape."""
    f = tmp_path / "outcomes.csv"
    f.write_text("hello")
    return f


def _prepare_static(db, literal_file, out_name):
    """Prepare a one-call run: static PathInput, no metadata iterables."""
    register_matlab_variable(out_name)
    prep = for_each_prepare(
        "loadOutcomes",
        "hash0",
        {"filePath": {"kind": "pathinput", "template": str(literal_file)}},
        [out_name],
        {},
        db=db,
    )
    combos = list(prep["full_combos"])
    assert len(combos) == 1, combos
    return prep, dict(combos[0].items())


def _nested_frame(combo, out_name, value):
    """The nested-mode table MATLAB's loop returns for one combination:
    metadata columns (none here) + the output column holding the raw value.
    Built from a list of dicts, as scifor's own collector does."""
    row = dict(combo)
    row[out_name] = value
    return pd.DataFrame([row])


def _n_records(db, var_name):
    return db._duck._fetchall(
        "SELECT COUNT(*) FROM _record WHERE type = ?", [var_name]
    )[0][0]


def _addresses(db, out_cls):
    return sorted(
        (str(v["schema"].get("subject")), str(v["schema"].get("session")))
        for v in db.list_versions(out_cls)
    )


class TestLabelledTableSpreadsOnBridge:
    def test_rows_filed_at_their_own_subject_session(self, db, literal_file, caplog):
        out_name = "Outcomes_SP"
        prep, combo = _prepare_static(db, literal_file, out_name)
        inner = pd.DataFrame(
            {
                "subject": ["A", "A", "B"],
                "session": ["01", "02", "01"],
                "score": [1.0, 2.0, 3.0],
            }
        )
        with caplog.at_level(logging.INFO):
            result = for_each_save(
                prep["handle"], [_nested_frame(combo, out_name, inner)], save=True
            )

        out_cls = register_matlab_variable(out_name)
        assert _addresses(db, out_cls) == [("A", "01"), ("A", "02"), ("B", "01")]
        # The returned table is flat (spread), one row per record.
        assert len(result) == 3
        assert out_name not in result.columns
        assert list(result["score"]) == [1.0, 2.0, 3.0]
        # The same log line the Python loop emits, plus the bridge's shape
        # line showing the nested (1, 1) -> spread transition.
        assert "discriminated by unpinned schema key(s)" in caplog.text
        assert "nested shape=(1, 1)" in caplog.text

    def test_each_record_holds_only_its_row(self, db, literal_file):
        out_name = "Outcomes_SP2"
        prep, combo = _prepare_static(db, literal_file, out_name)
        inner = pd.DataFrame(
            {"subject": ["A", "B"], "session": ["01", "01"], "score": [10.0, 20.0]}
        )
        for_each_save(prep["handle"], [_nested_frame(combo, out_name, inner)], save=True)
        out_cls = register_matlab_variable(out_name)
        assert _n_records(db, out_name) == 2
        loaded = out_cls.load(subject="B", session="01")
        assert not isinstance(loaded, list), "one record per (subject, session)"
        data = loaded.data
        score = data["score"].iloc[0] if isinstance(data, pd.DataFrame) else data
        assert float(score) == 20.0
        if isinstance(data, pd.DataFrame):
            assert len(data) == 1


class TestUnlabelledTableStaysWholeOnBridge:
    def test_one_record_per_combination(self, db, literal_file, caplog):
        """A table with no schema-key column has nothing to address its rows
        with: one dataset-level record holding the whole table — the
        pre-existing behaviour, now stated in the log."""
        out_name = "Outcomes_SP3"
        prep, combo = _prepare_static(db, literal_file, out_name)
        inner = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
        with caplog.at_level(logging.INFO):
            result = for_each_save(
                prep["handle"], [_nested_frame(combo, out_name, inner)], save=True
            )
        out_cls = register_matlab_variable(out_name)
        versions = db.list_versions(out_cls)
        assert len(versions) == 1
        assert len(result) == 1
        assert out_name in result.columns
        assert "saving each whole table as ONE record" in caplog.text
