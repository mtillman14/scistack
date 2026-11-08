"""What is inside cells, per location (scidb.inspect.cells, plan Stage 4).

The user's missing data was NaN inside per-minute vectors: one present,
non-blank cell to every row count. These tests pin that the cells are looked
inside, per location, by the preview and ``scidb nans``. What counts as
which kind is the shared reader's (scicanonicalhash._cells); its own tests
are scicanonicalhash/tests/test_cells.py.
"""

import json

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database
from scifor.project import schema_order
from scicanonicalhash._cells import ALL_NAN, CONSTANT, PARTIAL_NAN
from scidb.inspect.cells import cell_profile


class CellArr(BaseVariable):
    pass


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    schema_order.clear_cache()
    _scifor.set_schema([])
    yield
    _scifor.set_schema([])
    schema_order.clear_cache()


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "cells.duckdb", ["subject"])
    CellArr.save(np.array([1.0, 2.0, 3.0]), subject="S01")
    CellArr.save(np.array([1.0, np.nan, 3.0]), subject="S02")
    CellArr.save(np.array([np.nan, np.nan, np.nan]), subject="S03")
    CellArr.save(np.array([4.0, 4.0, 4.0]), subject="S04")
    yield database
    database.close()


class TestProfile:
    def test_problems_per_location(self, db):
        profile = cell_profile(db, CellArr)
        by_subject = {p.location["subject"]: p.kind for p in profile.problems}
        assert by_subject == {"S02": PARTIAL_NAN, "S03": ALL_NAN, "S04": CONSTANT}
        (stats,) = profile.columns.values()
        assert stats.nan_elements == 4 and stats.elements == 12

    def test_worst_first_and_detail(self, db):
        profile = cell_profile(db, CellArr)
        assert profile.problems[0].kind == ALL_NAN
        partial = next(p for p in profile.problems if p.kind == PARTIAL_NAN)
        assert partial.detail == "1 of 3 values NaN"

    def test_location_filter_and_json(self, db):
        profile = cell_profile(db, CellArr, location={"subject": "S02"})
        assert [p.location["subject"] for p in profile.problems] == ["S02"]
        assert json.loads(json.dumps(profile.to_dict()))["total_problems"] == 1

    def test_unknown_key_refused(self, db):
        with pytest.raises(ValueError, match="not schema keys"):
            cell_profile(db, CellArr, location={"visit": "1"})

    def test_preview_reports_inside_the_cells(self, db):
        from scidb.inspect.preview import data_preview

        (col,) = data_preview(db, "CellArr").columns
        assert col.non_null == 4  # every cell is "non-blank"...
        assert col.cells["nan_elements"] == 4  # ...but the NaNs are counted
        assert "4 of 12 values are NaN" in col.cells["description"]


class TestCli:
    @pytest.fixture
    def path(self, tmp_path):
        p = tmp_path / "cli.duckdb"
        database = configure_database(p, ["subject"])
        CellArr.save(np.array([1.0, np.nan]), subject="S01")
        CellArr.save(np.array([2.0, 3.0]), subject="S02")
        database.close()
        return p

    def run(self, path, *argv):
        from scidb.inspect.cli import main

        return main(["--db", str(path), *argv])

    def test_nans(self, path, capsys):
        assert self.run(path, "nans", "CellArr") == 0
        out = capsys.readouterr().out
        assert "partly NaN" in out and "1 of 2 values NaN" in out and "S01" in out
        assert self.run(path, "nans", "CellArr", "--by", "subject") == 0
        assert "S01" in capsys.readouterr().out
        assert self.run(path, "nans", "CellArr", "--kind", "bogus") == 1

    def test_show_types_has_the_column(self, path, capsys):
        assert self.run(path, "show", "CellArr", "--types") == 0
        assert "inside cells" in capsys.readouterr().out
