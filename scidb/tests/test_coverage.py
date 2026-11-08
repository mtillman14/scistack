"""Coverage and follow (scidb.provenance.coverage).

Both only summarise ``expectation()`` per variable, so these tests check the
summaries line up with the statuses ``test_expectation.py`` pins, the
pipeline order, the funnel, and where a followed location is lost.
"""

import json

import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scifor.project import schema_order
from scidb.provenance import expectation as ex
from scidb.provenance.coverage import EMPTY, coverage, follow
from scidb.provenance.query import variable_types_in_pipeline_order

SCHEMA = ["subject", "session", "trial"]


class CovRaw(BaseVariable):
    pass


class CovFilt(BaseVariable):
    pass


class CovSubject(BaseVariable):
    pass


def cov_double(signal):
    return signal * 2


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    schema_order.clear_cache()
    _scifor.set_schema([])
    yield
    _scifor.set_schema([])
    schema_order.clear_cache()


def populate():
    """CovRaw at S01-S03 (S03 has only trial 1); CovFilt computed for S01 and
    S03 only, so S02 is lost between them; CovSubject only at S04."""
    for subject, trials in (("S01", "12"), ("S02", "12"), ("S03", "1")):
        for trial in trials:
            CovRaw.save(1.0, subject=subject, session="BL", trial=trial)
    for_each(
        cov_double, {"signal": CovRaw}, [CovFilt],
        subject=["S01", "S03"], session=["BL"], trial=["1", "2"],
    )
    CovSubject.save(1.0, subject="S04")


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "cov.duckdb", SCHEMA)
    populate()
    yield database
    database.close()


def test_pipeline_order_puts_sources_first(db):
    assert variable_types_in_pipeline_order(db._duck) == ["CovRaw", "CovFilt", "CovSubject"]


class TestCoverage:
    def test_cells_summarise_expectation(self, db):
        cov = coverage(db, ["subject"])
        assert cov.variables == ["CovRaw", "CovFilt", "CovSubject"]
        assert cov.values == [("S01",), ("S02",), ("S03",), ("S04",)]
        assert cov.cell("CovRaw", ("S01",)).tone == ex.OK
        lost = cov.cell("CovFilt", ("S02",))
        assert lost.tone == ex.PROBLEM and lost.problems == 2 and lost.present == 0
        assert cov.cell("CovSubject", ("S04",)).present == 1

    def test_cells_agree_with_the_variable_view(self, db):
        cov = coverage(db, ["subject"])
        own = dict(ex.expectation(db, CovFilt).by("subject"))
        for value in cov.values:
            assert cov.cell("CovFilt", value).counts == own.get(value[0], ex._zero_counts())

    def test_funnel(self, db):
        assert coverage(db, ["subject"]).funnel() == [
            ("CovRaw", 3, 4), ("CovFilt", 2, 4), ("CovSubject", 1, 4),
        ]

    def test_a_variable_without_the_key_is_skipped_with_a_reason(self, db):
        cov = coverage(db, ["trial"])
        assert "CovSubject" in cov.skipped and "without" in cov.skipped["CovSubject"]

    def test_bad_keys_refused(self, db):
        with pytest.raises(ValueError, match="Choose schema key"):
            coverage(db, [])
        with pytest.raises(ValueError, match="unknown"):
            coverage(db, ["visit"])

    def test_empty_cell(self):
        from scidb.provenance.coverage import CoverageCell

        assert CoverageCell().tone == EMPTY

    def test_json(self, db):
        payload = json.loads(json.dumps(coverage(db, ["subject"]).to_dict()))
        assert payload["funnel"][1] == {"variable": "CovFilt", "with_data": 2, "expected": 4}


class TestFollow:
    def test_lost_between_raw_and_filtered(self, db):
        fol = follow(db, subject="S02", trial="1")
        steps = {s.variable: s for s in fol.steps}
        assert steps["CovRaw"].has_data
        assert not steps["CovFilt"].has_data
        assert "other variables have data here" in steps["CovFilt"].summary
        assert fol.lost_at == ["CovFilt"]
        assert steps["CovSubject"].ignored_keys == ["trial"]

    def test_a_location_that_is_fine_everywhere(self, db):
        fol = follow(db, subject="S01", trial="1")
        assert fol.lost_at == []

    def test_a_coarse_location_counts_rows(self, db):
        step = {s.variable: s for s in follow(db, subject="S01").steps}["CovRaw"]
        assert step.counts[ex.PRESENT] == 2
        assert "2 present, as expected" in step.summary

    def test_bad_location_refused(self, db):
        with pytest.raises(ValueError, match="Unknown schema key"):
            follow(db, visit="1")
        with pytest.raises(ValueError, match="Name a location"):
            follow(db)


class TestCli:
    @pytest.fixture
    def path(self, tmp_path):
        p = tmp_path / "cli.duckdb"
        database = configure_database(p, SCHEMA)
        populate()
        database.close()
        return p

    def run(self, path, *argv):
        from scidb.inspect.cli import main

        return main(["--db", str(path), *argv])

    def test_coverage_table_and_funnel(self, path, capsys):
        assert self.run(path, "coverage", "--by", "subject", "--no-color") == 0
        out = capsys.readouterr().out
        assert "with data" in out and "2/4" in out
        assert self.run(path, "coverage", "--by", "subject", "--funnel") == 0
        assert "CovFilt" in capsys.readouterr().out

    def test_follow(self, path, capsys):
        assert self.run(path, "follow", "subject=S02", "trial=1", "--no-color") == 0
        out = capsys.readouterr().out
        assert "first missing although an input has it: CovFilt" in out
        assert self.run(path, "follow", "subject=S02", "--json") == 0
        assert json.loads(capsys.readouterr().out)["lost_at"] == ["CovFilt"]
