"""The "no values found for '<key>' in database, 0 iterations" WARN must not
fire when PathInput discovery is about to supply those values from disk.

Found by reading examples/vo2max/scidb.log: a first run against a fresh
database logged

    WARN  no values found for 'subject' in database, 0 iterations
    WARN  no values found for 'session' in database, 0 iterations
    INFO  for_each(pandas.read_csv) - 4 iterations: subject=3 values [...]

The warning fired at Step 2 (resolve empty lists from the DB) while Step 3
(PathInput filesystem discovery) — the normal source of those values on a
first run — had not run yet. The claim "0 iterations" was then contradicted
8ms later by the real iteration count.

The warning now fires after discovery, only for keys NOTHING could fill.
"""

import logging

import pytest
import scifor as _scifor

from scidb import configure_database, for_each


def read_content(filepath):
    from pathlib import Path

    return Path(str(filepath)).read_text().strip()


@pytest.fixture
def empty_db(tmp_path):
    """A fresh database — no records, so the DB can fill no schema key."""
    db = configure_database(tmp_path / "test.duckdb", ["subject", "session"])
    yield db
    _scifor.set_schema([])
    db.close()


@pytest.fixture
def data_tree(tmp_path):
    """Four files over three subjects — SS02 and SS03 have no session 02,
    mirroring examples/vo2max/data (a non-rectangular tree)."""
    root = tmp_path / "data"
    for subject, session in [
        ("SS01", "01"),
        ("SS01", "02"),
        ("SS02", "01"),
        ("SS03", "01"),
    ]:
        d = root / subject
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{subject}_{session}_CPET.csv").write_text(f"{subject}-{session}")
    return root


def _warnings(caplog):
    return [r.message for r in caplog.records if r.levelno >= logging.WARNING]


class TestDiscoveryFillsBeforeWarning:
    def test_no_false_warning_when_discovery_supplies_values(
        self, empty_db, data_tree, caplog
    ):
        pi = _scifor.PathInput(
            "{subject}/{subject}_{session}_CPET.csv", root_folder=str(data_tree), name="{subject}/{subject}_{session}_CPET.csv"
        )

        with caplog.at_level(logging.DEBUG, logger="scidb"):
            result = for_each(
                read_content, {"filepath": pi}, [], save=False, subject=[], session=[]
            )

        assert len(result) == 4
        assert not [w for w in _warnings(caplog) if "no values found" in w], (
            "Discovery supplied every schema key from disk; the database being "
            "empty is not a problem worth warning about."
        )

    def test_reports_what_discovery_filled(self, empty_db, data_tree, caplog):
        """The replacement for the false warning: say where the values came
        from, so an empty database is visibly not the end of the story."""
        pi = _scifor.PathInput(
            "{subject}/{subject}_{session}_CPET.csv", root_folder=str(data_tree), name="{subject}/{subject}_{session}_CPET.csv"
        )

        with caplog.at_level(logging.INFO, logger="scidb"):
            for_each(
                read_content, {"filepath": pi}, [], save=False, subject=[], session=[]
            )

        filled = [
            r.message
            for r in caplog.records
            if "PathInput discovery filled" in r.message
        ]
        assert len(filled) == 1, f"expected one discovery summary, got {filled}"
        assert "'subject'=3 value(s)" in filled[0]
        assert "'session'=2 value(s)" in filled[0]


class TestWarningStillFiresWhenNothingCanFill:
    def test_empty_db_no_pathinput_still_warns(self, empty_db, caplog):
        """The real signal must survive: nothing on disk to fall back to, so
        an empty database really does mean zero iterations."""
        with caplog.at_level(logging.DEBUG, logger="scidb"):
            result = for_each(
                lambda: 1, {}, [], save=False, subject=[], session=[]
            )

        assert len(result) == 0
        warned = [w for w in _warnings(caplog) if "no values found" in w]
        assert any("'subject'" in w for w in warned), warned
        assert any("'session'" in w for w in warned), warned

    def test_pathinput_matching_nothing_warns(self, empty_db, tmp_path, caplog):
        """A template that discovers nothing is a genuine 0-iteration run."""
        empty_root = tmp_path / "nothing"
        empty_root.mkdir()
        pi = _scifor.PathInput(
            "{subject}/{subject}_{session}_CPET.csv", root_folder=str(empty_root), name="{subject}/{subject}_{session}_CPET.csv"
        )

        with caplog.at_level(logging.DEBUG, logger="scidb"):
            result = for_each(
                read_content, {"filepath": pi}, [], save=False, subject=[], session=[]
            )

        assert len(result) == 0
        assert [w for w in _warnings(caplog) if "no values found" in w]


class TestIterationBannerReportsPruning:
    def test_banner_states_filtered_combinations(
        self, empty_db, data_tree, caplog
    ):
        """3 subjects x 2 sessions reads as 6, but only 4 files exist. The
        banner must say the other 2 were filtered out rather than leaving the
        reader to spot an apparent arithmetic error."""
        pi = _scifor.PathInput(
            "{subject}/{subject}_{session}_CPET.csv", root_folder=str(data_tree), name="{subject}/{subject}_{session}_CPET.csv"
        )

        with caplog.at_level(logging.INFO, logger="scifor"):
            for_each(
                read_content, {"filepath": pi}, [], save=False, subject=[], session=[]
            )

        banner = [r.message for r in caplog.records if "4 iterations" in r.message]
        assert len(banner) == 1, f"expected one run banner, got {banner}"
        assert "of 6 possible combination(s)" in banner[0]
        assert "2 filtered out before iteration" in banner[0]
