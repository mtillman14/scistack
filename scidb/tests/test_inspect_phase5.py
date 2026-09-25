"""Phase 5 tests: the write facade (Mutator) + exclude/include/exclusions.

The bright line under test: writes go through a separate facade (Mutator),
reads stay on the read-only Inspector, and the only mutations are the
declarative exclusion flags — wrapping the existing exclusions.py
primitives, never records/invocations/lineage.
"""

import json

import numpy as np
import pytest
from scidb.inspect import Inspector, Mutator, render
from scidb.inspect.cli import main
from scidb.inspect.mutate import lock_errors_mapped

from scidb import BaseVariable, DatabaseLockedError, configure_database, for_each

SCHEMA_KEYS = ["subject", "session"]


class P5Raw(BaseVariable):
    schema_version = 1


class P5Out(BaseVariable):
    schema_version = 1


def double5(signal):
    return signal * 2


def build_p5_db(db_path):
    db = configure_database(db_path, SCHEMA_KEYS)
    # Zero-padded subject on purpose — verbatim round-trip is part of the contract.
    P5Raw.save(np.array([1.0]), subject="01", session="1")
    P5Raw.save(np.array([2.0]), subject="02", session="1")
    for_each(double5, {"signal": P5Raw}, [P5Out], subject=["01", "02"], session=["1"])
    db.close()


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "p5.duckdb"
    build_p5_db(path)
    return path


class TestMutatorRoundTrip:
    def test_exclude_then_visible_then_include(self, db_path):
        with Mutator.open(db_path) as mut:
            result = mut.exclude_schema("sensor slipped", subject="01", session="1")
        assert result.operation == "exclude_schema"
        assert result.target == {"subject": "01", "session": "1"}

        with Inspector.open(db_path) as insp:
            (exc,) = insp.exclusions()
            assert exc.schema == {"subject": "01", "session": "1"}
            assert exc.reason == "sensor slipped"
            assert exc.changed_at

        with Mutator.open(db_path) as mut:
            mut.include_schema("re-reviewed, data fine", subject="01", session="1")
        with Inspector.open(db_path) as insp:
            assert insp.exclusions() == []

    def test_wildcard_exclusion(self, db_path):
        with Mutator.open(db_path) as mut:
            mut.exclude_schema("participant withdrew", subject="02")
        with Inspector.open(db_path) as insp:
            (exc,) = insp.exclusions()
            assert exc.schema == {"subject": "02"}  # session omitted = wildcard

    def test_zero_padded_key_survives_verbatim(self, db_path):
        with Mutator.open(db_path) as mut:
            mut.exclude_schema("test", subject="01")
        with Inspector.open(db_path) as insp:
            (exc,) = insp.exclusions()
            assert exc.schema["subject"] == "01"  # not 1

    def test_primitive_validation_surfaces(self, db_path):
        with Mutator.open(db_path) as mut:
            mut.exclude_schema("x", subject="01")
            with pytest.raises(ValueError, match="already excluded"):
                mut.exclude_schema("y", subject="01")
            with pytest.raises(ValueError, match="no exclusion record"):
                mut.include_schema("z", subject="02")
            with pytest.raises(ValueError):
                mut.exclude_schema("bad key", nonexistent_key="v")

    def test_inspector_stays_read_only(self, db_path):
        # The bright line is structural: Inspector has no write methods and
        # its connection cannot write.
        with Inspector.open(db_path) as insp:
            assert not hasattr(insp, "exclude_schema")
            with pytest.raises(Exception, match="(?i)read.only"):
                insp._db._duck._execute("DELETE FROM __scidb_schema_overrides")


class TestLockContention:
    """The real contention scenario is cross-process (GUI/MATLAB holding the
    file), which can't be reproduced in-process — DuckDB shares the database
    instance for same-process connections. So the mapping is tested against
    the synthetic DuckDB lock error, and the CLI path via a patched open."""

    DUCKDB_LOCK_MSG = (
        'IO Error: Could not set lock on file "p5.duckdb": '
        "Conflicting lock is held in PID 12345"
    )

    def test_lock_error_is_mapped(self, db_path):
        with pytest.raises(DatabaseLockedError, match="locked by another"):
            with lock_errors_mapped(db_path):
                raise RuntimeError(self.DUCKDB_LOCK_MSG)

    def test_non_lock_errors_pass_through(self, db_path):
        with pytest.raises(RuntimeError, match="unrelated"):
            with lock_errors_mapped(db_path):
                raise RuntimeError("unrelated failure")

    def test_cli_write_reports_lock_cleanly(self, db_path, capsys, monkeypatch):
        def locked_open(cls_db_path):
            raise DatabaseLockedError(
                f"{cls_db_path} is locked by another session (a running GUI "
                f"or MATLAB session, or another process). Close it and retry."
            )

        monkeypatch.setattr("scidb.inspect.cli.Mutator.open", locked_open)
        rc = main(["--db", str(db_path), "exclude", "subject=01", "--reason", "r"])
        assert rc == 1
        err = capsys.readouterr().err
        assert "locked by another session" in err
        assert "Traceback" not in err

    def test_inspector_open_maps_lock_too(self, db_path, monkeypatch):
        msg = self.DUCKDB_LOCK_MSG

        def locked_keys(path):
            raise RuntimeError(msg)

        monkeypatch.setattr("sciduckdb.schema_keys_from_db", locked_keys)
        with pytest.raises(DatabaseLockedError):
            Inspector.open(db_path)


class TestCli:
    def test_exclude_list_include_cycle(self, db_path, capsys):
        assert (
            main(
                [
                    "--db",
                    str(db_path),
                    "exclude",
                    "subject=01",
                    "session=1",
                    "--reason",
                    "sensor slipped",
                ]
            )
            == 0
        )
        out = capsys.readouterr().out
        assert "exclude_schema" in out and "sensor slipped" in out

        assert main(["--db", str(db_path), "exclusions"]) == 0
        out = capsys.readouterr().out
        assert "01" in out and "sensor slipped" in out

        assert (
            main(
                [
                    "--db",
                    str(db_path),
                    "include",
                    "subject=01",
                    "session=1",
                    "--reason",
                    "re-reviewed",
                ]
            )
            == 0
        )
        capsys.readouterr()
        assert main(["--db", str(db_path), "exclusions"]) == 0
        assert "(no schema exclusions)" in capsys.readouterr().out

    def test_exclusions_json(self, db_path, capsys):
        assert (
            main(
                [
                    "--db",
                    str(db_path),
                    "exclude",
                    "subject=02",
                    "--reason",
                    "withdrew",
                    "--json",
                ]
            )
            == 0
        )
        payload = json.loads(capsys.readouterr().out)
        assert payload["operation"] == "exclude_schema"
        assert payload["target"] == {"subject": "02"}

        assert main(["--db", str(db_path), "exclusions", "--json"]) == 0
        (row,) = json.loads(capsys.readouterr().out)
        assert row["schema"] == {"subject": "02"}
        assert row["reason"] == "withdrew"

    def test_reason_is_required(self, db_path, capsys):
        with pytest.raises(SystemExit):
            main(["--db", str(db_path), "exclude", "subject=01"])
        assert "--reason" in capsys.readouterr().err

    def test_double_exclude_fails_cleanly(self, db_path, capsys):
        assert (
            main(["--db", str(db_path), "exclude", "subject=01", "--reason", "a"]) == 0
        )
        capsys.readouterr()
        assert (
            main(["--db", str(db_path), "exclude", "subject=01", "--reason", "b"]) == 1
        )
        assert "already excluded" in capsys.readouterr().err

    def test_exclusion_flips_pathinput_state(self, tmp_path, capsys):
        """Cross-check with Phase 3: excluding an un-imported combo removes
        it from the loader's should-run set → red flips back to green."""
        from scidb import scistack
        from scifor import PathInput

        class P5Loaded(BaseVariable):
            schema_version = 1

        @scistack
        def import5(filepath):
            with open(filepath) as fh:
                return float(fh.read().strip())

        db_file = tmp_path / "p5pi.duckdb"
        data = tmp_path / "data"
        for subj in ("01", "02"):
            d = data / f"sub{subj}"
            d.mkdir(parents=True)
            (d / "ses1.txt").write_text("1.0")
        db = configure_database(db_file, SCHEMA_KEYS)
        for_each(
            import5,
            {
                "filepath": PathInput(
                    "sub{subject}/ses{session}.txt", root_folder=str(data), name="sub{subject}/ses{session}.txt"
                )
            },
            [P5Loaded],
            subject=["01", "02"],
            session=["1"],
        )
        db.close()

        # A third file appears, never imported → discovery check goes red.
        (data / "sub03").mkdir()
        (data / "sub03" / "ses1.txt").write_text("3.0")
        with Inspector.open(db_file) as insp:
            (st,) = insp.pathinput_state("import5")
            assert st.state == "red"

        assert (
            main(
                [
                    "--db",
                    str(db_file),
                    "exclude",
                    "subject=03",
                    "--reason",
                    "corrupt acquisition",
                ]
            )
            == 0
        )
        capsys.readouterr()
        with Inspector.open(db_file) as insp:
            (st,) = insp.pathinput_state("import5")
            assert st.state == "green"


class TestRenderers:
    def test_exclusions_table_wildcards(self, db_path):
        with Mutator.open(db_path) as mut:
            mut.exclude_schema("withdrew", subject="02")
            mut.exclude_schema("slipped", subject="01", session="1")
        with Inspector.open(db_path) as insp:
            text = render.render_exclusions(insp.exclusions(), SCHEMA_KEYS)
        assert "withdrew" in text and "slipped" in text
        assert "*" in text  # the wildcard session on the subject-02 row

    def test_mutation_result_render(self, db_path):
        with Mutator.open(db_path) as mut:
            result = mut.exclude_schema("why", subject="01")
        text = render.render_mutation_result(result)
        assert "exclude_schema" in text
        assert "subject=01" in text and "reason: why" in text
