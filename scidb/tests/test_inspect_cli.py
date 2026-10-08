"""Tests for the ``scidb`` CLI (Phase 1: status, vars, schema, show).

Machine-readable assertions go through ``--json`` (parse stdout) to avoid
brittle human-format matching; human renders are only smoke-checked.
"""

import argparse
import json
from pathlib import Path

import pytest
from scidb.inspect.cli import (
    CLIError,
    add_db_subparser,
    main,
    resolve_db_path,
)
from test_inspect_api import build_populated_db


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("inspect_cli") / "insp.duckdb"
    build_populated_db(path)
    return path


def run_json(capsys, argv):
    rc = main(argv)
    out = capsys.readouterr().out
    assert rc == 0, out
    return json.loads(out)


class TestCommands:
    def test_status_json(self, db_path, capsys):
        payload = run_json(capsys, ["--db", str(db_path), "status", "--json"])
        assert payload["n_records"] == 7
        assert payload["n_runs"] == 2
        assert payload["schema_keys"] == ["subject", "session"]

    def test_status_human(self, db_path, capsys):
        assert main(["--db", str(db_path), "status"]) == 0
        out = capsys.readouterr().out
        assert "records" in out and "7" in out

    def test_vars_json(self, db_path, capsys):
        payload = run_json(capsys, ["--db", str(db_path), "vars", "--json"])
        by_name = {v["name"]: v for v in payload}
        assert by_name["InspFiltered"]["record_count"] == 4
        assert by_name["InspFiltered"]["variant_count"] == 2

    def test_vars_detail_json(self, db_path, capsys):
        payload = run_json(
            capsys, ["--db", str(db_path), "vars", "InspFiltered", "--json"]
        )
        assert payload["record_count"] == 4
        assert payload["data_columns"]

    def test_vars_unknown_type_fails(self, db_path, capsys):
        assert main(["--db", str(db_path), "vars", "NoSuchVar"]) == 1
        assert "Error" in capsys.readouterr().err

    def test_schema_tree_human(self, db_path, capsys):
        assert main(["--db", str(db_path), "schema", "--tree"]) == 0
        out = capsys.readouterr().out
        assert "subject=S01" in out and "session=1" in out

    def test_schema_json(self, db_path, capsys):
        payload = run_json(capsys, ["--db", str(db_path), "schema", "--json"])
        assert {r["value"] for r in payload["roots"]} >= {"S01", "S02"}

    def test_show_json(self, db_path, capsys):
        payload = run_json(
            capsys,
            ["--db", str(db_path), "show", "InspFiltered", "subject=S01", "--json"],
        )
        assert len(payload) == 2  # two coexisting low_hz variants
        assert all(r["schema"]["subject"] == "S01" for r in payload)

    def test_show_versions_json(self, db_path, capsys):
        payload = run_json(
            capsys,
            [
                "--db",
                str(db_path),
                "show",
                "InspRaw",
                "subject=S01",
                "--versions",
                "--json",
            ],
        )
        assert len(payload) == 2  # re-save trail

    def test_show_bad_kv_fails(self, db_path, capsys):
        assert main(["--db", str(db_path), "show", "InspRaw", "subject"]) == 1
        assert "key=value" in capsys.readouterr().err

    def test_global_flags_accepted_before_and_after_subcommand(self, db_path, capsys):
        # Regression: global flags must parse in either position.
        before = run_json(capsys, ["--db", str(db_path), "--json", "status"])
        after = run_json(capsys, ["status", "--db", str(db_path), "--json"])
        assert before == after

    def test_no_command_prints_help(self, capsys):
        assert main([]) == 1

    def test_missing_db_file_fails(self, tmp_path, capsys):
        assert main(["--db", str(tmp_path / "nope.duckdb"), "status"]) == 1
        assert "Error" in capsys.readouterr().err


class TestDiscovery:
    def test_flag_wins(self, db_path, monkeypatch):
        monkeypatch.setenv("SCIDB_DATABASE", "/elsewhere.duckdb")
        path, source = resolve_db_path(str(db_path))
        assert path == str(db_path)
        assert source == "--db flag"

    def test_env_var(self, db_path, tmp_path, monkeypatch):
        monkeypatch.setenv("SCIDB_DATABASE", str(db_path))
        path, source = resolve_db_path(None, cwd=tmp_path)
        assert path == str(db_path)
        assert "SCIDB_DATABASE" in source

    def test_scistack_toml_db_key(self, db_path, tmp_path, monkeypatch):
        monkeypatch.delenv("SCIDB_DATABASE", raising=False)
        (tmp_path / "scistack.toml").write_text(f'db = "{db_path.as_posix()}"\n')
        path, source = resolve_db_path(None, cwd=tmp_path)
        assert Path(path) == db_path
        assert "scistack.toml" in source

    def test_scistack_toml_relative_db_key(self, db_path, tmp_path, monkeypatch):
        monkeypatch.delenv("SCIDB_DATABASE", raising=False)
        (tmp_path / "scistack.toml").write_text('db = "data/x.duckdb"\n')
        path, _ = resolve_db_path(None, cwd=tmp_path)
        assert Path(path) == (tmp_path / "data" / "x.duckdb").resolve()

    def test_a_pyproject_db_key_is_never_read(self, db_path, tmp_path, monkeypatch):
        """2026-10-08: pyproject.toml is packaging only."""
        monkeypatch.delenv("SCIDB_DATABASE", raising=False)
        (tmp_path / "pyproject.toml").write_text(
            f'[tool.scistack]\ndb = "{db_path.as_posix()}"\n'
        )
        with pytest.raises(CLIError):
            resolve_db_path(None, cwd=tmp_path)

    def test_single_duckdb_in_cwd(self, db_path, monkeypatch):
        monkeypatch.delenv("SCIDB_DATABASE", raising=False)
        path, source = resolve_db_path(None, cwd=db_path.parent)
        assert path == str(db_path)
        assert "cwd" in source

    def test_multiple_duckdb_files_error(self, db_path, monkeypatch):
        monkeypatch.delenv("SCIDB_DATABASE", raising=False)
        (db_path.parent / "second.duckdb").touch()
        try:
            with pytest.raises(CLIError, match="Multiple"):
                resolve_db_path(None, cwd=db_path.parent)
        finally:
            (db_path.parent / "second.duckdb").unlink()

    def test_nothing_found_error(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SCIDB_DATABASE", raising=False)
        with pytest.raises(CLIError, match="No database found"):
            resolve_db_path(None, cwd=tmp_path)


class TestScistackAlias:
    """The `scistack db …` mount reuses this wiring via add_db_subparser."""

    def test_dispatch_through_alias(self, db_path, capsys):
        parser = argparse.ArgumentParser(prog="scistack")
        sub = parser.add_subparsers(dest="command")
        add_db_subparser(sub)
        args = parser.parse_args(["db", "--db", str(db_path), "status", "--json"])
        assert args._dispatch(args) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["n_runs"] == 2
