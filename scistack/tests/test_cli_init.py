"""``scistack init``: a thin front end over scidb.project.init_project.

Layout rules are tested where they live (scidb/tests/test_project_init.py);
here only the CLI's own behaviour: arguments, exit codes, output, and the
optional database.
"""

from __future__ import annotations

import pytest

from scistack.__main__ import main


def test_no_args_prints_help_and_returns_1():
    assert main([]) == 1


def test_init_creates_the_project(tmp_path, capsys):
    root = tmp_path / "gait"
    assert main(["init", str(root)]) == 0
    out = capsys.readouterr().out
    assert "created  pyproject.toml" in out
    assert "created  src/gait/scistack_entities.toml" in out
    assert (root / "scistack.toml").is_file()
    assert (root / "src" / "gait" / "__init__.py").is_file()


def test_init_defaults_to_the_current_directory(tmp_path, monkeypatch):
    root = tmp_path / "study"
    root.mkdir()
    monkeypatch.chdir(root)
    assert main(["init"]) == 0
    assert (root / "src" / "study" / "__init__.py").is_file()


def test_init_twice_keeps_everything(tmp_path, capsys):
    root = tmp_path / "gait"
    main(["init", str(root)])
    capsys.readouterr()
    assert main(["init", str(root)]) == 0
    out = capsys.readouterr().out
    assert "created" not in out
    assert out.count("kept") == 5


def test_an_invalid_name_returns_1(tmp_path, capsys):
    assert main(["init", str(tmp_path / "x"), "--name", "Bad Name"]) == 1
    assert "Invalid project name" in capsys.readouterr().err


def test_schema_keys_create_the_database(tmp_path, capsys):
    root = tmp_path / "gait"
    assert main(["init", str(root), "--schema-keys", "subject", "session"]) == 0
    assert (root / "gait.duckdb").is_file()
    assert "schema: subject, session" in capsys.readouterr().out

    from scidb import configure_database
    from scidb.database import clear_current_database

    db = configure_database(root / "gait.duckdb", ["subject", "session"])
    try:
        assert list(db.dataset_schema_keys) == ["subject", "session"]
    finally:
        db.close()
        clear_current_database()


def test_an_existing_database_is_kept(tmp_path, capsys):
    root = tmp_path / "gait"
    main(["init", str(root), "--schema-keys", "subject"])
    before = (root / "gait.duckdb").stat().st_mtime_ns
    capsys.readouterr()
    assert main(["init", str(root), "--schema-keys", "subject"]) == 0
    assert "database already exists" in capsys.readouterr().out
    assert (root / "gait.duckdb").stat().st_mtime_ns == before


def test_no_uv_anywhere():
    """SciStack does not manage environments (2026-10-08)."""
    with pytest.raises(ModuleNotFoundError):
        __import__("scistack.uv_wrapper")
    with pytest.raises(ModuleNotFoundError):
        __import__("scistack.project")
