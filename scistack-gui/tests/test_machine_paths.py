"""Machine-specific roots in the GUI's config handling (``scifor.machine``).

scidb.log 2026-10-19: a Linux path in the shared scistack.toml, read on
Windows as ``y:\\home\\mtillman\\mnt\\rto\\...``, was a "File does not exist"
load error on every refresh (22x). Now such entries are skipped, reported
once by a startup notice, and the Paths popup records ``{root}/rest``.

conftest's autouse ``_pin_machine_file`` points the machine file at tmp.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from scifor import machine
from scistack_gui import startup
from scistack_gui.config import _normalize, add_path, load_config, remove_path, tomllib


def _roots(path: Path, roots: dict[str, str]) -> None:
    body = "".join(f'{k} = "{v}"\n' for k, v in roots.items())
    path.write_text(f'machine_id = "m1"\n[roots]\n{body}', encoding="utf-8")
    machine._cache.clear()


@pytest.fixture(autouse=True)
def _clear_startup_errors():
    startup.clear_startup_errors()
    yield


def test_a_rooted_module_entry_loads_through_the_root(tmp_path, _pin_machine_file):
    lab = tmp_path / "lab"
    (lab / "repo").mkdir(parents=True)
    (lab / "repo" / "steps.py").write_text("")
    _roots(_pin_machine_file, {"rto": lab.as_posix()})
    (tmp_path / "scistack.toml").write_text('modules = ["{rto}/repo"]\n[matlab]\nsources = ["{rto}/repo"]\n')

    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert [p.name for p in config.modules] == ["steps.py"]


def test_an_unmapped_root_is_skipped_not_a_missing_file(tmp_path, caplog):
    (tmp_path / "scistack.toml").write_text(
        'modules = ["{rto}/repo"]\n[matlab]\nsources = ["{rto}/repo"]\n'
    )
    with caplog.at_level(logging.DEBUG):
        config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert config.modules == []
    assert config.matlab_sources == []
    assert "skipped" in caplog.text and "{rto}" in caplog.text


def test_the_startup_notice_names_each_unresolvable_path_once(tmp_path, _pin_machine_file):
    _roots(_pin_machine_file, {"other": "/x"})
    (tmp_path / "scistack.toml").write_text(
        'modules = ["{rto}/a", "src"]\n[matlab]\nsources = ["{rto}/a"]\n'
    )
    config = load_config(tmp_path, tmp_path / "dummy.duckdb")

    err = startup.check_machine_config_paths(config)

    assert err is not None and err.kind == "machine_config_paths" and not err.blocking
    assert err.details.count("{rto}/a\n") == 1  # one line per value, not per key
    assert str(_pin_machine_file) in err.details
    assert "  src" not in err.details.splitlines()  # resolvable: not reported


def test_no_notice_when_everything_resolves(tmp_path, _pin_machine_file):
    _roots(_pin_machine_file, {"rto": tmp_path.as_posix()})
    (tmp_path / "scistack.toml").write_text('modules = ["{rto}", "src"]\n')
    config = load_config(tmp_path, tmp_path / "dummy.duckdb")
    assert startup.check_machine_config_paths(config) is None


def test_check_config_paths_is_what_both_startup_routes_call():
    """server.main duplicates bootstrap's startup inline (drift trap):
    both must call the one aggregate, so a new check reaches both."""
    import inspect

    from scistack_gui import bootstrap, server

    for module in (bootstrap, server):
        source = inspect.getsource(module)
        assert "check_config_paths(" in source
        assert "check_windows_config_paths(" not in source


def test_add_path_records_the_root_form(tmp_path, _pin_machine_file):
    lab = tmp_path / "lab"
    repo = lab / "LabMembers" / "repo"
    repo.mkdir(parents=True)
    _roots(_pin_machine_file, {"rto": lab.as_posix()})
    (tmp_path / "scistack.toml").write_text("modules = []\n")
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")

    written = add_path(db_path, repo)
    with open(written, "rb") as f:
        data = tomllib.load(f)
    assert data["modules"] == ["{rto}/LabMembers/repo"]
    assert data["matlab"]["sources"] == ["{rto}/LabMembers/repo"]

    # Same directory again: recognised through the root, not duplicated.
    add_path(db_path, repo)
    with open(written, "rb") as f:
        assert tomllib.load(f)["modules"] == ["{rto}/LabMembers/repo"]

    # And removable by its absolute path.
    remove_path(db_path, repo)
    with open(written, "rb") as f:
        assert tomllib.load(f)["modules"] == []


def test_add_path_outside_every_root_stays_absolute(tmp_path, _pin_machine_file):
    _roots(_pin_machine_file, {"rto": (tmp_path / "lab").as_posix()})
    repo = tmp_path / "elsewhere"
    repo.mkdir()
    (tmp_path / "scistack.toml").write_text("modules = []\n")
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")

    written = add_path(db_path, repo)
    with open(written, "rb") as f:
        assert tomllib.load(f)["modules"] == [str(_normalize(repo))]


def test_the_paths_popup_gets_the_full_path_of_a_rooted_entry(tmp_path, _pin_machine_file):
    """User request 2026-10-19: the popup shows the full path on this
    machine, not the raw `{rto}/...`."""
    from scistack_gui.config import describe_managed_paths

    lab = tmp_path / "lab"
    (lab / "repo").mkdir(parents=True)
    _roots(_pin_machine_file, {"rto": lab.as_posix()})
    (tmp_path / "scistack.toml").write_text('modules = ["{rto}/repo", "{gone}/x"]\n')

    details = describe_managed_paths(tmp_path / "proj.duckdb")["managed_path_details"]

    rooted, unmapped = details
    assert rooted["entry"] == "{rto}/repo" and rooted["root"] == "rto"
    assert rooted["resolved"] == str(_normalize(lab / "repo"))
    assert rooted["problem"] is None
    assert unmapped["root"] == "gone" and unmapped["resolved"] is None
    assert "{gone}" in unmapped["problem"]


def test_remove_matches_an_unmapped_entry_as_written(tmp_path):
    """The popup sends the entry as written. An unmapped `{root}` entry names
    no directory here, so the exact entry is its only identity -- before,
    removing it silently matched nothing."""
    (tmp_path / "scistack.toml").write_text(
        'modules = ["{gone}/x", "keep"]\n[matlab]\nsources = ["{gone}/x"]\n'
    )
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")

    written = remove_path(db_path, "{gone}/x")
    with open(written, "rb") as f:
        data = tomllib.load(f)
    assert data["modules"] == ["keep"]
    # scidb.config_file omits an emptied [matlab] table entirely.
    assert data.get("matlab", {}).get("sources", []) == []


def test_remove_matches_a_mapped_entry_as_written(tmp_path, _pin_machine_file):
    lab = tmp_path / "lab"
    (lab / "repo").mkdir(parents=True)
    _roots(_pin_machine_file, {"rto": lab.as_posix()})
    (tmp_path / "scistack.toml").write_text('modules = ["{rto}/repo"]\n')
    db_path = tmp_path / "proj.duckdb"
    db_path.write_text("")

    written = remove_path(db_path, "{rto}/repo")
    with open(written, "rb") as f:
        assert tomllib.load(f)["modules"] == []
