"""Machine-specific path roots (``scifor.project.machine``).

``{rto}/LabMembers/...`` in a shared scistack.toml resolves through each
machine's ``~/.scistack/machine.toml``. Before, an absolute path was right on
one machine and a "File does not exist" load error on every other one, on
every refresh (scistack.log 2026-10-19).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from scifor.project import machine
from scifor.project.config import normalize_config_separators, resolve_config_path


@pytest.fixture
def machine_file(tmp_path, monkeypatch):
    path = tmp_path / "home" / ".scistack" / "machine.toml"
    monkeypatch.setenv(machine.MACHINE_FILE_ENV, str(path))
    machine._cache.clear()
    return path


def _write(path: Path, roots: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(f'{k} = "{v}"\n' for k, v in roots.items())
    path.write_text(f'machine_id = "abc"\n[roots]\n{body}', encoding="utf-8")
    machine._cache.clear()


def test_first_use_creates_the_file_with_a_machine_id(machine_file):
    assert not machine_file.exists()
    config = machine.machine_config()
    assert machine_file.exists()
    assert len(config.machine_id) == 32
    assert config.roots == {}
    # Stable: a second read does not mint a new id.
    machine._cache.clear()
    assert machine.machine_config().machine_id == config.machine_id


def test_a_mapped_root_expands(machine_file, tmp_path):
    base = tmp_path / "mnt" / "rto"
    _write(machine_file, {"rto": base.as_posix()})
    assert machine.expand_roots("{rto}/LabMembers/x") == f"{base.as_posix()}/LabMembers/x"
    assert machine.expand_roots("{rto}") == base.as_posix()
    assert machine.unresolvable_here("{rto}/LabMembers/x") is None


def test_a_trailing_separator_on_the_root_does_not_double(machine_file):
    _write(machine_file, {"rto": "Y:/"})
    assert machine.expand_roots("{rto}/LabMembers") == "Y:/LabMembers"


def test_resolve_config_path_expands(machine_file, tmp_path):
    base = tmp_path / "mnt"
    _write(machine_file, {"rto": base.as_posix()})
    resolved = resolve_config_path(tmp_path / "project", "{rto}/libs")
    assert resolved == Path(base.as_posix()) / "libs"
    assert normalize_config_separators("{rto}/libs/*.m") == f"{base.as_posix()}/libs/*.m"


def test_an_unmapped_root_is_unresolvable_and_unchanged(machine_file):
    _write(machine_file, {"other": "/x"})
    assert machine.expand_roots("{rto}/a") == "{rto}/a"
    reason = machine.unresolvable_here("{rto}/a")
    assert reason and "{rto}" in reason and str(machine_file) in reason


def test_a_brace_not_at_the_start_is_not_a_root(machine_file):
    _write(machine_file, {"rto": "/mnt"})
    assert machine.expand_roots("src/{rto}/a") == "src/{rto}/a"
    assert machine.unresolvable_here("src/{rto}/a") is None


def test_a_plain_value_never_touches_the_machine_file(machine_file):
    """Every config read goes through expand_roots; one with no root must not
    create ~/.scistack/machine.toml as a side effect."""
    assert machine.expand_roots("src/x.py") == "src/x.py"
    assert machine.unresolvable_here("src/x.py") is None
    assert not machine_file.exists()


@pytest.mark.skipif(os.sep == "\\", reason="POSIX host")
def test_a_windows_absolute_path_is_unresolvable_on_posix(machine_file):
    assert machine.unresolvable_here("Y:\\LabMembers\\x")
    assert machine.unresolvable_here("/home/me/x") is None


def test_a_posix_absolute_path_is_unresolvable_on_windows(machine_file, monkeypatch):
    """scistack.log 2026-10-19: /home/mtillman/mnt/rto/... read on Windows as
    y:\\home\\mtillman\\mnt\\rto\\... (relative to the current drive)."""
    monkeypatch.setattr(machine.os, "sep", "\\")
    assert "Windows" in machine.unresolvable_here("/home/mtillman/mnt/rto/x")
    assert machine.unresolvable_here("//server/share/x") is None


def test_portable_form_uses_the_longest_root(machine_file, tmp_path):
    lab = tmp_path / "rto"
    _write(machine_file, {"rto": lab.as_posix(), "me": (lab / "LabMembers" / "MT").as_posix()})
    assert machine.portable_form(lab / "LabMembers" / "MT" / "Repo") == "{me}/Repo"
    assert machine.portable_form(lab / "Other") == "{rto}/Other"
    assert machine.portable_form(lab) == "{rto}"
    assert machine.portable_form(tmp_path / "elsewhere") is None


def test_portable_form_is_case_insensitive_for_windows_paths(machine_file):
    _write(machine_file, {"rto": "Y:/"})
    assert machine.portable_form("y:\\LabMembers\\GitRepos") == "{rto}/LabMembers/GitRepos"


def test_portable_form_does_not_match_a_sibling_prefix(machine_file):
    _write(machine_file, {"rto": "/mnt/rto"})
    assert machine.portable_form("/mnt/rto2/x") is None


def test_portable_then_expand_round_trips(machine_file, tmp_path):
    base = tmp_path / "mnt"
    _write(machine_file, {"rto": base.as_posix()})
    target = base / "Lab" / "Repo"
    assert machine.expand_roots(machine.portable_form(target)) == target.as_posix()


def test_an_edit_takes_effect_without_a_restart(machine_file):
    _write(machine_file, {"rto": "/a"})
    assert machine.expand_roots("{rto}") == "/a"
    machine_file.write_text('machine_id = "abc"\n[roots]\nrto = "/b"\n', encoding="utf-8")
    os.utime(machine_file, ns=(1, 10**18))  # a distinct mtime, even on coarse clocks
    assert machine.expand_roots("{rto}") == "/b"


def test_invalid_toml_means_no_roots_not_a_crash(machine_file, caplog):
    machine_file.parent.mkdir(parents=True)
    machine_file.write_text("[roots\n", encoding="utf-8")
    machine._cache.clear()
    assert machine.machine_config().roots == {}
    assert "not valid TOML" in caplog.text


# ---------------------------------------------------------------------------
# PathInput root_folder (2026-11-21): a shared entities file says
# root_folder = "{lab}/raw"; each machine finds the files under its own root.
# ---------------------------------------------------------------------------


@pytest.fixture
def lab_share(machine_file, tmp_path):
    """A mapped {lab} root holding raw/S1.csv and raw/S2.csv."""
    share = tmp_path / "share"
    (share / "raw").mkdir(parents=True)
    for s in ("S1", "S2"):
        (share / "raw" / f"{s}.csv").write_text(s, encoding="utf-8")
    _write(machine_file, {"lab": share.as_posix()})
    return share


def test_pathinput_root_folder_expands_a_machine_root_on_load(lab_share):
    from scifor import PathInput

    pi = PathInput("{subject}.csv", name="Raw", root_folder="{lab}/raw")
    assert pi.load(subject="S1") == (lab_share / "raw" / "S1.csv").resolve()


def test_pathinput_root_folder_expands_a_machine_root_on_discover(lab_share):
    from scifor import PathInput

    pi = PathInput("{subject}.csv", name="Raw", root_folder="{lab}/raw")
    assert sorted(c["subject"] for c in pi.discover()) == ["S1", "S2"]


def test_pathinput_root_folder_expands_a_machine_root_in_regex_mode(lab_share):
    from scifor import PathInput

    pi = PathInput(r"{subject}\.csv", name="Raw", root_folder="{lab}/raw", regex=True)
    assert pi.load(subject="S2") == (lab_share / "raw" / "S2.csv").resolve()


def test_pathinput_keeps_the_portable_root_folder_it_was_given(lab_share):
    """Only file lookup expands; the spec, the GUI and the entities file see
    what was written, so a save never bakes one machine's drive into it."""
    import json

    from scifor import PathInput

    pi = PathInput("{subject}.csv", name="Raw", root_folder="{lab}/raw")
    assert json.loads(pi.to_spec())["root_folder"] == "{lab}/raw"
    assert pi.root_folder.as_posix() == "{lab}/raw"
    assert PathInput.from_spec(pi.to_spec()).load(subject="S1") == (
        lab_share / "raw" / "S1.csv"
    ).resolve()


def test_pathinput_unmapped_root_finds_nothing_and_says_why(machine_file, caplog):
    import logging

    from scifor import PathInput

    _write(machine_file, {})
    pi = PathInput("{subject}.csv", name="Raw", root_folder="{nas}/raw")
    with caplog.at_level(logging.WARNING, logger="scifor"):
        assert pi.discover() == []
        pi.discover()
    why = [r.getMessage() for r in caplog.records if "{nas}" in r.getMessage()
           and "not mapped" in r.getMessage()]
    assert len(why) == 1, caplog.text  # said once per PathInput, not per walk
    assert "Raw" in why[0] and str(machine_file) in why[0]
