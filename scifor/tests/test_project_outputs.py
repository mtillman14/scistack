"""The project output folder: ``[outputs] folder`` (scifor.project.outputs).

GitHub issue #19: many saved plots share one root folder. It is stated once
in scistack.toml, and each ``PathOutput`` template stays relative. Unset, a
relative path stays relative (the old behaviour, exactly).
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scifor import ColumnSelection, PathOutput, for_each, set_schema
from scifor.project import config_file, machine, outputs, schema_order
from scifor.project.root import clear_project_root, set_project_root


@pytest.fixture(autouse=True)
def _project(tmp_path, monkeypatch):
    """A pinned project root with a machine file of its own, caches clean."""
    monkeypatch.setenv(machine.MACHINE_FILE_ENV, str(tmp_path / "home" / ".scistack" / "machine.toml"))
    machine._cache.clear()
    outputs.clear_cache()
    schema_order.clear_cache()
    set_project_root(tmp_path)
    yield tmp_path
    clear_project_root()
    outputs.clear_cache()
    schema_order.clear_cache()
    machine._cache.clear()


def _config(root: Path, body: str) -> Path:
    path = root / "scistack.toml"
    path.write_text(f"modules = []\n{body}", encoding="utf-8")
    outputs.clear_cache()
    schema_order.clear_cache()
    return path


# --- the setting ------------------------------------------------------------


def test_unset_means_no_folder_and_relative_paths_stay_relative(_project):
    _config(_project, "")
    assert outputs.output_folder() is None
    assert PathOutput("plots/{subject}.png").resolve({"subject": 1}) == "plots/1.png"


def test_no_config_at_all_means_no_folder(_project):
    assert outputs.output_folder() is None
    assert PathOutput("a.png").resolve({}) == "a.png"


def test_a_relative_folder_is_under_the_project_root(_project):
    _config(_project, '[outputs]\nfolder = "outputs"\n')
    assert outputs.output_folder() == _project / "outputs"
    assert PathOutput("plots/{subject}.png").resolve({"subject": 1}) == str(
        _project / "outputs" / "plots" / "1.png"
    )


def test_an_absolute_folder_is_used_as_is(_project, tmp_path_factory):
    elsewhere = tmp_path_factory.mktemp("results")
    _config(_project, f'[outputs]\nfolder = "{elsewhere.as_posix()}"\n')
    assert PathOutput("x_{subject}.pdf").resolve({"subject": "S1"}) == str(elsewhere / "x_S1.pdf")


def test_a_machine_root_expands(_project):
    base = _project / "mnt" / "results"
    machine_path = Path(os.environ[machine.MACHINE_FILE_ENV])
    machine_path.parent.mkdir(parents=True, exist_ok=True)
    machine_path.write_text(f'machine_id = "abc"\n[roots]\nresults = "{base.as_posix()}"\n', encoding="utf-8")
    machine._cache.clear()
    _config(_project, '[outputs]\nfolder = "{results}/aim2"\n')
    assert outputs.output_folder() == Path(f"{base.as_posix()}/aim2")


def test_an_absolute_template_is_never_moved(_project, tmp_path_factory):
    _config(_project, '[outputs]\nfolder = "outputs"\n')
    absolute = tmp_path_factory.mktemp("abs") / "{subject}.png"
    assert PathOutput(str(absolute)).resolve({"subject": 2}) == str(absolute).replace("{subject}", "2")


def test_a_path_template_keeps_its_type(_project):
    _config(_project, '[outputs]\nfolder = "outputs"\n')
    resolved = PathOutput(Path("figs") / "{subject}.png").resolve({"subject": 1})
    assert isinstance(resolved, Path)
    assert resolved == _project / "outputs" / "figs" / "1.png"


def test_a_blank_or_wrong_type_folder_is_ignored_with_a_warning(_project, caplog):
    _config(_project, '[outputs]\nfolder = ""\n')
    assert outputs.output_folder() is None
    _config(_project, "[outputs]\nfolder = 3\n")
    with caplog.at_level("WARNING"):
        assert outputs.output_folder() is None


def test_an_edit_is_read_without_a_restart(_project):
    config = _config(_project, '[outputs]\nfolder = "a"\n')
    assert outputs.output_folder() == _project / "a"
    config.write_text('modules = []\n[outputs]\nfolder = "b"\n', encoding="utf-8")
    stat = config.stat()
    os.utime(config, (stat.st_atime + 5, stat.st_mtime + 5))
    schema_order.clear_cache()  # the LOCATION cache, not the content one
    assert outputs.output_folder() == _project / "b"


# --- the writer keeps it -------------------------------------------------------


def test_the_config_writer_renders_an_outputs_table_that_reads_back(_project):
    path = _project / "scistack.toml"
    config_file.write(path, {"modules": [], "outputs": {"folder": "outputs"}, "colors": {}})
    text = path.read_text(encoding="utf-8")
    assert "[outputs]\nfolder = \"outputs\"" in text
    assert outputs.output_folder() == _project / "outputs"


# --- through for_each ----------------------------------------------------------


def test_for_each_hands_the_function_a_path_under_the_folder(_project):
    _config(_project, '[outputs]\nfolder = "outputs"\n')
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "velocity": [3.0, 4.0]})
    received = []

    def fn(velocity, filename):
        received.append(filename)
        return float(np.max(velocity))

    for_each(
        fn,
        inputs={
            "velocity": ColumnSelection(df, ["velocity"]),
            "filename": PathOutput("plots/subject_{subject}.pdf"),
        },
        subject=[1, 2],
    )
    assert received == [
        str(_project / "outputs" / "plots" / "subject_1.pdf"),
        str(_project / "outputs" / "plots" / "subject_2.pdf"),
    ]
