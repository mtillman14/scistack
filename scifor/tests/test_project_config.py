"""``scifor.project.config.read_project_config``: the meaning of each section.

The GUI's code registry is built from this typed view, so a wrong default
or a value of the wrong type accepted here changes what every project
discovers (plan P3 stage 4, design review 07 G3).
"""

import pytest

from scifor.project.config import (
    MatlabSection,
    ProjectConfig,
    load_config_file,
    read_project_config,
)


def _write(tmp_path, text: str):
    path = tmp_path / "scistack.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_empty_file_is_all_defaults(tmp_path):
    config = read_project_config(_write(tmp_path, ""))
    assert config == ProjectConfig(path=tmp_path / "scistack.toml")
    assert config.root == tmp_path
    assert config.auto_discover is True
    assert config.matlab == MatlabSection()


def test_sections_are_typed_and_kept_as_written(tmp_path):
    config = read_project_config(
        _write(
            tmp_path,
            'modules = ["pipelines/*.py", "src"]\n'
            'packages = ["gaitlib"]\n'
            "auto_discover = false\n"
            'glue_dir = "src/scistack_glue"\n'
            "[matlab]\n"
            'sources = ["matlab"]\n'
            'variable_dir = "matlab/vars"\n',
        )
    )
    assert config.modules == ("pipelines/*.py", "src")
    assert config.packages == ("gaitlib",)
    assert config.auto_discover is False
    assert config.glue_dir == "src/scistack_glue"
    assert config.functions_dir is None
    assert config.matlab.sources == ("matlab",)
    assert config.matlab.variable_dir == "matlab/vars"
    assert config.section["packages"] == ["gaitlib"]


def test_resolve_reads_a_windows_written_entry(tmp_path):
    config = read_project_config(_write(tmp_path, 'modules = ["src\\\\pipe.py"]\n'))
    assert config.resolve(config.modules[0]) == tmp_path / "src" / "pipe.py"


@pytest.mark.parametrize(
    "text,message",
    [
        ('modules = "src"\n', "modules must be a list"),
        ('packages = "gaitlib"\n', "packages must be a list"),
        ('auto_discover = "yes"\n', "auto_discover must be true or false"),
        ('[matlab]\nsources = "m"\n', "matlab.sources must be a list"),
        ('matlab = 3\n', r"\[matlab\] must be a table"),
    ],
)
def test_wrong_type_names_the_key(tmp_path, text, message):
    with pytest.raises(ValueError, match=message):
        read_project_config(_write(tmp_path, text))


def test_strict_read_raises_on_a_broken_file(tmp_path):
    """A writer must never read a broken config as ``{}`` and overwrite it."""
    path = _write(tmp_path, "modules = [\n")
    with pytest.raises(ValueError):  # TOMLDecodeError is a ValueError
        load_config_file(path)


def test_read_is_logged(tmp_path, caplog):
    with caplog.at_level("INFO", logger="scifor.project.config"):
        read_project_config(_write(tmp_path, 'modules = ["a.py"]\n'))
    assert "[config] read" in caplog.text
