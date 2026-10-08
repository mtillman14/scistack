"""scidb.config_file: the one renderer/writer of scistack.toml.

Every writer (init, every GUI edit) hands it the WHOLE config, so the
properties that matter are: what was read comes back after a write, keys it
has no fixed form for are kept rather than deleted, and the tables land after
every top-level key.
"""

from __future__ import annotations

import logging

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

from scidb import config_file


def _round_trip(section: dict) -> dict:
    return tomllib.loads(config_file.render(section))


class TestRoundTrip:
    def test_empty_config_renders_modules_only(self):
        text = config_file.render({})
        assert text.startswith("# SciStack project configuration")
        assert tomllib.loads(text) == {"modules": []}

    def test_every_known_key_survives(self):
        section = {
            "modules": ["a.py", "pipelines/*.py"],
            "entities_file": "src/pkg/scistack_entities.toml",
            "glue_dir": "src/scistack_glue",
            "variable_file": "src/vars.py",
            "packages": ["lab_utils"],
            "auto_discover": False,
            "db": "data/study.duckdb",
            "matlab": {
                "functions": ["m/f.m"],
                "variables": ["m/types/*.m"],
                "sources": ["m"],
                "variable_dir": "m/types",
                "entities_file": "scistack_entities.m",
            },
            "schema_keys": {"session": ["BL", "POST"]},
            "aliases": {"session": {"name": "Session"}},
            "colors": {"session": {"BL": "#111111"}},
        }
        assert _round_trip(section) == section

    def test_an_empty_entities_file_is_the_opt_out_and_is_kept(self):
        assert _round_trip({"entities_file": ""})["entities_file"] == ""

    def test_defaults_are_not_written(self):
        out = _round_trip({"auto_discover": True, "packages": []})
        assert "auto_discover" not in out and "packages" not in out


class TestUnknownKeysSurvive:
    """A key this module has no fixed form for must come back, or the next
    GUI click silently deletes it."""

    def test_unknown_top_level_scalars_and_lists(self):
        section = {"modules": [], "my_flag": True, "n": 3, "names": ["a", "b"]}
        assert _round_trip(section) == section

    def test_unknown_nested_table_is_kept_as_an_inline_table(self):
        section = {"modules": [], "custom": {"x": 1, "deep": {"y": ["z"]}}}
        assert _round_trip(section) == section

    def test_unknown_matlab_key_is_kept(self):
        section = {"modules": [], "matlab": {"sources": ["m"], "extra": "v"}}
        assert _round_trip(section) == section

    def test_unknown_keys_come_before_any_table(self):
        """A top-level key below a [table] header would land inside it."""
        section = {"modules": [], "zzz": 1, "schema_keys": {"s": ["a"]}}
        out = _round_trip(section)
        assert out["zzz"] == 1
        assert out["schema_keys"] == {"s": ["a"]}

    def test_an_unrenderable_value_is_reported_not_silently_dropped(self, caplog):
        with caplog.at_level(logging.WARNING, logger="scidb"):
            out = _round_trip({"modules": [], "bad": object()})
        assert "bad" not in out
        assert "bad" in caplog.text and "NOT written" in caplog.text


class TestQuoting:
    def test_windows_paths_and_quotes_survive(self):
        section = {"modules": ["C:\\data\\x.py"], "entities_file": 'a"b.toml'}
        assert _round_trip(section) == section

    def test_odd_keys_are_quoted(self):
        section = {"modules": [], "schema_keys": {"has space": ["01"]}}
        assert _round_trip(section) == section


class TestWrite:
    def test_write_replaces_atomically_and_returns_the_path(self, tmp_path):
        path = tmp_path / "scistack.toml"
        assert config_file.write(path, {"modules": ["a.py"]}) == path
        assert tomllib.loads(path.read_text()) == {"modules": ["a.py"]}
        assert not (tmp_path / "scistack.toml.tmp").exists()

    def test_write_logs_the_keys(self, tmp_path, caplog):
        with caplog.at_level(logging.INFO, logger="scidb"):
            config_file.write(tmp_path / "scistack.toml", {"modules": [], "db": "x"})
        assert "wrote" in caplog.text and "'db'" in caplog.text


@pytest.mark.parametrize("value", [1.5, -2, "ünïcode"])
def test_generic_values_round_trip(value):
    assert _round_trip({"modules": [], "v": value})["v"] == value


def test_a_non_ascii_key_is_quoted():
    """TOML bare keys are ASCII only; str.isalnum() would let 'é' through."""
    section = {"modules": [], "schema_keys": {"séance": ["a"]}}
    assert '"séance"' in config_file.render(section)
    assert _round_trip(section) == section
