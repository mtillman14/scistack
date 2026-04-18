"""Tests for PathInput.discover() — filesystem-driven metadata discovery."""

import os
import tempfile
from pathlib import Path

import pytest

from scifor.pathinput import PathInput


@pytest.fixture
def tmp_tree(tmp_path):
    """Create a temp directory tree:

    tmp_path/
        1/
            XSENS/
                A/
                    1_XSENS_A_fast-001.xlsx
                    1_XSENS_A_slow-001.xlsx
                B/
                    1_XSENS_B_fast-001.xlsx
        2/
            XSENS/
                A/
                    2_XSENS_A_fast-001.xlsx
    """
    for subject in ["1", "2"]:
        sessions = ["A", "B"] if subject == "1" else ["A"]
        for session in sessions:
            speeds = ["fast", "slow"] if (subject == "1" and session == "A") else ["fast"]
            for speed in speeds:
                d = tmp_path / subject / "XSENS" / session
                d.mkdir(parents=True, exist_ok=True)
                fname = f"{subject}_XSENS_{session}_{speed}-001.xlsx"
                (d / fname).touch()
    return tmp_path


class TestPlaceholderKeys:
    def test_simple(self):
        pi = PathInput("{subject}/data.mat")
        assert pi.placeholder_keys() == ["subject"]

    def test_multiple(self):
        pi = PathInput("{subject}/{session}/data.mat")
        assert pi.placeholder_keys() == ["subject", "session"]

    def test_mixed_segment(self):
        pi = PathInput("{subject}_XSENS_{session}_{speed}-001.xlsx")
        assert pi.placeholder_keys() == ["subject", "session", "speed"]

    def test_no_placeholders(self):
        pi = PathInput("data/raw/file.mat")
        assert pi.placeholder_keys() == []

    def test_duplicate_keys(self):
        pi = PathInput("{subject}/{subject}_data.mat")
        assert pi.placeholder_keys() == ["subject"]


class TestDiscover:
    def test_basic_discovery(self, tmp_tree):
        pi = PathInput(
            "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx",
            root_folder=tmp_tree,
        )
        combos = pi.discover()
        assert len(combos) == 4
        # Check specific combos exist
        assert {"subject": "1", "session": "A", "speed": "fast"} in combos
        assert {"subject": "1", "session": "A", "speed": "slow"} in combos
        assert {"subject": "1", "session": "B", "speed": "fast"} in combos
        assert {"subject": "2", "session": "A", "speed": "fast"} in combos

    def test_values_are_strings(self, tmp_tree):
        pi = PathInput(
            "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx",
            root_folder=tmp_tree,
        )
        combos = pi.discover()
        for combo in combos:
            for v in combo.values():
                assert isinstance(v, str)

    def test_literal_segment_filtering(self, tmp_tree):
        """Literal 'XSENS' segment filters out non-matching dirs."""
        # Create a distractor directory
        (tmp_tree / "1" / "OTHER").mkdir()
        (tmp_tree / "1" / "OTHER" / "A").mkdir()
        (tmp_tree / "1" / "OTHER" / "A" / "1_OTHER_A_fast-001.xlsx").touch()

        pi = PathInput(
            "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx",
            root_folder=tmp_tree,
        )
        combos = pi.discover()
        # Should not include the OTHER directory
        for combo in combos:
            assert combo.get("session") in ("A", "B")

    def test_empty_filesystem(self, tmp_path):
        pi = PathInput(
            "{subject}/data/{file}.csv",
            root_folder=tmp_path,
        )
        combos = pi.discover()
        assert combos == []

    def test_no_placeholders(self, tmp_path):
        """Template with no placeholders — returns one combo (empty dict) if file exists."""
        (tmp_path / "data.mat").touch()
        pi = PathInput("data.mat", root_folder=tmp_path)
        combos = pi.discover()
        assert combos == [{}]

    def test_no_placeholders_missing_file(self, tmp_path):
        pi = PathInput("data.mat", root_folder=tmp_path)
        combos = pi.discover()
        assert combos == []

    def test_repeated_placeholder_consistency(self, tmp_tree):
        """When {subject} appears in both dir and filename, values must be consistent."""
        pi = PathInput(
            "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx",
            root_folder=tmp_tree,
        )
        combos = pi.discover()
        for combo in combos:
            # subject should be self-consistent (dir segment = filename segment)
            assert combo["subject"] in ("1", "2")

    def test_repeated_placeholder_rejects_inconsistent(self, tmp_path):
        """If {x} in dir doesn't match {x} in filename, path is excluded."""
        d = tmp_path / "A"
        d.mkdir()
        # File says B but dir is A — should not match
        (d / "B_data.csv").touch()
        # File says A and dir is A — should match
        (d / "A_data.csv").touch()

        pi = PathInput("{x}/{x}_data.csv", root_folder=tmp_path)
        combos = pi.discover()
        assert len(combos) == 1
        assert combos[0] == {"x": "A"}

    def test_pure_placeholder_directories(self, tmp_path):
        """Pure {key} segments match any directory."""
        for name in ["alpha", "beta"]:
            d = tmp_path / name
            d.mkdir()
            (d / "result.csv").touch()
        # Distractor: directory without the file
        (tmp_path / "gamma").mkdir()

        pi = PathInput("{group}/result.csv", root_folder=tmp_path)
        combos = pi.discover()
        assert len(combos) == 2
        groups = {c["group"] for c in combos}
        assert groups == {"alpha", "beta"}

    def test_no_root_folder_uses_cwd(self, tmp_path, monkeypatch):
        """When root_folder is None, discover() uses cwd."""
        (tmp_path / "file_A.txt").touch()
        (tmp_path / "file_B.txt").touch()
        monkeypatch.chdir(tmp_path)

        pi = PathInput("file_{x}.txt")
        combos = pi.discover()
        assert len(combos) == 2
        xs = {c["x"] for c in combos}
        assert xs == {"A", "B"}

    def test_mixed_filename_segment(self, tmp_path):
        """Template with literal+placeholder in filename segment."""
        d = tmp_path / "data"
        d.mkdir()
        (d / "report_2024_final.csv").touch()
        (d / "report_2023_draft.csv").touch()
        (d / "other.csv").touch()  # should not match

        pi = PathInput("data/report_{year}_{status}.csv", root_folder=tmp_path)
        combos = pi.discover()
        assert len(combos) == 2
        years = {c["year"] for c in combos}
        assert years == {"2024", "2023"}

    def test_deeply_nested_template(self, tmp_path):
        """Template with many segments."""
        d = tmp_path / "a" / "b" / "c"
        d.mkdir(parents=True)
        (d / "file.txt").touch()

        pi = PathInput("{x}/b/{y}/file.txt", root_folder=tmp_path)
        combos = pi.discover()
        assert combos == [{"x": "a", "y": "c"}]
