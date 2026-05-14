"""Tests for PathInput(regex=True) — regex matching of the last segment at load time.

The regex flag makes ``PathInput.load(**metadata)`` treat the final path
segment as a regular expression: placeholders are substituted first, then
the resulting last segment is matched against ``^pattern$`` over the files
(not directories) in the parent directory. Exactly one file must match.

These tests mirror the MATLAB-side regex tests in
``tests/matlab/scifor/TestPathInput.m`` so MATLAB and Python agree.
"""

import pytest

from scifor.pathinput import PathInput


@pytest.fixture
def tmp_files(tmp_path):
    """Create a small fixture tree exercising regex match cases:

    tmp_path/
        exact/
            report.txt              # exact filename, also a valid regex
        1/
            6mwt-001.xlsx           # zero-padded numeric suffix
        nomatch/                    # empty dir for "no match" case
        dup/
            data_v1.csv
            data_v2.csv             # two files matching one regex pattern
        abs_regex/
            result_final.csv        # absolute-path regex case
    """
    (tmp_path / "exact").mkdir()
    (tmp_path / "exact" / "report.txt").touch()

    (tmp_path / "1").mkdir()
    (tmp_path / "1" / "6mwt-001.xlsx").touch()

    (tmp_path / "nomatch").mkdir()

    (tmp_path / "dup").mkdir()
    (tmp_path / "dup" / "data_v1.csv").touch()
    (tmp_path / "dup" / "data_v2.csv").touch()

    (tmp_path / "abs_regex").mkdir()
    (tmp_path / "abs_regex" / "result_final.csv").touch()

    return tmp_path


class TestRegexLoad:
    def test_basic_match(self, tmp_files):
        # An exact filename used as a regex pattern matches itself.
        pi = PathInput(r"exact/report\.txt", root_folder=str(tmp_files), regex=True)
        path = pi.load()
        assert path == (tmp_files / "exact" / "report.txt").resolve()

    def test_zero_padding_pattern(self, tmp_files):
        # Quantifier in pattern matches a zero-padded numeric filename.
        pi = PathInput(
            r"{subject}/6mwt-0{0,2}1\.xlsx",
            root_folder=str(tmp_files),
            regex=True,
        )
        path = pi.load(subject=1)
        assert path == (tmp_files / "1" / "6mwt-001.xlsx").resolve()

    def test_no_match_errors(self, tmp_files):
        pi = PathInput(
            r"{subject}/nonexistent.*\.xyz",
            root_folder=str(tmp_files),
            regex=True,
        )
        with pytest.raises(FileNotFoundError):
            pi.load(subject="nomatch")

    def test_multiple_matches_errors(self, tmp_files):
        pi = PathInput(r"dup/data_v\d\.csv", root_folder=str(tmp_files), regex=True)
        with pytest.raises(RuntimeError):
            pi.load()

    def test_absolute_template_regex(self, tmp_files):
        # Absolute path in the template (no root_folder), regex matched on last segment.
        template = f"{tmp_files}/abs_regex/result_final\\.csv"
        pi = PathInput(template, regex=True)
        path = pi.load()
        assert path == (tmp_files / "abs_regex" / "result_final.csv").resolve()

    def test_returns_path(self, tmp_files):
        # Result is a pathlib.Path (the API contract).
        from pathlib import Path

        pi = PathInput(r"exact/report\.txt", root_folder=str(tmp_files), regex=True)
        assert isinstance(pi.load(), Path)


class TestRegexFlagPropagation:
    def test_to_key_includes_regex_when_true(self):
        import json

        pi = PathInput("{x}/a.csv", root_folder="/data", regex=True)
        key = json.loads(pi.to_key())
        assert key["regex"] is True

    def test_to_key_omits_regex_when_false(self):
        # Backwards compatible: pre-regex saved keys stay byte-identical.
        import json

        pi = PathInput("{x}/a.csv", root_folder="/data")
        key = json.loads(pi.to_key())
        assert "regex" not in key

    def test_discover_unaffected_by_regex(self, tmp_files):
        # ``regex`` is a load()-time flag only; discover() ignores it.
        pi_plain = PathInput("{subject}/6mwt-001.xlsx", root_folder=str(tmp_files))
        pi_regex = PathInput("{subject}/6mwt-001.xlsx", root_folder=str(tmp_files), regex=True)
        assert pi_plain.discover() == pi_regex.discover()
