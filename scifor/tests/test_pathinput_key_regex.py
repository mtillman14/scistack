"""Adjacent-placeholder disambiguation via ``key_regex``.

Two placeholders with no delimiter between them (e.g. ``"{speed}{trial}"``)
are ambiguous under the default greedy ``[^/\\]+`` capture: nothing anchors
where one field ends and the next begins, so the earlier placeholder
swallows everything but the last character. ``key_regex={"speed":
r"[A-Za-z]+", "trial": r"\\d+"}`` lets a caller declare the split explicitly.
See docs/claude/pathinput-key-patterns.md.
"""

import pytest
from scifor.pathinput import PathInput


@pytest.fixture
def emg_tree(tmp_path):
    """
    tmp_path/
        SS01_EMG_SSV1.mat
        SS01_EMG_SSV10.mat
        SS02_EMG_FV12.mat
    """
    for fname in ["SS01_EMG_SSV1.mat", "SS01_EMG_SSV10.mat", "SS02_EMG_FV12.mat"]:
        (tmp_path / fname).touch()
    return tmp_path


class TestKeyRegexValidation:
    def test_unknown_key_raises(self):
        with pytest.raises(ValueError, match="not a placeholder"):
            PathInput("{subject}/data.mat", key_regex={"session": r"\d+"}, name="{subject}/data.mat")

    def test_stored_on_instance(self):
        pi = PathInput(
            "{speed}{trial}.mat", key_regex={"speed": r"[A-Za-z]+", "trial": r"\d+"}, name="{speed}{trial}.mat"
        )
        assert pi.key_regex == {"speed": r"[A-Za-z]+", "trial": r"\d+"}


class TestKeyRegexDiscovery:
    def test_default_greedy_split_is_wrong(self, emg_tree):
        # Baseline: without key_regex, the greedy default hands everything
        # but the last character to the first placeholder.
        pi = PathInput(
            "{subject}_EMG_{speed}{trial}.mat",
            root_folder=emg_tree, name="{subject}_EMG_{speed}{trial}.mat",
        )
        combos = pi.discover()
        by_subject_trial = {(c["subject"], c["speed"]): c["trial"] for c in combos}
        # SS01_EMG_SSV10.mat -> speed="SSV1", trial="0" (wrong: real trial is 10)
        assert by_subject_trial[("SS01", "SSV1")] == "0"

    def test_letters_digits_split_resolves_ambiguity(self, emg_tree):
        pi = PathInput(
            "{subject}_EMG_{speed}{trial}.mat",
            root_folder=emg_tree,
            key_regex={"speed": r"[A-Za-z]+", "trial": r"\d+"}, name="{subject}_EMG_{speed}{trial}.mat",
        )
        combos = pi.discover()
        by_key = {(c["subject"], c["speed"], c["trial"]) for c in combos}
        assert by_key == {
            ("SS01", "SSV", "1"),
            ("SS01", "SSV", "10"),
            ("SS02", "FV", "12"),
        }

    def test_unrelated_key_regex_does_not_affect_delimited_segments(self, tmp_path):
        # A key_regex entry for one key shouldn't change matching for a
        # segment where every placeholder is already delimiter-separated.
        (tmp_path / "1").mkdir()
        (tmp_path / "1" / "session_A_fast.mat").touch()
        pi_plain = PathInput(
            "{subject}/session_{session}_{speed}.mat", root_folder=tmp_path, name="{subject}/session_{session}_{speed}.mat"
        )
        pi_with_key_regex = PathInput(
            "{subject}/session_{session}_{speed}.mat",
            root_folder=tmp_path,
            key_regex={"speed": r"[A-Za-z]+"}, name="{subject}/session_{session}_{speed}.mat",
        )
        assert pi_plain.discover() == pi_with_key_regex.discover()

    def test_no_match_when_value_violates_declared_pattern(self, tmp_path):
        # A trial value that isn't pure digits (e.g. "1a") simply fails to
        # match rather than being mis-split -- a safe failure mode.
        (tmp_path / "EMG_SSV1a.mat").touch()
        pi = PathInput(
            "EMG_{speed}{trial}.mat",
            root_folder=tmp_path,
            key_regex={"speed": r"[A-Za-z]+", "trial": r"\d+"}, name="EMG_{speed}{trial}.mat",
        )
        assert pi.discover() == []


class TestKeyRegexToKey:
    def test_to_spec_includes_key_regex_when_set(self):
        import json

        pi = PathInput("{speed}{trial}.mat", key_regex={"speed": r"[A-Za-z]+"}, name="{speed}{trial}.mat")
        key = json.loads(pi.to_spec())
        assert key["key_regex"] == {"speed": r"[A-Za-z]+"}

    def test_to_spec_omits_key_regex_when_default(self):
        # Backwards compatible: pre-key_regex saved keys stay byte-identical.
        import json

        pi = PathInput("{speed}/{trial}.mat", name="{speed}/{trial}.mat")
        key = json.loads(pi.to_spec())
        assert "key_regex" not in key


class TestPatternRule:
    """``key_regex_pattern_error`` is THE rule (constructor, entities file,
    GUI save all ask it)."""

    def test_capturing_group_is_refused(self):
        with pytest.raises(ValueError, match="capturing group"):
            PathInput("{subject}.mat", key_regex={"subject": r"(R2)_\d+"}, name="p")

    def test_non_capturing_group_is_fine(self):
        PathInput("{subject}.mat", key_regex={"subject": r"(?:R2|R3)_\d+"}, name="p")

    def test_pattern_that_does_not_compile_is_refused(self):
        # An unclosed class. (An unclosed `{3` is NOT an error: Python's re
        # reads an incomplete quantifier as literal text.)
        with pytest.raises(ValueError, match="does not compile"):
            PathInput("{subject}.mat", key_regex={"subject": r"R2_[0-9"}, name="p")

    def test_empty_pattern_is_refused(self):
        from scifor import key_regex_pattern_error

        assert key_regex_pattern_error("") is not None
        assert key_regex_pattern_error(r"R2_\d{3}") is None


class TestUnderscoreInValue:
    def test_subject_with_the_delimiter_is_captured_whole(self, tmp_path):
        """2026-10-21: subject ``R2_001`` in ``{subject}_{session}.xlsx``
        split at the first or last ``_`` without a pattern."""
        for f in ["R2_001_BL.xlsx", "R2_002_POST.xlsx"]:
            (tmp_path / f).touch()
        pi = PathInput(
            "{subject}_{session}.xlsx",
            root_folder=tmp_path,
            key_regex={"subject": r"R2_\d{3}"},
            name="p",
        )
        assert sorted(pi.discover(), key=lambda c: c["subject"]) == [
            {"subject": "R2_001", "session": "BL"},
            {"subject": "R2_002", "session": "POST"},
        ]


class TestPatternWithAliases:
    """Regex first, then alias canonicalization; declared alias spellings
    always match even when the pattern alone would not (user decision
    2026-10-21)."""

    def test_regex_match_is_canonicalized_by_the_aliases(self, tmp_path):
        (tmp_path / "S1_Baseline.csv").touch()
        pi = PathInput(
            "{subject}_{session}.csv",
            root_folder=tmp_path,
            key_regex={"session": r"[A-Z][a-z]+"},
            aliases={"session": {"BL": ["Baseline"]}},
            name="p",
        )
        assert pi.discover() == [{"subject": "S1", "session": "BL"}]

    def test_alias_spelling_outside_the_pattern_still_matches(self, tmp_path):
        for f in ["S1_Baseline.csv", "S1_02.csv", "S1_xyz.csv"]:
            (tmp_path / f).touch()
        pi = PathInput(
            "{subject}_{session}.csv",
            root_folder=tmp_path,
            key_regex={"session": r"\d{2}"},
            aliases={"session": {"01": ["Baseline"]}},
            name="p",
        )
        assert sorted(c["session"] for c in pi.discover()) == ["01", "02"]
