"""Folder-name alias fallback for PathInput (match-only).

A schema key can have several on-disk spellings that all mean one canonical
value, declared via ``aliases={"session": {"BL": ["Baseline", "1. Baseline"]}}``.
``load()`` resolves a canonical value to whichever spelling exists on disk;
``discover()`` canonicalizes whatever spelling it finds on disk. Neither
direction ever chooses a spelling to *write* — match-only.
"""

import logging

import pytest
from scifor.pathinput import PathInput


@pytest.fixture
def session_tree(tmp_path):
    """
    tmp_path/
        sub1/
            Baseline/
                data.mat
        sub2/
            BL/
                data.mat
        sub3/
            Unlabeled/
                data.mat
    """
    (tmp_path / "sub1" / "Baseline").mkdir(parents=True)
    (tmp_path / "sub1" / "Baseline" / "data.mat").touch()
    (tmp_path / "sub2" / "BL").mkdir(parents=True)
    (tmp_path / "sub2" / "BL" / "data.mat").touch()
    (tmp_path / "sub3" / "Unlabeled").mkdir(parents=True)
    (tmp_path / "sub3" / "Unlabeled" / "data.mat").touch()
    return tmp_path


SESSION_ALIASES = {"session": {"BL": ["Baseline", "1. Baseline"]}}


class TestConstructionValidation:
    def test_unknown_placeholder_key_rejected(self):
        with pytest.raises(ValueError, match="not a placeholder"):
            PathInput("{subject}/data.mat", aliases={"session": {"BL": ["Baseline"]}}, name="{subject}/data.mat")

    def test_ambiguous_spelling_rejected(self):
        with pytest.raises(ValueError, match="ambiguous"):
            PathInput(
                "{subject}/{session}/data.mat",
                aliases={
                    "session": {
                        "BL": ["Shared"],
                        "FU": ["Shared"],
                    }
                }, name="{subject}/{session}/data.mat",
            )

    def test_canonical_reused_as_own_spelling_is_fine(self):
        # "BL" is implicitly valid; listing it again under itself is a no-op.
        pi = PathInput(
            "{subject}/{session}/data.mat",
            aliases={"session": {"BL": ["BL", "Baseline"]}}, name="{subject}/{session}/data.mat",
        )
        assert pi.aliases["session"]["BL"] == ["BL", "Baseline"]


class TestLoadResolvesAlias:
    def test_canonical_folder_hit_is_literal_no_fallback(self, session_tree):
        pi = PathInput(
            "{subject}/{session}/data.mat",
            root_folder=str(session_tree),
            aliases=SESSION_ALIASES, name="{subject}/{session}/data.mat",
        )
        path, resolutions = pi.load_with_captures({"subject": "sub2", "session": "BL"})
        assert path == (session_tree / "sub2" / "BL" / "data.mat").resolve()
        assert resolutions == {}

    def test_alias_spelling_found_on_disk(self, session_tree):
        pi = PathInput(
            "{subject}/{session}/data.mat",
            root_folder=str(session_tree),
            aliases=SESSION_ALIASES, name="{subject}/{session}/data.mat",
        )
        path, resolutions = pi.load_with_captures({"subject": "sub1", "session": "BL"})
        assert path == (session_tree / "sub1" / "Baseline" / "data.mat").resolve()
        assert resolutions == {"session": "Baseline"}

    def test_load_bare_returns_path(self, session_tree):
        pi = PathInput(
            "{subject}/{session}/data.mat",
            root_folder=str(session_tree),
            aliases=SESSION_ALIASES, name="{subject}/{session}/data.mat",
        )
        assert (
            pi.load(subject="sub1", session="BL")
            == (session_tree / "sub1" / "Baseline" / "data.mat").resolve()
        )

    def test_unaliased_key_value_unaffected(self, session_tree):
        # "Unlabeled" isn't declared under session's aliases at all -> no
        # fallback triggers, literal-only behavior (regression guard).
        pi = PathInput(
            "{subject}/{session}/data.mat",
            root_folder=str(session_tree),
            aliases=SESSION_ALIASES, name="{subject}/{session}/data.mat",
        )
        path, resolutions = pi.load_with_captures(
            {"subject": "sub3", "session": "Unlabeled"}
        )
        assert path == (session_tree / "sub3" / "Unlabeled" / "data.mat").resolve()
        assert resolutions == {}


class TestLoadAmbiguousAlias:
    def test_two_alias_spellings_both_present_raises(self, session_tree):
        (session_tree / "sub1" / "1. Baseline").mkdir()
        (session_tree / "sub1" / "1. Baseline" / "data.mat").touch()
        pi = PathInput(
            "{subject}/{session}/data.mat",
            root_folder=str(session_tree),
            aliases=SESSION_ALIASES, name="{subject}/{session}/data.mat",
        )
        with pytest.raises(RuntimeError, match="matched 2 files"):
            pi.load(subject="sub1", session="BL")


class TestDiscoverCanonicalizes:
    def test_alias_spelling_reports_canonical(self, session_tree):
        pi = PathInput(
            "{subject}/{session}/data.mat",
            root_folder=str(session_tree),
            aliases=SESSION_ALIASES, name="{subject}/{session}/data.mat",
        )
        combos = pi.discover()
        sessions_by_subject = {c["subject"]: c["session"] for c in combos}
        assert sessions_by_subject["sub1"] == "BL"  # on disk: "Baseline"
        assert sessions_by_subject["sub2"] == "BL"  # on disk: "BL" itself

    def test_unrecognized_spelling_passes_through_and_logs(self, session_tree, caplog):
        pi = PathInput(
            "{subject}/{session}/data.mat",
            root_folder=str(session_tree),
            aliases=SESSION_ALIASES, name="{subject}/{session}/data.mat",
        )
        with caplog.at_level(logging.DEBUG, logger="scifor"):
            combos = pi.discover()
        sessions_by_subject = {c["subject"]: c["session"] for c in combos}
        assert sessions_by_subject["sub3"] == "Unlabeled"  # no alias entry -> unchanged
        msgs = [
            r.getMessage()
            for r in caplog.records
            if r.name == "scifor" and "pathinput_alias_unresolved" in r.getMessage()
        ]
        assert any("Unlabeled" in m for m in msgs)

    def test_no_aliases_declared_is_unaffected(self, session_tree):
        # Regression guard: discover() behaves exactly as before when no
        # aliases are declared at all.
        pi = PathInput("{subject}/{session}/data.mat", root_folder=str(session_tree), name="{subject}/{session}/data.mat")
        combos = pi.discover()
        sessions_by_subject = {c["subject"]: c["session"] for c in combos}
        assert sessions_by_subject["sub1"] == "Baseline"
        assert sessions_by_subject["sub2"] == "BL"


class TestCombinedNumericAndAlias:
    def test_numeric_and_alias_keys_resolve_together(self, tmp_path):
        (tmp_path / "sub-002" / "Baseline").mkdir(parents=True)
        (tmp_path / "sub-002" / "Baseline" / "6MWT-001.mat").touch()
        pi = PathInput(
            "sub-{subject}/{session}/6MWT-{trial}.mat",
            root_folder=str(tmp_path),
            aliases=SESSION_ALIASES, name="sub-{subject}/{session}/6MWT-{trial}.mat",
        )
        path, resolutions = pi.load_with_captures(
            {"subject": 2, "session": "BL", "trial": 1}
        )
        assert path == (
            tmp_path / "sub-002" / "Baseline" / "6MWT-001.mat"
        ).resolve()
        assert resolutions == {
            "subject": "002",
            "session": "Baseline",
            "trial": "001",
        }

    def test_discover_canonicalizes_alongside_raw_numeric_capture(self, tmp_path):
        (tmp_path / "sub-002" / "Baseline").mkdir(parents=True)
        (tmp_path / "sub-002" / "Baseline" / "6MWT-001.mat").touch()
        pi = PathInput(
            "sub-{subject}/{session}/6MWT-{trial}.mat",
            root_folder=str(tmp_path),
            aliases=SESSION_ALIASES, name="sub-{subject}/{session}/6MWT-{trial}.mat",
        )
        combos = pi.discover()
        assert combos == [{"subject": "002", "session": "BL", "trial": "001"}]
