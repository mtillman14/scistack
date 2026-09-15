"""LocationFilter: the combo half of docs/claude/location-filter-semantics.md.

The parity cases are LOADED from docs/claude/location-filter-cases.json, not
transcribed. The same rule exists four times — here over for_each combos, in
scistackplot.reduce as a pandas mask, in scistackplot.codegen as the emitted
copy of that mask, and in the GUI's locationSelection.ts — and scistackplot
cannot import scifor (pandas/numpy/scistacklog only), so the copies are real.
Reading one shared file means a case added to the spec FAILS every
implementation that has not adopted it, rather than passing quietly in the
suites whose author forgot to copy it across.
"""

import json
import logging
from pathlib import Path

import pandas as pd
import pytest

from scifor import LocationFilter, filter_combos, for_each, set_schema
from scifor.locations import value_spellings

CASES_FILE = (
    Path(__file__).resolve().parents[2] / "docs" / "claude" / "location-filter-cases.json"
)


def load_cases(kind: str) -> list[dict]:
    """Shared cases that say they apply to *kind* ("combo" here).

    A missing file is an ERROR rather than a skip: in the monorepo the file is
    the spec, and a suite that silently ran zero parity cases would be the
    exact failure this file exists to prevent.
    """
    if not CASES_FILE.exists():
        raise AssertionError(
            f"Shared location-filter cases not found at {CASES_FILE}. "
            "Every implementation of the rule reads this one file."
        )
    data = json.loads(CASES_FILE.read_text(encoding="utf-8"))
    return [case for case in data["cases"] if kind in case["applies_to"]]


def combo_cases() -> list[dict]:
    return load_cases("combo")


def case_id(case: dict) -> str:
    return f"{case['id']:02d}-{case['name']}"


def setup_function():
    set_schema(["subject", "session", "trial"])


def make_filter(include=(), exclude_levels=None):
    return LocationFilter.build(include=include, exclude_levels=exclude_levels or {})


def combo(subject=None, session=None, trial=None, **extra):
    out = {}
    if subject is not None:
        out["subject"] = subject
    if session is not None:
        out["session"] = session
    if trial is not None:
        out["trial"] = trial
    out.update(extra)
    return out


# --- parity cases -----------------------------------------------------------


class TestParityCases:
    """Every case in the shared file that says it applies to combos.

    Hand-copying the table into each suite is what this replaces: a case added
    to the JSON now FAILS here until scifor adopts it, instead of passing
    quietly because nobody transcribed it.
    """

    @pytest.mark.parametrize("case", combo_cases(), ids=case_id)
    def test_case(self, case):
        location_filter = LocationFilter.of(case["filter"])
        assert location_filter.matches(case["location"]) is case["in"], case["why"]


class TestSpellings:
    """The numeric-spelling rule (shared cases 16-18), at the unit level."""

    def test_an_integral_number_has_both_spellings(self):
        assert value_spellings("1") == frozenset({"1", "1.0"})
        assert value_spellings(1.0) == frozenset({"1.0", "1"})
        assert value_spellings("1.00") == frozenset({"1.00", "1", "1.0"})
        assert value_spellings(-3) == frozenset({"-3", "-3.0"})

    def test_a_zero_padded_value_is_only_itself(self):
        """The padding rule: "01" and "1" may be two distinct trials, and
        which one is identity is scidb's call, not a comparison shortcut."""
        assert value_spellings("01") == frozenset({"01"})
        assert value_spellings("007") == frozenset({"007"})

    def test_a_non_integral_number_is_only_itself(self):
        assert value_spellings("1.5") == frozenset({"1.5"})
        assert value_spellings("1e3") == frozenset({"1e3"})

    def test_zero_is_integral(self):
        assert value_spellings("0") == frozenset({"0", "0.0"})


# --- construction / coercion ------------------------------------------------


class TestConstruction:
    def test_of_accepts_none_a_mapping_and_itself(self):
        assert LocationFilter.of(None).is_empty()
        built = LocationFilter.of(
            {"include": [[["subject", "01"]]], "exclude_levels": {"session": ["BL"]}}
        )
        assert built.include == ((("subject", "01"),),)
        assert built.exclude_levels == (("session", ("BL",)),)
        assert LocationFilter.of(built) is built

    def test_mapping_form_round_trips(self):
        """The GUI holds the mapping form; a spec saves it to TOML. If to_dict
        and of() disagree, a reopened plot filters differently than the one
        that was saved."""
        raw = {
            "include": [[["subject", "01"]], [["subject", "02"], ["trial", "3"]]],
            "exclude_levels": {"session": ["BL"]},
        }
        assert LocationFilter.of(raw).to_dict() == raw

    def test_empty_exclusion_lists_are_dropped(self):
        """{"session": []} means "nothing excluded", which must be INERT — not
        a rule that happens to match nothing, or is_empty() would report a
        filter where there is none and callers would take the slow path."""
        assert make_filter(exclude_levels={"session": []}).is_empty()

    def test_duplicate_levels_collapse(self):
        f = make_filter(exclude_levels={"session": ["BL", "BL", "POST"]})
        assert f.exclude_levels == (("session", ("BL", "POST")),)

    def test_unknown_mapping_key_is_refused(self):
        with pytest.raises(ValueError, match="exclude_levels"):
            LocationFilter.of({"includes": []})

    def test_a_bad_step_shape_is_refused(self):
        with pytest.raises(ValueError, match="exactly 2"):
            make_filter(include=[[["subject", "01", "extra"]]])
        with pytest.raises(TypeError, match=r"\[key, value\] pair"):
            make_filter(include=[["subject"]])

    def test_wrong_type_is_refused_by_name(self):
        with pytest.raises(TypeError, match="locations="):
            LocationFilter.of(["subject", "01"])

    def test_keys_names_both_halves(self):
        f = make_filter(
            include=[[["subject", "01"], ["trial", "3"]]],
            exclude_levels={"session": ["BL"]},
        )
        assert f.keys() == {"subject", "trial", "session"}


# --- combos are coarser than locations --------------------------------------


class TestCoarserCombos:
    def test_an_ancestor_combo_is_kept(self):
        """A run iterating subject only, against a selection naming trial:
        the combo is an ancestor of a selected location, so some of what it
        covers was selected. Dropping it would silently run nothing."""
        f = make_filter(include=[[["subject", "02"], ["trial", "3"]]])
        assert f.matches({"subject": "02"})
        assert not f.matches({"subject": "01"})

    def test_a_filter_naming_only_uniterated_keys_warns_and_keeps_everything(
        self, caplog
    ):
        combos = [{"subject": "01"}, {"subject": "02"}]
        with caplog.at_level(logging.WARNING, logger="scifor"):
            kept = filter_combos(combos, {"exclude_levels": {"trial": ["3"]}})
        assert kept == combos
        assert "does not iterate" in caplog.text


# --- filter_combos reporting ------------------------------------------------


class TestFilterCombos:
    def test_inert_filter_returns_every_combo(self):
        combos = [combo("01", "BL", "1"), combo("02", "BL", "1")]
        assert filter_combos(combos, None) == combos
        assert filter_combos(combos, {}) == combos

    def test_counts_are_split_by_which_clause_dropped_them(self, caplog):
        combos = [
            combo("01", "BL", "1"),
            combo("01", "POST", "1"),
            combo("02", "POST", "1"),
        ]
        with caplog.at_level(logging.INFO, logger="scifor"):
            kept = filter_combos(
                combos,
                {
                    "include": [[["subject", "01"]]],
                    "exclude_levels": {"session": ["BL"]},
                },
            )
        assert kept == [combo("01", "POST", "1")]
        # "0 of 240 kept" is not a diagnosis; which rule did it is.
        assert "1 of 3 combo(s) kept" in caplog.text
        assert "1 dropped by prefix" in caplog.text
        assert "session=BL (1)" in caplog.text

    def test_removing_every_combo_warns(self, caplog):
        """The failure this feature introduces: a run that does nothing looks
        like a broken pipeline rather than a selection made three days ago."""
        combos = [combo("01", "BL", "1"), combo("02", "BL", "1")]
        with caplog.at_level(logging.WARNING, logger="scifor"):
            kept = filter_combos(combos, {"exclude_levels": {"session": ["BL"]}})
        assert kept == []
        assert "removed every one of the 2" in caplog.text
        assert "Nothing will run" in caplog.text

    def test_kept_combos_are_copies(self):
        """for_each mutates combo dicts downstream; handing back the caller's
        own dicts would let that leak into _all_combos."""
        combos = [combo("01", "BL", "1")]
        kept = filter_combos(combos, {"include": [[["subject", "01"]]]})
        kept[0]["subject"] = "99"
        assert combos[0]["subject"] == "01"


# --- through for_each -------------------------------------------------------


def _frame():
    return pd.DataFrame(
        {
            "subject": ["01", "01", "02", "02"],
            "session": ["BL", "POST", "BL", "POST"],
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )


# One row and one data column per combo, so scifor hands the function the
# SCALAR rather than a frame (`_extract_data`'s 1x1 unwrap). Taking a frame
# here would make every iteration raise, and for_each's continue-and-report
# would then return an empty result — which reads exactly like a location
# filter that dropped everything.
def _double(value):
    return float(value) * 2


def _scaled(value, scale):
    return float(value) * scale


class TestThroughForEach:
    def test_prefix_selection_narrows_the_run(self):
        set_schema(["subject", "session"])
        result = for_each(
            _double,
            inputs={"value": _frame()},
            subject=["01", "02"],
            session=["BL", "POST"],
            locations={"include": [[["subject", "01"]]]},
        )
        assert sorted(result["subject"].tolist()) == ["01", "01"]

    def test_level_rule_narrows_the_run(self):
        set_schema(["subject", "session"])
        result = for_each(
            _double,
            inputs={"value": _frame()},
            subject=["01", "02"],
            session=["BL", "POST"],
            locations={"exclude_levels": {"session": ["BL"]}},
        )
        assert result["session"].unique().tolist() == ["POST"]
        assert sorted(result["subject"].tolist()) == ["01", "02"]

    def test_both_halves_compose(self):
        set_schema(["subject", "session"])
        result = for_each(
            _double,
            inputs={"value": _frame()},
            subject=["01", "02"],
            session=["BL", "POST"],
            locations={
                "include": [[["subject", "01"]]],
                "exclude_levels": {"session": ["BL"]},
            },
        )
        assert len(result) == 1
        assert result["session"].tolist() == ["POST"]

    def test_omitted_locations_changes_nothing(self):
        set_schema(["subject", "session"])
        baseline = for_each(
            _double,
            inputs={"value": _frame()},
            subject=["01", "02"],
            session=["BL", "POST"],
        )
        with_inert = for_each(
            _double,
            inputs={"value": _frame()},
            subject=["01", "02"],
            session=["BL", "POST"],
            locations={},
        )
        pd.testing.assert_frame_equal(baseline, with_inert)

    def test_each_of_alternatives_all_see_the_filter(self):
        """EachOf recurses into for_each with the kwargs spelled out by hand,
        and a kwarg dropped there fails SILENTLY — `glue=` went missing from
        this same recursion in 2026-09-10. Every alternative must run over the
        same locations."""
        from scifor import EachOf

        set_schema(["subject", "session"])
        result = for_each(
            _scaled,
            inputs={"value": _frame(), "scale": EachOf(1.0, 10.0)},
            subject=["01", "02"],
            session=["BL", "POST"],
            locations={"include": [[["subject", "01"]]]},
        )
        assert set(result["subject"]) == {"01"}
        # Two alternatives × two sessions of subject 01, and nothing of 02.
        assert len(result) == 4

    def test_the_run_banner_reports_the_pruning(self, caplog):
        set_schema(["subject", "session"])
        with caplog.at_level(logging.INFO, logger="scifor"):
            for_each(
                _double,
                inputs={"value": _frame()},
                subject=["01", "02"],
                session=["BL", "POST"],
                locations={"include": [[["subject", "01"]]]},
            )
        assert "2 iterations" in caplog.text
        assert "filtered out before iteration" in caplog.text
        assert "locations=1 prefix(es)" in caplog.text
