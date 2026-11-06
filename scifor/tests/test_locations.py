"""LocationFilter: the combo half of docs/claude/location-filter-semantics.md.

The parity cases are LOADED from docs/claude/location-filter-cases.json, not
transcribed. The same rule exists four times — here over for_each combos, in
scistackplot.reduce.resolve as a pandas mask, in scistackplot.export.codegen as the emitted
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

from scifor import (
    Fixed,
    LocationFilter,
    Merge,
    filter_combo_indices,
    filter_combos,
    for_each,
    narrow_frame,
    narrow_inputs,
    set_schema,
)
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

    @pytest.mark.parametrize("case", load_cases("row"), ids=case_id)
    def test_row_case(self, case):
        """Rows reach a call through ``narrow_inputs``, so scifor answers the
        row cases too, and must agree with scistackplot's mask on each."""
        location_filter = LocationFilter.of(case["filter"])
        frame = pd.DataFrame([case["location"]])
        assert bool(location_filter.row_mask(frame)[0]) is case["in"], case["why"]


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

    def test_a_filter_naming_only_uniterated_keys_keeps_every_combo_and_says_so(
        self, caplog
    ):
        combos = [{"subject": "01"}, {"subject": "02"}]
        with caplog.at_level(logging.INFO, logger="scifor"):
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
        own dicts would let that leak into hooks.all_combos."""
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


def test_without_keys_drops_whole_prefixes_and_exclusions():
    """An import into a schema with no counterpart for a key removes every
    part of a selection that names it (scidb.project.schema_map.KeyMap.locations)."""
    lf = LocationFilter.build(
        include=[[("subject", "S01"), ("session", "BL")], [("trial", "1")]],
        exclude_levels={"session": ["FU"], "trial": ["3"]},
    )
    out = lf.without_keys({"session"})
    assert out.include == ((("trial", "1"),),)
    assert out.exclude_levels == (("trial", ("3",)),)
    assert lf.without_keys(()) is lf


# --- filter_combo_indices / JSON text (MATLAB's +scifor/for_each.m path) -------


def test_filter_combo_indices_agrees_with_filter_combos():
    combos = [{"subject": s, "trial": t} for s in (1, 2) for t in (1, 2)]
    sel = {"include": [[["subject", "1"]], [["subject", "2"], ["trial", "2"]]]}
    kept = filter_combo_indices(combos, sel)
    assert kept == [0, 1, 3]
    assert [combos[i] for i in kept] == filter_combos(combos, sel)


def test_json_text_is_a_locations_value():
    combos = [{"subject": 1.0}, {"subject": 2.0}]  # MATLAB doubles
    text = json.dumps({"include": [[["subject", "2"]]]})
    assert filter_combo_indices(combos, text) == [1]
    assert filter_combo_indices(combos, "") == [0, 1]
    assert LocationFilter.of(text) == LocationFilter.of(json.loads(text))


# --- rows below the iteration level -------------------------------------------


def _minutes_frame():
    return pd.DataFrame(
        {
            "subject": ["01", "01", "01", "02", "02", "02"],
            "minute": ["5", "6", "6", "5", "5", "6"],
            "value": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        }
    )


class TestRowsBelowTheIterationLevel:
    """A combo that stops above a selected key is kept as its ancestor, and the
    rows it reads must still be the selected ones. Found on a run at
    [group, subject] with only minute=6 selected: every minute arrived."""

    def setup_method(self):
        set_schema(["subject", "minute"])
        self.seen = {}

    def _record(self, table):
        self.seen[table["subject"].iloc[0]] = table["minute"].tolist()
        return float(len(table))

    def _run(self, locations):
        return for_each(
            self._record,
            inputs={"table": _minutes_frame()},
            subject=["01", "02"],
            as_table=True,
            locations=locations,
        )

    def test_a_prefix_naming_a_finer_key_narrows_each_calls_rows(self):
        self._run(
            {"include": [[["subject", "01"], ["minute", "6"]], [["subject", "02"], ["minute", "6"]]]}
        )
        assert self.seen == {"01": ["6", "6"], "02": ["6"]}

    def test_a_ragged_selection_narrows_each_call_differently(self):
        self._run({"include": [[["subject", "01"]], [["subject", "02"], ["minute", "5"]]]})
        assert self.seen == {"01": ["5", "6", "6"], "02": ["5", "5"]}

    def test_a_level_rule_on_a_finer_key_narrows_each_calls_rows(self):
        self._run({"exclude_levels": {"minute": ["5"]}})
        assert self.seen == {"01": ["6", "6"], "02": ["6"]}

    def test_the_result_is_built_from_the_narrowed_rows(self):
        result = self._run({"exclude_levels": {"minute": ["5"]}})
        assert sorted(result["output"].tolist()) == [1.0, 2.0]

    def test_a_table_without_the_key_is_unconstrained(self):
        """A coarser input has no such axis: it serves every selected minute."""
        coarse = pd.DataFrame({"subject": ["01", "02"], "weight": [70.0, 80.0]})
        narrowed = narrow_inputs({"coarse": coarse}, {"exclude_levels": {"minute": ["5"]}})
        assert narrowed["coarse"] is coarse

    def test_a_fixed_input_keeps_the_location_it_names(self):
        """Fixed(minute="5") reads minute 5 on purpose, even in a run about
        minute 6. Narrowing it would leave the function with nothing to read."""
        pinned = Fixed(_minutes_frame(), minute="5")
        narrowed = narrow_inputs({"pinned": pinned}, {"include": [[["minute", "6"]]]})
        assert narrowed["pinned"] is pinned

    def test_merge_constituents_are_narrowed_but_a_fixed_one_is_not(self):
        pinned = Fixed(_minutes_frame(), minute="5")
        merge = Merge(_minutes_frame(), pinned)
        narrowed = narrow_inputs({"m": merge}, {"include": [[["minute", "6"]]]})["m"]
        assert narrowed.tables[0]["minute"].unique().tolist() == ["6"]
        assert narrowed.tables[1] is pinned
        assert len(merge.tables[0]) == 6, "the caller's Merge must not be mutated"

    def test_an_inert_filter_returns_the_same_tables(self):
        frame = _minutes_frame()
        assert narrow_inputs({"table": frame}, {})["table"] is frame
        assert narrow_inputs({"table": frame}, None)["table"] is frame

    def test_a_null_cell_is_not_a_level(self):
        frame = pd.DataFrame({"subject": ["01", "01"], "minute": ["6", None]})
        kept = narrow_frame(frame, {"exclude_levels": {"minute": ["6"]}})
        assert kept["minute"].isna().tolist() == [True]

    def test_spell_lets_a_typed_key_match_its_stored_text(self):
        """scidb's date keys load with their original type while a selection
        names them by stored text: str(Timestamp) would match nothing."""
        when = pd.Timestamp("2026-07-09")
        frame = pd.DataFrame({"day": [when, when + pd.Timedelta(days=1)]})
        location = {"include": [[["day", "2026-07-09"]]]}
        assert len(narrow_frame(frame, location)) == 0
        kept = narrow_frame(frame, location, spell=lambda value: value.strftime("%Y-%m-%d"))
        assert kept["day"].tolist() == [when]

    def test_the_narrowing_says_which_levels_remain(self, caplog):
        with caplog.at_level(logging.INFO, logger="scifor"):
            narrow_inputs(
                {"table": _minutes_frame()},
                {"exclude_levels": {"minute": ["5"]}},
                context="for_each(f)",
            )
        assert "for_each(f): input 'table': locations: 3 of 6 row(s) kept" in caplog.text
        assert "minute=['6']" in caplog.text

    def test_removing_every_row_warns_and_names_the_selection(self, caplog):
        with caplog.at_level(logging.WARNING, logger="scifor"):
            kept = narrow_frame(
                _minutes_frame(), {"include": [[["minute", "9"]]]}, context="input 'x'"
            )
        assert len(kept) == 0
        assert "removed every one of the 6 row(s)" in caplog.text
