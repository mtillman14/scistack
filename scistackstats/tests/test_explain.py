"""The wizard's facts and lay wording (plan-statstudio-guided, Stage 1).

``explain`` is the one owner of plain-language text, ``guide`` gathers the
facts from their owners. Checked here: every catalog entry has a concept,
the steps come in ``GUIDE_STEPS`` order, the C-Brace shape (an empty cell,
incomplete units, a key above the unit) is explained, and the steps block
exactly when the user must decide something.
"""

from __future__ import annotations

from conftest import bar_spec
from scistackplot import Role
from scistackstats import (
    CONCEPTS, EFFECT_SIZES, GUIDE_STEPS, TESTS, Analysis, FakeEngine, GuideState, StatsSpec, guide, run,
    seed_from_plot, use_engine,
)
from scistackstats.codegen import generate_stats_function
from scistackstats.design import cell_counts, derive_design
from scistackstats.restore import restore_stats_spec
from scistackstats.run import CORRECTION_LABELS
from scistackstats.spec import DIFFERENCE_SPREADS, MISSING_POLICIES
from scistackstats.tables import analysis_frame
from test_empty_cells import _cbrace_table, _spec as _cbrace_spec
from test_missing_and_recommend import _incomplete_table, _spec as _session_spec


def _steps(spec: StatsSpec, table, *, results: bool = False) -> dict[str, dict]:
    tables = {spec.targets[0].variable: table}
    report = None
    if results:
        with use_engine(FakeEngine()):
            report = run(spec, tables)
    payload = guide(spec, tables, report)
    assert [s["id"] for s in payload["steps"]] == list(GUIDE_STEPS)
    assert payload["has_results"] is results
    for s in payload["steps"]:
        assert all(c in payload["concepts"] for c in s["concepts"]), s["id"]
    return {s["id"]: s for s in payload["steps"]}


def _choice(step: dict, choice_id: str) -> dict:
    return next(c for card in step["cards"] for c in card["choices"] if c["id"] == choice_id)


# --- the concepts ---------------------------------------------------------------


def test_every_test_correction_effect_size_and_policy_has_a_concept():
    missing = [f"test.{t}" for t in TESTS if f"test.{t}" not in CONCEPTS]
    missing += [f"correction.{c}" for c in CORRECTION_LABELS if f"correction.{c}" not in CONCEPTS]
    missing += [f"effect.{e}" for e in EFFECT_SIZES if f"effect.{e}" not in CONCEPTS]
    missing += [f"missing.{m}" for m in MISSING_POLICIES if f"missing.{m}" not in CONCEPTS]
    missing += [f"spread.{s}" for s in DIFFERENCE_SPREADS if f"spread.{s}" not in CONCEPTS]
    assert missing == []


def test_every_concept_is_short_and_has_both_names():
    for c in CONCEPTS.values():
        assert c.title and c.technical and c.lay, c.id
        assert c.lay.count(". ") <= 3, f"{c.id}: the short text is 1-3 sentences; move the rest to 'more'"


# --- the guide state --------------------------------------------------------------


def test_a_seeded_table_opens_guided_and_a_stored_one_without_a_mode_opens_advanced():
    seeded = seed_from_plot(bar_spec()).spec
    assert seeded.guide.mode == "guided"
    raw = seeded.to_dict()
    raw.pop("guide")
    assert StatsSpec.from_dict(raw).guide.mode == "advanced"
    assert restore_stats_spec(raw).spec.guide.mode == "advanced"


def test_the_guide_state_round_trips_in_step_order():
    spec = StatsSpec(design=bar_spec(), guide=GuideState(mode="guided", step="test", done=("unit", "measure")))
    back = StatsSpec.from_dict(spec.to_dict())
    assert back == spec and back.guide.done == ("measure", "unit")


def test_the_generated_function_does_not_embed_the_view_state():
    from conftest import popcorn_table

    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(),), guide=GuideState(mode="guided", step="test"))
    fn = generate_stats_function(spec, {"Ratings": popcorn_table()})
    assert "'guide'" not in fn.source


# --- the facts: cell counts ---------------------------------------------------------


def test_cell_counts_count_units_per_cell_and_mark_the_empty_one():
    spec = _cbrace_spec()
    frame = analysis_frame(spec, spec.targets[0], _cbrace_table())
    stratum, rows = frame.stratum_groups()[0]
    counts = {tuple(c["cell"].values()): (c["units"], c["rows"]) for c in cell_counts(derive_design(frame, stratum, rows), rows)}
    assert counts[("MO3", "Traditional")] == (0, 0)
    assert counts[("Post", "CBrace")] == (3, 3), "CB2 missed Post"
    assert counts[("BL", "Traditional")] == (4, 4)


# --- the C-Brace walk-through (preview: no tests run) ---------------------------------


def test_preview_explains_the_unit_above_key_and_the_empty_cell():
    steps = _steps(_cbrace_spec(), _cbrace_table())
    unit = steps["unit"]
    assert any("8 subjects" in f for f in unit["facts"])
    above = [f for f in unit["facts"] if "counted within its group" in f]
    assert above, unit["facts"]
    # The example names a real subject from the data, never a made-up "01".
    subjects = {str(s) for s in _cbrace_table().frame["subject"].unique()}
    assert any(f"(such as {s})" in above[0] for s in subjects), above[0]
    assert '"01"' not in above[0]
    assert "under two different groups counts as two different subjects" in above[0]
    kinds = steps["within_between"]
    assert _choice(kinds, "group:between")["recommended"] and _choice(kinds, "group:between")["selected"]
    assert _choice(kinds, "session:within")["recommended"]
    assert kinds["blocked"] is None
    check = steps["data_check"]
    assert any("Nobody was measured in: session=MO3 / group=Traditional" in f for f in check["facts"])
    assert "empty_cells" in check["concepts"]
    assert check["table"]["columns"] == ["session", "group", "subjects"]
    empty = check["table"]["empty_rows"]
    assert [check["table"]["rows"][i][:2] for i in empty] == [["MO3", "Traditional"]]
    assert _choice(check, "empty:separate:session")["action"] == {"op": "set_role", "factor": "session",
                                                                    "role": "iterate"}
    assert _choice(check, "empty:exclude:session:MO3")["action"]["op"] == "exclude_level"
    assert _choice(check, "missing:use_all")["recommended"]
    assert all(s["blocked"] is None for s in steps.values())


def test_the_steps_that_need_results_say_so_in_a_preview():
    steps = _steps(_cbrace_spec(), _cbrace_table())
    assert [k for k, s in steps.items() if s["needs_results"]] == ["test", "multiple_comparisons", "effect_size",
                                                                     "results"]
    assert steps["test"]["cards"] == []


def test_with_results_the_test_step_explains_why_no_overall_test():
    steps = _steps(_cbrace_spec(), _cbrace_table(), results=True)
    test = steps["test"]
    assert any("no overall test is recommended" in f for f in test["facts"])
    assert "no overall test" in test["recommendation"]
    assert _choice(test, "pairwise:recommended")["selected"]
    results = steps["results"]
    assert any(f.startswith("2 of 3 comparisons ran") for f in results["facts"])
    assert any("no rows for group = Traditional" in w for w in results["warnings"])
    mc = steps["multiple_comparisons"]
    assert _choice(mc, "correction:holm")["recommended"] and _choice(mc, "correction:holm")["selected"]


def test_an_effect_size_choice_clears_back_to_the_default():
    steps = _steps(_cbrace_spec(), _cbrace_table(), results=True)
    effect = steps["effect_size"]
    default = next(c for card in effect["cards"] if card["id"] == "effect" for c in card["choices"] if c["recommended"])
    assert default["action"] == {"op": "set_analysis", "patch": {"effect_size": None}}


# --- blocking -----------------------------------------------------------------------


def test_a_factor_needing_a_decision_blocks_its_step_with_the_choices():
    steps = _steps(_session_spec(Analysis()), _incomplete_table(session_in_schema=False))
    kinds = steps["within_between"]
    assert kinds["blocked"] and "one decision" in kinds["blocked"]
    assert "mixed_membership" in kinds["concepts"]
    within = _choice(kinds, "session:within")
    assert within["action"] == {"op": "set_override", "analysis": "a1", "factor": "session", "kind": "within"}
    assert not within["recommended"] and not _choice(kinds, "session:between")["recommended"]


def test_nothing_compared_blocks_the_factors_step():
    spec = StatsSpec(design=bar_spec(measures=["Dist_m"], groups=[],
                                     roles={"group": Role.ITERATE, "session": Role.ITERATE, "subject": Role.COLLAPSE}),
                     comparisons=(Analysis(),))
    steps = _steps(spec, _cbrace_table())
    assert steps["factors"]["blocked"] == "Choose at least one thing to compare."
    subject = next(c for c in steps["factors"]["cards"] if c["id"] == "role:subject")
    assert _choice(steps["factors"], "subject:group")["disabled"]
    assert _choice(steps["factors"], "subject:collapse")["selected"]
    assert subject["lay"].startswith("Your independent unit")
