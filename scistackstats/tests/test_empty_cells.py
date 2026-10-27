"""Empty cells and keys above the unit (scistack.log 2026-10-20, the C-Brace data).

The shape under test: ``group`` is a schema key ABOVE the unit
(``group / subject``), ``session`` is nested below it, one session has no
Traditional subjects at all (an empty group × session cell), and one CBrace
subject missed a session (an incomplete unit). The log showed three defects:

- the recommendation was a full-interaction mixed model, which csv-stats
  refused as rank deficient, taking every contrast with it (135 of 135
  refused);
- the model check called ``group`` "the experimental unit";
- the pairs in the empty cell were refused with csv-stats' raw
  "levels ['Traditional'] are not in column 'group'".
"""

from __future__ import annotations

import pandas as pd

from conftest import bar_spec
from scistackplot import LongTable, Role
from scistackstats import Analysis, DV, FakeEngine, StatsSpec, for_frame, run, use_engine
from scistackstats.design import BETWEEN, WITHIN, derive_design, empty_cells
from scistackstats.tables import analysis_frame

SESSIONS = ["BL", "Post", "MO3"]
CBRACE = ["CB1", "CB2", "CB3", "CB4"]
TRADITIONAL = ["T1", "T2", "T3", "T4"]
ROLES = {"group": Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE}


def _cbrace_table() -> LongTable:
    rows = []
    for group, subjects in (("CBrace", CBRACE), ("Traditional", TRADITIONAL)):
        for si, subject in enumerate(subjects):
            for ti, session in enumerate(SESSIONS):
                if group == "Traditional" and session == "MO3":
                    continue  # the empty cell: no Traditional subject has MO3
                if subject == "CB2" and session == "Post":
                    continue  # an incomplete unit
                value = 100.0 + 10 * ti + 3 * si + (5.0 if group == "CBrace" else 0.0) + 0.7 * ((si * 5 + ti) % 3)
                rows.append({"group": group, "subject": subject, "session": session, "Dist_m": value})
    return LongTable.from_frame(
        pd.DataFrame(rows), factors=["group", "subject", "session"], measures=["Dist_m"], name="Dist_m",
        schema_levels=["group", "subject", "session"], level_order={"session": SESSIONS},
    )


def _spec(analysis: Analysis | None = None) -> StatsSpec:
    # groups innermost first: group is the primary (compared within each session).
    design = bar_spec(measures=["Dist_m"], roles=ROLES, groups=["group", "session"])
    return StatsSpec(design=design, comparisons=(analysis or Analysis(),))


def _frame():
    return analysis_frame(_spec(), DV("Dist_m"), _cbrace_table())


def _run(analysis: Analysis | None = None, engine=None):
    with use_engine(engine or FakeEngine()):
        return run(_spec(analysis), {"Dist_m": _cbrace_table()})


# --- the design ----------------------------------------------------------------


def test_the_unit_includes_the_key_above_it_and_group_is_between():
    frame = _frame()
    assert frame.unit == "subject" and frame.unit_keys == ["group", "subject"]
    stratum, rows = frame.stratum_groups()[0]
    design = derive_design(frame, stratum, rows)
    assert design.factor("group").kind == BETWEEN
    assert design.factor("session").kind == WITHIN
    # Every Traditional subject counts as incomplete too: they lack MO3
    # because nobody in their group has it (the empty cell), as in the log's
    # "12 subject(s) lack some within levels".
    assert set(design.incomplete_units) == {"CBrace / CB2", *(f"Traditional / {s}" for s in TRADITIONAL)}


def test_empty_cells_names_the_missing_combination_outer_first():
    frame = _frame()
    stratum, rows = frame.stratum_groups()[0]
    design = derive_design(frame, stratum, rows)
    assert design.empty_cells == [{"session": "MO3", "group": "Traditional"}]
    assert empty_cells(design, rows) == design.empty_cells
    assert design.to_dict()["empty_cells"] == [{"session": "MO3", "group": "Traditional"}]


def test_one_factor_has_no_empty_cells():
    spec = StatsSpec(design=bar_spec(measures=["Dist_m"], roles={**ROLES, "session": Role.COLLAPSE},
                                     groups=["group"]))
    frame = analysis_frame(spec, DV("Dist_m"), _cbrace_table())
    stratum, rows = frame.stratum_groups()[0]
    assert derive_design(frame, stratum, rows).empty_cells == []


# --- the model check -------------------------------------------------------------


def test_a_key_above_the_unit_is_a_factor_not_the_unit():
    m = for_frame(None, (), _frame())
    assert m.ok, m.errors
    assert not any("identifies the experimental unit" in e["message"] for e in m.errors)
    assert "group" in m.fixed_factors


def test_the_unit_itself_in_the_fixed_part_is_still_an_error():
    m = for_frame("session * group * subject + (1 | subject)", (), _frame())
    assert any("subject identifies the experimental unit" in e["message"] for e in m.errors)


# --- the recommendation and the run -----------------------------------------------


def test_empty_cells_never_recommend_a_model_with_every_interaction():
    report = _run()
    (stratum,) = report.analyses[0].strata
    rec = stratum.selection["recommended"]
    assert rec["omnibus"] is None
    assert rec["pairwise"] not in ("lmm_pairwise", None)
    assert "session=MO3 / group=Traditional" in rec["evidence"][0]
    assert "can't be estimated" in rec["evidence"][0]
    assert stratum.selection["omnibus"] is None


def test_the_cells_with_data_are_compared_and_the_empty_one_says_why():
    report = _run()
    comps = report.analyses[0].comparisons()
    ran = [c for c in comps if c.refused is None]
    refused = [c for c in comps if c.refused is not None]
    assert {c.context["session"] for c in ran} == {"BL", "Post"}
    (empty,) = refused
    assert empty.context == {"session": "MO3"}
    assert empty.refused.startswith("no rows for group = Traditional at session=MO3")
    assert "are not in column" not in empty.refused


def test_a_chosen_mixed_model_is_still_run_and_refused_by_csvstats():
    """The recommendation changed, not the choice: lmm stays selectable and
    csv-stats' own refusal is what the user sees."""
    engine = FakeEngine(refuse={"lmm": ("the fixed-effect design is rank deficient (5 of 6 columns)", "invalid")})
    report = _run(Analysis(omnibus_test="lmm"), engine)
    (stratum,) = report.analyses[0].strata
    assert stratum.selection["omnibus"] == "lmm"
    assert "rank deficient" in stratum.omnibus.refused
