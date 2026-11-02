"""``scifor.for_each`` is ``plan_run`` -> ``run_combo`` per combo -> ``assemble``.

Characterizes the planning phase on its own (P4 stage 1, design review 02 R1):
the combos, output names, as_table set, distribute target and for_columns
axis it decides must be what the single 1,200-line ``for_each`` decided
before the split. Also pins the two contracts the split introduced:

* ``run_combo`` RETURNS a failure (the loop decides skip vs. stop) instead
  of raising it;
* loop hooks travel in ``hooks=LoopHooks(...)``; a leftover ``_progress_fn=``
  raises instead of silently becoming an iteration axis.
"""

import pandas as pd
import pytest

from scifor import ColumnSelection, LocationFilter, LoopHooks, for_each, set_schema
from scifor.foreach import RunTally, plan_run, run_combo


def setup_function():
    set_schema(["subject", "session"])


def _df():
    return pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "session": ["pre", "post", "pre", "post"],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [5.0, 6.0, 7.0, 8.0],
        }
    )


def _plan(inputs, outputs=None, hooks=None, **kwargs):
    metadata = {k: kwargs.pop(k) for k in list(kwargs) if k in ("subject", "session", "trial")}
    return plan_run(
        lambda **kw: None,
        inputs,
        outputs,
        metadata,
        hooks=hooks or LoopHooks(),
        key_types={},
        **kwargs,
    )


# --- plan_run ------------------------------------------------------------------


def test_cartesian_combos_in_key_order():
    plan = _plan({"x": _df()}, ["out"], subject=[1, 2], session=["pre", "post"])
    assert plan.keys == ["subject", "session"]
    assert plan.combos == [
        {"subject": 1, "session": "pre"},
        {"subject": 1, "session": "post"},
        {"subject": 2, "session": "pre"},
        {"subject": 2, "session": "post"},
    ]
    assert plan.total == 4
    assert plan.output_names == ["out"]


@pytest.mark.parametrize(
    "outputs, names",
    [(None, ["output"]), (2, ["output_1", "output_2"]), (["p", "q"], ["p", "q"])],
)
def test_output_names(outputs, names):
    assert _plan({"x": _df()}, outputs, subject=[1]).output_names == names


def test_empty_list_resolves_from_dataframe_inputs():
    plan = _plan({"x": _df()}, subject=[], session=["pre"])
    assert plan.metadata_iterables["subject"] == [1, 2]
    assert [c["subject"] for c in plan.combos] == [1, 2]


def test_data_and_constant_inputs_and_as_table():
    inputs = {"x": _df(), "y": _df(), "k": 3}
    plan = _plan(inputs, subject=[1], as_table=["y"])
    assert set(plan.data_inputs) == {"x", "y"}
    assert plan.constant_inputs == {"k": 3}
    assert plan.as_table_set == {"y"}
    assert _plan(inputs, subject=[1], as_table=True).as_table_set == {"x", "y"}
    assert _plan(inputs, subject=[1]).as_table_set == set()


def test_prebuilt_combos_win_and_locations_still_narrow_them():
    combos = [{"subject": 1, "session": "pre"}, {"subject": 2, "session": "pre"}]
    plan = _plan(
        {"x": _df()},
        hooks=LoopHooks(all_combos=combos),
        subject=[1, 2, 3],
        session=["pre", "post"],
        locations=LocationFilter.build(include=[[("subject", 2)]]),
    )
    assert plan.combos == [{"subject": 2, "session": "pre"}]


def test_distribute_target_is_one_level_below_the_deepest_iterated_key():
    set_schema(["subject", "session", "trial"])
    plan = _plan({"x": _df()}, subject=[1], session=["pre"], distribute=True)
    assert plan.distribute_key == "trial"
    assert _plan({"x": _df()}, subject=[1]).distribute_key is None


def test_for_columns_axis():
    plan = _plan(
        {"x": ColumnSelection(_df(), ["a", "b"], iterate=True)},
        subject=[1],
        session=["pre"],
    )
    assert plan.for_columns is not None
    assert plan.iterate_params == ["x"]
    assert plan.for_columns.columns == ["a", "b"]
    assert plan.for_columns is not None and plan.for_columns.mode


def test_plan_does_not_call_the_function():
    calls = []
    plan_run(
        lambda x: calls.append(x),
        {"x": _df()},
        None,
        {"subject": [1, 2]},
        hooks=LoopHooks(),
        key_types={},
    )
    assert calls == []


def test_option_parts_name_only_non_default_options():
    assert _plan({"x": _df()}, subject=[1]).option_parts == ()
    plan = _plan({"x": _df()}, subject=[1], dry_run=True, as_table=True)
    assert plan.option_parts == ("dry_run=True", "as_table=True")


# --- run_combo -----------------------------------------------------------------


def test_run_combo_returns_rows():
    plan = plan_run(
        lambda x: x * 10,
        {"x": ColumnSelection(_df(), ["a"])},
        ["y"],
        {"subject": [1], "session": ["pre"]},
        hooks=LoopHooks(),
        key_types={},
    )
    outcome = run_combo(plan, plan.combos[0], LoopHooks(), RunTally())
    assert outcome.failure is None
    [(meta, values)] = outcome.rows
    assert meta == {"subject": 1, "session": "pre"}
    assert values == (10.0,)


def test_run_combo_returns_a_failure_instead_of_raising():
    def broken(x):
        raise KeyError("missing_channel")

    plan = plan_run(
        broken,
        {"x": _df()},
        ["y"],
        {"subject": [1], "session": ["pre"]},
        hooks=LoopHooks(),
        key_types={},
    )
    tally = RunTally()
    outcome = run_combo(plan, plan.combos[0], LoopHooks(), tally)
    assert outcome.rows == []
    assert outcome.failure is not None and not outcome.failure.no_data
    assert isinstance(outcome.error, KeyError)
    assert len(tally.failure_groups) == 1


# --- for_each entry --------------------------------------------------------------


def test_private_keyword_raises_instead_of_becoming_an_axis():
    with pytest.raises(TypeError, match="hooks=LoopHooks"):
        for_each(lambda x: x, {"x": _df()}, subject=[1], _progress_fn=print)


def test_hooks_default_is_no_hooks():
    events = []
    with_hooks = for_each(
        lambda x: x,
        {"x": ColumnSelection(_df(), ["a"])},
        subject=[1, 2],
        session=["pre"],
        hooks=LoopHooks(progress_fn=events.append),
    )
    without = for_each(
        lambda x: x, {"x": ColumnSelection(_df(), ["a"])}, subject=[1, 2], session=["pre"]
    )
    pd.testing.assert_frame_equal(with_hooks, without)
    assert [e["event"] for e in events] == [
        "combo_start",
        "combo_done",
        "combo_start",
        "combo_done",
        "summary",
    ]
