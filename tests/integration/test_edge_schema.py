"""
Schema-key edge cases (plan section A) and order-of-steps scenarios (D).

Each test builds what it needs in a fresh scratch database (conftest's
``scratch_db``): a small slice of the example data, loaded by the example's
own functions, then the odd thing — a record with no keys, a key skipped in
the middle, a file deleted, a step run out of order.

The contract is conftest.attempt's: a result or a typed error with a reason.
Where the desired behaviour is not yet decided (A3, A4, A10) the test pins
the contract only, and says so.
"""

from __future__ import annotations

import shutil

import pandas as pd
import pytest

import scidb
from scidb import BaseVariable

from conftest import (
    CYCLES,
    DATA_ROOT,
    JOINTS,
    SESSIONS,
    SPEEDS,
    SUBJECTS,
    TRIALS,
    attempt,
    cell,
    run_summary,
)

ONE_SUBJECT = SUBJECTS[:1]
ONE_SESSION = SESSIONS[:1]
#: Records one subject x one session x 2 speeds x 3 trials x 10 cycles makes.
N_SMALL = len(SPEEDS) * len(TRIALS) * len(CYCLES)


# --- a mini pipeline over any data root, any subset -----------------------------------


def _templates(root):
    return {
        "cycle": scidb.PathInput(
            "{subject}/{session}/t{trial}/{subject}_{session}_{speed}_t{trial}_c{cycle}.csv",
            root_folder=str(root),
        ),
        "trial": scidb.PathInput(
            "{subject}/{session}/t{trial}/{subject}_{session}_{speed}_t{trial}_trial.csv",
            root_folder=str(root),
        ),
        "subject": scidb.PathInput("{subject}/{subject}_demographics.csv", root_folder=str(root)),
    }


def load_levels(pipeline, root=DATA_ROOT, subjects=ONE_SUBJECT, sessions=ONE_SESSION):
    """Cycle symmetry, trial info and demographics for a subset — the three
    inputs ``normalized_knee`` needs — returning each call's summary."""
    t = _templates(root)
    summaries = {}
    _, summaries["cycle"] = run_summary(
        scidb.for_each, pipeline.load_cycle_symmetry, {"csv_file_path": t["cycle"]},
        [pipeline.CycleSymmetry], subject=list(subjects), session=list(sessions),
        speed=[], trial=[], cycle=[],
    )
    _, summaries["trial"] = run_summary(
        scidb.for_each, pipeline.load_trial_info, {"csv_file_path": t["trial"]},
        [pipeline.TrialInfo], subject=list(subjects), session=list(sessions), speed=[], trial=[],
    )
    _, summaries["subject"] = run_summary(
        scidb.for_each, pipeline.load_demographics, {"csv_file_path": t["subject"]},
        [pipeline.Demographics], subject=list(subjects),
    )
    return summaries


def run_normalized_knee(pipeline, subjects=ONE_SUBJECT, sessions=ONE_SESSION):
    return run_summary(
        scidb.for_each, pipeline.normalized_knee,
        {
            "knee": pipeline.CycleSymmetry["knee"],
            "walking_speed_mps": pipeline.TrialInfo["walking_speed_mps"],
            "height_cm": pipeline.Demographics["height_cm"],
        },
        [pipeline.NormalizedKnee],
        subject=list(subjects), session=list(sessions), speed=[], trial=[], cycle=[],
    )


def _count(variable, **where) -> int:
    try:
        return len(variable.load(as_df=True, **where))
    except scidb.NotFoundError:
        return 0


# --- A1: no schema keys at all --------------------------------------------------------


class GlobalKneeMean(BaseVariable):
    """Dataset level: one number for the whole dataset."""


class KneeMinusGlobal(BaseVariable):
    """Cycle level: each cycle's knee minus the dataset-wide mean."""


def _global_mean(cycles):
    return float(cycles["knee"].mean())


def _minus_global(knee, global_mean):
    return float(pd.Series(knee).iloc[0]) - float(pd.Series(global_mean).iloc[0])


def test_a_record_with_no_schema_keys(scratch_db, pipeline):
    load_levels(pipeline)

    # Iterate NOTHING: one call over every record, one dataset-level record out.
    _, summary = run_summary(
        scidb.for_each, _global_mean, {"cycles": pipeline.CycleSymmetry},
        [GlobalKneeMean], as_table=True,
    )
    assert summary.get("completed") == 1, summary
    frame = GlobalKneeMean.load(as_df=True)
    assert len(frame) == 1
    for key in ("subject", "session", "speed", "trial", "cycle"):
        assert frame[key].isna().all(), key

    # It loads with no keys at all …
    record, error = attempt(GlobalKneeMean.load)
    assert error is None, error

    # … and a cycle-level step reads it, broadcast to every cycle.
    _, summary = run_summary(
        scidb.for_each, _minus_global,
        {"knee": pipeline.CycleSymmetry["knee"], "global_mean": GlobalKneeMean},
        [KneeMinusGlobal],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    assert summary.get("completed") == N_SMALL, summary
    centred = KneeMinusGlobal.load(as_df=True)
    assert abs(centred["data"].astype(float).mean()) < 1e-9, "centred on the global mean"


def test_a_dataset_level_record_plots_and_exports(scratch_db, pipeline):
    pytest.importorskip("scistackplotdb")
    from scistackplot import default_spec, plot_data, resolve
    from scistackplotdb import ScidbSource

    load_levels(pipeline)
    scidb.for_each(_global_mean, {"cycles": pipeline.CycleSymmetry}, [GlobalKneeMean], as_table=True)

    table, error = attempt(ScidbSource(scratch_db).get_table, ["GlobalKneeMean"])
    assert error is None, error
    spec = default_spec(table)
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    assert len(figures) == 1 and figures[0].row_count == 1
    data, error = attempt(plot_data, spec, table)
    assert error is None, error
    assert len(data) == 1 and "GlobalKneeMean" in data.columns


# --- A2: non-contiguous keys --------------------------------------------------------


class Calibration(BaseVariable):
    """subject + speed, session skipped: a per-speed gain per subject."""


class CalibratedKnee(BaseVariable):
    pass


def _apply_gain(knee, gain):
    return float(pd.Series(knee).iloc[0]) * float(pd.Series(gain).iloc[0])


def test_a_record_at_subject_and_speed_with_session_skipped(scratch_db, pipeline):
    load_levels(pipeline)
    gains = {"slow": 1.5, "fast": 0.5}
    for speed, gain in gains.items():
        _, error = attempt(
            Calibration.save, pd.DataFrame({"gain": [gain]}), subject=ONE_SUBJECT[0], speed=speed
        )
        assert error is None, f"a non-contiguous location must be saveable: {error}"

    stored = Calibration.load(as_df=True)
    assert len(stored) == 2
    assert stored["session"].isna().all() and stored["trial"].isna().all()
    assert set(stored["speed"]) == set(gains)

    combos = scratch_db.distinct_schema_combinations(["subject", "speed"])
    assert {(ONE_SUBJECT[0], s) for s in gains} <= {tuple(c) for c in combos}

    # A cycle-level step reads it broadcast on (subject, speed) only.
    _, summary = run_summary(
        scidb.for_each, _apply_gain,
        {"knee": pipeline.CycleSymmetry["knee"], "gain": Calibration["gain"]},
        [CalibratedKnee],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    assert summary.get("completed") == N_SMALL, summary
    out = CalibratedKnee.load(as_df=True)
    one = out.iloc[0]
    knee = float(cell(pipeline.CycleSymmetry.load(as_df=True, subject=one["subject"], session=one["session"],
                                                   speed=one["speed"], trial=one["trial"], cycle=one["cycle"])["data"].iloc[0], "knee"))
    assert float(one["data"]) == pytest.approx(knee * gains[one["speed"]])


def test_a_non_contiguous_record_keeps_its_place_in_the_hierarchy(scratch_db, pipeline):
    pytest.importorskip("scistackplotdb")
    from scistackplotdb import ScidbSource

    load_levels(pipeline)
    for speed in SPEEDS:
        Calibration.save(pd.DataFrame({"gain": [1.0]}), subject=ONE_SUBJECT[0], speed=speed)
    table, error = attempt(ScidbSource(scratch_db).get_table, ["Calibration"])
    assert error is None, error
    depths = table.factor_depths
    assert depths["subject"] < depths["speed"], depths


# --- A3 / A4: iteration shapes without a defined answer yet -------------------------------


class Anything(BaseVariable):
    pass


def _first(x):
    return float(pd.Series(x).iloc[0]) if not isinstance(x, pd.DataFrame) else float(len(x))


def test_a_deep_key_iterated_alone_is_a_result_or_a_typed_error(scratch_db, pipeline):
    """``cycle=[]`` and nothing else. Whether that discovers the ancestors or
    refuses is a decision still open; what it must not do is invent a
    Cartesian product or die with a KeyError."""
    load_levels(pipeline)
    outcome, error = attempt(
        run_summary, scidb.for_each, _first, {"x": pipeline.CycleSymmetry["knee"]}, [Anything], cycle=[]
    )
    if error is None:
        _, summary = outcome
        assert summary.get("total", 0) <= N_SMALL, summary


def test_an_input_finer_than_the_iteration_without_as_table(scratch_db, pipeline):
    """Ten cycles per trial, a trial-level loop, no ``as_table``: a typed
    error naming the shape, or a defined aggregation — never the silent
    first row."""
    load_levels(pipeline)
    outcome, error = attempt(
        run_summary, scidb.for_each, _first, {"x": pipeline.CycleSymmetry}, [Anything],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[],
    )
    if error is None:
        _, summary = outcome
        # Either it ran per trial with a defined rule, or every combo failed
        # with a recorded reason. Both are visible; a silent pick is not.
        assert summary.get("completed", 0) + summary.get("failed", 0) + summary.get("no_data", 0) == summary.get("total", 0)
        if summary.get("failed"):
            assert summary.get("failure_reasons"), summary


# --- A5: ragged data -----------------------------------------------------------------------


@pytest.fixture
def ragged_root(tmp_path):
    """The one-subject / one-session slice with trial 02's folder removed."""
    root = tmp_path / "data"
    src = DATA_ROOT / ONE_SUBJECT[0]
    dst = root / ONE_SUBJECT[0]
    shutil.copytree(src / ONE_SESSION[0], dst / ONE_SESSION[0])
    shutil.copy(src / f"{ONE_SUBJECT[0]}_demographics.csv", dst / f"{ONE_SUBJECT[0]}_demographics.csv")
    shutil.rmtree(dst / ONE_SESSION[0] / "t02")
    return root


def test_ragged_data_runs_every_combination_that_exists(scratch_db, pipeline, ragged_root):
    summaries = load_levels(pipeline, root=ragged_root)
    assert summaries["cycle"]["completed"] == N_SMALL - len(SPEEDS) * len(CYCLES)
    assert summaries["trial"]["completed"] == len(SPEEDS) * (len(TRIALS) - 1)

    _, summary = run_normalized_knee(pipeline)
    assert summary.get("completed") == N_SMALL - len(SPEEDS) * len(CYCLES), summary
    assert summary.get("failed", 0) == 0, summary.get("failure_reasons")
    assert set(pipeline.NormalizedKnee.load(as_df=True)["trial"]) == {"01", "03"}


def test_ragged_data_plots_and_exports_without_the_gap(scratch_db, pipeline, ragged_root):
    pytest.importorskip("scistackplotdb")
    from scistackplot import PlotKind, PlotSpec, Role, plot_data, render_plotly, resolve
    from scistackplot.xaxis import is_spacer
    from scistackplotdb import ScidbSource

    load_levels(pipeline, root=ragged_root)
    table = ScidbSource(scratch_db).get_table(["CycleSymmetry"])
    field = table.field_factors[0].name
    spec = PlotSpec(
        measures=["CycleSymmetry"],
        roles={field: Role.FACET, "subject": Role.ITERATE, "session": Role.ITERATE,
               "speed": Role.GROUP, "trial": Role.GROUP, "cycle": Role.COLLAPSE},
        groups=["trial", "speed"],
        kind=PlotKind.BAR,
    )
    figures, error = attempt(resolve, spec, table)
    assert error is None, error
    (figure,) = figures
    leaves = [key for key in figure.x_plan.order if key and not is_spacer(key)]
    assert len(leaves) == len(SPEEDS) * (len(TRIALS) - 1), leaves
    _, error = attempt(render_plotly, figure)
    assert error is None, error
    data = plot_data(spec, table, fields_as_columns=False)
    assert set(data["trial"]) == {"01", "03"}


# --- A6 / A7: key lists that match nothing, or repeat the discovery -------------------------


def test_a_key_value_that_matches_nothing_fails_visibly_and_saves_nothing(scratch_db, pipeline):
    """An EXPLICIT value the user typed is authoritative (scidb.foreach, Step
    3): the Cartesian product drives the iteration and every combination with
    no file behind it fails at runtime, which is what the GUI shows as
    "missing". So `session=["week99"]` is not zero iterations — it is N
    failures, each naming the file — and never a record."""
    t = _templates(DATA_ROOT)
    outcome, error = attempt(
        run_summary, scidb.for_each, pipeline.load_cycle_symmetry, {"csv_file_path": t["cycle"]},
        [pipeline.CycleSymmetry], subject=ONE_SUBJECT, session=["week99"], speed=[], trial=[], cycle=[],
    )
    if error is None:
        _, summary = outcome
        assert summary.get("completed", 0) == 0, summary
        if summary.get("failed"):
            reasons = " ".join(summary.get("failure_reasons") or {})
            assert "No such file" in reasons or "week99" in reasons, reasons
    assert _count(pipeline.CycleSymmetry) == 0


def test_an_explicit_key_list_narrows_the_discovery(scratch_db, pipeline):
    t = _templates(DATA_ROOT)
    _, summary = run_summary(
        scidb.for_each, pipeline.load_cycle_symmetry, {"csv_file_path": t["cycle"]},
        [pipeline.CycleSymmetry], subject=ONE_SUBJECT, session=ONE_SESSION,
        speed=["slow"], trial=["01"], cycle=[],
    )
    assert summary.get("completed") == len(CYCLES), summary
    frame = pipeline.CycleSymmetry.load(as_df=True)
    assert set(frame["speed"]) == {"slow"} and set(frame["trial"]) == {"01"}


# --- A8 / A9: a second variant, by parameter and by code ----------------------------------


def test_a_changed_parameter_is_a_second_variant(scratch_db, pipeline):
    pytest.importorskip("scistackplotdb")
    from scistackplotdb import ScidbSource

    load_levels(pipeline)
    for value in (50, 75):
        threshold = scidb.Parameter(value, description="edge")
        scidb.for_each(
            pipeline.ankle_over_threshold,
            {"ankle": pipeline.CycleSymmetry["ankle"], "threshold": threshold},
            [pipeline.AnkleOverThreshold],
            subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
        )
    every = pipeline.AnkleOverThreshold.load(as_df=True, version="all")
    assert len(every) == 2 * N_SMALL
    table = ScidbSource(scratch_db).get_table(["AnkleOverThreshold"])
    (axis,) = table.variant_factors
    assert sorted(str(v) for v in axis.levels) == ["50", "75"]


class KneeTwice(BaseVariable):
    pass


def test_a_changed_function_body_is_a_code_version_variant(scratch_db, pipeline):
    load_levels(pipeline)
    where = dict(subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[])

    def knee_twice(knee):
        return float(pd.Series(knee).iloc[0]) * 2

    scidb.for_each(knee_twice, {"knee": pipeline.CycleSymmetry["knee"]}, [KneeTwice], **where)

    def knee_twice(knee):  # noqa: F811 — the edit
        return float(pd.Series(knee).iloc[0]) * 2 + 1

    scidb.for_each(knee_twice, {"knee": pipeline.CycleSymmetry["knee"]}, [KneeTwice], **where)

    assert len(KneeTwice.load(as_df=True, version="all")) == 2 * N_SMALL
    latest = KneeTwice.load(as_df=True)
    assert len(latest) == N_SMALL
    one = latest.iloc[0]
    knee = float(cell(pipeline.CycleSymmetry.load(as_df=True, subject=one["subject"], session=one["session"],
                                                   speed=one["speed"], trial=one["trial"], cycle=one["cycle"])["data"].iloc[0], "knee"))
    assert float(one["data"]) == pytest.approx(knee * 2 + 1), "latest is the edited body"
    if "record_id" in latest.columns:
        tree = scratch_db.inspect.provenance(record_id=latest.iloc[0]["record_id"])
        root = next(n for n in tree.nodes if n.record_id == tree.root_record_id)
        assert root.function_hash


# --- A10 / A11: two producers; a direct save beside for_each ---------------------------------


def _fake_trial_info(csv_file_path):
    return pd.DataFrame({"duration_s": [1.0], "walking_speed_mps": [1.0]})


def test_the_same_variable_from_two_producers_is_visible_not_silent(scratch_db, pipeline):
    load_levels(pipeline)
    t = _templates(DATA_ROOT)
    scidb.for_each(
        _fake_trial_info, {"csv_file_path": t["trial"]}, [pipeline.TrialInfo],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[],
    )
    every = pipeline.TrialInfo.load(as_df=True, version="all")
    assert len(every) == 2 * len(SPEEDS) * len(TRIALS), "both producers' records exist"
    # Reading it downstream: a defined pick (the latest) or a typed ambiguity
    # error — never one producer's rows for some combos and the other's for
    # the rest without saying so.
    outcome, error = attempt(run_normalized_knee, pipeline)
    if error is None:
        _, summary = outcome
        assert summary.get("completed", 0) + summary.get("failed", 0) == summary.get("total"), summary


def test_a_direct_save_beside_for_each_records(scratch_db, pipeline):
    load_levels(pipeline)
    extra = dict(subject=ONE_SUBJECT[0], session=ONE_SESSION[0], speed="slow", trial="99", cycle="01")
    _, error = attempt(pipeline.CycleSymmetry.save, pd.DataFrame({j: [0.0] for j in JOINTS}), **extra)
    assert error is None, error
    frame = pipeline.CycleSymmetry.load(as_df=True)
    assert len(frame) == N_SMALL + 1
    assert "99" in set(frame["trial"])
    if "record_id" in frame.columns:
        rid = frame[frame["trial"] == "99"].iloc[0]["record_id"]
        tree = scratch_db.inspect.provenance(record_id=rid)
        root = next(n for n in tree.nodes if n.record_id == tree.root_record_id)
        assert root.function_name in (None, "", "__save__")


# --- A12: two spellings of one subject ---------------------------------------------------------


class Spelled(BaseVariable):
    pass


def test_two_spellings_of_one_key_never_become_two_types(scratch_db):
    _, error = attempt(Spelled.save, 1.0, subject="01")
    assert error is None, error
    _, error = attempt(Spelled.save, 2.0, subject=1)
    # Either the second spelling is refused, or both are stored — as strings.
    frame = Spelled.load(as_df=True)
    assert {type(v) for v in frame["subject"]} == {str}, frame["subject"].tolist()
    assert len(frame) in (1, 2)


# --- D: order of steps -------------------------------------------------------------------------


def test_a_step_run_before_its_loader_then_after(scratch_db, pipeline):
    """The GUI lets a user click a downstream node first."""
    outcome, error = attempt(run_normalized_knee, pipeline)
    if error is None:
        _, summary = outcome
        assert summary.get("completed", 0) == 0, summary
    assert _count(pipeline.NormalizedKnee) == 0, "nothing computed from nothing"

    load_levels(pipeline)
    _, summary = run_normalized_knee(pipeline)
    assert summary.get("completed") == N_SMALL, summary


def test_a_loader_rerun_with_nothing_changed_adds_nothing(scratch_db, pipeline):
    load_levels(pipeline)
    before = len(pipeline.CycleSymmetry.load(as_df=True, version="all"))
    load_levels(pipeline)
    assert len(pipeline.CycleSymmetry.load(as_df=True, version="all")) == before


@pytest.fixture
def editable_root(tmp_path):
    root = tmp_path / "data"
    src = DATA_ROOT / ONE_SUBJECT[0]
    dst = root / ONE_SUBJECT[0]
    shutil.copytree(src / ONE_SESSION[0], dst / ONE_SESSION[0])
    shutil.copy(src / f"{ONE_SUBJECT[0]}_demographics.csv", dst / f"{ONE_SUBJECT[0]}_demographics.csv")
    return root


def _rewrite(path, header: str, row: str) -> None:
    path.write_text(f"{header}\n{row}\n")


def test_one_changed_file_versions_only_its_record(scratch_db, pipeline, editable_root):
    load_levels(pipeline, root=editable_root)
    before_all = len(pipeline.CycleSymmetry.load(as_df=True, version="all"))

    target = editable_root / ONE_SUBJECT[0] / ONE_SESSION[0] / "t01" / f"{ONE_SUBJECT[0]}_{ONE_SESSION[0]}_slow_t01_c05.csv"
    _rewrite(target, ",".join(JOINTS), "1.0,2.0,3.0")
    load_levels(pipeline, root=editable_root)

    every = pipeline.CycleSymmetry.load(as_df=True, version="all")
    per_location = every.groupby(["speed", "trial", "cycle"]).size()
    doubled = per_location[per_location > 1]
    assert len(every) == before_all + 1, (
        f"exactly one record should gain a version; locations with >1 version:\n"
        f"{doubled.to_string()}"
    )
    latest = pipeline.CycleSymmetry.load(as_df=True, subject=ONE_SUBJECT[0], session=ONE_SESSION[0], speed="slow", trial="01", cycle="05")
    assert len(latest) == 1
    assert float(cell(latest.iloc[0]["data"], "ankle")) == 1.0


def test_a_coarse_rerun_reaches_only_the_fine_records_that_read_it(scratch_db, pipeline, editable_root):
    load_levels(pipeline, root=editable_root)
    run_normalized_knee(pipeline)
    before_all = len(pipeline.NormalizedKnee.load(as_df=True, version="all"))

    target = editable_root / ONE_SUBJECT[0] / ONE_SESSION[0] / "t02" / f"{ONE_SUBJECT[0]}_{ONE_SESSION[0]}_fast_t02_trial.csv"
    _rewrite(target, "duration_s,walking_speed_mps", "30.0,9.99")
    load_levels(pipeline, root=editable_root)
    run_normalized_knee(pipeline)

    after_all = len(pipeline.NormalizedKnee.load(as_df=True, version="all"))
    assert after_all == before_all + len(CYCLES), (
        "only the ten cycles of the changed trial should have been recomputed"
    )
    changed = pipeline.NormalizedKnee.load(as_df=True, subject=ONE_SUBJECT[0], session=ONE_SESSION[0], speed="fast", trial="02")
    assert len(changed) == len(CYCLES)
    knee = pipeline.CycleSymmetry.load(as_df=True, subject=ONE_SUBJECT[0], session=ONE_SESSION[0], speed="fast", trial="02", cycle="01")
    height = pipeline.Demographics.load(as_df=True, subject=ONE_SUBJECT[0])
    expected = float(cell(knee.iloc[0]["data"], "knee")) / (9.99 * float(cell(height.iloc[0]["data"], "height_cm")))
    got = changed[changed["cycle"] == "01"].iloc[0]["data"]
    assert float(got) == pytest.approx(expected)
