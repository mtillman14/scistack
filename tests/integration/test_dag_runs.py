"""
The DAG side: running a node the way the canvas does.

``api/run._run_in_thread`` is what the Run button reaches after the JSON-RPC
handler — targets derived from the node's history, the panel's run options,
where filters, the schema-location selection and the node config's column
selections applied, then ``for_each``, then ``run_done``. Called here on the
test thread with the pushes captured, over a scratch database seeded by the
example pipeline's own functions.
"""

from __future__ import annotations

import shutil

import pandas as pd
import pytest

pytest.importorskip("scistack_gui")

import scidb  # noqa: E402
from scistack_gui.api import run as run_api  # noqa: E402
from scistack_gui.api import ws as ws_mod  # noqa: E402
from scistack_gui import pipeline_store  # noqa: E402

from conftest import CYCLES, DATA_ROOT, PIPELINE_DIR, SPEEDS, TRIALS  # noqa: E402
from test_edge_schema import ONE_SESSION, ONE_SUBJECT, load_levels, run_normalized_knee  # noqa: E402


@pytest.fixture
def pushes(monkeypatch):
    messages: list = []
    monkeypatch.setattr(ws_mod, "push_message", messages.append)
    # api/run imports push_message by name in places; patch there too.
    if hasattr(run_api, "push_message"):
        monkeypatch.setattr(run_api, "push_message", messages.append)
    return messages


@pytest.fixture
def root(tmp_path):
    """An editable copy of one subject / one session, so a test can change a
    file and re-run the LOADER — the way a recompute really happens, and the
    only way that keeps one producer per location."""
    dst = tmp_path / "data" / ONE_SUBJECT[0]
    src = DATA_ROOT / ONE_SUBJECT[0]
    shutil.copytree(src / ONE_SESSION[0], dst / ONE_SESSION[0])
    shutil.copy(src / f"{ONE_SUBJECT[0]}_demographics.csv", dst / f"{ONE_SUBJECT[0]}_demographics.csv")
    return tmp_path / "data"


def _trial_file(root, speed, trial):
    return root / ONE_SUBJECT[0] / ONE_SESSION[0] / f"t{trial}" / f"{ONE_SUBJECT[0]}_{ONE_SESSION[0]}_{speed}_t{trial}_trial.csv"


def _rewrite(path, header, row):
    path.write_text(f"{header}\n{row}\n")


@pytest.fixture
def registered(pipeline):
    """The GUI's function registry, filled the way server startup fills it —
    the run path resolves a function by NAME through it."""
    from scistack_gui import registry

    registry.register_module(pipeline, module_path=PIPELINE_DIR / "pipeline.py")
    return registry


@pytest.fixture
def seeded(scratch_db, pipeline, as_gui_db, root, registered):
    """A scratch database with history for every function the tests run."""
    as_gui_db(scratch_db)
    pipeline_store._ensure_tables(scratch_db)
    load_levels(pipeline, root=root)
    t = scidb.PathInput(
        "{subject}/{session}/t{trial}/waveforms/{subject}_{session}_{speed}_t{trial}_c{cycle}.csv",
        root_folder=str(root),
    )
    scidb.for_each(
        pipeline.load_cycle_waveform, {"csv_file_path": t}, [pipeline.CycleWaveform],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    scidb.for_each(
        pipeline.knee_excursion, {"knee": pipeline.CycleWaveform["knee"]}, [pipeline.KneeExcursion],
        subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[], cycle=[],
    )
    run_normalized_knee(pipeline)
    scidb.for_each(
        pipeline.trial_mean_symmetry, {"cycles": pipeline.CycleSymmetry}, [pipeline.TrialMeanSymmetry],
        as_table=["cycles"], subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[],
    )
    return scratch_db


def _run(db, function_name, pushes, *, expect_success: bool = True, **kwargs) -> dict:
    """Run synchronously, as the RPC handler's thread would; return run_done.

    On an unexpected failure the assertion carries the run's OUTPUT lines —
    the for_each summary and the per-combination reasons — which is where a
    "0 of 66 completed" explains itself."""
    run_id = f"t-{function_name}-{len(pushes)}"
    run_api._run_in_thread(run_id, function_name, kwargs.pop("variants", []), db, **kwargs)
    done = [m for m in pushes if m.get("type") == "run_done" and m.get("run_id") == run_id]
    assert done, f"no run_done for {function_name}: {[m.get('type') for m in pushes]}"
    result = done[-1]
    if expect_success and not result.get("success"):
        output = [m.get("text", "") for m in pushes if m.get("type") == "run_output" and m.get("run_id") == run_id]
        tail = "\n".join(line for line in output if line.strip())[-2500:]
        from scistack_gui.services.execution_service import derive_fn_targets

        variants = [v for v in db.list_pipeline_variants() if v["function_name"] == function_name]
        targets = derive_fn_targets(db, function_name)
        raise AssertionError(
            f"{function_name} run failed: error={result.get('error')!r} "
            f"completed={result.get('completed_combos')} failed={result.get('failed_combos')}\n"
            f"--- history ---\n"
            f"selectors={[v.get('selectors') for v in variants]}\n"
            f"input_types={[v.get('input_types') for v in variants]}\n"
            f"--- derived targets ---\n"
            f"bindings={[t.get('bindings') for t in targets]}\n"
            f"--- run output (tail) ---\n{tail}"
        )
    return result


def _count(variable) -> int:
    try:
        return len(variable.load(as_df=True, version="all"))
    except scidb.NotFoundError:
        return 0


# --- a node with history re-runs from its own history -----------------------------------


def test_a_node_reruns_from_history_and_adds_nothing(seeded, pipeline, pushes):
    before = _count(pipeline.NormalizedKnee)
    done = _run(seeded, "normalized_knee", pushes)
    assert done["success"] is True, done
    assert _count(pipeline.NormalizedKnee) == before


def test_a_node_with_no_history_reports_why_it_cannot_run(scratch_db, pipeline, as_gui_db, pushes, registered):
    """The canvas lets a user click Run on a node before anything upstream
    exists: the message must say so, not spin."""
    as_gui_db(scratch_db)
    pipeline_store._ensure_tables(scratch_db)
    done = _run(scratch_db, "normalized_knee", pushes, expect_success=False)
    assert done["success"] is False
    assert done.get("error"), done
    assert "normalized_knee" in done["error"]


# --- run options ------------------------------------------------------------------------------


def test_dry_run_writes_nothing(seeded, pipeline, pushes):
    before = _count(pipeline.NormalizedKnee)
    done = _run(seeded, "normalized_knee", pushes, run_options={"dry_run": True})
    assert done["success"] is True, done
    assert _count(pipeline.NormalizedKnee) == before


def test_save_off_writes_nothing(seeded, pipeline, pushes):
    before = _count(pipeline.TrialMeanSymmetry)
    # as_table pools unless the panel's "Iterate over" keys are given — the
    # trial-level iteration this step was authored with.
    done = _run(seeded, "trial_mean_symmetry", pushes, run_options={"save": False, "as_table": True},
                schema_level=["subject", "session", "speed", "trial"])
    assert done["success"] is True, done
    assert _count(pipeline.TrialMeanSymmetry) == before


# --- the schema-location selection ------------------------------------------------------


def test_a_schema_selection_narrows_the_run(seeded, pipeline, pushes, root):
    """The picker's pair — include prefixes + exclude levels — reaches
    for_each as its iteration, so only the selected locations run."""
    before = pipeline.NormalizedKnee.load(as_df=True)
    # Make the step recomputable EVERYWHERE (every trial's speed changes), so
    # what the narrowed run leaves alone is visible.
    for speed in SPEEDS:
        for trial in TRIALS:
            _rewrite(_trial_file(root, speed, trial), "duration_s,walking_speed_mps", "30.0,5.0")
    load_levels(pipeline, root=root)

    done = _run(
        seeded, "normalized_knee", pushes,
        schema_selection={"include": [[["speed", "slow"], ["trial", "01"]]], "exclude_levels": {}},
    )
    assert done["success"] is True, done
    after = pipeline.NormalizedKnee.load(as_df=True, version="all")
    assert len(after) == len(before) + len(CYCLES), "only slow/t01's ten cycles were recomputed"


def test_an_exclude_level_in_the_selection_skips_it(seeded, pipeline, pushes, root):
    for speed in SPEEDS:
        for trial in TRIALS:
            _rewrite(_trial_file(root, speed, trial), "duration_s,walking_speed_mps", "31.0,7.0")
    load_levels(pipeline, root=root)
    before = _count(pipeline.NormalizedKnee)
    done = _run(
        seeded, "normalized_knee", pushes,
        schema_selection={"include": [], "exclude_levels": {"speed": ["fast"]}},
    )
    assert done["success"] is True, done
    added = _count(pipeline.NormalizedKnee) - before
    assert added == len(TRIALS) * len(CYCLES), "every slow cycle recomputed, no fast one"


# --- where filters ---------------------------------------------------------------------------


def test_a_where_filter_runs_only_the_matching_records(seeded, pipeline, pushes, root):
    excursion = pipeline.KneeExcursion.load(as_df=True)
    threshold = float(excursion["data"].astype(float).median())
    expected = int((excursion["data"].astype(float) > threshold).sum())
    # Force a recompute so the filter's effect is countable: a new height.
    _rewrite(root / ONE_SUBJECT[0] / f"{ONE_SUBJECT[0]}_demographics.csv",
             "age_years,height_cm,mass_kg,group", "50,999.0,70.0,control")
    load_levels(pipeline, root=root)
    before = _count(pipeline.NormalizedKnee)
    done = _run(
        seeded, "normalized_knee", pushes,
        where_filters=[run_api.WhereFilterSpec(variable="KneeExcursion", op=">", value=str(threshold))],
    )
    assert done["success"] is True, done
    assert _count(pipeline.NormalizedKnee) - before == expected


# --- column selections from the node config ---------------------------------------------------


def test_a_column_selection_in_the_node_config_reaches_the_run(seeded, pipeline, pushes):
    """The panel stores ``columnSelections`` per node; the run must hand the
    function only those columns. ``trial_mean_symmetry`` averages whatever
    joint columns it is given, so a two-column selection is visible in the
    output's columns."""
    pipeline_store.update_node_config(
        seeded, "fn__trial_mean_symmetry__test",
        {"columnSelections": {"cycles": {"columns": ["ankle", "knee"], "iterate": False}}},
    )
    done = _run(seeded, "trial_mean_symmetry", pushes, run_options={"as_table": True},
                schema_level=["subject", "session", "speed", "trial"])
    assert done["success"] is True, done
    latest = pipeline.TrialMeanSymmetry.load(as_df=True)
    columns = set()
    for data in latest["data"]:
        columns |= set(data.columns if isinstance(data, pd.DataFrame) else data.keys())
    selection_lines = [
        m.get("text", "") for m in pushes
        if m.get("type") == "run_output" and "column" in m.get("text", "").lower()
    ]
    assert "hip" not in columns, (
        f"columns={sorted(columns)}; the node config's selection did not reach the run.\n"
        f"column-selection log lines: {selection_lines or '(none — the config was never read)'}"
    )
    assert {"ankle", "knee"} <= columns


def test_a_for_columns_selection_in_the_node_config_runs_per_column(seeded, pipeline, pushes):
    scidb.for_each(
        pipeline.scale_joint, {"value": pipeline.TrialMeanSymmetry.for_columns(), "scale": pipeline.SCALE},
        [pipeline.ScaledTrialSymmetry], subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[],
    )
    pipeline_store.update_node_config(
        seeded, "fn__scale_joint__test",
        {"columnSelections": {"value": {"columns": ["hip"], "iterate": True}}},
    )
    done = _run(seeded, "scale_joint", pushes)
    assert done["success"] is True, done
    latest = pipeline.ScaledTrialSymmetry.load(as_df=True)
    one = latest.iloc[0]["data"]
    columns = set(one.columns if isinstance(one, pd.DataFrame) else one.keys())
    assert columns >= {"hip"}


# --- node state after the order-of-steps scenarios --------------------------------------------


def test_node_state_is_green_after_a_full_run_and_red_after_an_upstream_edit(seeded, pipeline, pushes, root):
    states = {s.function_name: s for s in seeded.inspect.node_state("normalized_knee", fn_registry={"normalized_knee": pipeline.normalized_knee})}
    assert states["normalized_knee"].state in ("green", "red", "unknown")
    green_before = states["normalized_knee"].state == "green"

    # An upstream edit: one trial's speed changes, and its loader re-runs.
    _rewrite(_trial_file(root, "fast", "02"), "duration_s,walking_speed_mps", "30.0,1.23")
    load_levels(pipeline, root=root)
    after = {s.function_name: s for s in seeded.inspect.node_state("normalized_knee", fn_registry={"normalized_knee": pipeline.normalized_knee})}
    assert after["normalized_knee"].missing >= len(CYCLES) or after["normalized_knee"].state != "green" or not green_before
    # Re-running clears it.
    done = _run(seeded, "normalized_knee", pushes)
    assert done["success"] is True, done
    final = {s.function_name: s for s in seeded.inspect.node_state("normalized_knee", fn_registry={"normalized_knee": pipeline.normalized_knee})}
    assert final["normalized_knee"].missing == 0



def test_a_python_column_selection_survives_a_gui_rerun(seeded, pipeline, pushes):
    """`normalized_knee` was authored as `CycleSymmetry["knee"]`. Re-run from
    the canvas with NO node config, the function must still receive that one
    column — the selection is recorded in provenance and read back — not the
    whole table it cannot handle. (Found 2026-09-19: every combination failed
    with "Data must be 1-dimensional".)"""
    done = _run(seeded, "normalized_knee", pushes)
    assert done["success"] is True and done.get("failed_combos", 0) == 0, done


def _producing_invocations(db, type_name: str) -> dict:
    """``{invocation_id: signature}`` for every record of *type_name* — the
    diagnostic for "a faithful re-run adds nothing": if the set grows, the
    re-run was a DIFFERENT computation (the two signatures say how); if it
    does not and the count still grew, the same call produced different
    content."""
    from scidb import provenance_query as pq

    duck = db._duck
    out: dict = {}
    for (rid,) in duck._fetchall("SELECT record_id FROM _record WHERE type = ?", [type_name]):
        inv = pq.producing_invocation(duck, rid)
        if inv is None:
            continue
        inv_id = inv[0]
        if inv_id not in out:
            sig = pq.stored_invocation_signature(duck, rid) or {}
            out[inv_id] = {
                "function_hash": sig.get("function_hash"),
                "var_inputs": sig.get("var_inputs"),
                "const_hashes": sig.get("const_hashes"),
                "run_options": duck._fetchone(
                    "SELECT distribute, as_table, for_columns FROM _invocation WHERE invocation_id = ?",
                    [inv_id],
                ),
            }
    return out


def test_a_python_for_columns_survives_a_gui_rerun(seeded, pipeline, pushes):
    scidb.for_each(
        pipeline.scale_joint, {"value": pipeline.TrialMeanSymmetry.for_columns(), "scale": pipeline.SCALE},
        [pipeline.ScaledTrialSymmetry], subject=ONE_SUBJECT, session=ONE_SESSION, speed=[], trial=[],
    )
    before = _count(pipeline.ScaledTrialSymmetry)
    invocations_before = _producing_invocations(seeded, "ScaledTrialSymmetry")
    # What history says, and what the run will therefore bind:
    from scistack_gui.services.execution_service import derive_fn_targets

    variants = [v for v in seeded.list_pipeline_variants() if v["function_name"] == "scale_joint"]
    targets = derive_fn_targets(seeded, "scale_joint")
    evidence = (
        f"\nvariants[].selectors={[v.get('selectors') for v in variants]}"
        f"\nvariants[].run_options={[v.get('run_options') for v in variants]}"
        f"\ntargets[].bindings={[t.get('bindings') for t in targets]}"
        f"\ntargets[].constants={[t.get('constants') for t in targets]}"
    )
    done = _run(seeded, "scale_joint", pushes)
    assert done["success"] is True and done.get("failed_combos", 0) == 0, str(done) + evidence
    after = _count(pipeline.ScaledTrialSymmetry)
    invocations_after = _producing_invocations(seeded, "ScaledTrialSymmetry")
    new_invocations = {k: v for k, v in invocations_after.items() if k not in invocations_before}
    assert after == before, (
        f"a faithful re-run adds nothing: {before} -> {after} record(s); "
        f"{len(invocations_before)} -> {len(invocations_after)} producing invocation(s)."
        + (
            f"\nNEW invocation(s) — the re-run was a different computation:\n"
            + "\n".join(f"  {k}: {v}" for k, v in new_invocations.items())
            + "\nOLD invocation(s):\n"
            + "\n".join(f"  {k}: {v}" for k, v in invocations_before.items())
            if new_invocations
            else "\nSame invocation(s) — same call, different CONTENT (column order? dtype?)."
        )
        + evidence
    )
    one = pipeline.ScaledTrialSymmetry.load(as_df=True).iloc[0]["data"]
    columns = set(one.columns if isinstance(one, pd.DataFrame) else one.keys())
    assert columns >= {"ankle", "knee", "hip"}, columns


# --- the run says what it fed the function -------------------------------------------


def test_the_run_logs_a_bindings_line_naming_every_input(seeded, pipeline, pushes, caplog):
    """"What did this run actually feed the function?" took four round trips
    through the logs on 2026-09-19. One INFO line answers it, and it is built
    from the SIGNATURE — a parameter nothing bound is named as unbound rather
    than silently absent. See docs/claude/input-binding-round-trip.md."""
    import logging

    with caplog.at_level(logging.INFO):
        done = _run(seeded, "normalized_knee", pushes)
    assert done["success"] is True, done

    lines = [ln for ln in caplog.text.splitlines() if "bindings —" in ln]
    assert lines, "no bindings line was logged"
    line = lines[-1]
    for param in ("knee", "walking_speed_mps", "height_cm"):
        assert f"{param}:" in line, f"{param} missing from: {line}"
        assert f"{param}: (unbound)" not in line, f"{param} bound to nothing: {line}"
    # The selection half is spelled by scidb.intent.describe_columns, the same
    # words the canvas chip uses: `("knee")` for one column, `(whole variable)`
    # for none. Every input of this step is one column, so the line carries
    # the one-column spelling for each.
    assert 'knee: CycleSymmetry ("knee")' in line, line


# --- a once-per-dataset step runs once ------------------------------------------------


def test_a_dataset_level_step_reruns_once_from_the_canvas(seeded, pipeline, pushes, caplog):
    """A function over a whole-dataset variable (saved with no schema key) is
    a once-per-dataset operation. Its history records an EMPTY level, and a
    canvas re-run with no Schema Level chosen must iterate nothing — one
    call, no new records — rather than once per cycle with the same input
    handed to every call. Spelled as for_each reads it: `schema_keys=None`
    iterates nothing; `[]` would mean every key.
    """
    import logging

    import numpy as np

    from scistack_gui import registry as _registry

    class DatasetNote(scidb.BaseVariable):
        pass

    class NoteLength(scidb.BaseVariable):
        pass

    DatasetNote.save(np.array([1.0, 2.0, 3.0]))  # no schema key at all

    def note_length(note):
        return float(len(note))

    _registry._functions["note_length"] = note_length
    # The Python-authored run: once over the dataset.
    scidb.for_each(note_length, {"note": DatasetNote}, [NoteLength])
    before = _count(NoteLength)
    assert before == 1

    with caplog.at_level(logging.INFO):
        done = _run(seeded, "note_length", pushes)
    assert done["success"] is True, done
    assert _count(NoteLength) == before, "a faithful re-run adds nothing"
    assert "iterating nothing: one call (" in caplog.text, caplog.text
