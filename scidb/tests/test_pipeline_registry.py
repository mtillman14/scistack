"""Pipeline registry: deferred for_each registration and pull execution.

``db.pipeline(name)`` activates a Pipeline as the ambient registration
target: subsequent ``for_each`` calls register StepSpecs (returning Step
handles) instead of executing. ``pipeline=None`` forces an eager call;
``pipeline=other`` targets a non-ambient pipeline. Dependency edges are
inferred from variable types, so ``run_all``/``run_until`` execute in
topological order regardless of registration order, with
``skip_computed=True`` by default (memoized pull execution). ``plan()`` is
the non-executing dry run. Design: docs/claude/endpoint-first-pipelines.md.
"""

import numpy as np
import pytest

from scidb import (
    BaseVariable,
    NotFoundError,
    NotRegisteredError,
    Pipeline,
    PipelineCycleError,
    Step,
    active_pipeline,
    configure_database,
    for_each,
)

# A never-computed variable type raises NotFoundError if registered (e.g. by
# an earlier save of the same class) and NotRegisteredError otherwise.
NOT_STORED = (NotFoundError, NotRegisteredError)
from scidb.database import _local
from scidb.pipeline import _reset_pipeline_state, _unrun_pipelines

SCHEMA = ["subject", "trial"]
SUBJECTS = ["1", "2"]
TRIALS = ["1", "2"]


@pytest.fixture
def db(tmp_path):
    _reset_pipeline_state()
    db = configure_database(tmp_path / "registry.duckdb", SCHEMA)
    yield db
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _reset_pipeline_state()


class RawSignal(BaseVariable):
    schema_version = 1


class Filtered(BaseVariable):
    schema_version = 1


class Speed(BaseVariable):
    schema_version = 1


class Unrelated(BaseVariable):
    schema_version = 1


class UnrelatedOut(BaseVariable):
    schema_version = 1


def _seed(db):
    for s in SUBJECTS:
        for t in TRIALS:
            RawSignal.save(np.array([2.0, 4.0, 6.0]), subject=s, trial=t)
            Unrelated.save(np.array([9.0]), subject=s, trial=t)


# Call-counting step functions. Counters are function attributes so each
# test can reset them; the functions themselves must be module-level and
# NAMED (stable identity for fn hashing / endpoint detection).


def halve(signal):
    halve.calls += 1
    return np.asarray(signal, dtype=float).ravel() / 2.0


def mean_of(filtered):
    mean_of.calls += 1
    return float(np.mean(np.asarray(filtered, dtype=float)))


def negate(unrelated):
    negate.calls += 1
    return -np.asarray(unrelated, dtype=float).ravel()


@pytest.fixture(autouse=True)
def _reset_counters():
    halve.calls = 0
    mean_of.calls = 0
    negate.calls = 0


def _register_chain(db):
    """Register the 3-step graph DELIBERATELY out of dependency order:
    consumer first, producer second, plus one unrelated branch."""
    for_each(
        mean_of, {"filtered": Filtered}, [Speed], subject=SUBJECTS, trial=TRIALS, db=db
    )
    for_each(
        halve, {"signal": RawSignal}, [Filtered], subject=SUBJECTS, trial=TRIALS, db=db
    )
    for_each(
        negate,
        {"unrelated": Unrelated},
        [UnrelatedOut],
        subject=SUBJECTS,
        trial=TRIALS,
        db=db,
    )


# ---------------------------------------------------------------------------
# Registration behavior
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_ambient_registration_defers_execution(self, db):
        _seed(db)
        pipe = db.pipeline("gait")
        assert active_pipeline() is pipe

        handle = for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        assert isinstance(handle, Step)
        assert halve.calls == 0  # nothing executed
        assert len(pipe.steps) == 1
        with pytest.raises(NOT_STORED):
            Filtered.load(subject="1", trial="1")  # nothing saved

    def test_pipeline_none_forces_eager_while_active(self, db):
        _seed(db)
        db.pipeline("gait")

        result = for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=None,
        )

        assert not isinstance(result, Step)
        assert halve.calls == len(SUBJECTS) * len(TRIALS)
        assert Filtered.load(subject="1", trial="1") is not None

    def test_pipeline_kwarg_targets_non_ambient_pipeline(self, db):
        ambient = db.pipeline("ambient")
        other = Pipeline("other", db=db)  # created, NOT activated

        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=other,
        )

        assert len(other.steps) == 1
        assert len(ambient.steps) == 0
        assert halve.calls == 0

    def test_no_active_pipeline_is_eager_unchanged(self, db):
        _seed(db)
        result = for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        assert not isinstance(result, Step)
        assert halve.calls == len(SUBJECTS) * len(TRIALS)

    def test_invalid_pipeline_kwarg_raises(self, db):
        with pytest.raises(TypeError, match="pipeline="):
            for_each(
                halve,
                {"signal": RawSignal},
                [Filtered],
                subject=SUBJECTS,
                trial=TRIALS,
                db=db,
                pipeline="not-a-pipeline",
            )

    def test_step_handle_fails_fast_on_data_use(self, db):
        db.pipeline("gait")
        handle = for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        with pytest.raises(AttributeError, match="deferred pipeline step"):
            handle.head()
        with pytest.raises(TypeError, match="not.*iterable"):
            list(handle)


# ---------------------------------------------------------------------------
# Graph + execution
# ---------------------------------------------------------------------------


class TestExecution:
    def test_run_all_executes_in_dependency_order(self, db):
        _seed(db)
        pipe = db.pipeline("gait")
        _register_chain(db)  # consumer registered BEFORE its producer

        pipe.run_all()

        assert halve.calls == 4
        assert mean_of.calls == 4
        assert negate.calls == 4
        # mean_of consumed real Filtered data → the chain ran in order.
        rec = Speed.load(subject="1", trial="1")
        value = rec.data if hasattr(rec, "data") else rec
        assert float(np.asarray(value).ravel()[0]) == pytest.approx(
            2.0
        )  # mean([1,2,3])

    def test_run_until_runs_only_ancestors_and_target(self, db):
        _seed(db)
        pipe = db.pipeline("gait")
        _register_chain(db)

        pipe.run_until(mean_of)

        assert halve.calls == 4  # ancestor ran
        assert mean_of.calls == 4  # target ran
        assert negate.calls == 0  # unrelated branch untouched
        with pytest.raises(NOT_STORED):
            UnrelatedOut.load(subject="1", trial="1")

    def test_second_run_skips_current_steps(self, db):
        _seed(db)
        pipe = db.pipeline("gait")
        _register_chain(db)
        pipe.run_until(mean_of)
        calls_after_first = (halve.calls, mean_of.calls)

        pipe.run_until(mean_of)  # default skip_computed=True → all current

        assert (halve.calls, mean_of.calls) == calls_after_first

    def test_pipeline_run_identity_matches_eager_run(self, db):
        """A step run via the pipeline must produce the same computation
        identity as the same call run eagerly: an eager re-run with
        skip_computed=True must skip every combo (zero fn calls)."""
        _seed(db)
        pipe = db.pipeline("gait")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        pipe.run_all()
        assert halve.calls == 4

        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=None,
            skip_computed=True,
        )

        assert halve.calls == 4  # all combos skipped → identical identity

    def test_run_deactivates_pipeline(self, db):
        _seed(db)
        pipe = db.pipeline("gait")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        pipe.run_all()

        assert active_pipeline() is None
        # Post-run for_each calls are eager again.
        result = for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            skip_computed=True,
        )
        assert not isinstance(result, Step)

    def test_run_until_unknown_target_raises(self, db):
        pipe = db.pipeline("gait")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        with pytest.raises(ValueError, match="no step matching"):
            pipe.run_until(mean_of)

    def test_cycle_raises(self, db):
        pipe = db.pipeline("gait")

        def refine(filtered):
            return filtered

        for_each(
            refine,
            {"filtered": Filtered},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        with pytest.raises(PipelineCycleError, match="cycle"):
            pipe.run_all()

    def test_multiple_producers_all_precede_consumer(self, db):
        """Two variant branches producing Filtered are BOTH prerequisites
        of a Filtered consumer (fan-in, not ambiguity — graph level only)."""
        pipe = Pipeline("graph-only", db=db)

        def halve_a(signal):
            return signal

        def halve_b(signal):
            return signal

        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=pipe,
        )
        for_each(
            halve_a,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=pipe,
        )
        for_each(
            halve_b,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=pipe,
        )

        pairs = pipe._composed_steps()
        order = pipe._topo_order(pairs)
        names = [pairs[i][1].name for i in order]
        assert names.index("mean_of") > names.index("halve_a")
        assert names.index("mean_of") > names.index("halve_b")


# ---------------------------------------------------------------------------
# last_run_report — honest per-step outcomes
# ---------------------------------------------------------------------------


def exploder(signal):
    """A step whose every combo fails — for_each's continue-and-report
    policy swallows these, so the run report is the only honest signal."""
    raise ValueError("boom")


class TestLastRunReport:
    def test_report_counts_failures_per_step(self, db):
        """Iteration failures never raise out of _run — the report carries
        them (regression: GUI showed success for completed=0/failed=N runs,
        2026-07-18)."""
        _seed(db)
        pipe = db.pipeline("gait")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        for_each(
            exploder,
            {"signal": RawSignal},
            [UnrelatedOut],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        pipe.run_all()

        report = {e["step"]: e for e in pipe.last_run_report}
        assert report["halve"]["completed"] == 4
        assert report["halve"]["failed"] == 0
        assert report["exploder"]["completed"] == 0
        assert report["exploder"]["failed"] == 4
        assert report["exploder"]["pipeline"] == "gait"
        assert report["exploder"]["cancelled"] is False

    def test_report_counts_no_data_separately_from_failed(self, db, monkeypatch):
        """A combo with no matching rows is expected/benign (schema-key
        cross-product sparser than the full grid), unlike a genuine fn
        failure, and last_run_report must keep the two counts separate.

        This targets _execute_step's _collecting_progress_fn directly by
        controlling the "summary" event scifor/scidb would emit, rather than
        engineering a real sparse dataset: scidb's own combo discovery
        already prunes combos with zero backing records before scifor ever
        sees them (verified empirically — a genuinely absent subject never
        reaches scifor's NoDataError path through scidb.for_each), so a
        real end-to-end no-data scenario doesn't exercise this reporting
        code at all. The plumbing this test guards is real regardless: it's
        exactly what fires when a combo *does* survive to scifor's own
        per-combo filtering (e.g. a `where=` clause, or one of several
        inputs missing data for a combo whose other inputs exist) and comes
        back with a NoDataError there."""
        import scidb.foreach as scidb_foreach

        _seed(db)
        pipe = db.pipeline("gait")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        def fake_for_each(fn, inputs, outputs, **kwargs):
            progress_fn = kwargs.get("_progress_fn")
            if progress_fn is not None:
                progress_fn(
                    {
                        "event": "summary",
                        "completed": 2,
                        "failed": 0,
                        "no_data": 2,
                        "total": 4,
                        "cancelled": False,
                    }
                )
            return None

        monkeypatch.setattr(scidb_foreach, "for_each", fake_for_each)

        pipe.run_all()

        entry = pipe.last_run_report[0]
        assert entry["completed"] == 2
        assert entry["failed"] == 0
        assert entry["no_data"] == 2
        assert entry["total"] == 4

    def test_memoized_rerun_reports_clean(self, db):
        """skip_computed removes up-to-date combos BEFORE the loop — they
        must never inflate the report's failed count."""
        _seed(db)
        pipe = db.pipeline("gait")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        pipe.run_all()
        assert pipe.last_run_report[0]["failed"] == 0
        assert pipe.last_run_report[0]["completed"] == 4

        pipe.run_all()  # everything current → all combos pre-skipped

        entry = pipe.last_run_report[0]
        assert entry["step"] == "halve"
        assert entry["failed"] == 0
        assert entry["completed"] == 0


# ---------------------------------------------------------------------------
# plan() dry run
# ---------------------------------------------------------------------------


class TestPlan:
    def test_plan_reports_red_then_green(self, db):
        _seed(db)
        pipe = db.pipeline("gait")
        _register_chain(db)

        before = pipe.plan()
        names = [e["step"] for e in before]
        assert names.index("halve") < names.index("mean_of")  # topo order
        assert all(e["state"] in ("red", "unknown") for e in before)
        assert halve.calls == 0  # plan never executes

        pipe.run_all()
        after = pipe.plan()
        assert all(e["state"] == "green" for e in after)

    def test_plan_with_target_excludes_unrelated_branch(self, db):
        _seed(db)
        pipe = db.pipeline("gait")
        _register_chain(db)

        entries = pipe.plan(target=mean_of)

        names = [e["step"] for e in entries]
        assert "negate" not in names
        assert set(names) == {"halve", "mean_of"}


# ---------------------------------------------------------------------------
# Footgun mitigations
# ---------------------------------------------------------------------------


class TestNeverRunWarning:
    def test_unrun_pipeline_is_flagged(self, db):
        pipe = db.pipeline("forgotten")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        assert pipe in _unrun_pipelines()

    def test_run_plan_or_deactivate_acknowledges(self, db):
        _seed(db)
        ran = db.pipeline("ran")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        ran.run_all()
        assert ran not in _unrun_pipelines()

        planned = db.pipeline("planned")
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        planned.plan()
        assert planned not in _unrun_pipelines()

        escaped = db.pipeline("escaped")
        for_each(
            negate,
            {"unrelated": Unrelated},
            [UnrelatedOut],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        escaped.deactivate()
        assert escaped not in _unrun_pipelines()

    def test_empty_pipeline_is_not_flagged(self, db):
        pipe = db.pipeline("empty")
        pipe.deactivate()
        assert pipe not in _unrun_pipelines()


# ---------------------------------------------------------------------------
# Endpoint interaction (finalized targeting)
# ---------------------------------------------------------------------------


class TestEndpointFinalized:
    def test_finalized_applies_to_target_endpoint_only(self, db, tmp_path):
        matplotlib = pytest.importorskip("matplotlib")
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        from scidb import PathOutput

        class GaitFigure(BaseVariable):
            schema_version = 1

        def plot_filtered(filtered, filename):
            fig, ax = plt.subplots()
            ax.plot(np.asarray(filtered).ravel())
            return fig

        _seed(db)
        plots = tmp_path / "figs"
        plots.mkdir()
        pipe = db.pipeline("report")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        for_each(
            plot_filtered,
            {
                "filtered": Filtered,
                "filename": PathOutput(str(plots / "{subject}_{trial}.png")),
            },
            [GaitFigure],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )  # registered as DRAFT

        pipe.run_until("plot_filtered", finalized=True)

        # Upstream processing saved normally; endpoint recorded (not draft).
        assert Filtered.load(subject="1", trial="1") is not None
        assert GaitFigure.load(subject="1", trial="1") is not None
        assert (plots / "1_1.png").exists()


# ---------------------------------------------------------------------------
# Composition (uses=)
# ---------------------------------------------------------------------------


def double_unrelated(unrelated):
    return np.asarray(unrelated, dtype=float) * 2


class TestComposition:
    """Pipelines declare other pipelines as dependencies (uses=): graphs
    union, run_until/plan resolve producers across the boundary, run_all
    stays scoped to own steps + ancestors."""

    def _loading(self, db):
        """A 'loading' pipeline with one useful producer (halve -> Filtered)
        and one step unrelated to the analyses (negate -> UnrelatedOut)."""
        loading = Pipeline("loading", db=db)
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=loading,
        )
        for_each(
            negate,
            {"unrelated": Unrelated},
            [UnrelatedOut],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=loading,
        )
        return loading

    def _analysis_using(self, db, loading):
        analysis = db.pipeline("analysis", uses=[loading])
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        return analysis

    def test_run_until_resolves_producer_in_used_pipeline(self, db):
        _seed(db)
        analysis = self._analysis_using(db, self._loading(db))

        analysis.run_until(mean_of)

        assert halve.calls == 4  # producer inside `loading` ran first
        assert mean_of.calls == 4
        assert negate.calls == 0  # unrelated used-pipeline step untouched
        rec = Speed.load(subject="1", trial="1")
        value = rec.data if hasattr(rec, "data") else rec
        assert float(np.asarray(value).ravel()[0]) == pytest.approx(2.0)

    def test_run_all_scope_is_own_steps_plus_ancestors(self, db):
        _seed(db)
        analysis = self._analysis_using(db, self._loading(db))

        analysis.run_all()

        assert halve.calls == 4  # ancestor of an own step
        assert mean_of.calls == 4  # own step
        assert negate.calls == 0  # in `loading` but not an ancestor

    def test_run_all_with_no_own_steps_runs_nothing(self, db):
        loading = self._loading(db)
        umbrella = db.pipeline("umbrella", uses=[loading])

        results = umbrella.run_all()

        assert results == []
        assert halve.calls == 0 and negate.calls == 0
        assert active_pipeline() is None  # early return still deactivates

    def test_target_inside_used_pipeline(self, db):
        _seed(db)
        analysis = self._analysis_using(db, self._loading(db))

        analysis.run_until(halve)

        assert halve.calls == 4
        assert mean_of.calls == 0

    def test_diamond_dedupes_shared_pipeline(self, db):
        base = Pipeline("base", db=db)
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=base,
        )
        left = Pipeline("left", db=db, uses=[base])
        right = Pipeline("right", db=db, uses=[base])
        top = Pipeline("top", db=db, uses=[left, right])

        pairs = top._composed_steps()

        base_specs = [s for (o, s) in pairs if o is base]
        assert len(base_specs) == 1  # same object everywhere -> appears once

    def test_pipeline_cycle_raises_at_declaration(self, db):
        a = Pipeline("a", db=db)
        b = Pipeline("b", db=db, uses=[a])
        with pytest.raises(PipelineCycleError, match="cycle between pipelines"):
            a.use(b)
        with pytest.raises(PipelineCycleError):
            a.use(a)

    def test_cross_database_uses_raises(self, db):
        other = Pipeline("other", db="a-different-db")
        with pytest.raises(ValueError, match="different database"):
            Pipeline("main", db=db, uses=[other])

    def test_db_none_used_pipeline_inherits(self, db):
        """A used pipeline bound to no db resolves to the user's db at run
        time (step option -> owner db -> running pipeline's db)."""
        _seed(db)
        loading = Pipeline("loading")  # no db bound anywhere
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            pipeline=loading,
        )
        analysis = db.pipeline("analysis", uses=[loading])
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        analysis.run_until(mean_of)

        assert halve.calls == 4
        assert Filtered.load(subject="1", trial="1") is not None

    def test_parent_run_acknowledges_executed_used_pipeline_only(self, db):
        _seed(db)
        loading = self._loading(db)
        spare = Pipeline("spare", db=db)  # used, but nothing consumes it
        for_each(
            double_unrelated,
            {"unrelated": Unrelated},
            [UnrelatedOut],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
            pipeline=spare,
        )

        analysis = db.pipeline("analysis", uses=[loading, spare])
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        assert loading in _unrun_pipelines()
        assert spare in _unrun_pipelines()

        analysis.run_until(mean_of)

        assert loading not in _unrun_pipelines()  # its step executed
        assert spare in _unrun_pipelines()  # used but never executed

    def test_plan_carries_owner_names(self, db):
        _seed(db)
        analysis = self._analysis_using(db, self._loading(db))

        entries = analysis.plan(target=mean_of)

        owners = {e["step"]: e["pipeline"] for e in entries}
        assert owners == {"halve": "loading", "mean_of": "analysis"}

    def test_interface_exposes_composed_ports(self, db):
        """interface() = the pipeline node's ports: consumed-not-produced
        types in, produced types out — across the composed graph."""
        analysis = self._analysis_using(db, self._loading(db))
        analysis.deactivate()

        iface = analysis.interface()

        names = lambda classes: [c.__name__ for c in classes]  # noqa: E731
        assert names(iface["inputs"]) == ["RawSignal", "Unrelated"]
        assert names(iface["outputs"]) == ["Filtered", "Speed", "UnrelatedOut"]

    def test_second_composed_run_skips_across_boundary(self, db):
        _seed(db)
        analysis = self._analysis_using(db, self._loading(db))
        analysis.run_until(mean_of)
        snapshot = (halve.calls, mean_of.calls)

        analysis.run_until(mean_of)

        assert (halve.calls, mean_of.calls) == snapshot


# ---------------------------------------------------------------------------
# Use-edge bindings (stage 3)
# ---------------------------------------------------------------------------


def scale_by(signal, factor):
    scale_by.calls.append(factor)
    return np.asarray(signal, dtype=float).ravel() * float(factor)


@pytest.fixture(autouse=True)
def _reset_scale_by():
    scale_by.calls = []


class TestBinding:
    """PipelineBinding: adapt a used pipeline (key_map / params / iterate)
    without touching its source. Non-mutating; different parents can bind
    the same pipeline differently."""

    def _scaling(self, db=None):
        """A pipeline with a constant input (factor=2) — the params surface."""
        scaling = Pipeline("scaling", db=db)
        for_each(
            scale_by,
            {"signal": RawSignal, "factor": 2},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            pipeline=scaling,
        )
        return scaling

    # -- params ---------------------------------------------------------------

    def test_params_override_reaches_the_function(self, db):
        _seed(db)
        scaling = self._scaling()
        analysis = db.pipeline("analysis", uses=[scaling.bind(params={"factor": 3})])
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        analysis.run_until(mean_of)

        assert scale_by.calls == [3, 3, 3, 3]  # bound value, not source's 2

    def test_original_pipeline_unaffected_by_binding(self, db):
        _seed(db)
        scaling = self._scaling(db=db)
        db.pipeline("analysis", uses=[scaling.bind(params={"factor": 3})])
        scaling.run_all()  # the UNBOUND pipeline still runs factor=2

        assert scale_by.calls == [2, 2, 2, 2]

    def test_two_parents_two_params_are_two_variants(self, db):
        _seed(db)
        scaling = self._scaling()

        a = db.pipeline("a", uses=[scaling.bind(params={"factor": 3})])
        a.deactivate()
        b = db.pipeline("b", uses=[scaling.bind(params={"factor": 5})])
        b.deactivate()

        a.run_until(scale_by)
        b.run_until(scale_by)

        assert sorted(scale_by.calls) == [3, 3, 3, 3, 5, 5, 5, 5]
        # Both variants exist as distinct records (constants are version keys).
        v3 = Filtered.load(subject="1", trial="1", factor=3)
        v5 = Filtered.load(subject="1", trial="1", factor=5)
        d3 = v3.data if hasattr(v3, "data") else v3
        d5 = v5.data if hasattr(v5, "data") else v5
        assert float(np.asarray(d3).ravel()[0]) == pytest.approx(6.0)
        assert float(np.asarray(d5).ravel()[0]) == pytest.approx(10.0)

    def test_identical_bindings_dedupe_in_diamond(self, db):
        scaling = self._scaling()
        left = Pipeline("left", db=db, uses=[scaling.bind(params={"factor": 3})])
        right = Pipeline("right", db=db, uses=[scaling.bind(params={"factor": 3})])
        top = Pipeline("top", db=db, uses=[left, right])

        pairs = top._composed_steps()
        scaling_specs = [s for (o, s) in pairs if o is scaling]
        assert len(scaling_specs) == 1  # equal signatures -> one computation

        differing = Pipeline(
            "top2",
            db=db,
            uses=[
                Pipeline("l2", db=db, uses=[scaling.bind(params={"factor": 3})]),
                Pipeline("r2", db=db, uses=[scaling.bind(params={"factor": 5})]),
            ],
        )
        pairs2 = differing._composed_steps()
        assert len([s for (o, s) in pairs2 if o is scaling]) == 2  # two variants

    def test_bind_time_param_validation(self, db):
        scaling = self._scaling()
        with pytest.raises(ValueError, match="no constant input matching"):
            scaling.bind(params={"nonexistent": 1})

        # Ambiguity: two functions with the same constant name.
        def scale_other(signal, factor):
            return signal

        both = Pipeline("both", db=db)
        for_each(
            scale_by,
            {"signal": RawSignal, "factor": 2},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            pipeline=both,
        )
        for_each(
            scale_other,
            {"signal": RawSignal, "factor": 4},
            [Speed],
            subject=SUBJECTS,
            trial=TRIALS,
            pipeline=both,
        )
        from scidb import AmbiguousParamError

        with pytest.raises(AmbiguousParamError, match="disambiguate"):
            both.bind(params={"factor": 9})
        # Namespaced targeting resolves it.
        binding = both.bind(params={"scale_by.factor": 9})
        assert binding.params == {"scale_by.factor": 9}

    # -- key_map ----------------------------------------------------------------

    def test_key_map_renames_iteration_keys_end_to_end(self, db):
        """A pipeline written for [session, trial] runs in this project's
        [subject, trial] schema via key_map, saving under PROJECT keys."""
        _seed(db)
        foreign = Pipeline("foreign")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            session=SUBJECTS,
            trial=TRIALS,  # foreign vocabulary
            pipeline=foreign,
        )

        analysis = db.pipeline(
            "analysis", uses=[foreign.bind(key_map={"session": "subject"})]
        )
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        analysis.run_until(mean_of)

        assert halve.calls == 4
        assert Filtered.load(subject="1", trial="1") is not None

    def test_key_map_rewrites_pathoutput_template(self, db):
        foreign = Pipeline("foreign")
        from scidb import PathOutput

        for_each(
            halve,
            {"signal": RawSignal, "out": PathOutput("figs/{session}_{trial}.png")},
            [Filtered],
            session=SUBJECTS,
            trial=TRIALS,
            pipeline=foreign,
        )

        analysis = Pipeline(
            "analysis", db=db, uses=[foreign.bind(key_map={"session": "subject"})]
        )
        pairs = analysis._composed_steps()
        (spec,) = [s for (o, s) in pairs if o is foreign]

        assert str(spec.inputs["out"].template) == "figs/{subject}_{trial}.png"
        assert list(spec.metadata_iterables) == ["subject", "trial"]
        # Source spec untouched.
        assert list(foreign.steps[0].metadata_iterables) == ["session", "trial"]

    def test_key_map_rewrites_pathinput_root_folder_regex_and_aliases(self, db):
        # Regression guard: _rewrite_input used to drop PathInput.aliases
        # entirely on key_map rewrite (only root_folder/regex were carried
        # over), silently losing folder-name alias declarations for any
        # PathInput reused across a bound pipeline composition.
        from pathlib import Path

        from scidb import PathInput

        foreign = Pipeline("foreign")
        for_each(
            halve,
            {
                "signal": PathInput(
                    "raw/{session}_{trial}.mat",
                    root_folder="/data",
                    aliases={"session": {"BL": ["Baseline"]}}, name="raw/{session}_{trial}.mat",
                )
            },
            [Filtered],
            session=SUBJECTS,
            trial=TRIALS,
            pipeline=foreign,
        )

        analysis = Pipeline(
            "analysis", db=db, uses=[foreign.bind(key_map={"session": "subject"})]
        )
        (spec,) = [s for (o, s) in analysis._composed_steps() if o is foreign]

        rewritten = spec.inputs["signal"]
        assert str(rewritten.path_template) == "raw/{subject}_{trial}.mat"
        assert rewritten.root_folder == Path("/data")
        assert rewritten.aliases == {"subject": {"BL": ["Baseline"]}}
        # Source spec untouched — key stays "session".
        assert foreign.steps[0].inputs["signal"].aliases == {
            "session": {"BL": ["Baseline"]}
        }

    def test_key_map_rewrites_fixed_and_structured_where(self, db):
        from scidb.filters import schema_key

        from scidb import Fixed

        foreign = Pipeline("foreign")
        for_each(
            mean_of,
            {"filtered": Fixed(Filtered, session="1")},
            [Speed],
            session=SUBJECTS,
            where=schema_key("session") == "1",
            pipeline=foreign,
        )

        analysis = Pipeline(
            "analysis", db=db, uses=[foreign.bind(key_map={"session": "subject"})]
        )
        (spec,) = [s for (o, s) in analysis._composed_steps() if o is foreign]

        assert spec.inputs["filtered"].fixed_metadata == {"subject": "1"}
        assert spec.options["where"].key == "subject"

    def test_key_map_raw_sql_where_warns_not_errors(self, db):
        foreign = Pipeline("foreign")
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            session=SUBJECTS,
            where="session == '1'",
            pipeline=foreign,
        )
        analysis = Pipeline(
            "analysis", db=db, uses=[foreign.bind(key_map={"session": "subject"})]
        )

        (spec,) = [s for (o, s) in analysis._composed_steps() if o is foreign]
        # Raw filter passes through unchanged (warned, not raised).
        assert spec.options["where"] is not None

    def test_iterate_override_and_transitive_binding(self, db):
        """iterate= replaces hardcoded lists; bindings reach nested uses
        with composed key_maps."""
        _seed(db)
        base = Pipeline("base")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            session=["9"],
            trial=TRIALS,  # wrong hardcoded list
            pipeline=base,
        )
        mid = Pipeline("mid", uses=[base])  # identity edge

        analysis = db.pipeline(
            "analysis",
            uses=[
                mid.bind(
                    key_map={"session": "subject"}, iterate={"subject": SUBJECTS}
                ),  # post-map key
            ],
        )
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        analysis.run_until(mean_of)

        assert halve.calls == 4  # real subjects, not session "9"
        assert Filtered.load(subject="2", trial="2") is not None


# ---------------------------------------------------------------------------
# Endpoint verbs (stage 3)
# ---------------------------------------------------------------------------


class TestEndpointVerbs:
    def _with_plot(self, db, tmp_path):
        matplotlib = pytest.importorskip("matplotlib")
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        from scidb import PathOutput

        class VerbFigure(BaseVariable):
            schema_version = 1

        def plot_filtered(filtered, filename):
            fig, ax = plt.subplots()
            ax.plot(np.asarray(filtered).ravel())
            return fig

        plots = tmp_path / "verb_figs"
        plots.mkdir(exist_ok=True)
        pipe = db.pipeline("report")
        for_each(
            halve,
            {"signal": RawSignal},
            [Filtered],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        for_each(
            plot_filtered,
            {
                "filtered": Filtered,
                "filename": PathOutput(str(plots / "{subject}_{trial}.png")),
            },
            [VerbFigure],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        return pipe, plots, VerbFigure

    def test_endpoints_lists_only_endpoint_steps(self, db, tmp_path):
        pipe, _, _ = self._with_plot(db, tmp_path)
        eps = pipe.endpoints()
        assert [(e["step"], e["kind"]) for e in eps] == [("plot_filtered", "plot")]

    def test_plan_flags_endpoints(self, db, tmp_path):
        _seed(db)
        pipe, _, _ = self._with_plot(db, tmp_path)
        flags = {e["step"]: e["endpoint"] for e in pipe.plan()}
        assert flags == {"halve": False, "plot_filtered": True}

    def test_show_draft_runs_and_returns_paths(self, db, tmp_path):
        _seed(db)
        pipe, plots, VerbFigure = self._with_plot(db, tmp_path)

        paths = pipe.show("plot_filtered")

        assert len(paths) == 4
        for p in paths:
            assert str(plots) in str(p)
        assert (plots / "1_1.png").exists()  # rendered to look at
        with pytest.raises(NOT_STORED):
            VerbFigure.load(subject="1", trial="1")  # draft: no record
        assert halve.calls == 4  # ancestry pulled

    def test_show_rejects_processing_steps(self, db, tmp_path):
        pipe, _, _ = self._with_plot(db, tmp_path)
        with pytest.raises(ValueError, match="endpoints"):
            pipe.show(halve)

    def test_run_endpoints_finalized_records(self, db, tmp_path):
        _seed(db)
        pipe, plots, VerbFigure = self._with_plot(db, tmp_path)

        pipe.run_endpoints(finalized=True)

        assert VerbFigure.load(subject="1", trial="1") is not None
        assert (plots / "2_2.png").exists()

    def test_run_endpoints_scope_default_own_only(self, db, tmp_path):
        """Endpoints inside a used pipeline are excluded by default,
        included with include_used=True."""
        _seed(db)
        sub, plots, VerbFigure = self._with_plot(db, tmp_path)
        sub.deactivate()
        parent = db.pipeline("parent", uses=[sub])

        assert parent.run_endpoints() == []  # own endpoints: none
        assert not (plots / "1_1.png").exists()

        parent.run_endpoints(include_used=True, finalized=True)
        assert (plots / "1_1.png").exists()


class TestLocationsSurviveDeferral:
    """cleanup-audit F18: ``locations`` was the one for_each option missing
    from the registered spec, so a deferred step replayed over EVERY
    location."""

    def test_a_registered_step_keeps_its_location_selection(self, db):
        foreign = Pipeline("foreign_locations")
        selection = {"include": [[["session", "1"]]], "exclude_levels": {}}
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            session=SUBJECTS,
            locations=selection,
            pipeline=foreign,
        )
        (spec,) = foreign.steps
        assert spec.options["locations"] == selection

    def test_a_key_map_binding_renames_the_location_keys(self, db):
        from scifor.locations import LocationFilter

        foreign = Pipeline("foreign_locations_km")
        for_each(
            mean_of,
            {"filtered": Filtered},
            [Speed],
            session=SUBJECTS,
            locations={"include": [[["session", "1"]]], "exclude_levels": {"session": ["2"]}},
            pipeline=foreign,
        )
        analysis = Pipeline(
            "analysis_locations",
            db=db,
            uses=[foreign.bind(key_map={"session": "subject"})],
        )
        (spec,) = [s for (o, s) in analysis._composed_steps() if o is foreign]
        renamed = spec.options["locations"]
        assert isinstance(renamed, LocationFilter)
        assert renamed.to_dict() == {
            "include": [[["subject", "1"]]],
            "exclude_levels": {"subject": ["2"]},
        }
