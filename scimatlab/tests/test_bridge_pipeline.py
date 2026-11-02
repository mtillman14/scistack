"""Tests for the pipeline-registry bridge (endpoint-first stage 4).

Runs entirely in Python without MATLAB: MATLAB-side registration is
simulated by calling the bridge entries with kind-tagged input specs (the
same wire format for_each_prepare consumes), and "MATLAB executes a step"
is simulated by checking the descriptor rather than running a loop.

Covers:
- pipeline_create / activate / active_name / deactivate
- pipeline_register_step: sentinel fn, surrogate-class inputs, step_index
- Graph edges through surrogate classes, incl. MATLAB<->Python mixing
- execution_order descriptors: topo order, is_matlab flag, post-binding
  surface (iterables / constants / path templates), finalized targeting
- The Python-side run_* guard on MATLAB steps
- Mixed pipeline: pipeline_run_python_step executes the Python step for
  real while MATLAB steps stay descriptors
- pipeline_bind validation surfacing + bound descriptor rewrites
- pipeline_plan / pipeline_endpoints forwarders
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np
import pytest
from scidb.database import _local, configure_database
from scidb.pipeline import _reset_pipeline_state
from scimatlab.bridge import (
    _pipeline_cache,
    pipeline_active_name,
    pipeline_bind,
    pipeline_create,
    pipeline_deactivate,
    pipeline_endpoints,
    pipeline_execution_order,
    pipeline_plan,
    pipeline_register_step,
    pipeline_run_free,
    pipeline_run_python_step,
    pipeline_use,
    register_matlab_variable,
)

from scidb import for_each

SCHEMA = ["subject", "trial"]
SUBJECTS = ["1", "2"]
TRIALS = ["1", "2"]


@pytest.fixture
def db(tmp_path):
    import scifor as _scifor

    _scifor.set_schema([])
    _reset_pipeline_state()
    db = configure_database(tmp_path / "bridge_pipe.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _reset_pipeline_state()


def _register_matlab_step(handle, fn_name, in_type, out_type, **extra):
    """One MATLAB-registered step over the standard subject/trial grid."""
    return pipeline_register_step(
        handle,
        fn_name=fn_name,
        fn_hash=f"{fn_name}-hash-0000",
        inputs_spec={"x": {"kind": "var_type", "type_name": in_type}},
        output_class_names=[out_type],
        metadata_iterables={"subject": SUBJECTS, "trial": TRIALS},
        **extra,
    )


def _seed_raw(db, cls):
    for s in SUBJECTS:
        for t in TRIALS:
            cls.save(np.array([2.0, 4.0]), subject=s, trial=t)


class TestRegistration:
    def test_create_activate_register_deactivate(self, db):
        h = pipeline_create("mpipe", db=db)
        assert pipeline_active_name() == "mpipe"

        register_matlab_variable("MRaw")
        register_matlab_variable("MFilt")
        idx0 = _register_matlab_step(h, "m_filter", "MRaw", "MFilt")
        assert idx0 == 0

        pipe = _pipeline_cache[h]
        spec = pipe.steps[0]
        assert spec.name == "m_filter"
        assert spec.options["__matlab__"] is True
        assert spec.options["__matlab_fn_hash__"] == "m_filter-hash-0000"
        # Sentinel must never be invoked from Python.
        with pytest.raises(RuntimeError, match="sentinel"):
            spec.fn()

        pipeline_deactivate(h)
        assert pipeline_active_name() == ""

    def test_python_run_guard_on_matlab_steps(self, db):
        h = pipeline_create("mpipe", db=db)
        register_matlab_variable("MRaw")
        register_matlab_variable("MFilt")
        _register_matlab_step(h, "m_filter", "MRaw", "MFilt")
        pipe = _pipeline_cache[h]

        with pytest.raises(RuntimeError, match="run this pipeline from MATLAB"):
            pipe.run_all()


class TestExecutionOrder:
    def test_topo_order_and_flags_across_languages(self, db):
        """MATLAB consumer of a MATLAB producer, registered out of order;
        plus one Python step consuming the MATLAB output type — type edges
        connect across languages via surrogate classes."""
        register_matlab_variable("MRaw")
        MFilt = register_matlab_variable("MFilt")
        register_matlab_variable("MSpeedOut")

        h = pipeline_create("mixed", db=db)
        # Consumer registered first, producer second (topo must reorder).
        _register_matlab_step(h, "m_speed", "MFilt", "MSpeedOut")
        _register_matlab_step(h, "m_filter", "MRaw", "MFilt")

        # A PYTHON step in the same (active) pipeline, consuming MFilt.
        def py_mean(x):
            py_mean.calls += 1
            return float(np.mean(np.asarray(x, dtype=float)))

        py_mean.calls = 0

        from scidb.variable import BaseVariable

        class PyMean(BaseVariable):
            schema_version = 1

        for_each(py_mean, {"x": MFilt}, [PyMean], subject=SUBJECTS, trial=TRIALS, db=db)

        result = pipeline_execution_order(h, mode="all")
        steps = result["steps"]
        names = [d["step"] for d in steps]

        assert names.index("m_filter") < names.index("m_speed")
        assert names.index("m_filter") < names.index("py_mean")
        by_name = {d["step"]: d for d in steps}
        assert by_name["m_filter"]["is_matlab"] is True
        assert by_name["py_mean"]["is_matlab"] is False
        assert by_name["m_filter"]["metadata_iterables"] == {
            "subject": SUBJECTS,
            "trial": TRIALS,
        }
        assert pipeline_active_name() == ""  # order resolution deactivates
        pipeline_run_free(result["run_handle"])

    def test_mixed_run_executes_python_step_for_real(self, db):
        """MATLAB drives: the Python step runs via
        pipeline_run_python_step; the MATLAB producer is 'executed' by
        seeding its output (what MATLAB's loop would have saved)."""
        MRaw = register_matlab_variable("MRaw")
        MFilt = register_matlab_variable("MFilt")
        _seed_raw(db, MRaw)

        h = pipeline_create("mixed", db=db)
        _register_matlab_step(h, "m_filter", "MRaw", "MFilt")

        calls = {"n": 0}

        def py_mean(x):
            calls["n"] += 1
            return float(np.mean(np.asarray(x, dtype=float)))

        from scidb.variable import BaseVariable

        class PyMean2(BaseVariable):
            schema_version = 1

        for_each(
            py_mean, {"x": MFilt}, [PyMean2], subject=SUBJECTS, trial=TRIALS, db=db
        )

        result = pipeline_execution_order(h, mode="all")
        steps = result["steps"]

        for pos, d in enumerate(steps):
            if d["is_matlab"]:
                # Simulate MATLAB's loop output for the m_filter step.
                for s in SUBJECTS:
                    for t in TRIALS:
                        MFilt.save(np.array([1.0, 2.0]), subject=s, trial=t)
            else:
                pipeline_run_python_step(result["run_handle"], pos)

        assert calls["n"] == 4
        assert PyMean2.load(subject="1", trial="1") is not None
        pipeline_run_free(result["run_handle"])

    def test_finalized_applies_to_target_only(self, db):
        register_matlab_variable("MRaw")
        register_matlab_variable("MFilt")
        register_matlab_variable("MFig")
        h = pipeline_create("endp", db=db)
        _register_matlab_step(h, "m_load", "MRaw", "MFilt")
        pipeline_register_step(
            h,
            fn_name="plot_m",
            fn_hash="plot-hash",
            inputs_spec={
                "x": {"kind": "var_type", "type_name": "MFilt"},
                "filename": {
                    "kind": "path_output",
                    "template": "figs/{subject}_{trial}.png",
                },
            },
            output_class_names=["MFig"],
            metadata_iterables={"subject": SUBJECTS, "trial": TRIALS},
        )

        result = pipeline_execution_order(
            h, mode="until", target_name="plot_m", finalized=True
        )
        by_name = {d["step"]: d for d in result["steps"]}

        assert by_name["plot_m"]["apply_finalized"] is True
        assert by_name["m_load"]["apply_finalized"] is None
        assert by_name["plot_m"]["path_templates"] == {
            "filename": "figs/{subject}_{trial}.png"
        }
        pipeline_run_free(result["run_handle"])


class TestBindingBridge:
    def test_bound_descriptor_carries_rewritten_surface(self, db):
        register_matlab_variable("MRaw")
        register_matlab_variable("MFilt")

        # Foreign-vocabulary MATLAB pipeline: iterates 'session', has a
        # constant factor=2 and a session-keyed PathOutput.
        h_sub = pipeline_create("m_loading", db=db, activate=False)
        pipeline_register_step(
            h_sub,
            fn_name="m_scale",
            fn_hash="scale-hash",
            inputs_spec={
                "x": {"kind": "var_type", "type_name": "MRaw"},
                "factor": {"kind": "constant", "value": 2},
                "out": {
                    "kind": "path_output",
                    "template": "figs/{session}_{trial}.png",
                },
            },
            output_class_names=["MFilt"],
            metadata_iterables={"session": ["9"], "trial": TRIALS},
        )

        b = pipeline_bind(
            h_sub,
            key_map={"session": "subject"},
            params={"factor": 3},
            iterate={"subject": SUBJECTS},
        )
        h_top = pipeline_create("m_analysis", db=db)
        pipeline_use(h_top, binding_handle=b)

        result = pipeline_execution_order(h_top, mode="until", target_name="m_scale")
        (d,) = result["steps"]

        assert d["metadata_iterables"] == {"subject": SUBJECTS, "trial": TRIALS}
        assert d["constant_inputs"] == {"factor": 3}
        assert d["path_templates"] == {"out": "figs/{subject}_{trial}.png"}
        assert d["pipeline"] == "m_loading"
        assert d["step_index"] == 0  # index into the OWNER's own steps
        pipeline_run_free(result["run_handle"])

    def test_bind_validation_surfaces(self, db):
        register_matlab_variable("MRaw")
        register_matlab_variable("MFilt")
        h = pipeline_create("m_loading", db=db, activate=False)
        _register_matlab_step(h, "m_scale", "MRaw", "MFilt")

        with pytest.raises(ValueError, match="no constant input matching"):
            pipeline_bind(h, params={"nonexistent": 1})


class TestForwarders:
    def test_plan_and_endpoints(self, db):
        register_matlab_variable("MRaw")
        register_matlab_variable("MFilt")
        register_matlab_variable("MFig")
        h = pipeline_create("endp", db=db)
        _register_matlab_step(h, "m_load", "MRaw", "MFilt")
        pipeline_register_step(
            h,
            fn_name="plot_m",
            fn_hash="plot-hash",
            inputs_spec={
                "x": {"kind": "var_type", "type_name": "MFilt"},
                "filename": {"kind": "path_output", "template": "figs/{subject}.png"},
            },
            output_class_names=["MFig"],
            metadata_iterables={"subject": SUBJECTS},
        )

        eps = pipeline_endpoints(h)
        assert [(e["step"], e["kind"]) for e in eps] == [("plot_m", "plot")]

        entries = pipeline_plan(h)
        by_name = {e["step"]: e for e in entries}
        assert by_name["plot_m"]["endpoint"] is True
        assert by_name["m_load"]["endpoint"] is False
        assert set(by_name["m_load"]) == {
            "step",
            "pipeline",
            "endpoint",
            "state",
            "n_combos",
        }
