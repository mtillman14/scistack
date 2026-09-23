"""A glued step keeps its glue in the whole-pipeline MATLAB script.

cleanup-audit F17: the single-node command passed ``glue=`` to its
``scidb.for_each`` call, but the pipeline script's per-step call did not, so a
glued step ran on unreshaped input when run as part of a pipeline.
"""

from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

VARIANTS = [
    {
        "input_types": {"sig": "RawEMG"},
        "output_type": "FilteredEMG",
        "constants": {},
        "record_count": 2,
    }
]


def _cmd(glue):
    return generate_matlab_pipeline_command(
        pipeline_id="p1",
        steps=[
            {
                "function_name": "filter_emg",
                "variants": VARIANTS,
                "schema_level": ["subject"],
                "glue": glue,
            }
        ],
        db_path="/data/exp.duckdb",
        schema_keys=["subject", "session"],
    )


def test_a_glued_step_registers_with_its_glue():
    cmd = _cmd({"sig": [{"name": "glue_drop_baseline", "language": "matlab"}]})
    assert "'glue', struct('sig', {{@glue_drop_baseline}})" in cmd


def test_a_step_without_glue_has_no_glue_pair():
    assert "'glue'" not in _cmd(None)
