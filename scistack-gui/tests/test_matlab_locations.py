"""The generated MATLAB command carries the EXACT location selection.

cleanup-audit F6: MATLAB's for_each had no ``locations`` option, so a ragged
selection was squashed into per-key value lists and ran more than was
selected. F36: the pipeline script ignored each node's own selection.
"""

import json

from scistack_gui.api.matlab_command import (
    generate_matlab_command,
    generate_matlab_pipeline_command,
)

RAGGED = {"include": [[["subject", "01"]], [["subject", "02"], ["session", "A"]]]}
VARIANTS = [
    {"input_types": {"sig": "RawEMG"}, "output_type": "FilteredEMG", "constants": {}, "record_count": 2}
]


def _locations_json(cmd: str) -> dict:
    line = next(line for line in cmd.splitlines() if "'locations'," in line)
    text = line.split("'locations', ", 1)[1].rstrip(" ,.);")
    return json.loads(text.strip("'").replace("''", "'"))


def test_a_first_run_command_carries_the_selection():
    cmd = generate_matlab_command(
        function_name="f", db_path="/d.duckdb", schema_keys=["subject", "session"],
        locations=RAGGED,
    )
    assert _locations_json(cmd)["include"] == RAGGED["include"]


def test_a_rerun_command_carries_the_selection():
    cmd = generate_matlab_command(
        function_name="f", db_path="/d.duckdb", schema_keys=["subject", "session"],
        variants=VARIANTS, locations=RAGGED,
    )
    assert _locations_json(cmd)["include"] == RAGGED["include"]


def test_no_selection_no_pair():
    cmd = generate_matlab_command(
        function_name="f", db_path="/d.duckdb", schema_keys=["subject"],
        locations={"include": [], "exclude_levels": {}},
    )
    assert "'locations'" not in cmd


def test_each_pipeline_step_carries_its_own_selection():
    cmd = generate_matlab_pipeline_command(
        pipeline_id="p1",
        steps=[{"function_name": "f", "variants": VARIANTS, "schema_level": ["subject"],
                "locations": RAGGED}],
        db_path="/d.duckdb",
        schema_keys=["subject", "session"],
    )
    assert _locations_json(cmd)["include"] == RAGGED["include"]
