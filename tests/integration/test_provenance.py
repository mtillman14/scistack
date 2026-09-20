"""
scidb.inspect (+ scilineage / scihist underneath): every record the pipeline
wrote can say what produced it.
"""

from __future__ import annotations

import pytest

from conftest import SESSIONS, SUBJECTS


def test_a_derived_record_traces_to_its_function(pipeline, example_db):
    """``NormalizedKnee`` was made by ``normalized_knee`` from three inputs at
    three levels; the provenance tree must name that function and reach the
    records it read."""
    tree = example_db.inspect.provenance(
        "NormalizedKnee",
        subject=SUBJECTS[0], session=SESSIONS[0], speed="slow", trial="01", cycle="03",
    )
    assert tree.root_record_id
    functions = {node.function_name for node in tree.nodes}
    assert "normalized_knee" in functions, functions
    variables = {node.variable for node in tree.nodes}
    for source in ("CycleSymmetry", "TrialInfo", "Demographics"):
        assert source in variables, (source, variables)


def test_a_loaded_record_traces_to_its_loader(pipeline, example_db):
    tree = example_db.inspect.provenance(
        "CycleSymmetry",
        subject=SUBJECTS[0], session=SESSIONS[0], speed="fast", trial="02", cycle="10",
    )
    functions = {node.function_name for node in tree.nodes}
    assert "load_cycle_symmetry" in functions, functions
    root = next(node for node in tree.nodes if node.record_id == tree.root_record_id)
    assert root.path_inputs, "the loader's PathInput is part of the record's provenance"


def test_the_two_threshold_variants_have_distinct_provenance(pipeline, example_db):
    """Same function, two Parameter values: two records at one location, each
    tracing to its own value."""
    where = dict(subject=SUBJECTS[0], session=SESSIONS[0], speed="slow", trial="01", cycle="01")
    records = pipeline.AnkleOverThreshold.load(as_df=True, version="all", **where)
    assert len(records) == 2
    if "record_id" not in records.columns:
        pytest.skip("load(as_df=True) exposes no record_id column here")
    thresholds = set()
    for record_id in records["record_id"]:
        tree = example_db.inspect.provenance(record_id=record_id)
        root = next(node for node in tree.nodes if node.record_id == tree.root_record_id)
        thresholds.add(str(root.constants.get("threshold")))
    assert thresholds == {"50", "100"}, thresholds
