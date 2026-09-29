"""scidb.provenance.split_call_site_outputs — THE owner of "which of a call
site's outputs form one wiring" (2026-09-29).

A call site (call_id) excludes outputs, so a rewired node's old and new
outputs share one. The canvas hashed their union and the run path one output
per variant; they disagreed and a phantom node was minted (scidb.log
2026-09-29, pandas.read_csv Demographics -> DemographicsTable). Claims made
by Runs decide the split.
"""

import logging

from scidb.database import call_site_wiring_ids
from scidb.provenance import (
    SPLIT_MAX_OUTPUTS,
    compute_wiring_id,
    split_call_site_outputs,
)

FN = "pandas.read_csv"
PI = {"filepath_or_buffer": "DemographicsPath"}


def _w(outputs):
    return compute_wiring_id(FN, {}, outputs, PI)


def test_the_hashes_from_the_log():
    """The ids scidb.log showed, so the tests below are about the real case."""
    assert _w(["Demographics"]) == "15506ac7814b3a09"
    assert _w(["DemographicsTable"]) == "7ade34e9ef0a9bdf"
    assert _w(["Demographics", "DemographicsTable"]) == "1540882c6bfa05de"


def test_one_output_is_one_wiring():
    assert split_call_site_outputs(FN, {}, ["A"], PI, {"x"}) == [(_w(["A"]), frozenset({"A"}))]


def test_no_claims_is_the_union():
    """A script-only history and the CLI: one wiring over every output."""
    both = ["Demographics", "DemographicsTable"]
    assert split_call_site_outputs(FN, {}, both, PI) == [(_w(both), frozenset(both))]


def test_a_rewire_splits_into_one_wiring_per_claimed_output():
    split = dict(
        split_call_site_outputs(
            FN, {}, ["Demographics", "DemographicsTable"], PI,
            {"15506ac7814b3a09", "7ade34e9ef0a9bdf"},
        )
    )
    assert split == {
        "15506ac7814b3a09": frozenset({"Demographics"}),
        "7ade34e9ef0a9bdf": frozenset({"DemographicsTable"}),
    }


def test_a_multi_output_claim_keeps_its_outputs_together():
    both = ["A", "B"]
    assert split_call_site_outputs(FN, {}, both, PI, {_w(both)}) == [
        (_w(both), frozenset(both))
    ]


def test_an_unclaimed_output_is_the_remainder():
    split = dict(split_call_site_outputs(FN, {}, ["A", "B", "C"], PI, {_w(["A"])}))
    assert split == {_w(["A"]): frozenset({"A"}), _w(["B", "C"]): frozenset({"B", "C"})}


def test_overlapping_claims_are_all_kept_and_logged(caplog):
    """The phantom's union claim next to the two real ones: all three are
    claims; identity resolution decides where each lands."""
    with caplog.at_level(logging.INFO, logger="scidb"):
        split = dict(
            split_call_site_outputs(
                FN, {}, ["Demographics", "DemographicsTable"], PI,
                {"15506ac7814b3a09", "7ade34e9ef0a9bdf", "1540882c6bfa05de"},
            )
        )
    assert set(split) == {"15506ac7814b3a09", "7ade34e9ef0a9bdf", "1540882c6bfa05de"}
    assert "OVERLAP" in caplog.text


def test_too_many_outputs_is_not_split(caplog):
    outs = [f"O{i}" for i in range(SPLIT_MAX_OUTPUTS + 1)]
    with caplog.at_level(logging.WARNING, logger="scidb"):
        split = split_call_site_outputs(FN, {}, outs, PI, {_w(["O0"])})
    assert split == [(_w(outs), frozenset(outs))]
    assert "not splitting" in caplog.text


def test_the_cli_step_grouping_is_unchanged():
    """call_site_wiring_ids has no claims: the union, as before."""
    aggregate = {
        "functions": {
            (FN, "cid1"): {"input_params": {}, "outputs": ["A", "B"]},
        },
        "path_inputs": {},
    }
    assert call_site_wiring_ids(aggregate) == {
        (FN, "cid1"): compute_wiring_id(FN, {}, ["A", "B"], {})
    }
