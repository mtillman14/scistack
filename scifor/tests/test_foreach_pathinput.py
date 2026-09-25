"""Tests for PathInput discovery/resolution wired into scifor.for_each.

Mirrors the MATLAB coverage in
scimatlab/tests/matlab/scifor/TestSciforForEachSchemaKeys.m (the
static-PathInput leniency fix) and the scidb coverage in
scidb/tests/test_pathinput_static_schema_keys.py -- this is the first time
pure Python scifor.for_each gets PathInput support at all.
"""

from pathlib import Path

import pandas as pd
import pytest

from scifor import ColumnSelection, PathInput, for_each, set_schema


def setup_function():
    set_schema([])


# ---------------------------------------------------------------------------
# Static PathInput: unresolved schema keys are dropped, not zeroed/errored
# ---------------------------------------------------------------------------


def test_schema_keys_dropped_for_static_pathinput(tmp_path):
    """A literal path with no {key} placeholders can't supply any key, so an
    unresolved schema key is dropped -- the run happens once against the
    literal path, as if the key had never been requested."""
    f = tmp_path / "6MWT_GR.xlsx"
    f.write_text("hello")
    pi = PathInput(str(f), name=str(str(f)))

    result = for_each(
        lambda filepath: Path(str(filepath)).read_text(),
        inputs={"filepath": pi},
        schema_keys=["subject", "pass"],
    )

    assert len(result) == 1
    assert result["output"].iloc[0] == "hello"


def test_not_dropped_when_pathinput_is_templated(tmp_path):
    """Contrast case: a PathInput that DOES have placeholders is a real
    candidate source for 'subject', so the drop leniency must not kick in --
    'pass' has no source at all (not a placeholder, no DataFrame column) and
    still hard-errors, exactly like the no-PathInput-at-all case."""
    pi = PathInput("{subject}/6MWT_GR.xlsx", root_folder=str(tmp_path), name="{subject}/6MWT_GR.xlsx")

    with pytest.raises(ValueError, match="pass"):
        for_each(
            lambda filepath: filepath,
            inputs={"filepath": pi},
            schema_keys=["subject", "pass"],
        )


def test_unresolvable_key_still_errors_without_pathinput():
    """No PathInput at all and no DataFrame column: still a hard error
    (unchanged pre-existing behavior)."""
    with pytest.raises(ValueError, match="session"):
        for_each(lambda x: x, inputs={"x": 1}, session=[])


# ---------------------------------------------------------------------------
# Discovery Case A / Case B through a real for_each() call
# ---------------------------------------------------------------------------


@pytest.fixture
def discovery_tree(tmp_path):
    """tmp_path/1/6MWT-001.mat (content "1.5"), tmp_path/2/6MWT-002.mat ("2.5")"""
    for subject, trial, content in [("1", "001", "1.5"), ("2", "002", "2.5")]:
        d = tmp_path / subject
        d.mkdir(parents=True, exist_ok=True)
        (d / f"6MWT-{trial}.mat").write_text(content)
    return tmp_path


def test_case_a_no_metadata_adopts_all_discovered_keys(discovery_tree):
    """No metadata_iterables at all -> every placeholder key + its discovered
    values are adopted directly from disk."""
    pi = PathInput("{subject}/6MWT-{trial}.mat", root_folder=str(discovery_tree), name="{subject}/6MWT-{trial}.mat")

    result = for_each(
        lambda filepath: Path(str(filepath)).read_text(),
        inputs={"filepath": pi},
    )

    assert len(result) == 2
    assert sorted(result["output"]) == ["1.5", "2.5"]


def test_case_b_empty_keys_filled_from_disk(discovery_tree):
    """Explicit empty lists for the template keys -> filled from disk."""
    pi = PathInput("{subject}/6MWT-{trial}.mat", root_folder=str(discovery_tree), name="{subject}/6MWT-{trial}.mat")

    result = for_each(
        lambda filepath: Path(str(filepath)).read_text(),
        inputs={"filepath": pi},
        subject=[],
        trial=[],
    )

    assert len(result) == 2
    assert sorted(result["output"]) == ["1.5", "2.5"]


# ---------------------------------------------------------------------------
# Discovered zero-padded numeric values condense to int (standalone-only)
# ---------------------------------------------------------------------------


def test_discovered_zero_padded_trial_condenses_to_int(discovery_tree):
    """'trial' is discovered as '001'/'002' on disk; standalone scifor
    condenses digit-only discovered values automatically."""
    pi = PathInput("{subject}/6MWT-{trial}.mat", root_folder=str(discovery_tree), name="{subject}/6MWT-{trial}.mat")

    result = for_each(
        lambda filepath: Path(str(filepath)).read_text(),
        inputs={"filepath": pi},
        subject=[],
        trial=[],
    )

    trial_values = result["trial"].tolist()
    assert sorted(trial_values) == [1, 2]
    assert all(isinstance(v, int) for v in trial_values)
    assert sorted(result["subject"].tolist()) == [1, 2]


def test_explicit_padded_trial_not_condensed(discovery_tree):
    """An explicit zero-padded string value is user intent, not a discovery
    -- it must stay verbatim, matching the literal file on disk."""
    pi = PathInput("{subject}/6MWT-{trial}.mat", root_folder=str(discovery_tree), name="{subject}/6MWT-{trial}.mat")

    result = for_each(
        lambda filepath: str(filepath),
        inputs={"filepath": pi},
        subject=["1"],
        trial=["001"],
    )

    assert len(result) == 1
    assert result["trial"].iloc[0] == "001"


# ---------------------------------------------------------------------------
# Per-combo resolution: fn receives a resolved path, not the PathInput object
# ---------------------------------------------------------------------------


def test_per_combo_resolution_receives_path(discovery_tree):
    pi = PathInput("{subject}/6MWT-{trial}.mat", root_folder=str(discovery_tree), name="{subject}/6MWT-{trial}.mat")

    result = for_each(
        lambda filepath: str(filepath),
        inputs={"filepath": pi},
        subject=["1"],
        trial=["001"],
    )

    assert len(result) == 1
    assert result["output"].iloc[0].endswith("6MWT-001.mat")


def test_path_input_resolver_override_hook(discovery_tree):
    """A custom ``_path_input_resolver`` overrides the default
    ``pathinput.load(**metadata)`` resolution."""
    pi = PathInput("{subject}/6MWT-{trial}.mat", root_folder=str(discovery_tree), name="{subject}/6MWT-{trial}.mat")
    seen = []

    def fake_resolver(pathinput, metadata):
        seen.append(dict(metadata))
        return "OVERRIDDEN"

    result = for_each(
        lambda filepath: filepath,
        inputs={"filepath": pi},
        subject=["1"],
        trial=["001"],
        _path_input_resolver=fake_resolver,
    )

    assert result["output"].iloc[0] == "OVERRIDDEN"
    assert seen == [{"subject": "1", "trial": "001"}]


# ---------------------------------------------------------------------------
# for_columns + PathInput: same combo-resolved path for every column
# ---------------------------------------------------------------------------


def test_for_columns_with_pathinput_constant(discovery_tree):
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1], "a": [1.0], "b": [3.0]})
    pi = PathInput("{subject}/6MWT-{trial}.mat", root_folder=str(discovery_tree), name="{subject}/6MWT-{trial}.mat")

    def fn(v, filepath):
        return {"sum": float(v.sum()), "path": str(filepath)}

    result = for_each(
        fn,
        inputs={"v": ColumnSelection(df, ["a", "b"], iterate=True), "filepath": pi},
        subject=[1],
        trial=["001"],
    )

    assert len(result) == 1
    # Both columns' per-column calls saw the same resolved path.
    assert result["a__path"].iloc[0].endswith("6MWT-001.mat")
    assert result["b__path"].iloc[0].endswith("6MWT-001.mat")
