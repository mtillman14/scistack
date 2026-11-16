"""MATLAB scidb.Variant -> Python scidb.Variant across the bridge.

MATLAB stores every name-value pair as given and keeps a nested Variant
nested; Python's scidb.Variant is the one owner of what ``fn=``,
``code_version=`` and ``run_options=`` mean and of how nested Variants merge
(.claude/plan-wrapper-api-cleanup.md, stage matlab_variant_parity).
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import pytest
from scidb.model.variant import CODE_PIN_PREFIX, RUN_PIN_PREFIX, Variant
from scidb.store.database import configure_database
from scimatlab.bridge import register_matlab_variable
from scimatlab.bridge.wire import _reconstruct_input_for_keys


@pytest.fixture
def db(tmp_path):
    db = configure_database(tmp_path / "bridge_variant.duckdb", ["subject"])
    register_matlab_variable("BridgeFiltered")
    yield db
    db.close()


def _variant_spec(inner, **pairs):
    return {"kind": "variant", "inner": inner, "branch_params": pairs}


VAR = {"kind": "var_type", "type_name": "BridgeFiltered"}


def test_fn_namespaces_the_pins_not_a_branch_param(db):
    v = _reconstruct_input_for_keys(_variant_spec(VAR, fn="bandpass", low_hz=20))
    assert isinstance(v, Variant)
    assert v.branch_params == {"bandpass.low_hz": 20}


def test_code_version_and_run_options_are_pins(db):
    v = _reconstruct_input_for_keys(
        _variant_spec(VAR, fn="bandpass", code_version="v1", run_options="distribute=true")
    )
    assert v.branch_params == {
        f"{CODE_PIN_PREFIX}.bandpass": "v1",
        f"{RUN_PIN_PREFIX}.bandpass": "distribute=true",
    }


def test_outer_fn_does_not_renamespace_inner_pins(db):
    # MATLAB used to flatten nested Variants into one struct, so an outer
    # fn="b" also namespaced the inner low_hz as "b.low_hz".
    inner = _variant_spec(VAR, fn="bandpass", low_hz=20)
    v = _reconstruct_input_for_keys(_variant_spec(inner, fn="rms", window=5))
    assert v.branch_params == {"bandpass.low_hz": 20, "rms.window": 5}


def test_nested_conflict_raises(db):
    inner = _variant_spec(VAR, low_hz=10)
    with pytest.raises(ValueError, match="Conflicting branch_param"):
        _reconstruct_input_for_keys(_variant_spec(inner, low_hz=20))
