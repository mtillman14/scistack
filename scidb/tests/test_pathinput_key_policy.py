"""THE PathInput key-type policy (scidb.run.foreach): one owner, two callers.

Python's ``_load_pathinput_checked`` and MATLAB's
``+scidb/+internal/load_pathinput_checked.m`` both call these two functions;
MATLAB keeps only its error message (plan stage 4: the twin that re-derived
the policy in MATLAB is gone).
"""

from pathlib import Path

from scidb.run.foreach import pathinput_key_type_offender, pathinput_numeric_eligible

_MATLAB_TWIN = (
    Path(__file__).resolve().parents[2]
    / "scimatlab" / "src" / "scimatlab" / "matlab" / "+scidb" / "+internal"
    / "load_pathinput_checked.m"
)


def test_string_keys_are_never_numeric_eligible():
    keys = ["subject", "trial", "side"]
    types = {"subject": "string", "trial": "numeric"}
    assert pathinput_numeric_eligible(keys, types) == ["trial", "side"]


def test_no_declared_types_everything_eligible():
    assert pathinput_numeric_eligible(["a", "b"], {}) == ["a", "b"]


def test_undeclared_schema_key_bridge_is_the_offender():
    assert pathinput_key_type_offender(
        {"trial": "001"}, {}, ["subject", "trial"]
    ) == ("trial", "001")


def test_numeric_declared_key_may_bridge():
    assert pathinput_key_type_offender(
        {"trial": "001"}, {"trial": "numeric"}, ["subject", "trial"]
    ) is None


def test_non_schema_key_may_bridge():
    assert pathinput_key_type_offender({"run": "01"}, {}, ["subject"]) is None


def test_matlab_calls_the_policy_instead_of_copying_it():
    text = _MATLAB_TWIN.read_text(encoding="utf-8")
    assert "pathinput_numeric_eligible" in text
    assert "pathinput_key_type_offender" in text
    assert "string_keys" not in text and "numeric_keys" not in text
