"""A PathInput crosses the MATLAB bridge whole — aliases and key_regex included.

2026-10-21: +scidb/for_each.m shipped name/template/root_folder/regex by hand
and bridge.py rebuilt the PathInput from those four fields, so a MATLAB run
of a declaration with ``aliases = {session = {BL = ["Baseline"]}}`` walked a
``Baseline`` folder, demanded ``_Baseline_`` in the file name, and found 0 of
the ``*_BL_*`` files a Python run found. Both directions now ship
``PathInput.to_spec()`` and rebuild with ``PathInput.from_spec()``.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import pytest
from scidb.database import configure_database
from scifor.pathinput import PathInput
from scimatlab import bridge
from scimatlab.bridge import (
    _reconstruct_input_for_keys,
    for_each_describe_loaded_input,
    for_each_prepare,
    register_matlab_variable,
)

SCHEMA = ["subject", "session", "speed", "trial"]
TEMPLATE = "{subject}/{session}/Xsens/{subject}_{session}_xsens_{speed}{trial}.xlsx"
ALIASES = {"session": {"BL": ["Baseline"], "PR": ["Pre-Test"]}}
KEY_REGEX = {"subject": r"R2_\d{3}"}


def _pathinput(root) -> PathInput:
    return PathInput(
        TEMPLATE, root_folder=root, name="xsens_path",
        aliases=ALIASES, key_regex=KEY_REGEX,
    )


@pytest.fixture
def data_root(tmp_path):
    d = tmp_path / "data" / "R2_001" / "Baseline" / "Xsens"
    d.mkdir(parents=True)
    for name in ("R2_001_BL_xsens_FV1.xlsx", "R2_001_BL_xsens_SSV1.xlsx"):
        (d / name).write_text("")
    (d / "xsens_adicht_files").mkdir()
    return tmp_path / "data"


@pytest.fixture
def db(tmp_path):
    db = configure_database(tmp_path / "bridge_pathinput_spec.duckdb", SCHEMA)
    yield db
    db.close()


def test_matlab_to_python_keeps_aliases_and_key_regex(data_root):
    pi = _pathinput(data_root)
    back = _reconstruct_input_for_keys({"kind": "pathinput", "spec": pi.to_spec()})
    assert back.aliases == ALIASES
    assert back.key_regex == KEY_REGEX
    assert back.to_spec() == pi.to_spec()


def test_python_to_matlab_ships_the_whole_spec(data_root):
    pi = _pathinput(data_root)
    assert for_each_describe_loaded_input(pi) == {
        "kind": "pathinput",
        "spec": pi.to_spec(),
    }


def test_prepare_discovers_files_spelled_by_alias(db, data_root):
    """The user's run: ``Baseline`` folder, ``BL`` in the file name."""
    register_matlab_variable("XSENS_PIS1")
    prep = for_each_prepare(
        "loadXSENSOneFile",
        "hash0",
        {"xsensFilePath": {"kind": "pathinput", "spec": _pathinput(data_root).to_spec()}},
        ["XSENS_PIS1"],
        # [] = every value, as the GUI's MATLAB command passes them; the
        # database is empty, so only discovery can fill them.
        {k: [] for k in SCHEMA},
        db=db,
    )
    try:
        combos = [dict(c.items()) for c in prep["full_combos"]]
        assert sorted(
            (str(c["subject"]), str(c["session"]), str(c["speed"]), str(c["trial"]))
            for c in combos
        ) == [("R2_001", "BL", "FV", "1"), ("R2_001", "BL", "SSV", "1")]
    finally:
        bridge._for_each_state_cache.pop(int(prep["handle"]), None)
