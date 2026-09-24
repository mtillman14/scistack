"""The MATLAB sentinel identifies itself by the real MATLAB digest.

cleanup-audit F13: without ``source_hash`` the save path AST-hashed the
sentinel's Python body, never matched the MATLAB hash it had stored, and
logged "source NOT captured … the two recipes have drifted" on every run.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scimatlab" / "src"))

from scidb.foreach_config import function_hash_for, function_sources_for
from scimatlab.bridge import _make_matlab_fn_sentinel


def test_the_sentinel_carries_the_matlab_digest():
    sentinel = _make_matlab_fn_sentinel("calculateSymmetryOneVector", "221afae652af0000")
    assert sentinel.source_hash == "221afae652af0000"
    assert function_hash_for(sentinel) == "221afae652af0000"


def test_the_sentinel_offers_no_source_to_capture():
    sentinel = _make_matlab_fn_sentinel("calculateSymmetryOneVector", "221afae652af0000")
    assert function_sources_for(sentinel) == (
        "221afae652af0000",
        "calculateSymmetryOneVector",
        {},
    )


# ---------------------------------------------------------------------------
# The .m text is captured with its digest (cleanup-audit F35)
# ---------------------------------------------------------------------------

SRC = "function out = calculateSymmetryOneVector(v, formulaNum)\n    out = v;\nend\n"


def test_matching_source_is_captured_under_its_hash():
    from scimatlab.bridge import compute_matlab_function_hash

    digest = compute_matlab_function_hash(SRC)
    sentinel = _make_matlab_fn_sentinel("calculateSymmetryOneVector", digest, SRC)
    assert function_sources_for(sentinel) == (
        digest,
        "calculateSymmetryOneVector",
        {"calculateSymmetryOneVector": SRC},
    )


def test_text_that_does_not_hash_to_the_digest_is_not_captured():
    """Filing text under a hash it does not produce would later return the
    wrong code for that version."""
    sentinel = _make_matlab_fn_sentinel(
        "calculateSymmetryOneVector", "0" * 64, SRC
    )
    assert not hasattr(sentinel, "source_text")
    assert function_sources_for(sentinel)[2] == {}


def test_prepare_carries_the_source_to_the_save(tmp_path):
    from scidb.database import configure_database

    from scimatlab import bridge
    from scimatlab.bridge import (
        compute_matlab_function_hash,
        for_each_prepare,
        register_matlab_variable,
    )

    db = configure_database(tmp_path / "src.duckdb", ["subject"])
    try:
        Raw = register_matlab_variable("RawSrc_F35")
        register_matlab_variable("OutSrc_F35")
        db.save_variable(Raw, 1.0, subject=1)
        digest = compute_matlab_function_hash(SRC)
        prep = for_each_prepare(
            "calculateSymmetryOneVector",
            digest,
            {"v": {"kind": "var_type", "type_name": "RawSrc_F35"}},
            ["OutSrc_F35"],
            {},
            db=db,
            schema_keys=["subject"],
            source_text=SRC,
        )
        state = bridge._for_each_state_cache.pop(int(prep["handle"]))["state"]
        assert function_sources_for(state.fn)[2] == {"calculateSymmetryOneVector": SRC}
    finally:
        db.close()
