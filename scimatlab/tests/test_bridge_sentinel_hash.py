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
