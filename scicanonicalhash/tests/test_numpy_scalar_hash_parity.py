"""Do numpy scalars hash like the Python scalars they hold? (P4 stage 10)

Characterizes the fact the stage-10 decision rests on. A re-save hashes
whatever value it is given:

* ``np.float64`` subclasses ``float`` and hashes exactly like it;
* ``np.int64`` and ``np.bool_`` do NOT subclass ``int`` / ``bool``, take the
  ndarray branch (dtype + raw bytes) and hash differently.

So a loaded numpy scalar, re-saved, would fork a new record of identical
data. ``sciduckdb.restore_types`` therefore returns Python scalars (P4 stage
10, 2026-11-01). If this file starts failing, the hash recipe changed and that
decision must be revisited.
"""

import numpy as np

from scicanonicalhash import canonical_hash


def test_float64_hashes_like_float():
    assert canonical_hash(np.float64(3.14)) == canonical_hash(3.14)


def test_int64_does_not_hash_like_int():
    assert canonical_hash(np.int64(42)) != canonical_hash(42)


def test_bool_does_not_hash_like_bool():
    assert canonical_hash(np.True_) != canonical_hash(True)
