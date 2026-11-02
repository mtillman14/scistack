"""P0 of version_keys elimination: direct-save non-schema kwargs are anchored in
the bipartite graph as a *synthetic save invocation* (function_name="__save__"),
so they become graph-derivable branch params instead of living only in
version_keys.

These tests assert the GRAPH side that P0 adds (dual-write — version_keys is still
written too, until P2). See
.claude/eliminate-version-keys-graph-traversal.md.
"""

import numpy as np
from scidb.provenance.graph import SAVE_FUNCTION_NAME

from scidb import BaseVariable
from scidb.provenance import query as pq


class DsgRaw(BaseVariable):
    schema_version = 1


def _producing(db, rid):
    return pq.producing_invocation(db._duck, rid)


class TestDirectSaveKwargAnchoring:
    def test_kwarg_save_creates_synthetic_invocation(self, db):
        """A save with a non-schema kwarg gets a synthetic save invocation that
        carries the kwarg as a constant input, recoverable via branch params."""
        rid = DsgRaw.save(np.array([1.0, 2.0]), subject=1, trial="A", run="x", db=db)

        inv = _producing(db, rid)
        assert inv is not None, (
            "kwarg save should have a synthetic producing invocation"
        )
        inv_id, fn_name, _fn_hash = inv
        assert fn_name == SAVE_FUNCTION_NAME

        bp = pq.derived_branch_params(db._duck, rid)
        assert bp == {f"{SAVE_FUNCTION_NAME}.run": "x"}, bp

    def test_plain_save_has_no_producing_invocation(self, db):
        """A save with ONLY schema kwargs stays raw — no synthetic invocation."""
        rid = DsgRaw.save(np.array([1.0]), subject=2, trial="B", db=db)
        assert _producing(db, rid) is None
        assert pq.derived_branch_params(db._duck, rid) == {}

    def test_distinct_kwargs_get_distinct_variants(self, db):
        """Two saves at the SAME schema with different kwargs are distinct variants
        (different synthetic invocations, different branch params)."""
        rid_x = DsgRaw.save(np.array([1.0]), subject=3, trial="A", run="x", db=db)
        rid_y = DsgRaw.save(np.array([2.0]), subject=3, trial="A", run="y", db=db)

        ix, iy = _producing(db, rid_x), _producing(db, rid_y)
        assert ix is not None and iy is not None
        assert ix[0] != iy[0], "different kwargs → different synthetic invocation_id"
        assert pq.derived_branch_params(db._duck, rid_x) == {
            f"{SAVE_FUNCTION_NAME}.run": "x"
        }
        assert pq.derived_branch_params(db._duck, rid_y) == {
            f"{SAVE_FUNCTION_NAME}.run": "y"
        }

    def test_resave_same_kwarg_is_idempotent_on_invocation(self, db):
        """Re-saving the SAME content+kwargs reproduces the same record_id and
        therefore the same synthetic invocation (ON CONFLICT DO NOTHING)."""
        a = DsgRaw.save(np.array([5.0]), subject=4, trial="A", run="x", db=db)
        b = DsgRaw.save(np.array([5.0]), subject=4, trial="A", run="x", db=db)
        assert a == b  # content-addressed
        assert _producing(db, a)[0] == _producing(db, b)[0]

    def test_synthetic_invocation_excluded_from_pipeline_structure(self, db):
        """Synthetic save invocations must not appear as pipeline nodes."""
        DsgRaw.save(np.array([1.0]), subject=5, trial="A", run="x", db=db)
        structure = pq.pipeline_structure(db._duck)
        fn_names = {n["function_name"] for n in structure}
        assert SAVE_FUNCTION_NAME not in fn_names

    def test_kwarg_save_still_loads(self, db):
        """Regression: the record is still loadable (version_keys dual-write)."""
        DsgRaw.save(np.array([7.0, 8.0]), subject=6, trial="A", run="x", db=db)
        loaded = DsgRaw.load(subject=6, trial="A", run="x", db=db)
        assert np.allclose(np.asarray(loaded.data), [7.0, 8.0])
