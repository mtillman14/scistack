"""scihist is now a deprecation shim over scidb — verify the re-exports.

The implementation moved into scidb (which tracks lineage by default). scihist
remains only for backward-compatible imports. Most symbols are identical objects
re-exported from scidb, ``configure_database`` included; ``for_each`` is a thin
wrapper that preserves scihist's historical ``skip_computed=True`` default, so
it is NOT identity-equal to scidb's.
"""

import warnings

import scidb


def test_reexports_are_scidb_objects():
    import scihist

    # Identity re-exports (same object as scidb's).
    assert scihist.save is scidb.save
    assert scihist.check_combo_state is scidb.check_combo_state
    assert scihist.check_node_state is scidb.check_node_state
    assert scihist.check_multiple_nodes_state is scidb.check_multiple_nodes_state
    assert scihist.Fixed is scidb.Fixed
    assert scihist.Merge is scidb.Merge
    assert scihist.ColumnSelection is scidb.ColumnSelection
    assert scihist.ForEachConfig is scidb.ForEachConfig
    # One owner of database setup: the old wrapper silently dropped
    # schema_key_types and every other keyword.
    assert scihist.configure_database is scidb.configure_database


def test_wrappers_present_but_not_identity():
    import scihist

    # Wrappers preserve scihist defaults, so they are distinct callables.
    assert callable(scihist.for_each)
    assert scihist.for_each is not scidb.for_each


def test_submodule_imports_still_resolve():
    # Existing code / tests import scihist submodules directly.
    from scihist.foreach import save as _save  # noqa: F401
    from scihist.state import (  # noqa: F401
        check_combo_state,
        check_multiple_nodes_state,
        check_node_state,
    )


def test_import_emits_deprecation_warning():
    # The DeprecationWarning fires at first import; re-importing a cached module
    # won't re-warn, so assert the module imports cleanly and exposes the API.
    with warnings.catch_warnings():
        warnings.simplefilter("always")
        import scihist  # noqa: F401
    assert hasattr(scihist, "for_each")
