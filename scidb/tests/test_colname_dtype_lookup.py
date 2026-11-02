"""Regression tests for ColName dtype-metadata resolution.

Background (2026-08-25): ``_resolve_colname_from_db`` queried the
``_variables`` table via::

    row = resolved_db.execute(...).fetchone()   # inside try/except Exception

Two defects in one line:

1. ``resolved_db`` is a ``DatabaseManager``, which defines no ``execute``
   (the method lives on ``SciDuck``, reached via ``._duck``) and has no
   ``__getattr__`` delegation — so this raised ``AttributeError`` every time.
2. The bare ``except Exception: row = None`` swallowed it, silently turning
   "the lookup crashed" into "the variable isn't saved yet". The dtype branch
   below was therefore dead code and the ``view_name()`` fallback was the only
   path ever taken.

Separately, ``execute(...).fetchone()`` fetches after the lock is released
— see sciduckdb/tests/test_fetch_locking.py.
"""

import pandas as pd
import pytest
from scidb import BaseVariable
from scidb.foreach import _resolve_colname_from_db
from scifor import ColName


class SignalFrame(BaseVariable):
    """DataFrame-valued variable with a single non-schema data column."""

    schema_version = 1


@pytest.fixture
def saved_signal(configured_db):
    """Save one SignalFrame so _variables carries real dtype metadata."""
    SignalFrame.save(
        pd.DataFrame({"voltage": [1.0, 2.0, 3.0]}),
        subject=1,
        trial="a",
    )
    return configured_db


class TestResolveColnameFromDb:
    def test_resolves_real_column_from_dtype_metadata(self, saved_signal):
        """The dtype query must actually execute and drive the result.

        Before the fix this raised AttributeError internally, was swallowed,
        and fell through to view_name()/type-name — so this assertion is what
        proves the lookup path is live.
        """
        assert _resolve_colname_from_db(ColName(SignalFrame), saved_signal) == "voltage"

    def test_uses_global_db_when_none_passed(self, saved_signal):
        assert _resolve_colname_from_db(ColName(SignalFrame), None) == "voltage"

    def test_unsaved_variable_falls_back(self, configured_db):
        """A genuinely absent row still takes the view_name/type-name fallback."""

        class NeverSaved(BaseVariable):
            schema_version = 1

        result = _resolve_colname_from_db(ColName(NeverSaved), configured_db)
        assert result == "NeverSaved" or result == NeverSaved.view_name()

    def test_db_error_propagates_instead_of_silent_fallback(self, saved_signal):
        """A broken lookup must raise, not masquerade as 'not yet saved'.

        This is the defect that hid the bug for so long: any exception became
        row=None, which is indistinguishable from a legitimately missing row.
        """

        class Boom:
            def fetchone(self, *a, **kw):
                raise RuntimeError("connection exploded")

            dataset_schema_keys = ["subject", "trial"]

        class Wrapper:
            _duck = Boom()
            dataset_schema_keys = ["subject", "trial"]

        with pytest.raises(RuntimeError, match="connection exploded"):
            _resolve_colname_from_db(ColName(SignalFrame), Wrapper())
