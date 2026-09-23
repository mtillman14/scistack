"""``scihist.configure_database`` IS ``scidb.configure_database``.

A re-export, not a wrapper. Until 2026-09-23 this was a ``db_path,
schema_keys=None, **kwargs`` wrapper that called scidb with only the first
two -- silently dropping ``schema_key_types`` and anything else passed -- a
second, diverging owner of the one setup call.
"""

from scidb import configure_database

__all__ = ["configure_database"]
