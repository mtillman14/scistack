"""Module-level schema key registry for scifor."""

_schema_keys: list[str] = []


def set_schema(keys: list[str]) -> None:
    """Set the global schema key list.

    Args:
        keys: Ordered list of schema key names (e.g. ["subject", "session"]).

    Called automatically by ``scidb.configure_database()`` as a side effect.
    Standalone users call this once before using ``for_each``.
    """
    global _schema_keys
    _schema_keys = list(keys)


def get_schema() -> list[str]:
    """Return a copy of the global schema key list."""
    return list(_schema_keys)
