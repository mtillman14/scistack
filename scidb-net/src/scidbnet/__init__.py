"""scidb-net: Network client-server layer for SciStack.

Provides:
    - ``create_app()`` — build a FastAPI server wrapping a DatabaseManager
    - ``RemoteDatabaseManager`` — drop-in HTTP client for DatabaseManager
    - ``configure_remote_database()`` — one-call setup that swaps in the client
"""

import threading

from .client import RemoteDatabaseManager
from .server import create_app

__version__ = "0.1.0"

__all__ = [
    "RemoteDatabaseManager",
    "create_app",
    "configure_remote_database",
]


def configure_remote_database(base_url: str, timeout: float = 30.0) -> RemoteDatabaseManager:
    """Configure SciStack to use a remote server.

    Sets ``Thunk.query`` and the thread-local database to a
    ``RemoteDatabaseManager`` so that all existing user code
    (``BaseVariable.save()``, ``.load()``, thunk caching) works
    transparently over the network.

    Args:
        base_url: URL of the SciStack server (e.g. ``"http://localhost:8000"``).
        timeout: HTTP request timeout in seconds.

    Returns:
        The ``RemoteDatabaseManager`` instance.
    """
    from scidb.database import _local
    from scidb.thunk import Thunk
    from scidb.variable import BaseVariable

    client = RemoteDatabaseManager(base_url, timeout=timeout)

    # Register all known variable subclasses with the server
    for cls in BaseVariable._all_subclasses.values():
        client.register(cls)

    Thunk.query = client
    _local.database = client
    return client
