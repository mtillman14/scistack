"""The public ``scidb.save()`` entry point.

``scidb.save(VariableClass, data, **metadata)`` saves raw data to the database.

Provenance is captured by ``for_each`` directly into the bipartite graph; this
entry point records no producing invocation.
"""

import logging

logger = logging.getLogger(__name__)


def save(variable_class, data, db=None, **metadata) -> str | None:
    """Save raw ``data`` as ``variable_class`` (thin wrapper over
    ``variable_class.save``, kept as the public ``scidb.save`` entry point)."""
    db_kwargs = {"db": db} if db is not None else {}
    rid = variable_class.save(data, **db_kwargs, **metadata)
    var_name = (
        variable_class.__name__
        if isinstance(variable_class, type)
        else type(variable_class).__name__
    )
    logger.debug(
        "save(): variable=%s, record_id=%s", var_name, rid[:12] if rid else None
    )
    return rid
