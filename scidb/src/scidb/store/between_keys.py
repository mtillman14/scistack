"""Between keys: the schema keys whose levels are taken within each stratum.

A key is **crossed** by default: a variable's planned locations are the
Cartesian product of every level each of its keys has in the database. A
**between** key (a study arm, a site, a protocol) splits the dataset into
strata instead: inside each existing combination of the between keys'
values, every other key's levels are those found in that stratum. So in a
two-arm study, declaring ``group`` between stops a subject of one arm from
being planned at the other arm's sessions.

**Declared explicitly, always** (GitHub #28, Resolved 3). Nothing here is
worked out from the data and nothing is suggested: an undeclared key is
crossed.

**One owner.** This module is the only reader and writer of the setting.
Consumers: planned locations (``scidb.provenance.status``), the data view,
the inventory's Within default (``Inspector.inventory``), and later Stat
Studio's default classification. The strata themselves are computed by
``scidb.provenance.inventory.strata``.

Storage: ``_schema_key_between``, append-only; the newest row per key is
current (``between`` FALSE = crossed again). No user identity is recorded.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from scidb.model.log import Log
from scidb.store.tables import SCHEMA_KEY_BETWEEN

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

TABLE = SCHEMA_KEY_BETWEEN.name


def between_keys(db: "DatabaseManager") -> list[str]:
    """The keys declared between, in schema order (``[]`` before the table
    exists, e.g. an old database opened read-only)."""
    duck = getattr(db, "_duck", None)
    if duck is None or not duck.table_exists(TABLE):
        return []
    rows = duck.fetchall(f"""
        SELECT key, is_between FROM (
            SELECT key, is_between,
                   ROW_NUMBER() OVER (
                       PARTITION BY key ORDER BY declared_at DESC, rowid DESC
                   ) AS rn
            FROM {TABLE}
        ) WHERE rn = 1
    """)
    declared = {k for k, b in rows if b}
    order = list(db.dataset_schema_keys)
    stale = sorted(declared - set(order))
    if stale:
        Log.debug(f"[between] declared key(s) {stale} are no longer schema keys; ignored")
    return [k for k in order if k in declared]


def set_between(
    db: "DatabaseManager", key: str, between: bool, source: str = "api"
) -> bool:
    """Declare *key* between (True) or crossed (False). Returns whether
    anything changed; writing the current setting again writes nothing."""
    order = list(db.dataset_schema_keys)
    if key not in order:
        raise ValueError(f"{key!r} is not a schema key. Schema keys: {order}")
    current = key in between_keys(db)
    if current == bool(between):
        Log.debug(f"[between] {key}: already {'between' if between else 'crossed'}")
        return False
    db._duck.execute(
        f"INSERT INTO {TABLE} (key, is_between, declared_at, source) VALUES (?, ?, ?, ?)",
        [key, bool(between), datetime.now(timezone.utc).isoformat(), source],
    )
    Log.info(f"[between] {key}: now {'between' if between else 'crossed'} (source={source})")
    return True


def set_between_keys(
    db: "DatabaseManager", keys: list[str], source: str = "api"
) -> list[str]:
    """Make exactly *keys* the between keys (the Schema Keys dialog's save).
    Returns the keys whose setting changed."""
    wanted = set(keys)
    order = list(db.dataset_schema_keys)
    unknown = sorted(wanted - set(order))
    if unknown:
        raise ValueError(f"{unknown} are not schema keys. Schema keys: {order}")
    if wanted and wanted == set(order):
        raise ValueError(
            "Every schema key cannot be between: at least one must be crossed."
        )
    changed = [k for k in order if set_between(db, k, k in wanted, source=source)]
    return changed
