"""DB-backed for_each wrapper — delegates core loop to scifor.for_each."""

from typing import Any, Callable

from scifor import for_each as _scifor_for_each
from scifor.foreach import _has_pathinput, _is_loadable
from scifor.fixed import Fixed
from scifor.column_selection import ColumnSelection
from scifor.merge import Merge
from scifor.pathinput import PathInput

from .foreach_config import ForEachConfig


def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    outputs: list[type],
    dry_run: bool = False,
    save: bool = True,
    pass_metadata: bool | None = None,
    as_table: list[str] | bool | None = None,
    db=None,
    distribute: bool = False,
    where=None,
    **metadata_iterables: list[Any],
) -> "pd.DataFrame | None":
    """
    Execute a function for all combinations of metadata, loading inputs
    and saving outputs automatically.

    This is the DB-backed wrapper.  It:
    1. Resolves empty lists ``[]`` via ``db.distinct_schema_values()``
    2. Pre-filters schema combos via ``db.distinct_schema_combinations()``
    3. Builds ``ForEachConfig`` version keys
    4. Delegates the core loop to ``scifor.for_each``

    See ``scifor.for_each`` for full parameter documentation.
    """
    # Resolve empty lists to all distinct values from the database
    needs_resolve = [k for k, v in metadata_iterables.items()
                     if isinstance(v, list) and len(v) == 0]
    resolved_db = None
    if needs_resolve:
        resolved_db = db
        if resolved_db is None:
            try:
                from scidb.database import get_database
                resolved_db = get_database()
            except Exception:
                raise ValueError(
                    f"Empty list [] was passed for {needs_resolve}, which means "
                    f"'use all levels', but no database is available. Either pass "
                    f"db= to for_each or call configure_database() first."
                )
        for key in needs_resolve:
            values = resolved_db.distinct_schema_values(key)
            if not values:
                print(f"[warn] no values found for '{key}' in database, 0 iterations")
            metadata_iterables[key] = values

    # Build ForEachConfig version keys (DB-specific; not part of scifor)
    config = ForEachConfig(
        fn=fn,
        inputs=inputs,
        where=where,
        distribute=distribute,
        as_table=as_table,
        pass_metadata=pass_metadata,
    )
    config_keys = config.to_version_keys()

    # Pre-filter to only schema combinations that actually exist in the database.
    # This avoids noisy [skip] messages for data that was never expected to exist.
    all_combos = None
    if needs_resolve and not _has_pathinput(inputs):
        from scidb.database import _schema_str
        filter_db = resolved_db
        schema_keys_set = set(filter_db.dataset_schema_keys)
        keys = list(metadata_iterables.keys())
        schema_indices = [i for i, k in enumerate(keys) if k in schema_keys_set]
        filter_keys = [keys[i] for i in schema_indices]

        if filter_keys:
            from itertools import product
            value_lists = [metadata_iterables[k] for k in keys]
            raw_combos = list(product(*value_lists))

            existing = filter_db.distinct_schema_combinations(filter_keys)
            existing_set = set(existing)

            filtered = [
                dict(zip(keys, combo))
                for combo in raw_combos
                if tuple(_schema_str(combo[i]) for i in schema_indices) in existing_set
            ]
            removed = len(raw_combos) - len(filtered)
            if removed > 0:
                print(f"[info] filtered {removed} non-existent schema combinations "
                      f"(from {len(raw_combos)} to {len(filtered)})")
            all_combos = filtered

    # Delegate core loop to scifor
    return _scifor_for_each(
        fn,
        inputs,
        outputs,
        dry_run=dry_run,
        save=save,
        pass_metadata=pass_metadata,
        as_table=as_table,
        db=db,
        distribute=distribute,
        where=where,
        _extra_save_metadata=config_keys,
        _all_combos=all_combos,
        **metadata_iterables,
    )
