# scihist

/// warning | Deprecated
`scihist` is now a thin compatibility layer over [`scidb`](scidb.md). Lineage-tracked `for_each`, `save`, and the run-state checks all live in `scidb`, and `scidb.for_each` records provenance by default. Importing `scihist` emits a `DeprecationWarning`. New code should import from `scidb`.
///

## What it still provides

Every name below is a re-export of the `scidb` (or `scifor`) object of the same name, with one exception:

| Name | Same as | Difference |
|---|---|---|
| `for_each(...)` | `scidb.for_each` | `skip_computed` defaults to **`True`**, so up-to-date combinations are skipped. |
| `save`, `configure_database` | `scidb.save`, `scidb.configure_database` | None. |
| `check_node_state`, `check_combo_state`, `check_pathinput_node_state`, `check_multiple_nodes_state` | `scidb.*` | None. |
| `Fixed`, `Merge`, `ColumnSelection`, `ForEachConfig`, `scistack` | `scidb.*` | None. |
| `PathInput`, `Col`, `set_schema`, `get_schema` | `scifor.*` | None. |

## Migrating

Change the import and, if you relied on skipping, pass `skip_computed=True` explicitly:

```python
# before
from scihist import for_each
for_each(fn, inputs=..., outputs=[Out], subject=[])

# after
from scidb import for_each
for_each(fn, inputs=..., outputs=[Out], subject=[], skip_computed=True)
```

In MATLAB, `scihist.for_each` and `scihist.configure_database` likewise forward to their `scidb.*` equivalents.
