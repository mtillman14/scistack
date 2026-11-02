# Database & Configuration

<!-- Ground truth (tests/source win over prose). Verified against:
     scidb/src/scidb/store/database.py: configure_database(dataset_db_path, dataset_schema_keys)
       -> DatabaseManager (auto-registers known subclasses, sets scifor schema, enables caching);
       get_database(); register() idempotent; list_versions(variable_class, include_excluded=False,
       **metadata) -> list[dict] with keys record_id/schema/branch_params/timestamp (sorted newest
       first); get_provenance(...) -> {function_name, function_hash, inputs, constants}|None;
       has_lineage(record_id)->bool; find_by_lineage(invocation); distinct_schema_values(key);
       save/load/etc accept db= for one-shot ops;
     scidb/src/scidb/project/discover.py: discover_module(module), scan_package(name), scan_project(root,*,
       skip_dists, library_filter) -> DiscoveryResult (tooling/GUI introspection);
     scidb/src/scidb/model/exceptions.py: SciStackError + NotRegisteredError/NotFoundError/
       DatabaseNotConfiguredError/ReservedMetadataKeyError/AmbiguousVersionError/AmbiguousParamError.
     NOTE: list_versions keys are timestamp + branch_params (NOT created_at/version);
     there is no load_all / include_record_id. -->

This guide covers configuring the database, the schema-key model, and the
operations on the database handle. For where it sits in the stack, see
[Architecture & Layers](../concepts/architecture.md).

## Configure the database

One call opens the DuckDB database, declares the dataset schema, auto-registers
known variable types, and enables lineage caching. It returns the database
handle:

```python
from scidb import configure_database

db = configure_database("experiment.duckdb", ["subject", "session"])
```

The two arguments are the database file path and the **dataset schema keys** —
both required.

## Schema keys vs. version keys

The schema keys you pass are the dividing line for *all* metadata:

- **Schema keys** (e.g. `subject`, `session`) identify the *location* of data —
  the coordinates you address records by.
- **Everything else** is a **version key**: metadata that distinguishes different
  computational *variants* at the same location.

So with `["subject", "session"]` declared, saving a result with an extra
`factor=2.0` keeps `subject`/`session` as the address and treats `factor` as a
variant discriminator (it lands in `branch_params`). This is the mechanism behind
[variants and caching](caching.md).

## Access the database anywhere

After configuration, retrieve the global handle without passing it around:

```python
from scidb import get_database

db = get_database()   # raises DatabaseNotConfiguredError if not configured yet
```

### Working with more than one database

`configure_database` sets the *global* default, but `save`, `load`,
`get_provenance`, and similar accept a `db=` argument for one-shot operations
against another database without changing the global:

```python
other = configure_database("aim2.duckdb", ["subject", "session"])  # also becomes global
StepLength.save(data, db=other, subject=1, session="A")            # explicit target
```

## Registration

Variable types are registered automatically — `configure_database` registers
every defined `BaseVariable` subclass, and using a type registers it on demand.
Registering creates the type's table if needed and is idempotent, so explicit
registration is only occasionally useful (e.g. a class defined *after* setup):

```python
db.register(LateDefinedVariable)   # safe to call repeatedly
```

## Inspect version history

`list_versions` returns every stored version at a location, newest first. Each
entry is a dict with `record_id`, `schema` (the location keys), `branch_params`
(the variant keys), and `timestamp`:

```python
for v in db.list_versions(StepLength, subject=1, session="A"):
    print(v["record_id"][:12], v["timestamp"], v["branch_params"])
```

Non-schema keyword arguments are treated as `branch_params` filters, so you can
narrow to a single variant. To list the distinct values a schema key takes across
the database, use `db.distinct_schema_values("subject")`.

## Query provenance

`get_provenance` returns what produced a stored value — or `None` if it was saved
without lineage:

```python
prov = db.get_provenance(StepLength, subject=1, session="A")
if prov:
    prov["function_name"]   # the producing function
    prov["inputs"]          # variable inputs it consumed
    prov["constants"]       # literal parameters

db.has_lineage(record_id)   # True if this record has a lineage record
```

See [Tracking Lineage](lineage.md) for how provenance is recorded.

## Discovering variable types

For tooling that needs to find the variable types and lineage functions defined
across a codebase, `scidb` provides introspection helpers — `discover_module(module)`
and `scan_package(name)` enumerate a module's or package's pipeline-relevant
exports, and `scan_project(root)` scans a whole project (this is what the GUI's
library panel uses). Day-to-day pipelines don't need these; reach for them when
building tooling on top of SciStack.

## Errors you might hit

All inherit from `SciStackError`:

| Exception | Cause |
|---|---|
| `DatabaseNotConfiguredError` | `get_database()` (or a save/load) before `configure_database()` |
| `NotRegisteredError` | Using a type that was never registered |
| `NotFoundError` | No record matches the query |
| `ReservedMetadataKeyError` | A reserved key used in metadata |
| `AmbiguousVersionError` | `version="latest"` but multiple variants exist at the location |
| `AmbiguousParamError` | A parameter reference matches more than one candidate |

## Where the data lives

Everything is one DuckDB file. Each variable type gets its own data table
(`{ClassName}_data`), alongside shared tables for schema, metadata, lineage, and
variable groups. Because values are stored in native, queryable DuckDB types
(`LIST`, nested `LIST`, `JSON`), you can open the file in DBeaver or any
DuckDB-compatible viewer and read it directly.

**Next:** [Defining Variables](variables.md) · [Tracking Lineage](lineage.md) ·
[Computation Caching](caching.md) · [API: Database](../api/database.md)
