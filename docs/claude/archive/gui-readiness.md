# GUI Readiness Assessment

> **Archived 2026-11-10** (grug follow-up, one live doc per concept). Superseded by
> scistack-gui-frontend-architecture.md and canvas-as-document-explained.md. Kept for history; do not treat as current.

This document captures what abstractions are already in place and what is missing for building a GUI on top of SciStack. The GUI concept is a git-style branching map of the pipeline, usable at any layer from scifor through scihist.

## The GUI Vision

The core GUI concept is a **git-style branching pipeline map**:

- Starts as a linear graph: variable types as nodes, functions as edges
- Branches whenever different versions of a variable are generated (e.g., a parameter sweep of `low_hz=20` vs `low_hz=30`)
- Each branch is a unique path through the pipeline defined by a unique parameter/input combination
- Users can browse data, inspect provenance, view version history, and trigger pipeline runs

---

## What's Already in Place

### Data browsing

- `load()`, `head()` with `where=` filters — a data browser can be built on these today
- `load(as_df=True)` (or `load(version="all", as_df=True)` for full history) provides tabular previews

### Schema and data discovery

- `db.dataset_schema_keys` — exposes the experiment schema (e.g., `["subject", "session"]`)
- `db.distinct_schema_values(key)` — enumerates all values for a schema key
- `db.distinct_schema_combinations(keys)` — enumerates all existing schema combinations

### Provenance queries

- `db.get_provenance(VariableClass, **metadata)` — one-hop "what function produced this?" query
- `db.get_provenance_by_schema(**schema_keys)` — all lineage records at a schema location
- `db.get_pipeline_structure()` — unique `(function, input_types, output_type)` combinations; the skeleton of a pipeline graph

### Version history

- `list_versions(**metadata)` — all stored versions at a schema location

### Manual intervention

- `scilineage.manual(data, label, reason)` — re-enters the pipeline after a user edit, documenting the intervention in lineage; conceptually hookable from a GUI

---

## What's Missing

### 1. Variable type discovery from DB ✓

`db.list_variables()` queries the `_variables` table and returns a DataFrame with `variable_name`, `schema_level`, `created_at`, `description` — one row per variable type stored in the database. This works without the original Python class definitions.

---

### 2. Constants not tracked in version keys → branch collision ✓ IMPLEMENTED

`ForEachConfig._get_direct_constants()` now extracts scalar constants and `to_config_keys()` serializes them under `__constants`. Two `for_each` calls that differ only in a constant now produce records with **distinct version keys**, so both are stored and discoverable.

Additionally, a `branch_params` column in `_record_metadata` accumulates all branch-discriminating parameters (both scalar constants, namespaced by function, and dynamic input discriminators) for each record. This enables the load API described under item 3.

---

### 3. No load API for branch filtering ✓ IMPLEMENTED

**What was added:**

- `branch_params` column in `_record_metadata`: flat JSON dict accumulating all discriminating params (constants namespaced as `fn.param`, dynamic input columns bare)
- `load(**branch_params)`: non-schema kwargs are treated as branch_params filters; `AmbiguousVersionError` is raised when multiple variants exist and no filter narrows to one
- `list_versions(...)`: returns `branch_params` per record; accepts non-schema kwargs as branch_params filters; `include_excluded=True` shows excluded variants
- `db.exclude_variant(record_id_or_type, **kwargs)` / `db.include_variant(...)`: mark variants as excluded from default queries
- `AmbiguousVersionError`, `AmbiguousParamError`: new exceptions exported from `scidb`
- Suffix matching: `load(low_hz=20)` matches `bandpass_filter.low_hz=20` in branch_params

**Also added:** `db.list_pipeline_variants()` — returns distinct `(function_name, output_type, input_types, constants, record_count)` per pipeline branch, sourced from `version_keys` in `_record_metadata` (does not require scilineage tracking).

---

### 4. Function/pipeline registry — SUPERSEDED for the pipeline-builder case

**Update 2026-07-16:** this conclusion was correct for a *visualization*
GUI but flips for goal-directed execution and a GUI pipeline *builder* —
both need the graph before first execution. A prescriptive registry now
exists: `db.pipeline(name)` + deferred `for_each` registration
(`scidb/run/pipeline.py`; see [endpoint-first-pipelines.md](endpoint-first-pipelines.md)).
The original reasoning, kept for the pure-visualization case:

A registry would only be necessary for a no-code pipeline builder that reconstructs and replays pipelines without executing Python. If the GUI is a visualization and control layer over an existing Python project, this is unnecessary:

- Function definitions already exist in Python modules
- `get_pipeline_structure()` provides the graph skeleton
- `list_pipeline_variants()` provides the branch enumeration
- `for_each(fn, ...)` is already the execution primitive — the pipeline IS the Python code

---

### 5. Progress/event callbacks — not needed

`for_each()` prints to stdout, which covers the logging use case. Outside a GUI, structured callbacks (on_start, on_iteration, on_save, on_error) offer little over the existing print output.

For a GUI, the cleanest integration is to run `for_each` in a subprocess or thread and poll `list_versions()` periodically — the DB is the progress channel. This avoids coupling the GUI lifecycle to `for_each` internals.

---

### 6. No multi-hop DB-side provenance traversal ✓ IMPLEMENTED

`db.get_upstream_provenance(record_id, max_depth=20)` — BFS traversal returning a flat list of provenance nodes from the queried record back to its roots.

Does not use `_lineage` (only populated by scilineage, not `for_each`). Instead uses branch_params subset matching: at each hop, `version_keys.__inputs` gives the input variable types, and the upstream record is found by locating the record of that type at the same schema location whose `branch_params` is a subset of the current record's `branch_params`. The mostntrofdu specific match (most keys) wins when multiple candidates pass the subset check.

Each node: `{record_id, variable_type, schema, branch_params, function_name, constants, depth, inputs}`.

---

## On Branch Naming

An important design question: do pipeline branches need explicit user-facing names (like git branch names), or are they inherently identified by their parameter values?

**Branch naming is probably not necessary.** In git, names are needed because commit content doesn't carry semantic information about _why_ branches diverge. In SciStack, the parameters themselves carry that meaning — `bandpass_filter(low_hz=20, high_hz=450)` is already self-documenting. A GUI can auto-label branches from their distinguishing constants without requiring user-supplied names.

Explicit branch names could be offered as an optional UX enhancement (let the user tag a branch as `"conservative_filter"` for easier navigation), but they are not architecturally necessary. The branch identity is the unique combination of `(function_name, constants)`.

---

## Summary Table

| Capability                         | Status       | Notes                                                                      |
| ---------------------------------- | ------------ | -------------------------------------------------------------------------- |
| Data browsing (load, filter, peek) | **Ready**    | `load()`, `load(as_df=True)`, `head()`, `where=`                           |
| Schema/data discovery              | **Ready**    | `distinct_schema_values/combinations()`                                    |
| One-hop provenance                 | **Ready**    | `get_provenance()`, `get_provenance_by_schema()`                           |
| Pipeline skeleton graph            | **Ready**    | `get_pipeline_structure()`                                                 |
| Version history                    | **Ready**    | `list_versions()`                                                          |
| Manual intervention hook           | **Ready**    | `scilineage.manual()`                                                      |
| Variable type discovery from DB    | **Ready**    | `db.list_variables()`                                                      |
| Parameter sweep branch isolation   | **Ready**    | `__constants` in version_keys + `branch_params` column                     |
| Branch-aware load API              | **Ready**    | `load(low_hz=20)`, `AmbiguousVersionError`, `list_versions(branch_params)` |
| Variant exclusion                  | **Ready**    | `db.exclude_variant()`, `db.include_variant()`                             |
| Pipeline branch enumeration        | **Ready**    | `db.list_pipeline_variants()`                                              |
| Function/pipeline registry         | **N/A**      | Not needed if GUI is a layer over Python code                              |
| Progress/event callbacks           | **N/A**      | DB polling via `list_versions()` is sufficient for GUI                     |
| Multi-hop DB provenance traversal  | **Ready**    | `db.get_upstream_provenance(record_id)`                                    |
| Explicit branch naming             | **Optional** | Parameters are self-documenting; names are a UX enhancement                |
