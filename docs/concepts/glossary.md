# Glossary

<!-- Ground truth (tests/source win over prose). Definitions confirmed against:
     scidb/src/scidb/{variable,fixed,variant,each_of,column_selection,colname}.py;
     scifor/src/scifor/{merge,pathinput,pathoutput}.py;
     scicanonicalhash/src/scicanonicalhash/hashing.py (canonical_hash, generate_record_id);
     scilineage/src/scilineage/{core,hashing,inputs,backend}.py;
     scidb/src/scidb/database.py (configure_database, content_hash/record_id, call_id);
     scihist/src/scihist/{foreach,state,database}.py (skip_computed, node state, configure_backend);
     scidb/src/scidb/library.py + scistack_gui/services/library_authoring.py and
     library_service.py (Library, Step value, Tunables);
     and the tests cited in the sibling Concepts pages. Keep definitions consistent
     with those pages; do not introduce claims not backed by them. -->

Definitions of the core terms used throughout SciStack, with links to the
concept page where each is explained in full.

**Addressing metadata** — The keyword coordinates a value is saved at and loaded
by (e.g. `subject=1, session="A"`). Matched as given on load; partial matches and
list "match-any" semantics are supported. → [Variables & Storage](variables.md)

**BaseVariable** — The class you subclass to define a variable *type*. It owns
serialization (`to_db` / `from_db`) and provides `save` / `load`.
→ [Variables & Storage](variables.md)

**branch_params** — The namespaced record of the constant values chosen along a
computation (e.g. `bandpass.low_hz: 20`), stored with an output to distinguish one
**variant** from another. → [Computation Caching](caching.md)

**call_id** — A stable hash of an output's version keys *excluding* the function
hash, so the same function used at multiple `for_each` call sites keeps separate
expected-combo bookkeeping. → [Computation Caching](caching.md)

**Column selection** — `MyVar["col"]` (one column → array) or
`MyVar[["a", "b"]]` (subset → DataFrame): extract columns of a table variable for
a `for_each` input. `MyVar.for_columns()` runs the function once per column.
→ [Variables & Storage](variables.md)

**Combo state** — The per-combination classification underlying node state:
`up_to_date`, `stale`, or `missing`. → [Node States](node-states.md)

**configure_backend** — Registers a cache backend with the lineage engine so
lineage hashes can be looked up; `scihist` wires the `scidb` database in as this
backend. → [Computation Caching](caching.md)

**configure_database** — One-call setup that opens the DuckDB database, declares
the dataset schema keys, and auto-registers known variable types.
→ [Variables & Storage](variables.md)

**Constant** (`constant()`) — A literal pipeline parameter (a sampling rate, a
weight) wrapped so it behaves transparently like its value while being recognized
as a constant rather than a data dependency. → [Variables & Storage](variables.md)

**Content hash** (`canonical_hash`) — A deterministic 16-character fingerprint of
a *value*: lists hash in order, dicts independent of key order, numpy arrays by
content + dtype + shape. → [Versioning & Content Hashing](hashing.md)

**Dataset schema (keys)** — The ordered condition columns of your experiment
(e.g. `["subject", "session"]`) declared at setup; the coordinates `for_each`
iterates over and values are addressed by.
→ [Architecture & Layers](architecture.md)

**EachOf** — `EachOf(a, b, …)`: declares alternatives for a `for_each` parameter
— variable types, constants, or `where=` filters. Each alternative becomes a
separate variant; multiple `EachOf` axes multiply (cartesian).
→ [Guide: Batch Processing](../guide/for_each.md)

**Fixed** — `Fixed(Var, session="BL")`: load an input with fixed metadata instead
of the current iteration's — e.g. always comparing against a baseline session.
→ [Guide: Batch Processing](../guide/for_each.md)

**for_each** — The batch engine that runs a function over every combination of
schema values. `scifor.for_each` works on plain tables; `scidb.for_each` and
`scihist.for_each` load inputs from and save outputs to the database by metadata.
→ [Architecture & Layers](architecture.md)

**Function hash** — A bytecode-based SHA-256 fingerprint of a function.
Reformatting, comments, and docstrings don't change it, and by default it also
reflects the functions it calls. Stored full-width (64 chars) in lineage and
truncated to 16 chars as `__fn_hash` in scidb's version keys.
→ [Versioning & Content Hashing](hashing.md)

**Library** — A submodule packaged as an ordinary Python package
(`pyproject.toml` + `src/<name>/` holding its code, pipelines and entities file),
so many projects can install it. Its functions are named after it
(`emg_tools.bandpass`). *Installed* libraries (🔒, from `pip`) are read-only
in a project; *authored* libraries (✏) live in the project's `libs/<name>/`, made
by **⇪ Share as library** or **✎ Make my own copy**, and are edited on the canvas
and published with **⇪**. A copy records its ancestors in `[library] derived_from`
and depends on none of them. From a terminal: `scistack library create`, `scistack
library copy <name> --as <new>`, `scistack library build`. → [Libraries](../sharing/libraries.md)

**Lineage** — The recorded provenance of a value: the function, its inputs, and
its function hash. → [Lineage & Provenance](lineage.md)

**lineage_fcn** (`@lineage_fcn`) — The decorator that wraps a function so each
call returns a `LineageFcnResult` carrying provenance.
→ [Lineage & Provenance](lineage.md)

**Lineage hash** — The identity of a *computation*: the function hash combined
with its classified inputs. It is the cache key.
→ [Computation Caching](caching.md)

**LineageFcnResult** — The value-plus-provenance object returned by a
`@lineage_fcn` call; the computed value is on `.data`. (Informally, a "thunk".)
→ [Lineage & Provenance](lineage.md)

**manual** — `manual(data, label=…, reason=…)`: re-enter the pipeline after an
out-of-band edit, recording it as a first-class `"manual"` lineage step.
→ [Lineage & Provenance](lineage.md)

**Merge** — `Merge(...)`: combine two or more table inputs column-wise into a
single input per iteration. → [Guide: Batch Processing](../guide/for_each.md)

**Node state** (green / grey / red) — A function or variable node's run state:
**green** = every expected output exists and is current; **grey** = partially done;
**red** = never run, or at least one output is stale. → [Node States](node-states.md)

**PathInput** — A path-template input that locates *existing* files by
substituting metadata into a template and walking the filesystem (discovery,
regex matching, per-combo loading).
→ [Guide: Batch Processing](../guide/for_each.md)

**PathOutput** — A pure *output* path template: it substitutes the current combo's
metadata (and the current `for_columns` column via `{ColName}`) and hands the
resolved path to the function to write — no discovery, no reading.
→ [Guide: Batch Processing](../guide/for_each.md)

**record_id** — A value's content-addressed identity, from
`generate_record_id(class_name, schema_version, content_hash, metadata)`.
Identical data at identical coordinates reproduces the same id (so saves dedup);
any field changing makes a new one. → [Variables & Storage](variables.md)

**schema_version** — An integer stamp on a variable's *structure*. Bump it when
the data layout changes so old and new records don't collide.
→ [Variables & Storage](variables.md)

**skip_computed** — `for_each`'s default behavior of skipping combinations whose
output already exists and is current, so the function isn't re-run for finished
work. → [Computation Caching](caching.md)

**Step value** — An argument of a step inside a placed library pipeline that
no Parameter or edge feeds (e.g. a function's `gain=1.0` default), addressed as
`emg_tools.filter_emg.gain`. It belongs to that one placement: set it in the
placement's **⚙ Tunables** in the GUI, or in code with
`pipeline.bind(params={"emg_tools.filter_emg.gain": 2.0})` (`scidb.PipelineBinding`). → [Libraries › Tunable values](../sharing/libraries.md#tunable-values)

**Tunables** — What a project can change about a placed library pipeline without
editing it: project Parameters (the project's value always wins over the
library's default), path-input root folders, and step values.
→ [Libraries › Tunable values](../sharing/libraries.md#tunable-values)

**Variant** — One of several parallel results of the
same function, distinguished by its constants / `branch_params` (or by `EachOf` /
`Variant` selection). The `Variant(Var, low_hz=20)` wrapper pins an input to a
chosen branch_param variant at load time. → [Node States](node-states.md)

**Version** — A successive save of the same variable at the same coordinates.
`load(version="all")` returns the full history; `load` defaults to the latest.
→ [Variables & Storage](variables.md)

## Packages

The three **user-facing** layers are all you import (see
[Architecture & Layers](architecture.md) for how they stack):

| Package | Role |
|---|---|
| `scifor` | The batch-iteration engine over plain tables (no database) |
| `scidb` | Typed, versioned variable storage + DB-backed `for_each` |
| `scihist` | Lineage-wrapped `for_each` + node-state / staleness (re-exports the lineage decorator) |

Everything else is an **internal package** you reach through those layers,
documented under [Internals](../internals/index.md): `scilineage` (provenance
engine), `sciduckdb` (DuckDB storage), `scicanonicalhash` (hashing), `scipathgen`
(path generation), `scimatlab` (MATLAB bridge), and `scidb-net` (networking).
