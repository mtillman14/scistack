# Inspecting a Database (`scidb` CLI)

The `scidb` command answers questions about an existing database from the
terminal — what's in it, how the pipeline is wired, what produced a specific
record, and what needs (re-)running — **without touching your data**: every
inspection command opens the database strictly read-only, so it never
contends with a running GUI or MATLAB session for the write lock.

The same functionality is available programmatically as
`scidb.inspect.Inspector` (`db.inspect` on a live `DatabaseManager`, or
`Inspector.open(path)` standalone), returning plain dataclasses. Every CLI
command accepts `--json` to emit exactly those dataclasses as JSON.

```console
$ scidb status                 # or: scistack db status / python -m scidb.inspect.cli
$ scidb pipeline
$ scidb trace MaxActivation subject=3 session=post --audit
```

## Finding the database

When `--db PATH` is omitted, the database is discovered in this order:

1. `--db PATH` flag
2. `SCIDB_DATABASE` environment variable
3. `db = "..."` in the project's `scistack.toml` (in the current directory
   only; a relative path is relative to that file)
4. exactly one `*.duckdb` file in the current directory
   (an error lists the candidates if there are several)

The resolved path and its source are logged to `scidb.log` next to the
database file on every invocation.

## Command reference

| Command | Question it answers |
|---|---|
| `scidb status` | What is in this database? (counts, size, last activity) |
| `scidb vars [Type]` | Which variable types exist / detail for one |
| `scidb schema [--tree]` | How is the experiment structured? |
| `scidb pipeline [--variants] [--values] [--type X] [--format tree\|mermaid\|dot\|json] [-o FILE]` | The whole pipeline DAG |
| `scidb variants <Type\|fn>` | Which variants coexist, and how do they differ? |
| `scidb trace <Type> [key=val …] [--record-id RID] [--audit]` | What produced this exact record, all the way down? |
| `scidb runs [--fn NAME] [-n 50]` | Who ran what, when, with what `where=`? |
| `scidb state [fn] [--missing] [--pathinput [key=val …]]` | What needs (re-)running? |
| `scidb show <Type> key=val … [--versions] [--values]` | The records (and versions) at a location |
| `scidb sql "SELECT …"` | Anything else (read-only) |
| `scidb pick <Type> [key=val …] [-i] [--table]` | Which record_id is this specific output? |
| `scidb exclusions` | What's currently excluded, and why? |
| `scidb exclude key=val … --reason "…"` | Exclude a schema location from every analysis (write) |
| `scidb include key=val … --reason "…"` | Re-include it (write; history preserved) |

### `pipeline`

Renders the type-level DAG: variables, function steps, coexisting variants,
constants, and run state. One node per **step** (function + variable-input
wiring); a parameter sweep shows as one node with its constants aggregated:

```text
● RawEMG    24 records
└─ bandpass_filter  [green, last-run recipe]  2 variants
     low_hz = {20, 30}
   └─▶ FilteredEMG    48 records
```

`--variants` expands one line per constants combination; `--values` adds the
input parameters and PathInput templates; `--format mermaid|dot` emits
shareable diagram source (`-o FILE` writes it); `--type X` restricts the
graph to `X` and everything upstream of it.

### `trace`

Resolves one record (schema keys plus branch params, or `--record-id`) and
prints its full upstream provenance: producing functions with their
`fn_hash`, run counts, constants, PathInput templates, input records, and
save events, down to the raw saves. If the metadata matches several records
(coexisting variants, or under-specified schema keys), the command errors
and lists the candidates — narrow with a branch param (`low_hz=20`) or use
`--record-id`. `--audit` appends every run that (re)produced the record,
including the `where=` filter as issued (display-only).

### `state`

Binary node state, same semantics as the GUI (§ node states): **green** iff
every expected invocation — derived live from the current input data — is
present; **red** otherwise (never run, partially run, input re-saved, new
input data). `--missing` lists exactly which schema combos are missing.

!!! note "The `last-run recipe` caveat"
    Standalone, the CLI has no access to your pipeline functions' source
    code, so expected invocations are computed against the most recently
    **run** `function_hash` (`state_basis: "stored_hash"`, tagged
    `last-run recipe`). This detects missing/partial coverage and re-saved
    inputs — but **not a source edit made since the last run**. For full
    semantics pass the live functions in Python:
    `db.inspect.node_state(my_fn)` or
    `db.inspect.pipeline(fn_registry={"my_fn": my_fn})`.

**PathInput loaders** (functions whose only inputs are files) are green
when run and red only when never run — un-run combos leave no trace. The
discovery check closes that gap by walking the filesystem *now*:

```console
$ scidb state import_emg --pathinput                      # all files on disk
$ scidb state import_emg --pathinput subject=S01 subject=S02   # restricted grid
```

should-run = `PathInput.discover()` ∩ grid − schema exclusions, compared
against the locations actually imported. A new file on disk that was never
imported turns the node red; excluding its schema location flips it back.

### `show` and `sql`

`show` lists the records at a location — latest per variant by default,
`--versions` for the full re-save trail, `--values` for a compact value
preview per record. `sql` runs arbitrary SQL against the read-only
connection (the per-type `<Type>` views are handy here); any write statement
fails at the DuckDB level.

### `pick` — resolve an output to its record_id

`pick` turns "this specific variable output" into a `record_id` other tools
can consume (e.g. opening the exact plot for that variant). **stdout carries
only the id** — menus and tables go to stderr — so it composes:

```console
$ open-plot $(scidb pick MaxActivation subject=3 session=post low_hz=20)
```

- Exactly one match → the id is printed, done.
- Several matches (coexisting variants, under-specified keys) → the command
  *fails* and prints a disambiguation table (schema keys, branch params,
  producing function) to stderr, so `$(…)` never captures garbage. Narrow
  the filters, or:
- `--interactive` / `-i` — numbered menus drill down variable → schema keys
  → variant, skipping any level that doesn't disambiguate. Omit the type to
  start from a variable menu. `q` cancels.
- `--table` / `--json` — list all candidates instead of selecting.

The picker only *selects* records — viewing data stays with `show --values`
and the GUI.

### `exclude` / `include` — the only writes

Exclusions mark a schema location as skipped by every `for_each` (see
*Permanent Schema-Level Exclusions* in the scidb README). The CLI commands
wrap `scidb.exclude_schema` / `include_schema` directly:

```console
$ scidb exclude subject=3 session=pre --reason "sensor slipped during trial"
$ scidb exclusions
$ scidb include subject=3 session=pre --reason "re-reviewed video, data valid"
```

`--reason` is required — every change is a new audit row (nothing is
deleted), attributed and timestamped. Omitted schema keys act as wildcards
(`exclude subject=3` excludes every session of subject 3). Excluding a
combo also removes it from `state --pathinput`'s should-run set, so an
unwanted file on disk stops flagging the loader red.

!!! info "Write commands and the read-only guarantee"
    These are the **only** commands that write, and they only flip
    declarative flags the pipeline already consults — never records,
    invocations, or lineage. They open a read-write session just for the
    one transaction; if a GUI or MATLAB session holds the database you get
    a one-line "locked by another session" error, not a stack trace. All
    other commands remain strictly read-only.

## key=value matching rules

- **Schema keys** match verbatim as strings — `subject=01` matches the
  stored `"01"` (zero-padded values are never converted).
- **Everything else** (branch params) is parsed as a Python literal, so
  `low_hz=20` matches the stored integer `20`, and bare names suffix-match
  their namespaced form (`low_hz` → `bandpass_filter.low_hz`).

## Output styles

Tree glyphs, state-tag wording, and colors all come from one `RenderStyle`
object (`scidb.inspect.render`). `--style ascii` (or `SCIDB_STYLE=ascii`)
switches to pure-ASCII output for terminals without box-drawing support.
State tags are colored only on a real terminal; `--no-color` (or piping the
output) disables color entirely.

## Notes

- A DuckDB database can be open read-write by only one process: if the GUI
  or MATLAB currently holds the database, `scidb` reports the lock error —
  close the writer and retry.
- `scidb` is also mounted as `scistack db <command>`, and works without
  installation via `python -m scidb.inspect.cli`.
