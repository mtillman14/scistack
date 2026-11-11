# Plan: Database Observability API + CLI (`scidb.inspect` / `scidb <command>`)

> Status: **ALL PHASES (1–6) COMPLETE and test-verified 2026-07-05**
> (branch `dev`, user-run suite green; uncommitted as of completion).
> Phase 6 design: stdout carries ONLY the selected record_id (composable
> `$(scidb pick …)`); ambiguity without a listing/interactive flag fails
> with the disambiguation table on stderr; `--interactive` = stdlib
> numbered menus (chooser injected — `pick.drill_down` is pure and
> unit-tested; a future `scidb[tui]` extra would replace only the chooser).
> Phase 6 fix worth keeping: variant menus must be sorted by label —
> candidate order from `_find_record` is newest-save-first and therefore
> run-dependent (regression: `test_variant_menu_order_is_deterministic`).
> Follow-ups now live in the "Later" list: GUI onto `scidb.inspect`
> (graph + open-plot-by-record_id consuming `pick`), MATLAB bridge,
> branch/variant-level exclusion (model work), further write capabilities
> via the mutate.py checklist.
> Phase 4: `sql` escape hatch (SqlResult, JSON-safe cells), `show --values`
> previews, RenderStyle ANSI colors (TTY-only, --no-color honored),
> `docs/guide/inspect.md` + mkdocs nav.
> Phase 5: **generic write seam, not exclusion-specific** (user request:
> future writes incl. hypothesis/finding documentation): `mutate.Mutator`
> facade (per-invocation write session, `@_mutation` audit logging,
> uniform `MutationResult`), `DatabaseLockedError` mapping (also applied to
> Inspector.open), CLI `_write_handler` registration seam in dispatch;
> exclusions (`exclude`/`include`/`exclusions`) are the first plugin —
> new-write-capability checklist documented in mutate.py's docstring.
> Phase 3: `trace` (batched enrichment, fn_hash surfaced, ambiguity =
> AmbiguousVersionError w/ candidates), `runs`/`audit`, `state [--missing]
> [--pathinput]` (PathInput reconstructed from the stored spec for the
> standalone discovery check), CLI literal-coercion of NON-schema key=val
> values (schema keys stay verbatim strings — zero-padded rule pinned).
> Semantics pinned in tests: expected-invocation prediction cross-products
> all coexisting input variants, so a mid-chain variable gaining a variant
> correctly reds its consumer. Tests: `test_inspect_phase3.py`. Phase 1: read-only plumbing
> (sciduckdb `read_only=` + `schema_keys_from_db`, DatabaseManager
> `read_only=` + lazy `db.inspect`), `scidb/inspect/` (`api.py`, `render.py`,
> `cli.py` with discovery), `scidb` console script + `scistack db` alias.
> Phase 2: `graph.py` PipelineGraph + `pipeline`/`variants` commands with
> tree/mermaid/dot/json renderers. Two Phase 2 decisions:
> (a) **step grouping = (fn_name, variable-input wiring)**, coarser than
> call_id (which folds constants in) — one node per step with constants
> aggregated as variants, each variant keeping its call_id; PathInput specs
> displayed but excluded from grouping.
> (b) **node state carries `state_basis`**: "stored_hash" (CLI default —
> most-recently-run hash; detects missing/partial/input-resave but NOT
> source edits) vs "live_fn" (pass fn_registry for full check_node_state
> semantics) vs "none". Tests: `test_inspect_{api,cli,pipeline}.py`.
> Rev 3 (2026-07-05): added the **render style seam** (see "Render style
> seam" section) so tree layout and state-tag wording stay cheaply tunable
> after Phase 3; applied to the Phase 2 renderers and test-verified
> (`TestRenderStyle`; `ASCII_STYLE` preset via `--style ascii` /
> `SCIDB_STYLE`).
> Implementation gotcha worth keeping: global argparse flags must be
> duplicated onto each subcommand parser **with `default=SUPPRESS`** — since
> Python 3.9 a subparser parses into a fresh namespace and copies its
> defaults back, clobbering flags parsed before the subcommand (regression
> test: `test_global_flags_accepted_before_and_after_subcommand`).
> Next: Phase 2 (pipeline graph + variant aggregation moved down from
> scistack-gui `domain/variant_resolver.py`).
> Rev 2 (2026-07-04): primary command renamed `scistack db …` → `scidb …`;
> added Phase 5 (declarative write commands) and Phase 6 (`pick` /
> record-id selector); recorded the read/write bright-line decision.

## Goal

Make it trivial to ask questions of a scidb database from the terminal and from
Python/MATLAB: "what's in here?", "show me the pipeline with all variants and
input values", "what produced this record?", "what's stale?", "who ran what
when?". Additionally (Phase 5–6): flip the small set of declarative flags the
pipeline already consults (schema exclusions), and resolve a specific variable
output to its `record_id` interactively so other tools (e.g. the plot opener)
can consume it.

## Guiding constraints

- **Layering (CLAUDE.md NOTE 3):** all query + graph-shaping logic lives in
  **scidb** (the layer that owns the provenance graph). The CLI is a thin
  rendering shell. Nothing observability-related goes in the GUI layer; instead
  the GUI should eventually consume the same facade (avoids the
  scidb↔GUI duplication that `domain/variant_resolver.py` currently has).
- **Read/write bright line:** all inspection commands open DuckDB with
  `read_only=True` so they never contend with a running GUI/MATLAB session for
  the write lock. The only writes the CLI may ever perform are **declarative
  flags the pipeline already consults** (schema exclusions now; possibly
  variant marks later) via a separate write facade — the CLI never mutates
  records, invocations, or lineage. `Inspector` itself stays strictly
  read-only, and the read-only regression guard stays in force.
- **Everything is derived from the graph** — no new tables, no stored
  denormalizations. This is purely a read-side API over the existing
  `provenance_query` / `state` / `database` primitives.
- **Batch-first:** all multi-record paths use the `*_batch` helpers
  (`branch_params_batch`, `producing_invocation_batch`, `_build_upstream_closure`)
  — no per-record N+1 loops.

## Architecture: two layers

```
┌──────────────────────────────────────────────────────────┐
│  CLI (scidb …)                                            │
│  argparse wiring in scidb/inspect/cli.py; primary entry  │
│  point `scidb`; also mounted as `scistack db` alias by   │
│  scistack/__main__.py                                    │
├──────────────────────────────────────────────────────────┤
│  Renderers (scidb/inspect/render.py)                     │
│  pure functions: graph → ASCII tree / table / mermaid /  │
│  dot / JSON.  No DB access.                              │
├──────────────────────────────────────────────────────────┤
│  Inspector facade (scidb/inspect/api.py)                 │
│  typed result dataclasses; wraps provenance_query,       │
│  state, database.  The ONE read API for CLI, GUI, MATLAB │
├──────────────────────────────────────────────────────────┤
│  existing primitives (provenance_query.py, state.py,     │
│  database.py, sciduckdb)                                 │
└──────────────────────────────────────────────────────────┘
```

### 1. Python facade — `scidb.inspect.Inspector`

Constructed from a `Database` (also exposed as a lazy `db.inspect` property).
All methods return plain dataclasses (JSON-serializable via `asdict`) so the
CLI, GUI, and MATLAB bridge all consume the same shapes.

```python
insp = db.inspect

insp.overview()   -> DbOverview        # schema keys, counts (records/invocations/runs/variables), db size, last activity
insp.variables()  -> list[VariableSummary]   # name, record_count, schema_level, dtype, variant_count, last_saved
insp.variable(X)  -> VariableDetail    # + variants, data columns, per-schema-level record counts
insp.schema_tree()-> SchemaTree        # the _schema hierarchy with counts per node

insp.pipeline(output_type=None, expand_variants=False, include_values=False)
                  -> PipelineGraph     # nodes (variable/function) + edges; per-function:
                                       #   constants {param: {values}}, variant_count,
                                       #   node state (green/red), record counts
insp.variants(X | fn_name) -> list[VariantSummary]   # branch_params, output_num, record_count, call_id
insp.trace(X, **metadata)  -> ProvenanceTree  # resolve record via find_record_id, then
                                              # pipeline(rid): full upstream DAG w/ fn names,
                                              # constants, input records, PathInput specs
insp.runs(fn=None, limit=50) -> list[RunRecord]      # _run audit: timestamp, user, fn, where_clause
insp.audit(X, **metadata)    -> list[RunRecord]      # execution_audit(rid)
insp.node_state(fn)          -> NodeState             # green/red + which expected invocations are missing
insp.combo_state(fn, outputs, inputs, **grid) -> list[ComboState]  # up_to_date/stale/missing per combo
insp.records(X, latest=True, **metadata) -> list[RecordSummary]    # incl. superseded versions when latest=False
```

Mapping to existing primitives (nothing new is computed):

| Facade | Built on |
|---|---|
| `overview` | `_schema`/`_variables`/`_record`/`_run` counts (new small SQL) |
| `pipeline` | `pipeline_variants` + `pipeline_structure` + `state.check_node_state`; variant aggregation logic **moved down** from scistack-gui `domain/variant_resolver.aggregate_variants` |
| `variants` | `pipeline_variants` / `function_variant_configs` |
| `trace` | `find_record_id` → `provenance_query.pipeline(rid)` + `branch_params_batch` |
| `runs`/`audit` | `_run` scan / `execution_audit` |
| `node_state`/`combo_state` | `state.py` (binary state; `check_combo_state`; `check_pathinput_node_state`) |
| `records` | `_find_record` machinery (latest collapse) |

### 2. CLI — `scidb` as the primary entry point

Primary console script: `scidb = "scidb.inspect.cli:main"` (implementation
lives in the owning layer, so the name matches). `scistack/__main__.py` also
mounts the same subparser factory as a `scistack db` alias so the meta entry
point stays complete. Naming note: "SciDB" is also Paradigm4's array database
— cosmetic console-script collision risk only, accepted.

```
scidb status                         # overview: schema keys, variables, record/run counts, red nodes
scidb vars [Type]                    # list variable types / detail for one
scidb schema [--tree]                # schema keys + hierarchy with counts
scidb pipeline [--variants] [--values] [--type X]
               [--format tree|mermaid|dot|json] [-o file]
scidb variants <Type|fn>             # coexisting variants w/ branch params + record counts
scidb trace <Type> [key=val ...]     # full upstream provenance of one record
scidb runs [--fn name] [-n 50]       # execution audit log
scidb state [fn] [--combos key=val…] # green/red per node; per-combo staleness on demand
scidb show <Type> key=val ... [--versions]  # record metadata (+ value preview for scalars/small)
scidb sql "SELECT …"                 # read-only escape hatch (rendered as a table)

# Phase 5 — declarative writes (separate write facade, see below)
scidb exclusions                     # list current schema exclusions (read-only)
scidb exclude key=val … --reason "…" # exclusions.exclude_schema
scidb include key=val … --reason "…" # exclusions.include_schema

# Phase 6 — record selection
scidb pick <Type> [key=val …]        # table of matching records incl. variant/branch-param
                                     # columns + record_ids; --json for scripting;
                                     # --interactive for the drill-down picker
```

Global flags: `--db PATH`, `--json` (on every command — machine-readable output
= `asdict(facade result)`), `--no-color`.

**Database discovery order** (when `--db` omitted):
1. `--db` flag
2. `SCIDB_DATABASE` env var
3. `[tool.scistack] db =` in `pyproject.toml` found upward from cwd (add this
   key to the project scaffold; the GUI can honor it too)
4. exactly one `*.duckdb` in cwd (error listing candidates if several)

Log the resolved path + resolution source on every invocation (NOTE 2).

### Headline render: `scidb pipeline`

Default is a topologically-sorted ASCII tree, collapsed per
`(fn_name, call_id)` like the GUI, with state color, variant count, and
constant values:

```
● RawEMG                                    24 records
└─ bandpass_filter                          [green] 2 variants
     low_hz = {20, 30}   high_hz = 450
   └─▶ FilteredEMG                          48 records
       ├─ compute_max                       [red — 12/24 combos missing]
       │  └─▶ MaxActivation                 12 records
       └─ psd_estimate                      [green]
            window = "hann"
          └─▶ PowerSpectrum                 48 records
```

- `--variants` expands each function node into one line per variant
  (branch-param set + record count) instead of the `{…}` value sets.
- `--values` adds input value previews (constants always shown; variable inputs
  show record counts, PathInput shows the template spec via
  `invocation_path_inputs`).
- `--format mermaid|dot` emits shareable diagram source; `--format json` emits
  the raw `PipelineGraph` for scripting.
- Non-DAG corner cases (multi-producer variables, self-referential
  input==output fns) render as repeated node references (`FilteredEMG (↑)`),
  never infinite recursion.

### Render style seam (rev 3 — keeps presentation tunable after Phase 3)

All *presentation* constants — tree glyphs (bullets, branch/continuation
characters, arrows), state-tag wording (`[green, last-run recipe]`,
`— N/M combos missing`), count/variant suffix formats, and diagram colors —
live in a single `RenderStyle` dataclass in `render.py`, with
`DEFAULT_STYLE` as the one source of truth. Rules:

1. **No renderer may hardcode a presentation literal.** Structure
   (what is a node, what nests under what) is renderer logic; how it is
   drawn comes from the style object. Every renderer takes
   ``style: RenderStyle | None = None`` (None → `DEFAULT_STYLE`).
2. **Phase 3+ renderers (trace, runs, state) must consume the same
   `RenderStyle`** — no parallel constants. New presentation needs
   (e.g. trace's record/audit lines) add *fields* to RenderStyle, so one
   dataclass stays the complete inventory of tunables.
3. **Tuning = edit `DEFAULT_STYLE` defaults in one place**; renderer code
   and Phase 3 logic don't change. Tests that match output either assert
   semantic content (names, counts) or pin DEFAULT_STYLE explicitly.
4. **Presets are just instances**: `ASCII_STYLE` (no-Unicode terminals)
   ships as proof the seam works and is test-covered. A CLI `--style`
   flag / env var mapping onto presets is a cheap later add — wiring only,
   no renderer changes.

### `scidb trace` example

```
MaxActivation  subject=3 session=post          record 9f3a…  saved 2026-07-01 14:22 by mt
└─ compute_max                                  fn_hash 77ab…  (run 2×, last 2026-07-01)
   ├─ values ◀ FilteredEMG subject=3 session=post   record 41c2…
   │  └─ bandpass_filter    low_hz=30  high_hz=450
   │     └─ signal ◀ RawEMG subject=3 session=post  record 08d1…  (raw save)
   └─ (constants) —
```

Backed by `pipeline(rid)` — the "show me the entire pipeline that produced X"
feature the migration built. `--audit` appends the `execution_audit` rows.

## Implementation phases

Each phase is independently shippable and test-verified before the next.

**Phase 1 — facade skeleton + basic commands.**
`scidb/inspect/` package: `api.py` (Inspector + dataclasses: `overview`,
`variables`, `variable`, `schema_tree`, `records`), `cli.py` (argparse group,
db discovery, `--json`), mount in `scistack/__main__.py`. Commands: `status`,
`vars`, `schema`, `show`. Read-only connection plumbing in sciduckdb if not
already exposed.

**Phase 2 — pipeline graph.**
Move/port variant aggregation from scistack-gui `domain/variant_resolver.py`
into `scidb/inspect/graph.py` (`PipelineGraph` from `pipeline_variants` +
`pipeline_structure` + node state). `render.py`: tree/mermaid/dot/json.
Commands: `pipeline`, `variants`. (GUI migration onto this module is a noted
follow-up, not in scope.)

**Phase 3 — provenance + audit + state.**
`trace` (find_record_id → `pipeline(rid)` render), `runs`, `audit`, `state`
(binary node state; `--combos` runs `check_combo_state`;
`check_pathinput_node_state` for PathInput loaders).

**Phase 4 — escape hatch + polish.**
`sql` (read-only), value previews in `show`, `--no-color`/TTY detection,
docs page `docs/guide/inspect.md`.

**Phase 5 — declarative write commands.**
Thin wrappers over the *existing* primitives in `scidb/exclusions.py`
(`exclude_schema` / `include_schema` / `list_exclusions` — all already take a
`reason`). New small write facade (e.g. `scidb.inspect.mutate`) kept separate
from `Inspector` so the read-only guarantee on `Inspector` is structural, not
conventional. Write commands open a write connection only for the duration of
the transaction and log every mutation (NOTE 2). DuckDB is single-writer:
lock contention with a running GUI/MATLAB session must produce a clear
one-line error ("database is locked by another session — close the GUI or
retry"), never a stack trace. `--reason` is required (the primitive requires
it anyway). Tests: mutation round-trip (exclude → `list_exclusions` →
include), lock-contention error path, and the existing read-only regression
guard extended to assert `Inspector`'s connection still cannot write.

**Phase 6 — `pick`: record-id selection.**
Non-interactive first: `scidb pick <Type> [key=val…]` renders a table of
matching records (via `Inspector.records` + `variants`) with schema keys,
branch-param columns, and record_ids; `--json` for scripting
(`open-plot $(scidb pick … --json | …)`). This is the 80% case — metadata is
only ambiguous when multiple variants coexist, and the table disambiguates by
showing branch params side by side. Interactive drill-down
(variable → schema keys → variant) ships second, behind `--interactive`:
start with stdlib numbered-menu prompts (zero deps); a full TUI
(`textual`/`prompt_toolkit`) only as an optional extra
(`pip install scidb[tui]`) if the numbered menus prove insufficient. Scope
guard: the picker *selects* records, it never *displays data* — that is the
GUI's job. The consuming side ("open the plot for record_id X") is GUI-layer
work (NOTE 3) against the same facade, out of scope here.

**Later (explicitly out of scope now):** MATLAB bridge for the Inspector,
GUI consuming `scidb.inspect`, `watch` mode, `diff` between two variants,
HTML report export, branch/variant-level exclusion (graph-model work — see
gaps list in `docs/claude/observability-api-design.md`), record
annotation/tagging, retract/supersede of bad records, force-recompute
(invalidate an invocation), canonical-variant marking, database merge/import,
prune/vacuum of superseded versions (destructive — would need `--dry-run`
default + confirmation if ever built).

## Logging & tests (NOTE 2)

- Every CLI command logs (via `scidb.log`): resolved db path + discovery
  source, per-facade-call timing, row counts returned. `-v` raises verbosity.
- **Unit tests** (pure, no DB): renderers over hand-built `PipelineGraph`
  fixtures — golden-file tests for tree/mermaid output; JSON round-trip of all
  dataclasses.
- **Integration tests** (in `scidb/tests/test_inspect_*.py`): build a small
  real pipeline via `for_each` (reusing existing fixtures), then assert on
  `Inspector` results and on `main([...])` with `--json` (parse stdout as JSON
  — avoids brittle human-format asserts). Cover: multi-variant fn, Fixed,
  PathInput spec surfacing, `__save__` kwargs, excluded records, multi-producer
  variable, self-referential fn.
- **Regression guard**: a test asserting the CLI opens the DB read-only
  (attempting a write through the CLI connection must fail).

## Decisions taken (flag if you disagree)

1. **Command surface = `scidb <verb>`** (dedicated console script, shorter to
   type; implementation in `scidb/inspect/` so the name matches the owning
   layer). `scistack db <verb>` is kept as an alias mounted by
   `scistack/__main__.py`.
2. **No new dependencies**: argparse + hand-rolled table/tree rendering
   (`rich` considered and skipped to keep scidb's footprint zero-extra).
3. **Facade returns dataclasses, renderers are pure** — the GUI/MATLAB reuse
   path and the testing story both fall out of this split.
4. **GUI is not migrated in this effort** — but Phase 2 deliberately creates
   the module it should later consume.
5. **Write bright line:** the CLI may flip declarative flags the pipeline
   already consults (schema exclusions; possibly variant marks later), and
   nothing else — no record/invocation/lineage mutation, no deletion. Writes
   live in a separate facade so `Inspector` stays provably read-only.
6. **`pick` before TUI:** ship the non-interactive record-id table first and
   only invest in an interactive picker (stdlib menus → optional `[tui]`
   extra) if the table proves insufficient in practice.
