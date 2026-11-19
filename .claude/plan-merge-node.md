# Plan: Merge node (how= / name= in code, Merge node in the GUI)

Written 2026-11-17. Concept doc: `docs/claude/merge-node.md`. User decisions
2026-11-17: every recommendation in the question round was accepted.

## Decisions

- `how=` ∈ `MERGE_HOWS = ("exact", "inner", "outer", "left", "right")`, default
  `"exact"` (today's strict rule). Owner: `scifor.merge`.
- One `how` per Merge, applied at every left-to-right step. Port/slot order
  is meaningful.
- Joins stay on shared schema keys only (no `on=`).
- Non-exact joins drop or pad rows, and each one is logged at WARN with counts
  and the first few keys. A shared non-key column, a many-to-many match and an
  empty constituent stay errors under every `how`.
- `name=` is required, in Python and MATLAB alike (clean break, beta rule).
- `to_key()` = name + how + constituents → in `__inputs`. Changing `how` or a
  constituent makes a new variant.
- GUI: `mergeNode`, ports `in__m{slot}` (slot ids never reused, so a closed-up
  gap needs no rename), one trailing empty port, one `out`.
- Only variables may feed a Merge port. Merge→Merge, PathInput, Parameter and
  glue are refused. A Merge-fed function argument has no other source.
- Endpoint ref `mg:{name}`. The name is unique per project.
- Merge nodes are drawn from history (code runs show up on the canvas).
- MATLAB gets `how`/`name` in the same piece of work, held to `merge-cases.json`.

## Stages

### diagnose_merge_provenance (no behaviour change)
Write a scidb integration test that runs `for_each` with a Merge input and
asserts what it records:
- `__inputs[param]`
- the provenance edges (do constituent record ids appear, and under which
  param?)
- `compute_input_selectors`

Add DEBUG logging `[merge] provenance: param=… constituents=… rids=…` where
the Merge-fed call stamps its bindings.
**Why first:** the GUI-history stage depends on this. If the constituent rids
are not recorded, that lineage gap is fixed here, in scidb (NOTE 3), before
any GUI work.

### scifor_how_and_name
- `scifor/merge.py`:
  - `Merge(*tables, name, how="exact")`. Validate that `name` is a non-empty
    str and that `how in MERGE_HOWS`.
  - `to_key()` → `Merge(name=…, how=…, A, B)`.
  - `__name__` shows the name.
- `merge_frames(..., how="exact")` → `_merge_two(..., how)`:
  - `exact` is the current code path, unchanged.
  - Other hows: key-based join keeping the existing clash, many-to-many and
    empty checks. Row order: left order, then right-only rows. Pad with NaN.
  - WARN `[merge] {l}+{r} how={how}: dropped {n} row(s) only in {side} {keys…}; padded {m}`.
- Callers pass `merge.how`:
  - scifor foreach `_prepare_merge`
  - `scifor/csv_export.py`
  - `as_df` / `to_csv`
- `merge-cases.json`: add an optional `how` per case (default exact) and new
  cases: inner drops, left pads, right pads, outer both, many-to-many still
  errors under `outer`, clash still errors.
- Tests: `test_merge_cases.py` reads `how`. Update every `Merge(...)` call in
  scifor tests to pass `name=`.

### scidb_forwarding
- `foreach_load.py`, where it wraps loaded tables (~l.574): build
  `_scifor.Merge(*loaded_tables, name=spec.name, how=spec.how)`.
- `foreach_common._resolve_per_combo_merge`: pass `how`.
- `foreach_load._compute_merge_effective_ids` assumes an INNER join (it
  intersects constituent schema ids to validate filter coverage). Make it
  follow `how`: union for outer, the left/right side's ids for left/right.
  Otherwise an outer Merge's extra locations are refused as uncovered.
- `_DryRunMerge`, `scidb/csv_export.py`: pass `how`.
- `scidb.provenance.graph.parse_merge_spec(value) -> {"name", "how", "constituents"} | None`
  is the ONE parser of the `to_key()` spelling, living beside
  `parse_path_input_spec`. Add a round-trip test `parse(to_key(m))` in scifor
  and scidb.
- Tests:
  - an integration test of `how="left"` in `for_each` (padded NaN reaches the
    function, WARN logged)
  - an identity test: `how` change → new variant; same how → skip
  - `to_csv` with `how`
- Fix every existing `Merge(` call in scidb tests and examples to pass `name=`.

### matlab_how_and_name
- `+scifor/Merge.m`, `+scidb/Merge.m`: `name` (required) and `how` name-value
  args. Validate `how` against the list fetched from Python
  `scifor.merge.MERGE_HOWS` (one owner). Fall back to a hard-coded copy only
  if the bridge isn't up; that copy is guarded by a parity test.
- `+scifor/merge_tables.m`: implement the four non-exact hows, with a WARN
  through the MATLAB logger.
- Bridge: carry `name`/`how` wherever `scidb.Merge` is converted for
  `for_each_prepare` (find it with LSP references to `scidb.Merge`).
- `TestMergeCases.m` reads `how`. The user runs the MATLAB suite.

### gui_merge_node_backend
- Node kind `mergeNode`, manual-node meta `{label: name, how}`. Add it to
  `ENTITY_NODE_TYPES` (one entity per name).
- `graph_builder.endpoint_ref`: `mg:{name}`. Add a `TestConnectionId` case.
- `layout_service.put_edge`: refuse non-variable sources into a Merge port,
  Merge→Merge, and a second source on a Merge-fed function argument. Each
  refusal has a message, a WARN and a test.
- `edge_resolver`: a function argument fed by a merge node → read the
  merge's inbound edges from `effective_edges` (no raw reads; the
  `test_edge_view_guard` rule), sort by slot, build
  `scidb.Merge(*specs, name, how)`. DEBUG `[merge-node] {fn}.{arg} ← Merge(name, how, [A, B])`.
- `matlab_command_format`: emit `scidb.Merge(A(), B(), name="…", how="…")`.
- `code_export_service`: emit the Python spelling.
- History: `graph_builder` uses `parse_merge_spec` on `__inputs` to build the
  merge node and its edges (constituent → `in__m{i}`, `out` → fn port).
  Adoption stores them as usual.
- Name uniqueness: create/rename refuses a duplicate. If history holds one
  name with two different specs, the node shows the latest spec plus a
  `conflict` flag and a WARN.
- Panel API: GET/PUT `{name, how}`; the dropdown options come from
  `MERGE_HOWS`.
- Tests:
  - an `edge_parity` scenario where canvas, run and disconnected-check agree
    for a merge-fed function
  - a resolver test that builds the Merge with slot order
  - a history → node test
  - the refusals
  - the MATLAB command string
- Check portability/bundle: manual node meta should travel as-is (verify,
  don't assume).

### gui_merge_node_frontend
- `components/DAG/MergeNode.tsx`: input handles = connected slots sorted,
  plus one trailing empty slot; one output handle. Register it in
  `PipelineDAG.tsx` `nodeTypes`.
- Pure helper `mergePorts.ts` (connected handles → rendered port list +
  next slot id), with a vitest test.
- Palette entry "Merge" next to PathInput. Drop prompts for a name through
  the in-app dialog (no `window.prompt`; see the webview memory).
- `Sidebar/MergeSettingsPanel.tsx`: name and how (each option with a
  one-line description of what it drops or pads).
- Rebuild the bundle. Add an entry to `docs/gui-manual-testing-todo.md`.

### docs
- Update `merge-node.md` (clear the "Unverified" section), the
  `merge-cases.json` "about" text, `edge-model.md` (the `mg:` row and Merge
  binding semantics), and the scifor/scidb READMEs (`name=`, `how=`).

## Status

| Stage | Status |
|---|---|
| diagnose_merge_provenance | DONE 2026-11-17: gap confirmed, fixed via scidb/run/merge_lineage.py; all scidb tests pass (user); uncommitted |
| scifor_how_and_name | built 2026-11-18; pytest not run |
| scidb_forwarding | built 2026-11-18, plus the __merge__ identity record (call-id parity, skip gate, node-state predictor); pytest not run |
| matlab_how_and_name | built 2026-11-18; MATLAB suite not run |
| gui_merge_node_backend | built 2026-11-18 (tests/test_merge_node.py); pytest not run |
| gui_merge_node_frontend | built 2026-11-18; npm test passes (777), both bundles rebuilt; GUI unchecked |
| docs | done 2026-11-18 (merge-node.md, edge-model.md, merge-cases.json, api/guide docs, gui-manual-testing-todo.md) |
