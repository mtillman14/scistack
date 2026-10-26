# Plan: ColName on a function node's input port (GUI)

## Goal

When a function node has an input with **Run once per column** ticked, every
other `n/a` row in the Inputs section becomes a dropdown `n/a | ColName`.
Choosing `ColName` binds that parameter to deferred `ColName()`: each
per-column call receives the current column name as a string, the same as
`inputs={"df": MyVar.for_columns(), "col": ColName()}` in code.

## Design decisions

1. **One concept, one owner: a new intent aspect.** `scidb.intent` gets
   `ASPECT_COLNAME = "colname"` (meaning, normalizer). `intent_store` stores it
   as one statement per parameter on the call site (the same shape as
   `columns`), with `set_colname_inputs` in `SETTERS`/`split_node_config` and
   the read in `node_config_overlay`. The blob key is `colNameInputs: string[]`.
   It is **not** folded into `columnSelections`: a column selection restricts
   a variable, and ColName is a different input source. Keeping them in one
   value would mean two concepts sharing one shape.
2. **A new binding kind**, `BINDING_COLNAME = "colname"` (ref `None`), in
   `edge_resolver`. `_attach_colname_inputs` stamps it onto target bindings in
   **both** derivation paths, right beside `_attach_column_selections`, and
   reuses the same placement-insensitive, name/node-scoped matching (factored
   out of `column_selections_for_nodes` so there is only one matcher).
   - ColName **replaces** whatever bound that parameter before (unwired /
     PathInput / Parameter / recorded constant), and logs a WARN that names
     what it replaced.
   - No iterate binding on the target: the ColName binding is dropped with a
     WARN (a stale statement), so the run never reaches scifor's "deferred
     ColName requires an iterate input" error.
3. **Every consumer renders the binding**, none of them interprets the
   statement on its own:
   - `build_run_inputs` → `scidb.ColName()`, plus an INFO line
     `[execution] '<fn>': input '<p>' receives the current column name (ColName)`.
   - `code_export_service` → `ColName()` in Python, `scidb.ColName()` in MATLAB.
   - `matlab_command_service` → `scidb.ColName()` in the generated `for_each`
     struct (the bridge already round-trips `kind='colname'`).
   - `variant_resolver.compute_call_id` ignores the kind, matching scidb, which
     leaves ColName out of version keys. Pinned by a parity test.
4. **Port visibility:** a ColName parameter counts as fed
   (`apply_port_visibility`'s `node_fed`), so an optional argument set to
   ColName stays drawn. (An optional argument behind "+ argument" has to be
   revealed first, as today.)
5. **Frontend:**
   - `ColumnSelectRow`: when `unbound` and the node has an iterate selection,
     the `n/a` button becomes a `<select>` with `n/a` / `ColName`.
   - Unticking the last "Run once per column" clears `colNameInputs` in the
     same save, so no hidden statement is left over.
   - `FunctionNode` chip/tooltip on that port reads `⟨ColName⟩`.
   - `colNameInputs` is added to graph_builder's saved-config key list, or it
     snaps back on refetch.

## Stages

1. scidb: `ASPECT_COLNAME` + normalizer + unit test.
2. intent_store: setter/overlay/split + test round-trip (`put_node_config` →
   `get_node_configs`), scope copy and rekey covered by the generic paths.
3. edge_resolver + execution_service: binding kind, `_attach_colname_inputs`,
   `build_run_inputs` case, logging. Tests: binding replaces n/a / PathInput /
   constant; dropped without iterate; call_id unchanged.
4. Integration test: a GUI-derived run of a `for_columns` function with a
   ColName param saves per-column results that depend on the column name.
5. code export + MATLAB command rendering + tests.
6. Port visibility + saved-config key + frontend (dropdown, clear-on-untick,
   chip). Rebuild both vite bundles.
7. Docs: `docs/claude/column-selection.md` §From the GUI,
   `input-markers-colname-pathinput-pathoutput.md`, GUI manual-testing
   checklist.

## Out of scope (v1)

- "Unused intent" marking for ColName (history doesn't record ColName, so
  every GUI run reapplies it from the statement).
- A ColName option on rows that are variable-fed.
