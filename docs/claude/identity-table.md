# Identity table: which facts of a call reach which id

*Owner: the `IDENTITY_TABLE` dict in `scidb/tests/test_identity_table.py`.
This page mirrors it for reading; when the two disagree, the dict is right and
this page is stale. The test varies one fact at a time on a tiny `for_each`
run (a baseline and a variant, each in a fresh database) and asserts that
exactly the ids marked **yes** changed. Design review 04b, W4.*

## The four ids

| id | names | recipe (one owner) |
|---|---|---|
| `record_id` | one stored value | `scicanonicalhash.generate_record_id(type, schema_version, content_hash, {schema, version})`; the `version` half holds the call's config keys (`ForEachConfig.to_config_keys`), its constants and `__invocation_id` |
| `invocation_id` | one activity (one function call) | `scidb.provenance.graph.compute_invocation_id`, via `scidb.provenance.save.invocation_identity` |
| `call_id` | one call site in code | `scidb.run.foreach_config.CallSite.call_id` (keys: `_CALL_ID_INCLUDED_KEYS`) |
| `wiring_id` | one canvas node | `scidb.provenance.graph.compute_wiring_id` |

## The table

| fact varied | `record_id` | `invocation_id` | `call_id` | `wiring_id` |
|---|---|---|---|---|
| nothing (control: two databases, same call) | no | no | no | no |
| function hash (body edited, same name) | yes | yes | no | no |
| function name (same body, renamed) | yes | yes | yes | yes |
| input types (another variable type feeds an argument) | yes | yes | yes | yes |
| input record ids (an input re-saved with new data) | yes | yes | no | no |
| selectors (`Var["a"]` vs `Var["b"]`) | yes | yes | no | no |
| constants (`k=2` vs `k=3`) | yes | yes | yes | no |
| `as_table` | yes | yes | yes | no |
| `distribute` | yes | yes | yes | no |
| `across_variants` (`AcrossVariants(Var)`) | yes | yes | yes | no |
| iteration level (per subject vs once over the dataset) | yes | yes | no | no |
| output types (save into another variable) | yes | no | no | yes |
| PathInput names (same files, another name) | yes | yes | yes | yes |

Reading it:

- **`record_id` is the widest.** It holds the invocation (`__invocation_id`)
  plus the output's own type and content, so every row that moves
  `invocation_id` moves it too, and so does the output type.
- **`call_id` is the code's view.** Which function, which TYPES feed it, its
  constants and run options. It ignores what the data is (record ids,
  selectors) and how the call was iterated (level), and it ignores the
  function body (an edit is a new version at the same call site).
- **`wiring_id` is the canvas's view.** Function name, input types, output
  types and PathInput names: constants and run options are variants of one
  node, not separate nodes.
- **A PathInput counts by its NAME** (`PathInput.to_key()`), never by its
  template or folder, so moving the data changes no id.

Not in the table (covered elsewhere): glue chains (`docs/claude/free-code-glue-nodes.md`
§2), `where=` (never in `call_id`; see `ForEachConfig.to_config_keys`), `on_error` (never identity).
