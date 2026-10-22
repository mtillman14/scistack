# for_each kwarg parity: scifor <-> scidb, Python + MATLAB

Goal: a call written for `scifor.for_each` runs on `scidb.for_each` and vice
versa, as far as kwargs go, in both languages. Not forced parity — what an
input IS differs by design (DataFrames/tables in scifor, variable types in
scidb), and `outputs` is required in scidb, optional in scifor.

Built 2026-10-18. Plan: `.claude/plan-for-each-kwarg-parity.md`.

## The contract

| Kwarg | scifor | scidb |
|---|---|---|
| `fn`, `inputs` | 1st, 2nd positional | same |
| `outputs` | 3rd positional, optional: names, classes, or a count | 3rd positional, required: variable types |
| `dry_run`, `as_table`, `distribute`, `where`, `locations`, `share_limits`, `schema_keys` | real | real (same defaults) |
| `schema_key_types` | real: the call owns it (standalone) | real: RECORDED in the database, which owns it (`docs/claude/schema-key-types.md`) |
| `save`, `db`, `introspect`, `skip_computed`, `schema_filter`, `finalized`, `glue`, `parameter_names`, `pipeline` | accepted, logged (INFO `discard_db_only_kwargs`), ignored | real |
| `categorical` (MATLAB only) | real | real — applied to the RETURNED table after saving |
| `preload`, `parallel` (MATLAB only, legacy) | ignored | accepted, ignored |

Consequence: the DB-only names cannot be schema keys in either layer.

## Owners

| Concept | Python | MATLAB |
|---|---|---|
| DB-only kwarg list | `scifor.DB_ONLY_KWARGS` (`scifor/outputs.py`) | `db_only_kwargs()` in `+scifor/for_each.m` = Python set + preload/parallel |
| Output -> column name | `scifor.output_column_name` | `+scifor/output_column_name.m` (cases shared via `docs/claude/output-column-name-cases.json`) |
| Location filter | `scifor.locations.filter_combo_indices` (`filter_combos` wraps it) | none — `+scifor/for_each.m` calls `py.scifor.filter_combo_indices` |
| `locations` JSON text | `scifor.locations.locations_from_text` (bridge's `_locations_from_matlab` delegates) | — |
| categorical conversion | — | `+scifor/categorize_metadata.m` (scifor's loop and scidb.m both call it) |

## column_name

Rule, first hit wins: a string is itself; a non-empty `column_name` class
attribute; `view_name()`; the class name. In MATLAB a `Constant` property
(`properties (Constant) column_name = "step_len" end`) — NOT declared on
`scidb.BaseVariable`, because MATLAB subclasses cannot redefine a superclass
property.

- Renames the RESULT column only. The record still saves as its type; scidb's
  save log lines use `_output_type_label` (the type), not the column.
- Applies only when an output fills one cell. DataFrame outputs whose rows
  spread keep their own columns; scifor logs INFO when explicit names go unused.
- MATLAB scidb: Python names the result columns (`state.output_names` from the
  bridge), so `+scidb/for_each.m` calls `bridge.sync_output_column_name` per
  output per call, copying the Constant (or clearing it) onto the Python
  surrogate. Only classes marked `_matlab_surrogate` are cleared; a
  Python-defined class of the same name keeps its own `column_name`.

## MATLAB argument shapes

- `scifor.for_each(fn, inputs, OUTPUTS, Name=Value...)`: an odd number of
  trailing args means the first is OUTPUTS (name-value always pairs); it is
  rewritten to `outputs=` before parsing, so the EachOf recursion re-parses it.
- `scidb.for_each(fn, inputs, outputs={...}, ...)`: MATLAB's name=value syntax
  lands `'outputs'` in the OUTPUTS slot; it is shifted back before parsing.
- `categorical` is never forwarded to scidb's inner scifor loop: those tables
  go back to Python to be saved, and categorical columns must not cross.

## Guards

- `scidb/tests/test_foreach_signature_parity.py` — Python signatures: scifor ⊆
  scidb, scidb extras == `DB_ONLY_KWARGS`, `outputs` 3rd, shared defaults equal.
- `scimatlab/tests/test_matlab_for_each_parity.py` — parses both .m
  `split_options` `case "..."` lists and `db_only_kwargs()`; runs without MATLAB.
- Behavioural: `scifor/tests/test_outputs_parity.py`,
  `scidb/tests/test_output_column_name.py`, MATLAB
  `TestSciforForEachParity.m` / `TestForEachParity.m`.

Adding a public scidb kwarg: add it to scifor (both languages) or to
`DB_ONLY_KWARGS` AND MATLAB's `db_only_kwargs()` — the guards fail otherwise.

## Removed

`track_lineage` (2026-10-18): a no-op since d80b96ec (bipartite rewrite);
provenance is always recorded and is part of record identity. `skip_computed`
is gated only by its own value (plus db/outputs/not-dry-run preconditions).
