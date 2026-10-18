# for_each kwarg parity: scifor <-> scidb (Python + MATLAB)

Goal: a call written for `scifor.for_each` runs unchanged on `scidb.for_each`
and vice versa (as far as kwargs go), in both languages. Not forced parity:
`outputs` stays required in scidb / optional in scifor; `inputs` values differ
by design (DataFrames vs variable types).

Done already (stage 0): `output_names` -> `outputs`, 3rd positional in Python;
`outputs` name-value in MATLAB scifor; `scidb/tests/test_foreach_signature_parity.py`.

## Stage 1 — scifor accepts + discards DB-only kwargs (Python)

- Owner: `scifor.DB_ONLY_KWARGS` (frozenset) in `scifor/foreach.py`:
  save, db, introspect, track_lineage, skip_computed, schema_filter,
  finalized, glue, parameter_names, pipeline.
- At the very top of `scifor.for_each` (before EachOf expansion), pop those
  names out of `**metadata_iterables`; log once per call:
  `INFO discard_db_only_kwargs: ignored save=False, db=<...> (scifor has no database)`.
- Consequence (same as scidb today): these names can't be schema keys in scifor.
- Test: parity test asserts scidb-only public params == `scifor.DB_ONLY_KWARGS`;
  scifor test asserts `for_each(..., save=False, db=object())` runs, result
  identical to the call without them, no `save`/`db` column, log line emitted.

## Stage 2 — scifor accepts classes in `outputs` (Python)

- Owner: `scifor.output_column_name(obj) -> str` (moved from
  `scidb.foreach._output_name`). Rule, first hit wins:
  1. `str` -> itself
  2. class attribute `column_name` (user-set, str) -> that
  3. class / object with `__name__` -> `__name__`
- Attribute name DECIDED: `column_name`. Declared with default `None`
  on `scidb.BaseVariable` for discoverability; scifor reads it by duck typing
  so plain classes work too.
- scidb consumes: `_output_name` deleted, all ~12 call sites use the owner.
  Result-column -> output mapping stays positional, all readers use
  `state.output_names` (already one source), so a renamed column can't diverge.
- Multi-column values: the name only applies when an output lands in ONE
  cell (scalar/array/dict, or a DataFrame kept whole). When every output is a
  DataFrame and rows spread, the DataFrame's own columns are used and output
  names are not applied (true today for `output_names` too; MATLAB:
  `test_output_names_does_not_apply_to_table_output`). New INFO log when an
  explicit `column_name`/string name is dropped for that reason.
- Tests: scifor — `outputs=[MyClass]` names column `MyClass`; with `column_name`
  set names it that; mixed `[MyClass, "b"]`. scidb — attr-renamed output saves
  and reloads, result column carries the attr name.

## Stage 3 — MATLAB parity

Option sets today:
- scifor.m public: dry_run, as_table, distribute, where, categorical, outputs,
  share_limits, schema_keys
- scidb.m public: dry_run, save, preload, as_table, db, parallel, distribute,
  where, introspect, skip_computed, finalized, share_limits, schema_keys,
  schema_filter, glue, parameter_names, locations, pipeline

Changes:
- 3a `outputs` positional in scifor.m: `for_each(fn, inputs, outputs, Name=Value...)`.
  Rule: odd `numel(varargin)` -> `varargin{1}` is outputs (name-value always
  comes in pairs, so this is unambiguous). Name-value `outputs=` still works.
- 3b scifor.m accepts + logs + discards DB-only names: same list as Python plus
  MATLAB's legacy `preload`, `parallel`. Owner in MATLAB: a local
  `db_only_kwargs()` in scifor.m.
- 3c `categorical` (scifor-only) -> scidb.m accepts it and forwards to scifor.
- 3d `locations` (scidb-only in MATLAB, but a scifor concept in Python) —
  DONE (changed from "port"): MATLAB scifor calls py.scifor.filter_combo_indices
  (it already calls py.scifor.get_schema), so the rule keeps ONE owner. Was:
  port to MATLAB scifor (`scifor.LocationFilter` +
  combo filter), driven by the existing shared cases
  `docs/claude/location-filter-cases.json` so the two languages can't diverge.
- 3e Classes in MATLAB `outputs`: MATLAB owner `scifor.output_column_name.m`,
  same rule (char/string -> itself; Constant property `column_name` -> that;
  object -> `class(obj)`). scidb.m: `column_name` is mirrored onto the Python
  surrogate at `register_matlab_variable` time, so Python's owner yields the
  same name and the bridge's `output_names` matches MATLAB's table columns.
  Shared cases file `docs/claude/output-column-name-cases.json` tested from
  both languages.
- Guard: Python test that parses the `case "..."` lists of both .m parsers and
  asserts scidb.m - scifor.m == MATLAB DB-only list, scifor.m public ⊂ scidb.m,
  and the MATLAB DB-only list == Python's plus {preload, parallel}.
- MATLAB tests: scifor accepts each DB-only name; positional outputs; class
  outputs; categorical through scidb.

## Logging
- INFO `discard_db_only_kwargs` (both languages), DEBUG `resolve_outputs`
  listing obj -> column name and which rule fired.

## Out of scope (noted)
- `track_lineage`: a no-op in Python scidb since d80b96ec (2026-06-20, the
  bipartite-graph rewrite removed the LineageFcn wrapping it controlled).
  Provenance is always recorded; the flag's only remaining effect is gating
  `skip_computed` in `scidb/pipeline.py:1161,1298`. Docstring and
  `docs/api/scidb.md:98` wrongly say it disables provenance. MATLAB scidb
  never had it. PENDING user decision: remove it (recommended) vs re-implement.
- Private hooks (`_all_combos`, `_select_rows`, ...) differ by design.

## Status (2026-10-18)
All stages implemented, tests written, NONE run. track_lineage removed
(stage 0). MATLAB categorical is applied by scidb.m to the RETURNED table via
the extracted owner `+scifor/categorize_metadata.m` (never forwarded to the
inner loop: those tables go back to Python to be saved).
