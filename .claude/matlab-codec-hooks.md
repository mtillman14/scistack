# Plan: `to_db` / `from_db` hooks on MATLAB `scidb.BaseVariable`

Status: Stage 0 implemented 2026-10-18, tests pass (uncommitted;
scidb/tests/test_custom_serialization.py + existing codec suites).
Stages 1-3 implemented 2026-10-18, tests PASS (scimatlab/tests/test_bridge_codec.py,
scimatlab/tests/matlab/scidb/TestCodec.m). Stages 4-6 not started.

Stages 1-3 as built:
- bridge.register_matlab_variable(type_name, schema_version=1, codec=None):
  True/False attaches/detaches shared pass-through _matlab_codec_to_db /
  _matlab_codec_from_db; None = leave as is (GUI callers). Applies to
  surrogates AND `_declared_only` TOML classes; a Python-defined class is
  never modified (one WARN per type).
- +scidb/+internal/codec.m = the owner (meta.class detection, both-or-neither,
  Static checks, istable check, batched encode/decode over cells, [codec]
  timing log). Not cached, so classdef edits take effect immediately.
- ensure_registered returns [py_class, codec]; register_variable.m passes
  codec too.
- BaseVariable.save encodes before (and skips) table auto-distribute;
  save_from_table encodes the data column first (then Strategy A vertcat);
  load decodes after wrap_py_vars_batch (latest/all/record_id/as_table).
- Found while testing (both pre-existing, not codec bugs):
  * save_from_table numeric metadata crossed as float (1.0) while save() sent
    int (1): canonical_hash differs, so the same location got two record ids.
    One owner now: +scidb/+internal/number_to_python.m (used by
    metadata_to_pydict, metadata_to_pykwargs, include/exclude_schema,
    save_from_table). One-row columns (0-d arrays / scalars) fixed in
    bridge.save_batch_bridge.
  * +scifor/for_each.m split_options (281c8ceb) compared string-array
    metadata VALUES against db_only -> MATLAB:dimagree in 10 TestForEach
    tests; now only a scalar name is checked.
  * Full-suite triage 2026-10-18 (both pre-existing, tests post-date or equal
    91547a1e): Pipeline.replay_matlab_step forwarded the empty schema_keys
    default (-> schema_keys=[] "every key" + metadata_iterables -> ValueError
    in 5 TestPipelineRegistry tests); now forwarded only when
    stored.opts.schema_keys_given. to_python.m element-by-element fallback
    flattened 2-D matrix outputs to a list (TestEndToEnd/
    test_matrix_through_pipeline: 4x3 saved as list len=12); matrices now
    cross as shaped ndarrays.
  * MATLAB for_each saved `subject=1.0` (result-table metadata crossed as
    float). Impact is the record id's metadata hash and non-schema metadata
    spelled "1.0"; NOT for_each-vs-save() rid parity (every for_each record id
    includes __invocation_id anyway). Fixed 2026-10-18: owner
    +scidb/+internal/metadata_number.m (whole -> int64); number_to_python
    builds on it; whole_metadata_columns.m casts result-table metadata
    (skips outputs and any "__" column, e.g. x__combo) before to_python;
    the returned table needs no restore: from_python turns every numpy
    numeric dtype into double. Python's _save_results uses to_dict("records"), which
    boxes int64 to Python int, matching save(). Tests: TestMetadataNumbers.m.
  * Full run 2026-10-18 21:02: 3 failures, all PASS in isolation (InProcess
    pyenv, so not an execution-mode effect). Fixed regardless: numpy scalars
    (np.int64 from a BIGINT .iloc) now convert in from_python (.item()) and
    take wrap_batch_bridge's scalar fast path (TestSaveFromTable/
    test_integer_data). UNEXPLAINED, suspected intermittent/state-dependent:
    TestForEachSchemaFiltering/test_dry_run_reflects_filtered_count (evalc
    captured no Python dry-run print) and TestForEachWhere/
    test_schema_key_isin_filters_merge_as_table (convert_dataframe row
    mismatch on a Merge input). Both run BEFORE the new TestMetadataNumbers,
    and nothing that runs before them changed between the passing 20:37 run
    and 21:02. If they recur in a full run: bisect by suite prefix.
- Interim until Stage 4: a codec class as a MATLAB for_each OUTPUT fails with
  the surrogate's TypeError ("reached Python without MATLAB's to_db()");
  as an INPUT the function receives the stored table.

Stage 0 as built: storage form for codec classes stays the columnar
`{"custom": true}` table (keeps `where=` on stored columns and the iterator
load path). Owners in `DatabaseManager`: `_custom_storage_frame` (calls
`to_db`, checks DataFrame), `_ensure_columnar_table` (table shape),
`_insert_columnar_frames` (one INSERT for N records, one dedup query).
`save` hashes the to_db frame; `save_batch` runs to_db on every item first,
then the normal batch on the frames.

## Goal

A MATLAB variable class can override two methods, exactly like Python:

```matlab
classdef MarkerTrajectory < scidb.BaseVariable
    methods
        function tbl = to_db(obj)            % obj.data -> table
            ...
        end
    end
    methods (Static)
        function data = from_db(tbl)         % table -> original value
            ...
        end
    end
end
```

`save`, `load` and `scidb.for_each` (inputs and outputs) then convert
automatically. Override both or neither.

## What exists today

- `+scidb/BaseVariable.m` has no codec hooks. Every MATLAB type is backed by a
  Python *surrogate* class built in `scimatlab/bridge.py:register_matlab_variable`
  (plain `BaseVariable`, no codec).
- Python decides "custom serialization" with one owner,
  `DatabaseManager._has_custom_serialization` (`"to_db"`/`"from_db"` in the
  class `__dict__`). It drives: `save_variable` (columnar store),
  `_deserialize_custom_subdf` (load), the as_table/fast-path gates.
- **CONFIRMED 2026-10-18 (tmp.py repro): object codecs are broken in Python.**
  - `DatabaseManager.save` hashes `variable.data` (`canonical_hash`, ~line
    2951) BEFORE the `to_db()` call (~line 2981). A non-DataFrame value raises
    `ValueError: Unserializable data type: <class 'Trajectory'>`, even on a
    direct `MarkerTrajectory.save(...)`.
  - `DatabaseManager.save_batch` (every `for_each` save, Python and MATLAB)
    hashes the raw value (~line 1868) and never calls `to_db` at all.
  - Existing codec tests only use DataFrame -> DataFrame codecs, so they pass.
    The `Trajectory` example in docs/getting-started/writing-functions.md
    does not work today.

## Design

**D1. Conversion runs in MATLAB; storage shape is owned by Python.**
MATLAB objects exist only in MATLAB, so `to_db`/`from_db` must run there. The
Python surrogate for a codec class is marked as custom (pass-through
`to_db`/`from_db` on a DataFrame), so Python stores the MATLAB table through
the same columnar path as a Python codec class. Python and GUI readers of a
MATLAB-codec variable get the table (they can't build a MATLAB object).

**D2. One owner per side.**
- Python: one helper inside `DatabaseManager` turns a variable value into its
  storage DataFrame. Used by both `save_variable` and `save_batch` (fixes the
  gap above).
- MATLAB: one helper `scidb.internal.codec(type_name)` detects the overrides
  (via `meta.class`, with a defining class other than `scidb.BaseVariable`),
  enforces both-or-neither, caches per class, and exposes `encode(data)` /
  `decode(tbl)`. Every MATLAB save and load site calls it; none detects on
  its own.

**D3. Surrogate carries the flag.**
`register_matlab_variable(type_name, schema_version, codec=False)`. The
current early `return existing` must update a surrogate registered earlier
without the flag (otherwise the first-registration-wins state goes stale).

**D4. Encoded tables never auto-distribute.**
`BaseVariable.save` auto-distributes tables that contain schema-key columns.
The codec's output must skip that branch (Python's custom path never
distributes).

**D5. Changing the codec is a type change.**
Same rule as Python: raise `schema_version` (MATLAB already has
`scidb.register_variable(Type(), schema_version=N)`).

## Sites to wire (MATLAB)

| Direction | Site |
| --- | --- |
| save | `BaseVariable.save` (plain branch) |
| save | `BaseVariable.save_from_table` (per-row data) |
| save | `for_each.m` outputs, after the loop, before `bridge.for_each_save` |
| load | `BaseVariable.load` (latest / `'all'` / by record_id) |
| load | `BaseVariable.wrap_py_var`, `wrap_py_vars_batch` |
| load | `for_each.m` inputs: `build_scifor_input_from_desc` (`dataframe` + wrapper kinds) |
| leave as table | `head`, `to_csv` (they show the stored form; decision to confirm) |

## Main unknown (spike in Stage 4)

MATLAB `for_each` loads each input as **one table for all combos** and MATLAB
scifor slices it per iteration. Glue (`apply_matlab_glue`) is whole-table and
must keep the row set. Decoding fits there only if a codec input arrives as
one row per record with the record's table in a cell column. Then a
whole-table pre-pass maps `from_db` over that column, and the row set doesn't
change. If it arrives as frame rows (one row per stored row), decoding must
happen per slice and the plan changes. Diagnose first: log the input's
height, columns and a cell-class sample at `build_scifor_input_from_desc`
for a codec type.

## Stages

0. **Python object codecs (confirmed broken).** One owner in
   `DatabaseManager`, e.g. `_storage_value(variable_class, data)`: returns
   `variable_class(data).to_db()` for a codec class, else `data`. Both `save`
   and `save_batch` hash AND store that value (hash the stored form, so same
   stored table = same record). Pass-through DataFrame codecs hash the same
   as before, so no existing record_id changes. `save_batch` routes codec
   items through the columnar storage that `save` uses. Log
   `serialization=custom|native` in `save_batch` as `save` does.
   Regression tests (scidb/tests): dataclass-valued codec through direct
   `save`/`load`, and through `for_each` output -> next step's input.
1. **Bridge surrogate flag** (D3) + pytest in scimatlab/tests:
   `_has_custom_serialization(surrogate)` true; a re-registration flips a stale
   surrogate; a saved table round-trips as a table.
2. **MATLAB codec owner** (`scidb.internal.codec`): detection, both-or-neither
   error (`scidb:CodecError`), cache, INFO log once per class
   (`[codec] MarkerTrajectory: custom to_db/from_db`).
3. **Direct save/load sites** + MATLAB tests (TestCodec.m): object
   round-trip via save/load, `'all'` versions, table output does not
   auto-distribute, one-sided override errors.
4. **for_each** outputs + inputs (spike above first), `[timing]` line for
   total encode/decode time per type. MATLAB tests: object output -> object
   input in a two-step pipeline; Python `load` sees the table.
5. **Discovery / entities / GUI**: confirm the `.m` classifier still sees a
   classdef with a `methods` block as a variable; `same_definition` between a
   codec surrogate and the declared-only entities class; GUI shows the table.
6. **Docs**: writing-functions.md MATLAB tab shows the real override; drop the
   "Python variables only" sentence; `project-files.md:354` row covers MATLAB.
   Add to docs/gui-manual-testing-todo.md only if Stage 5 changes GUI output.

## Out of scope

- A Python class and a MATLAB class of the same name with different codecs
  (they'll conflict through `same_definition`; same as any codec mismatch).
- Loading a MATLAB-codec variable as an object from Python.
