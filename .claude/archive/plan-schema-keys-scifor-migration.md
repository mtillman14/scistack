# `schema_level` → `schema_keys`: move to scifor, MATLAB parity, fix latent bug

## Context

We've been discussing schema-agnostic sub-pipelines. Python's `scidb.for_each()`
currently has a `schema_level=`/`schema_filter=` pair that lets a caller say
"iterate over these particular schema keys" without hardcoding literal
key=value kwargs — but the mechanism lives entirely inside `scidb`
(`scidb/src/scidb/foreach.py`), duplicates a resolution loop that already
exists elsewhere in the same file, and has no MATLAB equivalent at all. Per
this project's layering convention (lower-level, DB-agnostic logic belongs in
`scifor`; `scidb` adds the DB-backed persistence layer on top), the user
asked to:

1. Move the underlying mechanism down into `scifor` (which currently has NO
   concept of "iterate a named subset of the configured schema" — it only
   resolves bare `key=[]` kwargs that the caller already spelled out) and
   have `scidb` reuse it, in both Python and MATLAB.
2. Rename `schema_level=` → `schema_keys=`, since "level" reads as numeric/
   positional but the value is actually a list of key **names**.

While tracing the current implementation I found:

- **Duplicate logic**: scidb's `schema_level`/`schema_filter` handling
  (`foreach.py:353-393`, called "Step 0") already fully resolves DB values
  itself (`active_db.distinct_schema_values(key)` at line 387), even though
  `_for_each_prepare()` (the function it calls next, `foreach.py:1271`)
  has its own **already-generic** empty-list-from-DB resolver at
  `foreach.py:1322-1349` that would do the exact same thing for any `key: []`
  entry regardless of where it came from. Step 0 doesn't need to query the DB
  at all — it only needs to seed `metadata_iterables` with `[]` placeholders.
- **A naming collision**: `scifor/foreach.py:132` already has a local var
  `schema_keys = get_schema()` (the *full* configured schema), reused as a
  parameter name in ~50 places across the file (`_resolve_colnames`,
  `_filter_df_for_combo`, `_is_per_combo_df`, etc.). The new public
  `schema_keys=` (a *subset* to iterate) needs that identifier freed up.
  Same collision confirmed in MATLAB's `+scifor/for_each.m:85` (also ~50
  occurrences).
- **A latent bug** (confirmed by direct code reading, not just the test
  suite): `schema_filter={"session": ["A"]}` combined with
  `schema_keys=["subject"]` (i.e. filtering a key that isn't being iterated)
  is silently ignored — `foreach.py`'s Step 0 loop only ever touches keys in
  `iterate_keys`, so `schema_filter["session"]` never reaches anything. The
  existing test (`scihist/tests/test_schema_filter_params.py::
  test_filter_on_non_iterated_key`) only asserts row *count*, so it passes
  despite the bug. **Decision (confirmed with user): fix this** by routing
  such entries through the existing `SchemaKeyInFilter` (`scidb/src/scidb/
  filters.py:992`) ANDed into `where=`, which is exactly what that filter
  class already exists for.
- **Mutual exclusivity** (schema_keys/schema_filter cannot combine with
  explicit `**metadata_iterables` in the same call) stays exactly as strict
  as today — **confirmed with user**, not relaxed.
- A **completely separate, unrelated** `schema_level` concept exists in
  `scidb/src/scidb/database.py`, `inspect/api.py`, `inspect/render.py`, and
  `filters.py` (`_validate_filter_schema_level`) — the `_schema` table's
  "deepest provided key" column. **This must NOT be touched** — it predates
  and has nothing to do with the `for_each(schema_level=...)` parameter.

## Design

### 1. New shared primitive — `scifor` (Python)

Add a small, pure (no I/O) function, e.g. in `scifor/src/scifor/schema.py`:

```python
def expand_schema_keys(schema_keys: list[str], metadata_iterables: dict) -> dict:
    """Seed metadata_iterables with an empty list for each requested schema
    key not already present. Raises if metadata_iterables is non-empty
    (schema_keys is mutually exclusive with explicit **metadata_iterables).
    Pure bookkeeping — actual value resolution happens downstream (DataFrame
    scan in scifor, DB query in scidb)."""
```

This is the "machinery" both layers reuse. It does NOT know about
`schema_filter` — that stays a scidb-only concept (see below), since its
non-iterated-key case needs `SchemaKeyInFilter`/`where=`, which is DB-query
machinery scifor doesn't have. `schema_filter` on an *iterated* key is
already just "explicit values instead of auto-resolve," which scidb handles
by overwriting the seeded `[]` before delegating onward — no new machinery
needed for that half either.

### 2. `scifor.for_each()` gains `schema_keys=`

- Rename the internal "full schema" identifier throughout
  `scifor/src/scifor/foreach.py` (~50 occurrences: the `for_each()` local var
  at line 132, and same-named parameters in `_resolve_colnames`,
  `_is_per_combo_df`, `_filter_df_for_combo`, `_prepare_input`,
  `_prepare_merge`, `_resolve_iterate_columns`, `_all_data_columns`, etc.) to
  `full_schema_keys`. Pure rename, no behavior change.
- Add new public param `schema_keys: list[str] | None = None`. When given,
  call `expand_schema_keys(schema_keys, metadata_iterables)` to seed
  `metadata_iterables`, then fall through unchanged into the **existing**
  Step 2 DataFrame-scan empty-list resolver (`_distinct_values_from_inputs`,
  already at `foreach.py:1536`) — no new resolution logic needed here either.
- New capability for standalone (no-DB) scifor users, previously impossible.

### 3. `scidb.for_each()`: rename + delegate + fix

In `scidb/src/scidb/foreach.py`:

- Rename `schema_level` → `schema_keys` in the signature, docstring, and
  `ForEachConfig`/version-key wiring (`foreach.py:328-329`).
- Replace the current Step 0 block (`foreach.py:353-393`) with:
  1. `iterate_keys = schema_keys if schema_keys is not None else active_db.dataset_schema_keys`
  2. `metadata_iterables = scifor.expand_schema_keys(iterate_keys, metadata_iterables)`
     (this raises the same "cannot combine" `ValueError` the current code
     raises today — same message substring `"Cannot use both"` so the
     existing test keeps passing unchanged).
  3. Apply `schema_filter` overrides: for each `key, values` in
     `schema_filter`, if `key` is in `metadata_iterables` (i.e. an iterated
     key) → overwrite with `values` directly (unchanged from today). If NOT
     (the latent-bug case) → build `SchemaKeyInFilter(key, values)` and AND
     it into `where=` (`where = where & SchemaKeyInFilter(...)` using
     `Filter.__and__`, `filters.py:67`).
  4. Delete the direct `active_db.distinct_schema_values(key)` call — the
     remaining `[]` placeholders now flow into `_for_each_prepare`'s already-
     existing generic DB resolver (`foreach.py:1322`) unchanged, exactly as
     bare `subject=[]` kwargs already do today.
- Update `scidb/src/scidb/pipeline.py` (`PipelineBinding` key_map rewriting,
  `pipeline.py:319, 480-486`): rename `schema_level` references to
  `schema_keys` (the rewrite logic itself — remapping key names inside the
  list — is unaffected).
- Keep the debug breadcrumb (`foreach.py:391`, currently `"built
  metadata_iterables from schema params: ..."`) under the new name.

### 4. MATLAB parity

**`+scifor/for_each.m`** (standalone, table-based):
- Rename the internal `schema_keys` local (line 85, ~50 occurrences across
  nested helpers) → `full_schema_keys`, mirroring the Python rename exactly.
- Add a `schema_keys` Name-Value option, implemented with a small local
  subfunction mirroring `expand_schema_keys()`'s logic (seed `meta_keys`/
  `meta_values` with empty entries for requested keys not already given),
  inserted before the existing empty-list resolution block (`for_each.m:
  134-197`) — unchanged after that point.

**`+scidb/for_each.m`** + **`bridge.py`**:
- Confirmed: `+scidb/for_each.m` has ZERO DB-based empty-list logic of its
  own — it ships `metadata_iterables` across the bridge as-is and
  `for_each_prepare()` (`bridge.py:402`) resolves everything by calling
  `_for_each_prepare()` (`scidb/foreach.py:1271`), the **same function** the
  pure-Python path uses. This means MATLAB needs NO new DB-querying code —
  only option plumbing:
  - `+scidb/for_each.m`: add `schema_keys`/`schema_filter` cases to
    `split_options` (pattern at `for_each.m:1236-1293`; add to
    `reserved_opts`, `for_each.m:1225-1228`), forward the raw values through
    the existing `py.scimatlab.bridge.for_each_prepare(...)` call
    (`for_each.m:206-214`).
  - `bridge.py`'s `for_each_prepare()`: add `schema_keys=None,
    schema_filter=None` params; before building `meta` (line ~519) and
    calling `_for_each_prepare`, run the **same** Step-0-equivalent logic as
    scidb's Python path (steps 1-3 from section 3 above, importing
    `scifor.expand_schema_keys` directly) to seed `meta` and adjust
    `where_arg`. `_for_each_prepare`'s existing generic DB resolver then
    handles the rest — identical code path to pure-Python scidb, just
    entered from the bridge instead of `scidb.for_each()`.

### 5. Tests

- **Rename** `scihist/tests/test_schema_filter_params.py`: all
  `schema_level=` → `schema_keys=` call sites and class names
  (`TestSchemaLevel` → `TestSchemaKeys`, etc.).
- **Fix-verification test**: strengthen
  `test_filter_on_non_iterated_key` (or add a sibling) to assert the actual
  loaded/returned values were constrained to session="A" (not just row
  count) — this is the regression test for the bug fix, per the "add tests
  to prevent regression" guidance.
- **New scifor-level tests** (new file, e.g. `scifor/tests/
  test_schema_keys.py`): standalone `schema_keys=` against in-memory
  DataFrames (no DB) — a genuinely new capability with no prior coverage —
  plus the mutual-exclusivity error case.
- **MATLAB**: extend `scimatlab/tests/matlab/scifor/
  TestSciforForEachNoSchema.m` (or add `TestSciforForEachSchemaKeys.m`) with
  a standalone `schema_keys=` case; add
  `scimatlab/tests/matlab/scidb/TestForEachSchemaKeys.m` mirroring the
  Python scidb-level coverage end-to-end.
- **Bridge-level pytest coverage** (runs in pure Python, no MATLAB needed —
  same pattern as `scimatlab/tests/test_bridge_where.py`): new
  `scimatlab/tests/test_bridge_schema_keys.py` calling
  `bridge.for_each_prepare(...)` directly with `schema_keys=`/
  `schema_filter=` and asserting on `extended_metadata_iterables`/
  `full_combos`.

### 6. Docs

Update the `schema_level`/`schema_filter`-as-for_each-parameter mentions in
`docs/claude/scidb-for-each-internals.md` and
`docs/claude/endpoint-first-pipelines.md` to the new name. Leave every other
`schema_level` mention alone (the unrelated `_schema` column concept).

## Verification

I don't have Python or MATLAB execution access in this environment (per
CLAUDE.md) — I'll implement + write tests, then hand you exact commands to
run yourself:

```
cd /workspace && python -m pytest scifor/tests/test_schema_keys.py -v
cd /workspace && python -m pytest scihist/tests/test_schema_filter_params.py -v
cd /workspace && python -m pytest scimatlab/tests/test_bridge_schema_keys.py -v
cd /workspace && python -m pytest scidb/tests/ -k "schema or pipeline" -v
```

Plus the two new/extended `.m` test files to run in your MATLAB environment
(`TestSciforForEachSchemaKeys.m`, `TestForEachSchemaKeys.m`).

## Non-goals (explicit)

- Not touching the unrelated `_schema`-table `schema_level` concept anywhere.
- Not relaxing schema_keys/schema_filter's mutual exclusivity with explicit
  `**metadata_iterables`.
- Not adding `schema_filter=` to scifor's standalone API — a scifor user
  wanting to constrain a non-iterated key already has `where=`/`Col(...)`
  for that; no new sugar needed there.
