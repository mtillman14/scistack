> Archived 2026-11-01: describes the removed scihist package (its for_each is now scidb.for_each with skip_computed=True). Current: database-model.md and docs/design-review/04b, 04c.

# scihist.for_each Execution Steps

## Overview

`scihist.for_each()` is the Layer 3 (lineage-aware) wrapper around `scidb.for_each()`.
It auto-wraps functions in `LineageFcn`, delegates iteration to scidb, then handles
saving outputs with full lineage tracking.

Source: `scihist-lib/src/scihist/foreach.py`

## Major Steps

### 1. Auto-wrap in LineageFcn

If the provided `fn` is not already a `LineageFcn`, it gets wrapped in one
automatically. This ensures every function call produces lineage tracking data
(function hash, input classification, etc.).

### 2. Wrap LineageFcn in a plain callable

`_make_plain()` wraps the `LineageFcn` in a simple function that calls it and
returns a `LineageFcnResult`. This is needed because `scidb.for_each` expects a
plain callable — it doesn't know about lineage.

### 3. Build the skip_computed hook (if enabled)

When `skip_computed=True` (the default), `_build_skip_hook()` creates a pre-combo
callback. For each metadata combo, this hook checks:

1. **Output exists?** — looks up each output type in the DB with the combo's
   metadata (including constant values, `__fn`, `__fn_hash` in lookup keys).
2. **Function hash matches?** — compares `_lineage.function_hash` to the current
   `LineageFcn.hash`.
3. **Input record_ids unchanged?** — compares `__rid_*` values (both variable and
   Fixed inputs) against stored `rid_tracking` entries in lineage.
4. **Constant hashes unchanged?** — compares canonical hashes of constant inputs
   against stored lineage constants.

If all checks pass, the combo is skipped (`[skip]`); otherwise it's marked
`[recompute]` with a reason.

### 4. Delegate to scidb.for_each (with save=False)

The actual iteration engine lives in `scidb.for_each`. scihist calls it with
`save=False` because scihist handles saves itself. scidb.for_each:

- Generates all metadata combinations from `**metadata_iterables`
- Loads inputs for each combo (using `load()` on variable types, resolving
  `Fixed`/`Merge`/`ColumnSelection` wrappers, etc.)
- Injects `__rid_*` keys into the combo for each loaded input's record_id
- Calls the `_pre_combo_hook` (the skip check) before executing each combo
- Calls the wrapped function and collects results into a pandas DataFrame
- Applies `_progress_fn` and `_cancel_check` callbacks if provided

### 5. Classify inputs for lineage save

After `scidb.for_each` returns the result DataFrame, scihist classifies the
original inputs dict:

- **Variable types** (BaseVariable subclasses) — skipped (tracked via `__rid_*`)
- **Wrappers** (Fixed, Merge, ColumnSelection) — Fixed inputs get their record_ids
  resolved for `rid_tracking`
- **PathInputs** — skipped
- **Everything else** — treated as **constant inputs**, added to save metadata as
  version keys for variant disambiguation

### 6. Save with lineage (`_save_with_lineage`)

For each row in the result DataFrame, for each output:

1. Extract `__rid_*` keys from row metadata; merge in Fixed input record_ids.
2. Strip `__` prefixed keys from save metadata.
3. Add constant inputs to save metadata as version keys.
4. Route based on output value type:

#### LineageFcnResult path
- Extract the `LineageRecord` (function_name, function_hash, inputs, constants)
  via `extract_lineage()`.
- Convert to dict via `_lineage_to_dict()`.
- Append `rid_tracking` entries via `_append_rid_tracking()`.

**generates_file=True**: Save a lineage-only metadata record (no data blob).
Creates a `generated:<hash>` record_id, writes `_record_metadata` and `_lineage`
rows directly.

**Normal functions**: Instantiate the output variable class with raw data
(extracted via `get_raw_value()`), save via `db.save()` with lineage dict,
lineage_hash, and pipeline_lineage_hash. Version keys include `__fn` and
`__fn_hash`.

#### Plain data path
- Save directly via `output_obj.save()` (no lineage tracking).

## Key Design Decisions

- **save=False delegation**: scihist never lets scidb save outputs. This is because
  scidb doesn't understand `LineageFcnResult` and would fail or silently skip.
- **__rid_* tracking**: Record IDs of loaded inputs are injected into combos by
  scidb.for_each and then extracted by scihist for lineage. This enables
  `skip_computed` to detect upstream changes without timestamps.
- **Constant inputs as version keys**: Constants are included in save metadata so
  that different constant values produce different output records (variant
  disambiguation).
- **Fixed input rid resolution**: Fixed inputs have static metadata, so their
  record_ids are resolved once before the save loop (not per-combo).

## Related Docs

- `docs/claude/for-each-input-unwrapping.md` — how inputs are delivered to functions
- `docs/claude/for-each-caching.md` — skip_computed staleness detection design
- `docs/claude/archive/for-each-kwargs.md` — keyword argument handling
