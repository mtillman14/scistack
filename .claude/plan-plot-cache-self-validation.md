# Plot cache self-validation + one owner for stored-cell semantics

*2026-09-15. Trigger: after the NaN fix, `GAITRiteLoaded_UA` plotted as zeros
even though the database held NaN. Two independent causes, implemented as two
independent stages.*

## Stage 1 — the plot cache validates itself against content

### Why not a run-completion hook

`_notify_records_changed()` already exists in `scistack-gui/api/run.py` and
already calls `plot_service.invalidate(...)`. It never fired, because it lives
in `_drive_matlab_in_thread`'s `finally` — the **sidecar** path. The user's
MATLAB runs are dispatched to a **terminal**, which never reports completion
back to the GUI (the known gap in `project_matlab_terminal_run_tracking`). The
log for the whole session contains zero `source cache invalidated` lines.

A push hook also cannot cover the `scidb` CLI mutator, a second GUI, or MATLAB
run by hand. So the cache **pulls** instead of being pushed to.

### The fingerprint

`scidb.provenance_query.variable_content_fingerprint(duck, variable)` →
`(n_records, bit_xor(hash(record_id)))` over non-excluded records of that type.

Both halves derive from `record_id`, which is a **content hash**. That is what
makes this precise rather than conservative, and it is exactly the user's
requirement:

| situation | record_ids | fingerprint | cache |
|---|---|---|---|
| re-run, identical output | unchanged | unchanged | **KEPT** |
| re-run, any different value | new ids | changes | **DROPPED** |
| record excluded / un-excluded | set changes | changes | DROPPED |
| a different variable written | untouched | unchanged | KEPT |

Deliberately **not** `max(timestamp)` on `_record_save`: that table is the
audit trail and gets a row on every execution including no-ops, so a timestamp
watermark would invalidate on every re-run — the opposite of what was asked.

`bit_xor` is order-independent, so this is one aggregate scan with no sort.

### Wiring

`ScidbSource` keeps `_fingerprints: {variable: fingerprint}`, stamped from
**before** each load (a run committing mid-read would otherwise be stamped as
already included and never picked up). `_variable_frame` and `_variant_frame`
both check on every hit and call `invalidate(variable)` on a mismatch — the
stale `variant_table` in the 2026-09-15 session came from `_variant_frames`,
not `_frames`, so both entry points need it.

Failure is non-fatal (a plot that won't draw is worse than one drawn from a
probably-good cache) but **warns once per source**: degrading silently to
"keep the cache forever" is precisely the bug being fixed, so it must be
visible in the log rather than inferred.

### Cost

One aggregate over `_record` filtered by type, per cache hit. Against the
alternative — re-reading a 174 M-sample variable, or showing wrong data — this
is not a close call. If it ever shows up in a profile, the answer is to cache
the fingerprint for a short TTL, not to remove the check.

## Stage 2 — one owner for "what a stored cell means"

`scistackplotdb._float_row` had its own copy of the masked-array rule. The
identical bug was found and fixed twice, two days apart (2026-09-13 in the plot
layer, 2026-09-15 in sciduckdb). `sciduckdb.array_from_storage` is now public
and `_float_row` delegates to it.

**The bulk load path stays separate**, deliberately: `load_variable` bypasses
`load()`/`load_all_as_df` for measured reasons (18x on a 17.4 M-sample column;
174 M samples / 5.2 GB to answer a variant question). The plot layer decides
WHEN to convert a cell — it has no dtype metadata and infers shape from the
value — while sciduckdb owns WHAT the conversion means.

## Tests

`scistackplotdb/tests/test_cache_content_validation.py`:
- `TestFingerprint` — identical re-save keeps it, new content changes it,
  unknown variable is `(0, 0)`.
- `TestCacheKeptWhenDataIsUnchanged` — identical re-save returns the SAME frame
  object; an untouched variable survives a write to a different one; repeated
  hits never rebuild.
- `TestCacheDroppedWhenDataChanges` — new value, new location, the variant
  frame's own entry point, and the derived `get_table` cache.
- `TestFailureIsNotFatal` — a `None` fingerprint and a raising query both keep
  the plot working.
- `TestSharedStorageRule` — both layers agree on a NULL-bearing cell, and a
  real zero survives both.

## Not done

- The terminal-run completion gap itself (`project_matlab_terminal_run_tracking`
  Stage 2) is untouched; this makes the plot correct despite it, rather than
  closing it.
- `_notify_records_changed` is left in place: it is still the fast path for
  sidecar runs, and now merely redundant rather than load-bearing.
- No frontend change. The panel gets correct data on its next request without
  knowing why.
