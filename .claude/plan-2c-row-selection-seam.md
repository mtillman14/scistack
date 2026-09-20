# Stage 2c — the row-selection seam between scidb and scifor

*Written 2026-09-20 after Stage 2b went green. This is the "layering
question" `.claude/plan-architecture-2026-09-20.md` deferred: scifor cannot
import scidb, so "scifor asks `RunBindings`" cannot be spelled as an import.
One decision needs the user before code is written (§4).*

---

## 1. The contract today, and why it is a column convention

scidb tells scifor which records a combination reads by **putting the answer
in the frame, the schema and the combo, under a reserved column prefix**:

| step | scidb does | scifor relies on |
|---|---|---|
| Step 12 | renames `__record_id` → `__rid_{param}` on every loaded frame (full iteration), or adds `__vsig_{param}` (aggregation) | — |
| Step 15 | `scifor.set_schema(current + rid_keys_for_schema)` — the global schema is *extended* with those keys, then restored at Step 18 | `_filter_df_for_combo` filters on every schema key present in both frame and combo, so the extension turns the rid column into a row selector |
| Step 15 | adds the keys to `metadata_iterables` too | scifor's combo validation and `distribute` resolution see them as iterated keys |
| combo expansion | each combo carries `__rid_*` / `__vsig_*` values | `dict(metadata)` becomes the result row, so the columns land in the result table and the save reads them back (`RunBindings.for_combo(row)`) |
| — | — | four places strip `"__rid_"` / `"__vsig_"` / `"__"`-prefixed schema keys: distribute resolution (`:384`), display keys (`:625`), `_extract_data` (`:1588`), the result-shape analyser (`:2197`) |
| MATLAB | the bridge renames `__rid_x` → `x__rid_x` (MATLAB cannot have a leading underscore), and `+scifor/for_each.m` repeats the same four strips on the sanitised names | |

So scifor "knows" scidb's spellings by string prefix, in eight places across
two languages, and scidb has to mutate a *global* (the schema) around every
call. Stage 2a gave the spellings one owner **inside scidb**
(`bindings.rid_column` / `vsig_column`); the boundary is still spelled by
hand on the other side.

## 2. What is essential and what is convention

Two things are essential and must survive in some form:

* **A combination must be identifiable.** Full iteration expands one combo
  per variant record at a location; two combos at the same schema location
  differ only in which record they read. Something on the combo has to say
  which. Today that is `__rid_{param}`; it could be an opaque combo id, but
  the rid IS the identity, so keeping it on the combo is right.
* **The result row must carry that identity back**, so the save can bind
  edges per row. Today: the combo's keys become the row's columns. Also
  right.

Everything else is convention:

* the frame does not need `__rid_{param}` — it already has `__record_id`;
* the **schema** does not need extending — the only reason is to make
  `_filter_df_for_combo` select rows by rid, and that can be an explicit
  hook;
* `metadata_iterables` does not need the keys — the only reason is that
  the extended schema made scifor expect them;
* scifor does not need to know the prefixes — if the schema is never
  extended, there is nothing to strip.

## 3. The seam: one hook, one rule

**scifor grows one keyword argument:**

```python
scifor.for_each(
    ...,
    _select_rows: Callable[[str, pd.DataFrame, dict], pd.DataFrame] | None = None,
)
```

Called per input, per combo, **after** `_filter_df_for_combo` (the schema
filter), **before** `_extract_data`: `_select_rows(param, frame, combo)`
returns the rows this combination reads, with any caller-private columns
already removed. scifor's contract: "I filtered by my schema; you narrow
further if you have more to say." It replaces the schema extension exactly —
the extended-schema filter *was* a row selector; this names it.

**scidb passes one function**, built on the typed spine:

```python
def _select_rows(param, frame, combo):
    rids = state.bindings.rids_for_combo(combo).get(param)
    if rids is not None and RECORD_ID_COLUMN in frame.columns:
        frame = frame[frame[RECORD_ID_COLUMN].isin(rids)]
    return frame.drop(columns=[RECORD_ID_COLUMN], errors="ignore")
```

`rids_for_combo` already answers every mode — `__rid_*` off the combo for
ITERATE/LINEAGE_ONLY, the pool's group named by `__vsig_*` for AGGREGATED,
the pin for PINNED — so **the selection rule and the edge rule are the same
call**. That is the actual point: today the frame filter (scifor's schema
match on `__rid_x`) and the edge assembly (`for_combo`) are two
implementations that agree by construction *only* because both read the
same column. After 2c they are one function.

What goes away in scidb: the `__record_id` → `__rid_{param}` rename on
frames (Step 12 keeps the column as is), the `__vsig_{param}` frame column
(the pool already knows each rid's group), `set_schema(extended)` + the
Step 18 restore, `rid_keys_for_schema` on the state, the
`extended_metadata_iterables` padding, and `_sanitize_rid_key` in the
bridge. The combo keeps `__rid_*` / `__vsig_*` as **combo identity** — that
is scidb's data passing through scifor untouched, and the save reads it off
the row as today.

What goes away in scifor: all four prefix strips, and the "internal
`__`-prefixed schema key" concept entirely. scifor's schema is the dataset
schema again. The one rule scifor keeps is general, not scidb-shaped: *combo
keys that are not schema keys and not iterables pass through to the result
row* — which it already does (`row = dict(metadata)`).

## 4. The decision: MATLAB

`+scifor/for_each.m` runs the loop in MATLAB. It cannot call a Python
closure per combo cheaply (each Python↔MATLAB call is ~ms and a run has
thousands of combos × inputs). Three options:

**(a) Pre-computed selection table — recommended.** The bridge already
computes everything "Python decides, MATLAB applies" (PathInput resolution,
PathOutput paths, combos). Add one more: for every combo index and every
param, the list of rids that combination reads — `rids_for_combo` over
`state.full_combos`, serialised once at prepare. MATLAB's loop selects rows
with `ismember(frame.x__record_id, rids{combo_i}{param})` and drops the
column. No callback, no schema extension, no `x__rid_*` renames, no prefix
strips in `.m`. Cost: one list per combo per param, built in Python at
prepare — the same data the combos already carry, in a different shape.
**Python and MATLAB then apply the SAME rule from the SAME source**
(`rids_for_combo`), which is the parity property every earlier stage fought
for.

**(b) Callback per combo.** Faithful to the Python seam, but a Python call
per (combo, input) from MATLAB. Rejected on cost.

**(c) Keep the column convention on the MATLAB path only.** Python retires
it, MATLAB keeps `x__rid_*` and its strips. Rejected: it leaves two
contracts for one boundary, which is the disease this whole plan treats.

Recommendation: **(a)**. It needs the MATLAB `for_each.m` loop to take an
optional `row_selection` argument (cell array indexed like combos) and
apply it where it currently relies on the extended schema, and the bridge
to build it. `TestAcrossVariants.m` / `TestAsTable.m` and the MATLAB
endpoint tests already exercise these paths.

## 5. Order of work (each step green before the next)

1. **scifor: add `_select_rows`**, called after the schema filter. No
   behaviour change when None. Tests in `scifor/tests`: hook receives the
   schema-filtered frame; its return is what the function gets; `as_table`
   and scalar extraction unchanged.
2. **scidb: pass the hook; stop extending the schema.** Step 12 stops
   renaming/adding columns; Step 15 stops `set_schema`; Step 18 stops
   restoring; `rid_keys_for_schema` and `extended_metadata_iterables`
   padding go. The combos are unchanged. Guard: the whole scidb suite —
   `test_identity_parity.py`, `test_aggregation*`, `test_as_table*`,
   `test_column_selection*`, `test_coarse_input_provenance.py`,
   `test_pathoutput_variants.py`. The parity suite is the load-bearing one:
   edges must be byte-identical before and after, because the rule that
   selects the rows IS the rule that writes the edges.
3. **scifor: remove the four prefix strips.** Dead once (2) lands; removing
   them is what proves it.
4. **MATLAB: (a).** Bridge builds the per-combo selection; `for_each.m`
   applies it; `_sanitize_rid_key` and the `.m` strips go. Guard:
   `scimatlab/tests` (Python side) and the MATLAB suite the user runs.
5. **Docs**: `scidb-for-each-internals.md` Steps 12/15/18, `docs/claude/
   scidb-for-each-internals.md` §aggregation, `variant-space.md` §6,
   `input-binding-round-trip.md` (the write side now has one selector).

## 6. What this does NOT change

* Combo identity (`__rid_*` / `__vsig_*` on the combo) and result-row
  columns — the save path reads them exactly as today.
* `invocation_id`, `record_id`, `call_id` — no identity recipe is touched.
  The parity suite must show zero diffs.
* `RunBindings` — it already has `rids_for_combo`; 2c gives it a second
  caller, which is the test of whether it was the right abstraction.

## 7. Risks

* **`as_table` inputs.** Today `_extract_data(as_table=True)` returns the
  frame minus internal schema columns. With the hook dropping
  `__record_id`, the frame the function sees is unchanged — verify with
  `test_as_table*`.
* **A frame with no `__record_id`** (a DataFrame passed directly, a
  PathInput): the hook must pass it through untouched. `rids_for_combo`
  has no entry for such a param, so it does.
* **Coarse inputs.** Today the rid probe (`_rid_probe_key`) blanks
  unpopulated positions so a subject-level input matches beneath its
  location. The hook selects by rid, not by location, so this is
  automatically right — and `test_coarse_input_provenance.py` guards it.
* **MATLAB performance.** (a) adds one cell array to the bridge payload;
  size is `#combos × #inputs` rid lists — the same order as the combos
  themselves.
