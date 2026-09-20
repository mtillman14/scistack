# Schema key types: numeric/string declaration + canonicalization

## Purpose

Zero-padded filenames (`6MWT-001.mat`) mean the same logical trial can be
spelled `"001"` (disk), `1` (explicit iterable), or `1.0` (MATLAB double).
PathInput's numeric fallback makes every spelling *resolve*
(`docs/claude/pathinput-zero-padded-matching.md`), but stored schema-key
identity needs one spelling. The hybrid contract (user-designed, 2026-07-12)
fixes identity with **minimal syntax burden**: declarations are only required
when the dataset proves the ambiguity real.

## The contract

| Situation | Behavior |
|---|---|
| All path matches exact | No declaration needed. Verbatim spellings, exactly as before. |
| PathInput bridges spellings (`trial=1` → `"001"`) on an **undeclared** schema key | `SchemaKeyTypeError` telling the user to declare the key once. Aborts the whole run (`scifor_fatal`), not a per-combo skip. |
| Key declared `"numeric"` | Canonicalization is **unconditional** — every spelling from every source (explicit iterables, discovery combos, DB fills, direct save/load) collapses to the unpadded form before storage. Path lookups still bridge padding via the fallback. |
| Key declared `"string"` | Verbatim everywhere; spelling IS identity. PathInput matches these keys **exactly only** — `"1"` never bridges to `"001"` (both could legitimately exist as distinct trials). |

Key insight from the design discussion: the *error* is resolution-triggered,
but the *canonicalization* must not be — a discovery-driven run
literal-matches `"001"` (no resolution event) while an explicit run resolves,
so resolution-triggered-only canonicalization would reintroduce the mixed
spellings it exists to fix.

## API

```python
configure_database(path, ["subject", "trial"],
                   schema_key_types={"trial": "numeric"})
```

Validation at construction: declared keys must be schema keys; values must be
`"numeric"` or `"string"`. Not persisted in the DB (dataset in testing mode;
declarations live in user code like the schema keys themselves).

## Where each piece lives (layering per CLAUDE.md NOTE 3)

| Piece | Location |
|---|---|
| `load_with_captures(metadata, db=None, numeric_match=None)` — resolve + report bridged spellings; `numeric_match` restricts fallback-eligible keys | `scifor/src/scifor/pathinput.py` (policy-free reporting) |
| `scifor_fatal` escape hatch — exceptions with this attr abort the loop instead of per-combo skip | `scifor/src/scifor/foreach.py` main loop |
| `SchemaKeyTypeError` (`scifor_fatal = True`) | `scidb/src/scidb/exceptions.py` |
| `_canonical_numeric_value(key, value)` — "001"→1, 1.0→1, "1.50"→1.5, else raise | `scidb/src/scidb/database.py` |
| `schema_key_types=` param + validation + `canonicalize_metadata()` | `scidb/src/scidb/database.py` (`configure_database`, `DatabaseManager`) |
| Entry-point canonicalization — `save_variable`, `save_batch`, `load`, `load_all_as_df` all call `canonicalize_metadata` (one seam covers Python direct calls AND the MATLAB bridge's `save_batch_bridge`/`load_and_extract`, which bypass `BaseVariable`) | `scidb/src/scidb/database.py` |
| Step 5 iterable canonicalization + discovered-combo canonicalization/dedupe | `scidb/src/scidb/foreach.py` (`_prepare_foreach_state`) |
| Per-combo enforcement (`_load_pathinput_checked`) — string keys excluded from `numeric_match`, undeclared resolved keys raise | `scidb/src/scidb/foreach.py` (Step 16 wrapper → `_resolve_per_combo_loader`) |

Standalone Python scifor (no database) keeps the silent fallback: there is no
stored identity to protect, and scifor never sees schema policy.

## Notes / edges

- Canonicalization can collapse discovered combos that differed only in
  spelling (both `6MWT-1.mat` and `6MWT-001.mat` on disk) — deduped in Step 5.
  If such a tie needs *resolving* per combo, the fallback's ambiguity
  RuntimeError still fires at load time.
- Non-schema combo keys (version params) keep the silent fallback; their
  spelling doesn't define dataset identity. Revisit if version-key spelling
  splits ever bite.
- `canonicalize_metadata` handles list values (load()'s OR semantics)
  element-wise; bools on numeric keys are rejected.
- Floats: integral → int ("1.0" ≡ 1); non-integral normalize via float
  ("1.50" ≡ 1.5).
- **MATLAB parity implemented (2026-07-12, same session).** Architecture
  differs from Python because the MATLAB loop resolves PathInput itself:
  - Canonicalization comes free: `for_each_prepare` runs Python Steps 2-15
    (combos return canonical), and the entry-point canonicalization in
    `DatabaseManager` covers the bridge save/load functions.
  - Declaration: `scidb.configure_database(..., schema_key_types=
    struct('trial','numeric'))` (`+scidb/configure_database.m` → py.dict).
  - Enforcement: `+scidb/for_each.m` injects a `_pathinput_loader` callback
    into `+scifor/for_each.m` (new opt; scifor stays policy-free). The
    callback is `+scidb/+internal/load_pathinput_checked.m`, which calls
    the new `+scifor/PathInput.m::load_with_captures` and raises error ID
    `scidb:SchemaKeyTypeError`.
  - Fatal abort comes free in MATLAB: the constant-resolution block in the
    scifor loop has no per-combo try/catch, so the error propagates and
    aborts the run (no `scifor_fatal` mirror needed).
  - Standalone MATLAB scifor (no loader injected) keeps the silent
    fallback, same as standalone Python scifor.

## Standalone-only auto-condensation: `condense_numeric` (2026-07-28)

Separate, narrower opt-in than the `schema_key_types` contract above — no
database, no declaration, automatic. When `PathInput.apply_discovery(...,
condense_numeric=True)` (Python) / `pi.apply_discovery(..., true)` (MATLAB),
a **discovered** value that is purely digits (`"001"`) collapses to a
number (`1`, stripping the leading zero) before it enters the iterables or
the returned combos. Off by default (`condense_numeric=False`/omitted),
so every pre-existing call site — including scidb's — is unaffected.

- **Only for values scifor itself discovers from disk.** An explicit value
  the caller passes (`subject=["001"]`) is never touched — same
  identity-preservation reasoning as `feedback_zero_padded_schema_keys`,
  just narrower in scope (there's no stored DB identity to protect in
  standalone use, only the caller's own explicit-vs-discovered intent).
- **Isolation from scidb is structural, not a runtime check.** Both layers
  share `PathInput.apply_discovery` / `scifor.foreach.resolve_pathinput_discovery`,
  but scidb always pre-builds its own combos (`_all_combos=state.full_combos`
  in Python, `opts.all_combos` in MATLAB) before calling into scifor, and
  scifor's own discovery call site is gated by `_all_combos is None` /
  `isempty(opts.all_combos)` — so scidb's Step 3 discovery call
  (`condense_numeric` omitted, defaults False) and scifor's standalone call
  (`condense_numeric=True`, hardcoded) never both fire for the same run.
  scidb's `schema_key_types` declared-only contract is completely unaffected.
- **Python:** `scifor/src/scifor/pathinput.py::PathInput.apply_discovery`
  condenses each digit-only combo value via `int(value)`; the standalone
  call site in `scifor/src/scifor/foreach.py` (`resolve_pathinput_discovery`,
  around the `_all_combos is None` block) passes `condense_numeric=True`.
- **MATLAB:** `+scifor/PathInput.m::apply_discovery` takes a 4th
  `condense_numeric` arg (default `false`, so every pre-existing 3-arg call
  site is unaffected), forwarded to Python via `pyargs('condense_numeric',
  ...)`. Returned Python `int`/`float` values are converted to MATLAB
  `double` via the new private static `PathInput.condense_py_value`,
  instead of the old unconditional `char(string(...))` — so a condensed
  key actually ends up numeric, not just a shorter string. `+scifor/for_each.m`
  passes `true` at its own standalone discovery call site only.
- Tests: `scifor/tests/test_pathinput_discover.py::TestApplyDiscoveryCondenseNumeric`,
  `scifor/tests/test_foreach_pathinput.py` (condensed vs explicit-passthrough
  end-to-end), `scimatlab/tests/matlab/scifor/TestPathInput.m` (condense_numeric
  section, mirrors the Python cases).

## MATLAB schema-key column TYPE round-trip (2026-07-13)

Contract (user-designed): output metadata columns from `scifor.for_each`
come back as **exactly the input column's type** — double stays double,
string stays string, categorical stays categorical (categories + ordinality
preserved). Two cooperating pieces in `+scifor/for_each.m`:

1. **Internal canonical iteration** (`decategorize_schema_column`, called
   from `distinct_values_from_inputs`): MATLAB `categorical` stores labels
   as text, erasing whether the source was numeric or string
   (`categorical([1;2])` ≡ `categorical(["1";"2"])`). When resolving
   `key=[]` from a categorical column, iterate by numerics **only when
   every label round-trips losslessly** through `str2double`
   (`"1"` → 1 → `"1"`) — this gives numeric (not lexical) iteration order
   and lets explicit `key=1:n` iterables match. Any non-canonical spelling
   (zero-padded `"01"`, mixed `"1"`/`"01"`, text, missing) keeps ALL labels
   verbatim as strings. Same lossless rule as `_canonical_numeric_value`,
   applied as inference only where categorical already destroyed the type;
   plain string columns are never inferred on.
2. **Output type restoration** (`capture_schema_column_types` before the
   loop → `restore_schema_column_types` in `build_single_output_table`):
   each schema/meta key's input column class is captured (plus categories +
   ordinality for categorical) and the output metadata column is cast back
   to it. Lossless-only: a cast that cannot round-trip warns and leaves the
   column at the internal canonical type, as does a key whose input tables
   disagree on class. Keys with no table column (pure explicit iterables)
   keep the iterable's own type. `categorical=true` still force-converts
   afterwards. Decisions are logged at DEBUG, failures/conflicts at WARN.

Regression this fixes: numeric-backed categorical keys came back as
lexically-sorted string columns ("10" < "2"), changing both schema-key
identity and column type.

**Python parity (ported 2026-07-13):** pandas keeps value dtypes inside
categoricals, so the identity bug never existed there — no
`decategorize_schema_column` analog is needed, and int-backed categoricals
already iterate in numeric order. The dtype round-trip IS ported:
`_capture_schema_column_dtypes` (after Step 3 in `scifor/foreach.py`) records
each key's input column dtype (a `CategoricalDtype` carries categories +
orderedness), and `_restore_schema_column_dtypes` (end of
`_results_to_output_dataframe`) casts output metadata columns back,
lossless-or-leave-with-WARN, with an `astype("category")` fallback when
values fall outside the captured category set. Conflicting input dtypes for
one key → recorded as None, warned, left at the natural dtype. The scidb
Python path is covered automatically (it delegates to `scifor.for_each`).

**Known ordering nuance (both languages):** text-label categoricals iterate
in LEXICAL order of the distinct labels, not in category order — e.g.
`categorical(["pre","post"], ordered)` iterates "post" then "pre". The
restored output column still carries the true category order, so
`sortrows`/`sort_values` recovers it. Deliberate: identical behavior whether
the same labels arrive as strings or categorical; revisit if category-order
iteration is requested.

## Tests

- `scidb/tests/test_schema_key_types.py` — validation, canonical-value unit
  tests, direct save/load identity, for_each numeric/string/undeclared paths,
  discovery+explicit identity sharing.
- `scifor/tests/test_pathinput_padded.py::TestLoadWithCaptures` — reporting
  API and `numeric_match` exclusion.
- `scimatlab/tests/matlab/scidb/TestSchemaKeyTypes.m` — MATLAB mirror:
  declaration round-trip, numeric/string/undeclared for_each paths,
  discovery+explicit identity sharing (helper: `read_file_value.m`).
- `scimatlab/tests/matlab/scifor/TestSciforForEachCategorical.m` section F —
  schema-key column TYPE round-trip: double/string/categorical in → same
  type out, numeric iteration order for numeric-backed categoricals,
  zero-padded/mixed labels verbatim, ordinal + category-order preservation,
  explicit-iterable typing, type-conflict tolerance, categorical=true
  round-trip, two-key nested-struct regression mirror.
- `scifor/tests/test_schema_dtype_roundtrip.py` — Python mirror: int/int32/
  object/categorical dtype round-trip, ordered categoricals, numeric
  iteration order for int-backed categoricals, flatten-mode restore,
  explicit-iterable typing, dtype-conflict tolerance.

## See also

- `docs/claude/pathinput-zero-padded-matching.md` — the resolution mechanism.
- `.claude/plan-schema-key-type-canonicalization.md` — design history and
  the rejected alternatives.

## Loaded values: the type is the key's, not the value's (2026-09-19)

`_from_schema_str` restores a stored VARCHAR to a number when the spelling
round-trips (`"10"` → 10, `"01"` stays). Applied per value it produced a
column with two types — cycles `"01".."09"` beside the int 10 — which the
example integration suite caught (`tests/integration/test_storage_roundtrip.py`).

Every load path now calls `DatabaseManager.restore_schema_value(key, value)`,
which asks `schema_key_is_numeric(key)`:

1. a declared `schema_key_types` entry wins (`"numeric"` / `"string"`);
2. otherwise the key is numeric only when **every** stored value of it
   round-trips. One zero-padded value makes the whole key a string key.

The answer is cached per key and dropped whenever a schema row is created
(`_forget_schema_key_kinds`), because a first `"01"` after `1, 2` changes it.
Logged once per key at DEBUG: `schema key 'cycle' loads as strings: 1 of 10
stored value(s) keep their spelling (e.g. '01')`. Regression tests:
`scidb/tests/test_schema_key_type_consistency.py`.
