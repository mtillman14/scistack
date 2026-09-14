# Run options (`distribute` / `as_table`) as the third variant dimension

*Written 2026-09-14, from the "two points per index per trial" investigation.
Concerns `scidb`, `scistackplot`, `scistackplotdb`, and the Plot Studio variant
popup. Plan: `.claude/plan-run-option-variants.md`. Prerequisites:
`variant-selection.md` (§1 the three "variants", §2 the one-hop bug),
`gui-run-options-flow.md` (what a run option is and why it is identity-bearing).*

## The gap

`distribute` and `as_table` are folded into `invocation_id`
(`provenance.compute_invocation_id`). Re-running unchanged code at unchanged
constants under a different flag is therefore a **different invocation** that
writes a **second record at every schema location** — same code hash, same
branch params. That is correct (nothing is overwritten, ever) but until
2026-09-14 nothing downstream could tell the two records apart:

| layer | what it keyed on | consequence |
|---|---|---|
| `_find_record` latest-collapse | `(fn, branch_params, output_num, consumed)` | a distributed run's `output_num` is the slice index, so the two records at one trial rarely shared a key → **both survived**; `load()` handed a two-row table to downstream steps |
| `variant_identity_batch.is_latest` | code chain only | both "current" → Plot Studio's default pin kept both |
| `attach_variants` | branch params + `Code:<fn>` | no column → no pooling guard → drawn as replicates |

Observed: `loadGaitRiteOneFile` run with `distribute=false` at trial level
(every trial got the whole file), then `distribute=true` one level up.
`scidb show GAITRiteLoaded subject=SS01 session=BL speed=SSV trial=1` listed both
records as "latest per variant".

## The fix, layer by layer

**One label, spelled once.** `provenance_query.run_options_label(distribute,
as_table)` → `"distribute=false"`, `"distribute=true"`,
`"distribute=false, as_table=[cfg, df]"`. Every layer below uses this string:
as the `is_latest` signature component, as the `Run:<fn>` level, and as the
`Variant(run_options=...)` value. There is no ordinal — `distribute=true` is not
"newer" than `distribute=false`, it is *different*; recency is `is_latest`'s job.

### scidb

- `_build_upstream_closure` now also returns `inv_run_options`; `chain_batch`
  walks once and returns `{"code": {fn: hash}, "run": {fn: label}}` per record.
  `code_versions_batch` / `run_options_batch` are the two halves.
- `variant_identity_batch`: `_chain_signature(chain, run_chain)` folds each hop's
  label in, so a re-run under other options is a different chain at that
  location and the older one is `is_latest=False`. New field `run_chain`
  (`{fn: label}`), restricted to functions in `run_option_axes` — the same
  presence-means-axis rule as `code_chain`, so an ordinary project sees `{}`.
- `run_option_axes(duck, fn_names)` → `{fn: [labels]}` for functions with >1
  distinct `(distribute, as_table)` in `_invocation`.
- **Load-path supersession** (`_find_record`): records at one
  `(variable, schema_id)` are first grouped into *families*
  `(fn, branch_params, consumed)` — the variant key minus `output_num` —
  bucketed by run-options label. A family with >1 label keeps only the label
  holding the most recently saved record; the per-`output_num` rule then applies
  within it. One-label families (the ordinary case, and every genuine
  multi-output invocation) are untouched. Logged at INFO:
  `run-option supersession dropped N record(s) across M location(s)`.
- `Variant(X, run_options="distribute=true")` / `fn=` → reserved key
  `__run__` / `__run__.<fn>` (`variant.RUN_PIN_PREFIX`), filtered by
  `_filter_records_by_run_options`. `"latest"` shares `is_latest` with
  `code_version="latest"` — one notion of "current". Bare pins resolve like bare
  code pins (one candidate, several → `AmbiguousParamError`, none → no-op).
  `foreach._load_var_type_as_spread` loads **uncollapsed** for a run pin, as it
  does for a code pin — otherwise the superseded option set could never be
  selected.

### scistackplot

- `RUN_FACTOR_PREFIX = "Run:"` beside `CODE_FACTOR_PREFIX`.
- `variants.is_function_axis` = code **or** run. Every rule that meant "the axes
  the Variants section owns" now tests this: `default_selection` (answered by
  the latest flag; run axes without a flag open on the first level — no
  ordinal to prefer), `_answered`, `spanned_code_axes`, `variant_label`.
  Ordinal-specific rules (`_highest_ordinal`, `resolve_selection`'s
  pinned-elsewhere fallback) stay code-only; a `"latest"` on a run axis with
  something else pinned resolves to *unconstrained* rather than to a level.
- `capability.variant_summary` factors carry `is_run`.

### scistackplotdb

- `attach_variants`: one `Run:<fn>` column per function in `run_chain`, kept
  only if it holds >1 level over *these* records (the branch-param rule — the
  function may have run both ways producing a different type). `LATEST_COLUMN`
  is attached when there is a code axis **or** a run axis (was `if code_keys:`).
  Axis descriptor `{"kind": "run", "function": fn, "param": None}`.
- `variants.selection_for` / `branch_params_for`: `Run:<fn>` ↔ `__run__.<fn>`;
  `_bare_run_column` mirrors `_bare_code_column`.
- `endpoint.variant_expression`: `Run:<fn>` → `Variant(X, fn='<fn>',
  run_options='...')`.

### GUI (Plot Studio variant popup)

- `VariantAxis.kind` gains `'run'`; `runAxisForFunction(label)` binds by
  function NAME like code (never by port).
- `VariantFunctionNode` renders a **second dropdown** — "latest run options" +
  the levels — only when a run axis exists for that function. Same
  `setVersion`/`versionFor` plumbing as the code dropdown. The node is dimmed
  only when it has neither a code nor a run axis.
- Unmapped-axis list treats `run` like `code`. Span banners say "version (or
  run options)".

## Reading it in `scidb.log`

- `run_option_axes: N function(s) ran under >1 run-option set — ...` (scidb,
  INFO) — the axis exists.
- `_find_record(T, latest): run-option supersession dropped N record(s) across
  M location(s)` (scidb, INFO) — the load path is collapsing the older run.
- `attached N code column(s) [...] and M run-option column(s) ['Run:fn'] over R
  record(s) (K row(s) current)` (scistackplotdb, INFO) — the plot table has the
  column and the flag; `K` should be one per location.
- `fn ran under >1 run-option set overall but only 'label' over these N
  record(s) — not an axis here` — the type-level axis was thin for this
  variable and was dropped.

## Decisions

- **`as_table` is included** with `distribute` (user, 2026-09-14). Both are
  identity-bearing; a toggle of either is treated as supersession in `load()`
  and as a selectable axis in the plot.
- **Nothing is deleted or auto-excluded.** The older run stays selectable via
  the popup dropdown, a variant row, or `Variant(run_options=...)`.
- **Supersession key is the family, not `output_num`.** `output_num` is the
  slice index under `distribute`, so it cannot be part of "is this the same
  thing re-run"; it only separates outputs *within* one invocation.

## Tests

- `scidb/tests/test_run_option_variants.py` — label, chain, axis presence,
  load-path supersession (both orders, single-set untouched, downstream
  fan-out), `is_latest`, `Variant(run_options=)` incl. superseded-run pin,
  unknown label, bare-pin ambiguity.
- `scistackplotdb/tests/test_run_option_variants.py` — `Run:` column, axis
  descriptor, latest flag, default pin, pooling refusal, explicit older-run
  selection, `variant_graph` levels, both translations, endpoint expression.
  Uses `as_table` rather than `distribute` so both runs stay scalar.

## Not done / open

- `scidb variants <fn>` (inspect CLI) does not yet list run-option sets.
- MATLAB `Variant` has no `run_options=` kwarg yet.
- A `"latest"` on a run axis while a *code* ordinal is pinned elsewhere
  resolves to unconstrained (see scistackplot section) — acceptable but a
  possible overplot; the span banner will report it.
