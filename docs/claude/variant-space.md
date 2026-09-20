# Variant space

*Written 2026-09-20, after the call-id consolidation, to fill the gap the
question "isn't `branch_params` just an early `Variant`?" exposed. Short
answer: no — one is the coordinate, the other the selector. Long answer
below, together with the four different questions the codebase asks about
"which variant is this record" and which function answers each.*

---

## 1. The coordinate and the selector

A schema location can hold **several records of one variable type at once**,
and they are not versions of each other — they coexist, legitimately. What
tells them apart is where each one sits in *variant space*.

| | What it is | Where it lives |
|---|---|---|
| **`branch_params`** | a record's POSITION in variant space | derived from the graph on every read (`provenance_query.branch_params_batch`) — never a stored column |
| **`Variant(X, low_hz=20)`** | a PREDICATE over that space | `scidb/variant.py`; its payload *is* a `branch_params` dict, threaded to the loader as `branch_params_filter` |

So `Variant` cannot absorb `branch_params`: it is defined in terms of it.
`Variant.branch_params` is literally the same dict shape that
`branch_params_batch` returns, and `database._filter_records_by_branch_params`
compares one against the other. Remove the coordinate and the selector has
nothing to select on.

`branch_param("bandpass", low_hz=30)` is the same dict built by hand, for the
`load()` kwarg path. `AcrossVariants(X)` is not a point or a predicate at all
— it is a *run option* saying "do not split by this coordinate" (see §6).

## 2. Three dimensions, one dict

Variant space has three axes. All three travel in **one** `dict[str, Any]`,
distinguished by reserved key prefixes:

| Axis | Key shape | Resolved against |
|---|---|---|
| **upstream constants** | `{producing_fn}.{param}` — e.g. `bandpass.low_hz` | `branch_params_batch` (the accumulated constants of the whole upstream chain) |
| **code version** | `__code__` or `__code__.{fn}` → `"v1"` / `"latest"` | `chain_batch` (`code_version_ordinals`) |
| **run options** | `__run__` or `__run__.{fn}` → a `run_options_label` string | `chain_batch` |

Plus one synthetic namespace on the constants axis: **`__save__.{kwarg}`** —
a direct save's non-schema kwargs, anchored as constants on a synthetic
`__save__` invocation so they sit in the same coordinate system as a
`for_each` constant. (They are also the only branch params that are *also* a
loaded data column, which is why the aggregation split has to check them for
contradiction — see `bindings.signature_conflicts_with`.)

They share one dict on purpose: "which variant of this input" is one question
with three dimensions, and threading three filters through ten call sites
that already carry one buys nothing. The prefixes cannot collide with a real
branch param, because those are always `{producing_fn}.{param}` and
`__code__` is not a producing function.

**The cost of the shared dict** is that the axes are untyped: every consumer
re-parses the prefixes. `variant.py` is largely that machinery
(`is_code_or_run_pin`, `normalize_pin_key`, `normalize_selection`,
`pin_loads_uncollapsed`), and `database._filter_records_by_branch_params`
opens by splitting the dict back into three filters. A typed coordinate
(named `constants` / `code` / `run_options` fields, prefixes spelled once at
the dict boundary) is the obvious next move; it is not done.

### The display dialect

The plotting layer and the GUI picker name these columns `Code:bandpass`,
`Run:loader`, `CodeIsLatest`. `variant.normalize_selection` is the ONE
translation to scidb's spelling, and it lives in scidb — not in the display
layer — because `scistackplot` must never import scidb (the CSV path depends
on that), so the alternative was the same mapping written once per consumer.
It is idempotent: an already-canonical dict passes through unchanged.

## 3. `version="latest"` collapses on a variant key — which is why pins care

`load()` defaults to `version_id="latest"`, which collapses each
`(variable, schema location, variant)` family to its newest record. The key
(`database.py`, `_find_record`) is:

```
(function_name, canonical branch_params, consumed input locations)
```

`function_hash` is deliberately NOT in it — a body re-run is a newer version
of the same variant, not a rival (`function-version-variants.md`) — and since
2026-09-14 an older `distribute`/`as_table` run is likewise superseded.

**Consequence:** a code or run pin against a collapsed load always matches
nothing, because the collapse merged those variants *before* the filter ran.
A constants pin is unaffected (branch_params IS in the key). That one rule is
`variant.pin_loads_uncollapsed`, and every path that resolves a pin to records
— the `for_each` input loader and `provenance_query.records_for_variant` —
consults it, so "load this variant" and "introspect this variant" cannot
disagree about which records exist.

## 4. Four answers to "which variant is this record?"

These look like duplication and mostly are not. They answer different
questions, and the difference is load-bearing.

| Function | Question | Scope |
|---|---|---|
| `bindings.variant_signature(bp)` | *"Which GROUP does this record aggregate into?"* | canonical JSON of the full chain's branch params |
| `provenance_query._producing_variant_key` / `variant_keys_batch` | *"Does this new record replace that old one, for 'current records'?"* | constants of the **directly producing** invocation only (one hop) |
| `database._find_record`'s collapse key | *"Does this new record supersede that one, on a `latest` load?"* | `(fn_name, branch_params, consumed locations)` |
| `provenance_query.variant_identity_batch` | *"These coexist on screen — what do I CALL each one?"* | branch_params + fn_name + fn_hash + fn_version + is_latest |

The first three are supersession-ish; the fourth deliberately is not. A re-run
after a body edit *supersedes* (so downstream sees only the newest) but still
needs a distinct **name**, or the UI labels two rows identically and a plot
overplots two function versions as replicates.

The genuinely accidental duplication was in the *first* row: `variant_signature`
had three hand-rolled twins (`database.py`'s `bp_json`, `foreach.py`'s
PathOutput `{variant}` text, the GUI's variant-summary grouping key), and they
had already drifted — `{variant}` digested `json.dumps(merged_bp)` in full
iteration and `"|".join(signatures)` in aggregation, so one PathOutput
template wrote to two different directories for the same group. One owner now.

## 5. Bare names and the suffix rule

Branch params are namespaced per producing function. A **bare** name
(`low_hz`) is resolved by suffix-match against `.{name}`; more than one hit is
an error naming the candidates (`AmbiguousParamError`), not a silent pick.
`Variant(X, fn="detect_spikes", threshold=0.5)` is the disambiguating form —
a dotted string cannot be a kwarg.

One owner: `scidb.variant.match_bare_name`. It used to be spelled twice —
once for load filtering, once for PathOutput `{placeholder}` resolution — with
the same `.endswith` and the same ambiguity error.

## 6. What is NOT a coordinate: the run options

`AcrossVariants(X)`, `as_table`, `distribute` and `for_columns` say **how the
call was made**, not where its output sits. They are call-site identity
(`foreach_config.CallSite`) and invocation identity
(`provenance.compute_invocation_id`), stored as columns on `_invocation`, and
reported as one string by `provenance_query.run_options_label`.

They reach variant space only through the back door: because they are folded
into `invocation_id`, re-running unchanged code under a different flag writes
a *second* record at the same location that nothing in constants or code tells
apart. That is what the `__run__` axis exists to pin.

`AcrossVariants` specifically is the opt-out from the aggregation auto-split:
pool every variant group into one call and attach the branch params as
ordinary columns, for multiverse / specification-curve analysis. It must be
recorded (`_invocation.across_variants`) because a pooled call and a
one-group split call write **identical edges** — nothing else could tell them
apart, and until 2026-09-20 the expected-invocation predictor split what the
run had pooled, so such a node could never plan green.

## 7. Where each thing lives

| | File |
|---|---|
| the pin vocabulary, prefixes, `Variant`, `branch_param()` | `scidb/variant.py` |
| deriving a record's coordinate from the graph | `provenance_query.branch_params_batch` / `derived_branch_params` |
| the canonical signature of a coordinate, and variant GROUPS | `scidb/bindings.py` (`variant_signature`, `VariantGroup`, `RecordPool`) |
| filtering records by a pin | `database._filter_records_by_branch_params` (+ the code / run halves) |
| the latest-collapse | `database._find_record` |
| naming coexisting records | `provenance_query.variant_identity_batch` |
| the pooling opt-out | `scidb/across_variants.py`, `_invocation.across_variants` |

## See also

- `docs/claude/variant-selection.md` — code-as-a-dimension, the one-hop trap
- `docs/claude/function-version-variants.md` — why `function_hash` is not in the collapse key
- `docs/claude/scidb-for-each-internals.md` §aggregation — the auto-split
- `docs/claude/scidb-identity-and-data-flow.md` — `CallSite`, `invocation_id`, `record_id`
