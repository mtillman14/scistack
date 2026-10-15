# Function-Version Variants

> Status: **implemented 2026-09-06, all tests passing, uncommitted.** Written
> first as a diagnosis, then updated once the fix landed. Sections 1–4 and "The
> two keys" describe the model as it now stands; the ✅ boxes mark what changed
> and the tests that hold it there. Stage detail:
> `.claude/plan-function-version-variants.md`.
>
> The short version of what a future session needs: **there are two different
> notions of "what makes two records distinct", they are scoped differently on
> purpose, and conflating them is what caused four separate bugs.**

## The question

Two records of `RawEMG` sit at the same schema location (`pass=1`). One was
produced by `loadDelsysEMGOneFile` before its body was edited (13 struct fields),
one after (12 fields). Same variable, same schema location, same constants, same
call site. **What, if anything, makes them different?**

The stack has three separate answers to that question and a fourth place that
never asks it. None of them mentions the function body.

## Answer 1 — the load-path collapse (`scidb/database.py`)

`_find_record(version_id="latest")` groups candidate rows and keeps the newest
per group (`database.py:1788-1836`). The group key is:

```
(variable_name, schema_id, variant_key)

variant_key = (fn_name,                       # inv[1] — the NAME, not the hash
               json(branch_params),
               output_num,
               consumed_input_schema_ids)     # cross-level where= distinctness
        or   ("__raw__", None)                # no producing invocation
```

Two records that differ only by function body land in the **same** group.
The newer one wins; the older is invisible to `load`.

## Answer 2 — the node-state collapse (`provenance_query._producing_variant_key`)

`_current_records_by_schema` (`provenance_query.py:960`) enumerates the *current*
records of a type — latest per `(schema_id, producing-variant)` — where the
variant key is `_producing_variant_key` (`provenance_query.py:941`):

```
tuple(sorted((param, repr(value)) for constants of the producing invocation))
or None for raw records
```

Constants only. Not the function name, not the hash, not the inputs — the
docstring is explicit that input record_ids are excluded because "re-running on
a changed upstream input is the *same* variant, just newer."

Again: a body edit collapses. The new record supersedes the old.

## Answer 3 — `invocation_id` (`scidb/provenance.py:144`)

```
invocation_id = sha16(function_hash | as_table | distribute | sorted(bindings))
```

Here the function hash **is** part of identity. This is the one place in the
stack where "which version of the code produced this" is load-bearing rather
than merely recorded.

## Answer 4 — the display paths, which do not collapse at all

`scistack_gui/api/variables.py` and `scistackplotdb/load.py` both select **every
non-excluded record** of a type. No latest-collapse, no variant key. They used
to label/group by `branch_params_batch`, which accumulates only *constants*
along the upstream chain (`provenance_query.py:243`) — so they saw both records
and had nothing to tell them apart.

> ✅ **Fixed.** Both now call `provenance_query.variant_identity_batch`, which
> returns branch params *plus* the producing function's version.

---

## The two keys — the load-bearing distinction

There are genuinely **two different questions**. The original code answered only
the first, and used its vocabulary to answer the second:

| | question | answer | consumer |
|---|---|---|---|
| **Supersession key** | "does this new record *replace* that old one, or coexist with it?" | constants (+ fn name, output_num, consumed locations on the load path) | `load`, node state, `find_record_id` |
| **Display discriminator** | "these records coexist on screen — what do I *call* each one?" | `variant_identity_batch` — branch params + `fn_version` / `is_latest` | variable panel, Plot Studio |

The supersession answer is **right and did not change**: a body edit produces a
newer version of the same thing, not a rival variant. That is why downstream
consumers correctly saw the new 12-key record all along.

The display discriminator was **wrong by construction**: it reused the
supersession vocabulary (`branch_params`) to answer a question supersession
never asks. When two records survive to the display layer that the supersession
key calls the *same* variant, the display had no word for the difference — so it
printed `(raw)` twice.

### Consequence 1 — the UI lied by omission

`get_variable_records` returned `records[2], variants[1]`: two rows, one variant,
both labelled `(raw)`. Nothing on screen said the difference was the function body.

> ✅ **Fixed.** Grouping key is `(branch_params, fn_hash)`; labels read
> `low_hz=20 · bandpass_filter v2 (latest)`. The sidebar grows a "Code" column
> and a banner, shown only when versions exist.

### Consequence 2 — silent overplotting

`scistackplot` has a guard for exactly this. `roles.validate` refuses to let a
multi-level **variant factor** sit in `FREE`/`AGGREGATE`, because "their levels
are different pipeline variants, not replicates, so averaging or overplotting
them silently mixes results."

The guard is armed by `attach_variants`, which adds one column per branch param
— and its own docstring calls this "the correctness-critical step" producing "a
figure that is wrong in a way that looks like data."

With empty branch params there was **no variant column**, so the guard never
fired and the two function versions overplotted as replicates. The machinery to
prevent the failure existed; it was simply never handed the fact.

> ✅ **Fixed.** `attach_variants` adds `CodeVersion` (constant
> `VERSION_FACTOR`) to the *variant* column list, which arms the guard.
> `default_roles` also had to change: it gave extra variants `Role.FREE`, which
> `validate` refuses outright, so a table with two variant factors produced an
> error instead of a figure. Extras now default to `FACET`.

---

## Why function version is absent from the supersession keys but present in `invocation_id`

This asymmetry is deliberate and worth preserving:

- **Record level** — an output whose producing function no longer matches
  current source is still valid, traceable data. Old records are preserved, not
  invalidated. (`feedback_defer_content_staleness`: hash mismatch is
  traceability, not staleness.)
- **Node level** — the node colors on coverage of the *current recipe*: have
  these inputs been run through the code as it exists now? A body edit shifts
  the expected `invocation_id`, so the answer becomes no.

Both hold simultaneously. `database-model.md` §12 states this correctly.

**The node-level half used to work only for functions with variable inputs.**
`expected_invocations_for_function` has two sources, and
`_predict_config_invocations` — the only one that folds `fn_hash` in — returns
early for a config with no DB-variable inputs. A PathInput-only loader therefore
drew its entire expected set from `realized_inputless_invocations`, a pure
structural read of what already exists with **no reference to `fn_hash` at
all**. Expected ≡ present, unconditionally, forever.

> ✅ **Fixed.** `realized_inputless_invocations(duck, fn_name, fn_hash)` filters
> by version. An edited body empties the realized set → the node reports
> needs-run; re-running refills it under the new hash → green. It cannot get
> stuck red, because the realized set is rebuilt from the graph each time.
>
> `fn_hash=None` remains the default and means any-version. That matters:
> `realized_inputless_schema_ids` (the PathInput *discovery* check) must keep it,
> because it asks "where has this loader produced output", not "under which
> code" — and its only caller, `check_pathinput_node_state` via
> `inspect/api.py`, passes a bare stub with no meaningful hash. Filtering there
> would redden every node permanently.
>
> **`check_pathinput_node_state` was deliberately NOT rewired into the GUI.** It
> ignores `fn_hash` entirely, so it would not have fixed this at all; it solves
> a different problem (spotting files that appeared but were never run). That
> remains open and is its own feature.

### The MATLAB corollary

For MATLAB the read side did not compute the MATLAB hash at all:

- **Save**: `_invocation.function_hash` ← `__fn_hash` ←
  `scidb.internal.hash_function` → `scimatlab.source_hash.matlab_function_source(which(...))`
  = `sha256(raw file bytes)` (the one owner since 2026-10-14; it used to be
  `sha256(fileread(...).encode("utf-8"))`, which diverged from the GUI's
  raw-bytes hash on non-UTF-8-decoded files).
- **Read**: `check_node_state` did `_compute_fn_hash(fn.fcn)`. For a
  `MatlabLineageFcn`, `fn.fcn` is a `_FunctionProxy` carrying only `__name__`,
  so `compute_function_hash` fails `inspect.getsource` and falls back to
  `_hash_bytecode_only` on a non-function — a constant unrelated to the `.m`
  file.
- `MatlabLineageFcn.hash`, which *does* encode the source hash, was computed in
  `__init__` and never read by the state path.

This was masked only because the inputless gap short-circuited first. Any MATLAB
function with variable inputs would have been **permanently red**, since its
predicted `invocation_id` could never match a stored one — which is why the two
fixes had to ship together.

> ✅ **Fixed.** `MatlabLineageFcn` exposes `source_hash` (the `.m` digest
> verbatim, as stored), and `foreach_config.function_hash_for(fn)` is now the
> single read-side recipe: an explicit `source_hash` wins, else the AST hash
> with the existing `.fcn` unwrap. Duck-typed, so scidb still does not import
> scimatlab. Applied at all three read sites — `check_node_state`,
> `_check_via_graph`, `inspect/graph._node_states`.
>
> `ForEachConfig.to_version_keys` is deliberately untouched: that is the
> **write** recipe, and changing it would invalidate every hash already stored
> in existing databases.

---

## The decision: supersession is visible, not destructive

Consistent with the project ethos that removal means hide-and-exclude, never
delete (`feedback_never_delete_mark_hidden`):

1. The supersession keys stay as they are. A body re-run makes a newer version
   of the same variant; `load` and node state keep returning the newest.
2. The superseded record is **not** hidden from the display layers. It remains
   listed and plottable, explicitly labelled with the function version that
   produced it.
3. The display layers get a discriminator of their own, distinct from
   `branch_params`, derived from the producing invocation's `function_hash`.
4. The Plot Studio **pins the latest** version by default rather than faceting
   across versions (decided 2026-09-06) — the older version stays one click
   away, and the default figure for a 13-field EMG struct does not double to 26
   panels the moment a function is edited.

## Which layer owns what

Per CLAUDE.md NOTE 3, "what makes two records distinct" is a **scidb** question:

- **scidb** — owns variant identity. Gains one batched read
  (`variant_identity_batch`) returning branch params *plus* the producing
  function name, hash, a version ordinal, `is_latest`, and `saved_at`. Recency
  comes from `_record_save.timestamp`, which is the only per-save recency source
  (`_record` is `ON CONFLICT DO NOTHING`, so its `created_at` is frozen at first
  save).

  **The ordinal and `is_latest` are scoped differently, and it matters.**
  Ordinals are numbered per *variable type*: Plot Studio turns `fn_version` into
  one factor column spanning every schema location in a single frame, so
  numbering per location would let one `function_hash` be `v1` at `subject=1`
  and `v2` at `subject=2` — an incoherent column and a figure that is wrong in
  the same way the missing variant column was. `is_latest` is instead resolved
  per *location*, because pinning "the latest" must keep each location's own
  newest version; a type-wide "latest" would silently drop every subject that
  was never re-run under the newest code.
- **scistack-gui** — renders labels. No independent notion of variant.
- **scistackplotdb** — attaches the version as a *variant column*, arming the
  existing `roles.validate` guard.
- **scistackplot** — unchanged. Its variant machinery already works; it was
  simply never given a variant.

Duplicating the derivation into the GUI would be the exact mistake
`feedback_avoid_scifor_scidb_duplication` warns about, one layer up.

---

## Ground truth

Behavior is defined by tests, not by this prose (`docs-ground-truth-test-map.md`).

Verified against disk 2026-09-06 — `docs-ground-truth-test-map.md` is stale on
several of these paths, so prefer this table.

| Topic | Tests |
|---|---|
| **Function-version identity** (this feature) | `scidb/tests/test_variant_identity.py` |
| **Version labels / variant grouping (GUI)** | `scistack-gui/tests/test_api.py::TestVariableRecordsFunctionVersions` |
| **Version as a plot factor + pin-latest** | `scistackplotdb/tests/test_source.py` (`two_code_versions`, `constants_and_code_versions` fixtures) |
| **Body edit reddens a loader** | `scihist/tests/test_state_pathinput.py::TestBodyEditRedensAPathInputLoader` |
| **MATLAB read/save hash parity** | `scimatlab/tests/test_bridge.py::TestMatlabLineageFcn` |
| **Plot cache invalidation after a run** | `scistack-gui/tests/test_plot_service.py` |
| Variant identity / latest-collapse (pre-existing) | `scidb/tests/test_variant_queries.py`, `test_variant_pinning.py`, `test_aggregation_with_variants.py`, `test_pathoutput_variants.py` |
| Node state (pre-existing) | `scihist/tests/test_state.py`, `test_state_workflows.py`, `test_state_realworld.py`, `test_state_call_id.py` |

The **≥2 historical function hashes for one combo** regression case that
`project_latest_record_selection_future_issue` has been asking for since 2026-04
now exists, in `TestBodyEditRedensAPathInputLoader`
(`test_two_historical_hashes_do_not_confuse_the_current_one`).

⚠️ **Never run two packages' tests in one pytest invocation** — each `tests/`
dir uses bare `from conftest import …` with no `__init__.py`, so the module name
resolves to whichever loads first and collection fails. One invocation per
package.

## Corrections to sibling docs

- **`database-model.md` §12, "Function-source edit recolors a GUI node —
  RESOLVED"** — was true only for functions with variable inputs; inputless
  (PathInput-only) loaders never reached the `invocation_id` prediction and so
  never recolored. **As of 2026-09-06 the claim is true as written**, via the
  `fn_hash` filter on `realized_inputless_invocations`. No edit needed.
- **`state.py:361`** — `check_node_state`'s docstring says combos are "checked
  via `check_combo_state`". They are not: `check_node_state` works purely from
  expected-vs-present invocation membership. `check_combo_state` (and
  `_check_via_graph`, the only surviving `stored_hash != current_hash`
  comparison in the codebase) is called by **nothing in production** — only by
  tests and the `scihist` re-export shim.
- **`docs-ground-truth-test-map.md`** — its "Caching & node states" and
  "MATLAB setup & parity" rows cite `scihist/tests/test_cache_hit.py`,
  `test_state_matlab_pathinput.py` and `scidb/tests/test_call_id.py`, none of
  which exist on disk as of 2026-09-06 (`test_skip_computed.py` does). Use the
  table above for this topic.
