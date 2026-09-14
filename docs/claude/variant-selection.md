# Variant Selection

> Status: **§2 confirmed and FIXED (Stages 1–2, 2026-09-08, tests unrun).
> §3–§6 remain design.** Plan + staging: `.claude/plan-variant-selection.md`.
>
> What exists: `code_versions_batch` / `code_version_ordinals` in scidb,
> `code_chain` on `variant_identity_batch`, chain-aware `is_latest`, one
> `Code:<fn>` column per multi-version function in scistackplotdb (the single
> `CodeVersion` column is gone — see the plan for why), and `_function_source`
> **for Python only** (Stage 3). What does not: MATLAB source capture, any
> declared version axis, and every GUI item.
>
> Written after a design conversation to record
> two things a future session will otherwise get wrong: (a) `CodeVersion` is a
> **one-hop** discriminator, which `function-version-variants.md` does not say
> and reads as though it were fully general; (b) the stack models a code change
> as *supersession* only, so "compare method A against method B" is not
> expressible — and the obvious fix is not the one it looks like.
>
> **§5's GUI design was built on 2026-09-08 and is documented in
> `plot-variant-rows.md`** — named variant rows, the DAG-popup picker, and how a
> variant figure is written as code. Read that for anything about the picker;
> the notes below are the reasoning that led to it, and one item (the induced
> subgraph) was deliberately not followed.
>
> Prerequisite reading: `function-version-variants.md` (what a variant *is*),
> `plotting-library-design.md` (roles, `VariantPolicy`), `each-of-variant-expansion.md`.

> **2026-09-14 addendum — a fourth referent.** Run options (`distribute`/
> `as_table`) are identity-bearing too and had the same one-axis-over gap:
> same code, same constants, two records per location, overplotted. They are
> now a `Run:<fn>` axis and part of `is_latest`, AND (unlike code) part of the
> load-path supersession. See `run-option-variants.md`.

## 1. Three things are called "variant"

The word is overloaded, and the three referents behave differently. Untangling
them is most of the work.

| # | what | where it comes from | accumulates upstream? | re-runnable? |
|---|---|---|---|---|
| 1 | **Parameter variant** | `EachOf` / `Parameter` constants → `branch_params_batch` | **Yes** (`max_depth=20`) | Yes |
| 2 | **Code variant** | producing function's `function_hash` → `CodeVersion` | **No — one hop** | **No** |
| 3 | **Field factor** | dict/struct columns melted → `ColName` | n/a | n/a |

Only #1 and #2 carry `is_variant=True` and arm the `roles.validate` pooling
guard. #3 is an ordinary factor that merely defaults to `FACET`.

The asymmetry in the last two columns is the whole subject of this document.

## 2. The one-hop limitation

`variant_identity_batch` derives `CodeVersion` from
`producing_function_versions_batch` (`provenance_query.py:299`), which reads
**only the immediate producing invocation**. It does not walk the chain.
`branch_params_batch` does.

### The failure this permits

Edit `loadDelsysEMGOneFile`. Leave the downstream `bandpass_filter` alone.
Re-run **the whole pipeline**.

> The "whole pipeline" is load-bearing and an earlier draft of this section got
> it wrong. Re-running only the edited loader is not enough: the downstream step
> reads its input through the load path, which collapses to the latest record
> per variant group, so it would see one input and write one output. Two
> downstream records coexist only once the second layer runs again too — which
> is what "edit a loader and hit Run" actually does.

Identity is content-addressed (`provenance.py:18-23`):

```
output record_id = hash(type | schema_version | content_hash(data)
                        | invocation_id | output_num)
invocation_id    = hash(function_hash | as_table | distribute | sorted(bindings))
```

A new upstream `record_id` changes the downstream `invocation_id`, which changes
the downstream `record_id`. **Two `FilteredEMG` records now coexist.** Both
survive to the display path, which selects every non-excluded record of a type.

But both were produced by the *same* `bandpass_filter` hash. So in
`variant_identity_batch`, `_order_versions` over the type finds one distinct
hash → `len(ordered) < 2` → no `fn_version` → `attach_variants` emits no
`CodeVersion` column → **no variant factor exists** → `roles.validate`'s pooling
guard is never armed → the two rows plot as replicates of each other.

That is precisely the failure mode `attach_variants` was written to prevent —
"a figure that is wrong in a way that looks like data" — reached one hop
downstream of where the fix was applied. `function-version-variants.md` §
"Consequence 2" describes the guard being armed; it is armed only at the
producing layer.

> ✅ **Confirmed, then fixed.** `scistackplotdb/tests/test_variant_chain.py`
> first encoded this as two `xfail(strict=True)` tests; the user ran them
> 2026-09-08 and they failed as designed, which is what turned this section from
> a code-reading hypothesis into an observed bug. Stages 1–2 then closed it and
> the xfails became ordinary assertions.
>
> **Known unverified edge: glue nodes.** The chain walk reads
> `_invocation.function_name`/`function_hash` uniformly, so a glue invocation
> would contribute a chain entry named after its node chain (`glue_a > glue_b`).
> Since `code_version_ordinals` omits single-version functions, a stable glue
> chain costs nothing — but an *edited* one would surface as a code axis under
> that name. Whether that is desirable (glue is code) or should route through
> `glue_source` like other read paths is untested and undecided.

## 3. "What if code modification is my axis of change?"

The question that prompted this doc, and it exposes something bigger than the
one-hop gap.

**A body edit has two possible intents, and the stack can only represent one.**

| intent | meaning | what you want |
|---|---|---|
| **Supersession** (a fix) | v2 replaces v1; v1 was wrong | latest everywhere; v1 kept for traceability |
| **Variation** (a comparison) | v1 and v2 are rival methods | both coexist; both fan out downstream; version is a real factor |

Everything in the stack assumes supersession:

- `_find_record`'s latest-collapse (`database.py`, the `version_id == "latest"`
  branch) groups on `variant_key = (fn_name, branch_params, output_num,
  consumed_input_schema_ids)`. **`function_hash` is absent.** Two body versions
  land in one group; the newer wins. `load()` returns one.
- `_producing_variant_key` — constants only, by explicit docstring decision.
- `EachOf` varies inputs, constants and `where=` (`each_of.py:8-10`). **There is
  no axis over the callable.**

So today, code becomes an axis only *by accident* — you edit a body, re-run, and
two records exist that nothing was designed to tell apart. The 2026-09-06 work
made the display layer stop lying about that. It did not make it a supported
workflow.

### Why body-edit history is a poor experimental axis

Worth stating plainly, because "just make the supersession key hash-aware" looks
like the fix and is not.

`_invocation` stores `function_name` and `function_hash` and **no source text**
(`provenance.py:406-413`). (`glue.py`'s `source_text` is for glue nodes — a
different thing.) The consequence:

**A superseded code version is a fossil.** You can plot it. You cannot re-run
it, cannot extend it to a new subject, cannot reproduce it. The hash identifies
it; nothing recovers it.

That is a fundamental asymmetry with parameter variants, where both alternatives
stay executable forever. A "v1 vs v2" figure built from edit history compares a
live method against a dead one, and it silently stops being extensible the
moment a new subject arrives — v2 covers them, v1 never will.

### Two situations, and they need different answers

An earlier draft of this document recommended `EachOf` over callables
(`EachOf(bandpass_butterworth, bandpass_fir)`) as *the* answer. That was
half-wrong, and the half it gets wrong is the common half. Applied to iterative
method development it reduces to: **copy-paste your function, edit the copy,
rename it, register both.** That is manual duplication of something the database
already records, it forks the DAG node, and the two copies drift the moment a
bug is fixed in one of them.

Distinguish:

| situation | example | right answer |
|---|---|---|
| **Parallel methods**, maintained long-term | Butterworth vs FIR | Two named functions; `EachOf` over callables. Genuinely different code with different names. |
| **Iterative refinement** of one method | "I changed how the cutoff is chosen — did it help?" | **One function, many recorded versions.** Copy-paste is the wrong ask. |

Only the first is served by naming. The second is the case that motivated this
document, and for it the axis should be **versions of a single function**, drawn
from the history the database already holds:

```python
for_each(EachOf(Version("bandpass_filter", "v1"),
                Version("bandpass_filter", "v3")), ...)
```

One DAG node, no duplication, axis explicit rather than accidental.

### The actual blocker is that source is discarded

Nothing about §3's fossil problem is fundamental. `_invocation` stores
`function_hash` and no source (`provenance.py:406-413`) — but the source text is
**in hand at the moment the hash is computed**, in both languages:

- MATLAB: `compute_matlab_function_hash(source_text, ...)`
  (`scimatlab/bridge.py:1444`) is literally handed the text.
- Python: `compute_function_hash` (`scilineage/hashing.py:180`) AST-hashes the
  live callable, so `inspect.getsource` is available at that point.

And there is precedent for persisting it — `GlueSpec.source_text`
(`glue.py:144`) already stores source for glue nodes.

So a `_function_source` table keyed by `function_hash` is **additive**: no
existing identity changes, because the hash is already the key. It converts a
superseded version from a fossil into something inspectable and — with the
caveat below — re-runnable. That, not naming, is what makes version-as-an-axis
real.

> ✅ **Built for Python 2026-09-08** (Stage 3, tests unrun). One row per unit,
> keyed `(function_hash, unit_name)`. The hash-walk and the source-walk are the
> **same traversal** — `_hash_source` gained an optional `collect` dict rather
> than gaining a twin that could drift from it. The write site verifies the
> derived hash equals the stored one and refuses to write on mismatch.
>
> ⚠️ **MATLAB captures nothing yet**: `MatlabLineageFcn` holds the digest, not
> the text. The `source_text` duck-typed hook exists; nothing supplies it. Read
> an empty `units` as "not captured", never "no code" — old records legitimately
> have none, and offering to re-run a version whose source was never stored is
> the one thing this API must not enable.

> ⚠️ **"Re-runnable" is narrower than it sounds.** Python's
> `compute_function_hash` is *recursive over the AST*: a function's hash depends
> on every user-defined function it transitively calls
> (`scilineage/hashing.py:185-188`). So recovering v1 needs the **closure** of
> user callees at that version, not one string. The set is well-defined — the
> hasher already walks it — but this is meaningfully bigger than "store a
> string", and it is still not a hermetic environment snapshot (library
> versions, MATLAB toolboxes and data on disk are all out of scope). Treat
> re-runnability as best-effort and design the UI to say so when a version
> cannot be reconstituted.
>
> ⚠️ **The closure resolves through `__globals__` only.**
> `_resolve_call_target` (`scilineage/hashing.py:96-110`) looks a call target up
> in the calling function's module globals, so a helper defined inside an
> enclosing function's scope is invisible: it contributes neither to the hash
> nor to the captured source. Long-standing and listed as accepted in
> `.claude/recursive-function-hashing.md`, but the versioning consequence is
> worth stating outright — **editing a nested helper does not re-version its
> caller, so a node whose only change is inside one will not redden.**
> Module-level helpers, which is what pipeline code normally uses, are fine.
> Pinned by `scidb/tests/test_function_source.py::
> TestFunctionSourcesFor::test_a_locally_defined_callee_is_invisible`.

Whatever is decided, **do not make `function_hash` part of the supersession
key.** That would destabilise `load()`, node state and `find_record_id`
simultaneously — see `project_latest_record_selection_future_issue`, already
open on exactly that seam. Supersession stays the default for a body edit;
variation becomes something the user *declares*, on top of recorded history.

`CodeVersion` meanwhile retains a narrower and honest job: *"records here were
made by code that no longer matches; here is which."* Traceability, consistent
with `feedback_defer_content_staleness`.

## 4. Does accumulation still matter? Yes.

Even with §3 done, `CodeVersion` should accumulate up the chain, for the §2
correctness hole alone: two downstream records that differ only by upstream code
version must not overplot as replicates.

The objection to accumulation is combinatorial — every downstream variable in a
mature project sprouts a version column for every function ever edited anywhere
upstream, and the default figure explodes.

**That cost lands on the default, not on the model, and the default already has
an answer.** `roles.default_spec` opens on `VariantPolicy.PIN` when the source
supplies `table.default_pin` (`roles.py:216`). If `CodeIsLatest` is made
**chain-aware** — "this record's entire upstream version chain is the newest
chain present at its location" — then by default every accumulated version
column collapses to one level and costs nothing. The complexity appears only
when the user unpins.

That is the property to preserve above all others:

> **However many layers generate versions, the default stays one checkbox.**

`CodeIsLatest` must remain resolved **per schema location** when made
chain-aware, for the reason already documented at `load.py:37-46`: a type-wide
"latest" silently drops every subject never re-run under the newest code.

## 5. Selecting from a combinatorial variant space

The scaling question: several layers each generating variants gives a product.
How does a user pick one, or a family, or all?

### Name the coordinates, not the combinations

A flat dropdown of variants does not survive the product — three layers × three
levels is 27 concatenated labels, which is the remember-the-names problem
wearing a different hat. It works for one variant factor and collapses at two.

**The architecture already has the right primitive: variants are *columns*, not
opaque names.** So all three questions are one control:

| question | control |
|---|---|
| just one variant | every variant factor pinned to one level |
| all where x=1, y=2 | those two pinned, the rest left open |
| all together | none pinned; assign to color / facet / iterate |

And the coordinate names come free from the DAG — a record is not "variant 17",
it is `loadEMG v2 · bandpass(low_hz=20) · normalize v1`. Nothing must be
memorised because nothing was invented: the columns are named after nodes the
user already knows.

This is why §4's accumulation is a prerequisite for the UI and not merely a
correctness fix. One column per upstream function *is* the picker's data model.

### The DAG canvas is the right idiom — and half of it already exists

The coordinate space maps onto node kinds the canvas already draws
(`frontend/src/components/DAG/`): parameter variants are `ParameterNode`, code
variants are `FunctionNode`. Both sources of variance already have a visible
home, which is unusual luck.

More than that: **`ParameterNode` already implements this exact interaction.**
Its docstring describes "a checkboxed list of the distinct values it has taken",
with per-value selection that persists. The gesture the variant picker needs is
already built and already familiar to the user.

`FunctionNode` has **no version display whatsoever**. That asymmetry — one
variant source with a mature selection UI, the other with none — is the real
shape of the gap, and it is a much smaller gap than "design a variant picker".

> ⚠️ **The trap: those checkboxes are execution state, not display state.**
> Unchecking a `ParameterNode` value "excludes unchecked values from multi-value
> fan-outs" — it changes **what future runs execute**. Variant selection for a
> plot must change **what is shown**. Binding a plot's variant picker to the
> canvas checkboxes would make a display action silently alter the run
> configuration, which is far worse than the two-sources-of-truth problem below.
>
> Same gesture, same visual language, **two different scopes**. Keep the state
> separate even though the widget looks identical.

### Placement

The picker belongs **inside Plot Studio**, not in a separate view. The selection
is part of `PlotSpec` and must round-trip through `codegen`; separate state is
two sources of truth, and preview/export divergence is exactly what
`scistackplotdb/tests/test_fanout_parity.py` exists to catch.

So: reuse the canvas *idiom*, not the canvas *instance*. What Plot Studio wants
is a **plot-scoped variant subgraph** — the induced subgraph of the pipeline DAG
on nodes that (a) are ancestors of the plotted measure and (b) contribute more
than one level. That is typically 2–5 nodes, which is what makes it usable; the
full project DAG is not, because most of it is irrelevant to any one figure and
the user would have to know which parts to ignore.

### What the canvas is bad at

A canvas shows **coordinates**, not the **product**. Two nodes on screen can
mean twenty-four combinations, and the thing that actually bites the user is the
figure exploding — which neither a canvas nor a factor list reveals until it
renders.

Pair the picker with a running readout: *"4 of 24 variant combinations · 12
panels"*. That single line is arguably worth more than the canvas, and it is far
cheaper — `plan_layout` already computes panel counts
(`facet-layout-grid.md`), so the number is in hand before rendering.

## 6. Concrete changes, in dependency order

Nothing below is built. Rough order; each is independently useful.

1. **Regression test for §2** — the two-layer overplot. Before any fix.
2. **`code_versions_batch` in scidb**, beside `branch_params_batch`, walking the
   same chain: one column per *upstream function* holding more than one version,
   named after the function. Closes §2 and supplies the picker's data model.
   - Scope ordinals **per function**, not per variable type as `CodeVersion`
     does today. A function's v1/v2/v3 is coherent globally and sidesteps the
     scope argument `function-version-variants.md` had to work through.
   - Belongs in scidb: "what makes two records distinct" is a scidb question
     (CLAUDE.md NOTE 3), and duplicating the chain walk into scistackplotdb is
     the `feedback_avoid_scifor_scidb_duplication` mistake one layer up.
3. **Chain-aware `CodeIsLatest`**, still per schema location. Keeps the default
   at one checkbox (§4).
4. **`_function_source` table keyed by `function_hash`** (§3) — additive, no
   identity changes, source already in hand at hash time. The prerequisite for
   any version-as-an-axis work, and independently valuable: it makes "what did
   the code that produced this record actually say?" answerable at all.
   - Python needs the *callee closure*, not one function — see the warning
     in §3.
5. **A version axis** — `EachOf(Version("fn", "v1"), ...)` over recorded
   versions of one function (§3). Requires 4 to be re-runnable; without 4 it
   can still *select among already-computed* records, which is most of the
   plotting value.
6. **`EachOf` over callables** — the narrower parallel-methods case (§3). Cheap
   and independent of 4–5.
7. **`pinned_variant: dict[str, Any]` → `dict[str, Any | list[Any]]`.**
   `reduce._apply_variant_policy` already builds a per-column equality mask;
   widening to level *sets* turns "pin one" into "select a subcube".
8. **Factor ordering** — user request, cheap. `source.py:247-249` puts schema
   keys before variants; `load.py` appends `CodeVersion` *after* the branch
   params (`keys.append(column)`, ~line 259). Flip both: code version first,
   then parameter variants, then schema keys.
9. **Always emit `CodeVersion`**, even at one level, so the control has a
   permanent home instead of materialising only when something goes
   interesting. Safe: `default_roles` and `validate` both guard on
   `len(f.levels) > 1`, so a single-level variant factor sits in `FREE`
   harmlessly.
10. **A version list on `FunctionNode`**, mirroring `ParameterNode`'s existing
    checkboxed value list (§5). Closes the asymmetry between the two variant
    sources and reuses an interaction the user already knows.
11. **The picker UI** — a plot-scoped variant subgraph in Plot Studio (§5),
    borrowing the canvas idiom but bound to `PlotSpec`, **never** to the
    canvas's execution-scoped checkboxes. Labelled from
    `variant_identity_batch`'s `fn_name` / `fn_version` / `is_latest` /
    `saved_at`. Where a version's source was never captured (records predating
    item 4), say so rather than offering a re-run that cannot happen.
12. **The combination readout** — *"4 of 24 combinations · 12 panels"*. Cheap,
    high value, and independent of everything above; `plan_layout` already
    computes the panel count.

### Loose end

The two-measure join drops the latest flag and falls back to showing every
version (`source.py:204-213`, and `hierarchy.join_frames` keeps only levels,
values and variant columns). Tolerable at one hop; noticeably worse with an
accumulated chain. Needs fixing as part of 2–3.

## 7. Defaults worth keeping distinct

Code variants and parameter variants deserve different defaults, because their
meanings differ:

- **Code variant** — an improvement axis. You almost always want latest. → pin.
- **Parameter variant** — an experimental condition. You often want all of them
  separated. → facet / color.

This is roughly what happens today by accident (`default_roles` gives the first
variant `COLOR` and the rest `FACET`; `default_spec` pins when `default_pin`
exists). Making it explicit and ordered is the improvement.

## 8. Open questions

- ~~**Does a version axis interact correctly with node state?**~~ **Examined
  2026-09-08 — and the answer is worse than the question assumed.** The bug is
  not hypothetical and not specific to code versions: **a node pinned to a
  branch-param variant already reads needs-run permanently.**
  `config_from_inputs` drops the `Variant` (it is not a `type`, so it never
  reaches `input_types`) and `_predict_config_invocations` enumerates every
  current record of the input type via `_current_records_by_schema`. So the node
  is predicted to owe invocations over the variant it was told to skip.
  Characterised by `scidb/tests/test_variant_pin_node_state.py`.

  The remaining decision is genuinely a product one, recorded in the plan: from
  the graph alone, *"pinned to low_hz=20"* and *"has not run low_hz=50 yet"* are
  indistinguishable. Resolving it means persisting the declaration, inferring it
  from consistency, or honouring only a live one.
- **`Variant` is the axis primitive, not a new `Version` type.** `scidb.Variant`
  already pins an input to a variant at load time and already documents
  `EachOf(Variant(...), Variant(...))`. Since Stages 1–2 put `Code:<fn>` beside
  branch params as a variant column, a code pin belongs on the same wrapper —
  one concept, one filter.
- **How does a version axis render in the GUI DAG?** One node showing "v1 + v3
  selected", or two nodes? Affects the picker's shape.
- **When is source captured?** Write-time (every invocation) is simplest and
  matches where the text already is. Capturing it lazily on first edit would
  miss the baseline version entirely.
- **Does the callee closure get stored per-callee or as a flattened blob?**
  Per-callee dedupes across versions and matches how the hasher walks; a blob is
  simpler and never has to resolve a partial closure. Unexamined.
- **What does the UI do with an unreconstitutable version?** Records written
  before source capture exists will never have it. That state is permanent, not
  transient, and needs a real answer rather than a spinner.
- **Should `CodeVersion` levels be pruned to versions that actually differ in
  output?** Two hashes producing identical data are a distinction without a
  difference on a figure. Probably not worth it — it would require content
  comparison on every load — but it is the obvious next question once chains
  accumulate.
- **MATLAB parity.** `EachOf` is mirrored in `+scifor/EachOf.m`; a callable
  alternative would need a MATLAB function-handle equivalent, and
  `project_matlab_classmethod_dispatch` is the relevant trap.

## Ground truth

Behavior is defined by tests, not by this prose. **This document describes
nothing that exists**, so it has no tests. The code it *describes* is covered by
the table in `function-version-variants.md` § Ground truth.

⚠️ Never run two packages' tests in one pytest invocation
(`project_pytest_one_package_at_a_time`).
