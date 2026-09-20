# Free-Code "Glue" Nodes

> **Status: IMPLEMENTED across all six stages, 2026-09-03. Amended
> 2026-09-10** after the first real GUI test, which found four defects: a glue
> chain could only start from a *variable* (§4a), a constant-fed chain had no
> application site (§1), the MATLAB run command never emitted `glue` at all,
> and `for_each`'s EachOf recursion dropped `glue=` (§1). All fixed;
> **uncommitted, and the Python test suites have not been run.**
> Plan of record: `.claude/plan-free-code-glue-nodes-26-09-03.md` (decisions
> D1–D6), which carries the per-stage status and the two places the
> implementation deliberately departs from the plan. If you are reading this
> and the code disagrees, **the code wins**. (See
> `gui-export-to-plain-python.md`, which sat at "not yet built" long after the
> feature shipped, for why this warning is here.)
>
> Entry points: `scidb/glue.py` (the contract and the fusion),
> `scidb.provenance.compute_glue_record_id` (§2's virtual record),
> `scidb.roles.function_role` (§0), `scistack_gui/services/glue_service.py`
> and `frontend/src/components/Sidebar/GlueSettingsPanel.tsx` (the panel).

## The question this answers

Function nodes are project-agnostic computations. Between them, real projects
need one-off reshaping: rename a column, change a dtype, drop or append a
column, restructure a dict. Today that forces a full pipeline function with a
declared output variable type, so the database fills with data that is purely
transitional.

A **glue node** is free-form user code — a `glue_`-prefixed function in a
GUI-owned file — that runs *in memory* between a variable and the function
consuming it, and is never saved.

| # | Decision | Choice |
|---|---|---|
| D1 | Where the code lives | A real `.py`/`.m` file per node in `glue_dir` (default `src/scistack_glue/`) |
| D1a | How it is edited | A Monaco panel inside the GUI; the file is persistence, not a place the user navigates to |
| D2 | Graph attachment | A standalone node wired by edges |
| D3 | Output persistence | Not saved; fused in-memory into the consuming function's call |
| D4 | Iteration | The whole loaded table by default; per-schema-key is an opt-in toggle |
| D5 | Runnability | No run button, no own run state |
| D6 | Sidebar presentation | A function-role filter over the Functions list — Process / Plots / Stats / Glue / All, defaulting to Process |

## 0. Function role

The naming convention is the existing one: `glue_`, alongside `plot_`/`stat_`.
It is checked on the function *name* (`foreach.py:3708-3710`,
`inspect/report.py:183`), and names cross the MATLAB bridge unchanged — which is
why it needs no decorator (Python-only) and no classdef (MATLAB-only).

A fourth prefix makes the previously-implicit concept worth naming. **Function
role** is a first-class classification:

| Role | Identified by |
|---|---|
| `process` | **No recognized prefix** — the default bucket, the ordinary pipeline step |
| `plot` | `plot_` |
| `stat` | `stat_` — **singular**. A function named `stats_summary` is a `process` function |
| `glue` | `glue_` |

**The classifier lives in `scidb`, not the GUI**:
`scidb.discover.function_role(name)`. These prefixes already drive execution
behaviour inside scidb, so the GUI must not hold a second copy of the strings.
It also collapses the three existing hardcoded `startswith("plot_")` /
`startswith("stat_")` sites into one call, so a fifth role later touches one
function instead of four.

The **dropdown** that consumes it is pure display and lives in the GUI layer
(CLAUDE.md NOTE 3) — the same reasoning that put
`_pipeline_parameter_value_groups` there. It filters the sidebar list only,
never the canvas, which always shows whatever is wired.

Two details that make the default safe. Filtering to Process by default is a
behaviour change from today's show-everything list, so the dropdown **shows a
count per role** (`Process (12) · Plots (3) · Stats (2) · Glue (7)`) — nothing
becomes invisible, only collapsed. And creating a function of a given role
selects that role, so a just-created node is never filtered out of view.

This is what keeps glue from polluting the Functions list even when a project
accumulates dozens of one-line reshapers.

---

## 1. The fusion point, and why it is the only good one

Glue applies to the **bulk-loaded input table for one parameter of the consuming
function**, between Step 10 (`_convert_inputs` / `_load_var_type_all`) and
Step 11 (`__record_id` → `__rid_{param}` renaming) of `scidb/foreach.py`.

Everything good about this feature follows from that one placement:

- The loaded table already carries schema-key columns, so the consumer's
  per-combo slicing (Steps 12/15/17) keeps working with **no changes**.
- It is *before* rid renaming and combo expansion, so variant tracking, the save
  path and `branch_params` inheritance are untouched.
- **MATLAB gets Python glue for free.** `+scidb/for_each.m` delegates all
  pre-loop work to `scimatlab.bridge.for_each_prepare` — its own header says
  "MATLAB owns only step 2 and the bridge plumbing. All correctness-critical
  work is Python's." Anything done inside `_convert_inputs` therefore applies
  identically to a MATLAB run.

**MATLAB-authored glue is the one thing that cannot live there**, because a `.m`
function cannot execute inside Python's prepare step. It is applied instead in
`+scidb/for_each.m`, after `for_each_prepare` returns and before the loop
begins, on the table MATLAB already rebuilds via
`for_each_describe_loaded_input`. `prepare` therefore returns a `glue_chains`
entry telling MATLAB which params carry MATLAB glue.

This is also why glue language must match the run language: the alternative is
two application sites whose ordering semantics have to be kept in agreement for
a mixed chain, for no user benefit.

### The one exception: a constant-fed parameter

A `Parameter`-fed parameter has **no loaded table at all** — a `scidb.Parameter`
*is* a `scifor.EachOf`, a fan-out axis, and by the time prepare runs,
`for_each`'s Step 1 has already expanded it into one recursive call per
concrete value. Its glue is therefore applied earlier still, in
`scidb.glue.apply_constant_glue`, **before Step 8 builds the version keys**.

That placement is not a convenience, it is the identity story. The value that
lands in `__constants` *is* the glued value, so an edited glue body changes the
constant's content hash — one of the bindings `skip_computed` compares — and
downstream results invalidate **with no virtual glue record at all**. §2's
staleness hole is closed here rather than merely warned about.

The language rule inverts with it, for the same reason it exists: a glue node
runs where its input exists, and prepare is Python on **both** run paths. So a
constant-fed chain must be **Python glue even in a MATLAB run**, and MATLAB glue
on one is refused. Applying it per-combo on the MATLAB side instead would record
the *pre*-glue constant and reintroduce exactly the silent staleness this
placement removes.

That is also why Python glue can cross into a MATLAB run at all: the GUI emits
the chain element as `struct('name', …, 'language', 'python', 'source_file', …)`
rather than a handle (`+scidb/for_each.m:build_glue_chains`), and
`scimatlab.bridge` loads the body with `scidb.glue.resolve_python_glue` —
MATLAB's interpreter has no function registry, so the *path* is what travels.

**A multi-valued Parameter needs no special handling.** The fan-out and the glue
compose without either knowing about the other: Step 1 makes one recursive call
per value, and each call glues its own. That recursion did **not** forward
`glue=` until 2026-09-10, so any glue on a function with a multi-valued
Parameter (or a multi-type variable input) was silently dropped in every
alternative — reshaping nothing, writing no virtual record, and reporting
success.

The D4 per-schema-key toggle uses a different site again — the existing
`PerComboLoader` function wrapper (Step 16), applied to the already-sliced value
just before `fn` is called. Same code, different hook.

---

## 2. The central trap: there are TWO identity systems

**This is the thing to read this document for.** Folding a glue's hash into
`version_keys` looks sufficient, is easy to do, and is wrong on its own.

`scidb` decides "has this already been computed?" in two independent places:

| | `version_keys` | The provenance graph |
|---|---|---|
| Where | `_record_metadata.version_keys`, built by `ForEachConfig.to_version_keys()` | `_invocation`, `_invocation_input`, `_record` |
| Drives | `load(version="latest")` partitioning; which variant group a record belongs to | **`skip_computed` and node state** |
| Key | `__fn`, `__fn_hash`, `__inputs`, `__constants`, … | `invocation_id = sha16(fn_hash \| as_table \| distribute \| sorted(bindings))` |

`skip_computed` compares `stored_invocation_signature` to the current inputs
**binding by binding** — function hash, each input `record_id` + selector,
constant content hashes. It never reads `version_keys`.

So if glue only enters `version_keys`, then editing your column-rename produces:
the same `fn_hash`, the same input `record_id`, therefore the same
`invocation_id`, therefore **`skip_computed` skips and every downstream record
stays green and stale.** The user's edit silently does nothing. This is the
single highest-consequence failure mode in the feature.

### The resolution: the data is not saved, but the provenance node is

For each input record flowing through a glue chain, write a **virtual record**:
a `_record` row with `type = '__glue__'` and the same `schema_id`, plus one
`_invocation` for the glue and its two edge rows.

```
virtual_rid = sha16(glue_chain_hash | input_record_id | input_set_signature)
```

`input_set_signature` is `sha16` of the sorted full input rid set. It is there
because a whole-table glue may legitimately read across rows, so its result
depends on the whole input set — and it makes a *growing* input set force a
recompute, closing the same hole that was already closed for aggregation
`skip_computed`.

There is **no `_record_metadata` row and no data-table row.** The virtual record
is not loadable, never appears in `scidb report`, and must be excluded from the
`"latest"` collapse and from `_current_records_by_schema`.

The consuming function's `__graph_var_bindings` then point at the virtual rid
instead of the raw one. Every existing mechanism works through **one extra graph
hop with no special-casing**: `invocation_id` changes when the glue changes,
`skip_computed` staleness detects it, `upstream_provenance`'s BFS traverses it,
and the pipeline reads honestly as `RawEMG → glue_drop_baseline → analyze_emg`.

Cost: ~4 metadata rows per (glue, input record), and zero data bytes. That is
what lets "the output is not saved" coexist with the project's audit story.

`version_keys` still gets a `__glue` entry (`{param: [(name, hash), …]}`), and
it follows the existing `__fn`/`__fn_hash` split exactly: glue **names**
contribute to `call_id`, glue **hashes** do not. A different glue chain is a
different call site; an edited glue body is a new version at the same call site.

### Where the virtual record cannot be written

Two input shapes have **no record id at the fusion point**, so no virtual glue
record can be written for them. The glue still reshapes the data correctly;
what is lost is the invalidation — editing its body will not make downstream
results recompute.

| Input | Why |
|---|---|
| `Merge` | Structural. A Merge stays a set of *constituent* frames until scifor joins them per combo, and the constituent loader strips every `__`-prefixed column on the way in — so there is neither a single table nor a rid here |
| `PerComboLoader` | The data is not loaded yet; there is nothing to reshape or identify until the loop |

Both are applied **per combo** instead (on the already-sliced value, where the
merged frame does exist), and both log a WARNING naming the reason. That
warning is asserted by a test — this is precisely the silent-staleness failure
the virtual record exists to prevent, so it must not quietly become silent.

If this matters for a real pipeline, the fix is to make the Merge itself a
saved step, or to glue the constituents individually before merging.

### The risk this leaves

`expected_invocations_for_function` predicts invocation ids live, and is "safe
because it shares the one hash fn." Virtual rids must be *predicted* exactly as
the save path *computes* them, or nodes go falsely red. This is the highest-risk
part of the implementation.

---

## 3. The row-preservation contract

**A glue node may change the column space. It may not change the row set.**

Add, drop, rename, retype columns; restructure cell values — all fine. Filter
rows, aggregate, explode, re-index — refused, with `GlueRowsChangedError` /
`scidb:glue:rowsChanged` naming the glue, the param and the before/after counts.

This is a design line, not a limitation to apologise for:

- it is exactly the use case (rename/retype/drop/append);
- it makes re-attaching the hidden `__record_id` column **provably** safe, so
  per-row provenance survives glue without the user ever knowing it exists;
- anything that changes the row set is a real computation with a real result —
  which is what an ordinary function node with a saved output is for.

### Column visibility rules

| Columns | Treatment | Why |
|---|---|---|
| `__record_id`, `__branch_params` | **Hidden** from the glue, re-attached by index afterwards | Internal bookkeeping; the user must never have to think about it |
| Schema keys (`subject`, …) | **Visible but protected** — verified present and value-identical after the call | Visible because whole-table glue may need `df.groupby("subject")`; protected because altering one silently breaks slicing |

The protection matters more than it looks. Step 5 stringifies schema values so
they match the loaded table's stringified columns; an int/str change on a schema
column therefore makes *every* combo filter miss, and the run "succeeds" having
produced nothing — the documented `succeeds while doing no work` failure mode.
Violations raise `scidb:glue:schemaKeysAltered` naming the key.

Scalar (non-DataFrame) inputs skip the row check; there are no rows.

---

## 4. What the glue actually receives

All glue is a **function definition** — never a bare statement body with magic
variable names. It is a real file found by the ordinary scanner, and a
node wired by edges needs named parameters for edges to target.

```python
def glue_drop_baseline(emg):
    return emg.drop(columns=["baseline"])
```

```matlab
function out = glue_drop_baseline(emg)
    out = removevars(emg, "baseline");
end
```

**Parameter names are irrelevant to binding.** Edges bind inputs
(`function-input-resolution.md`, the rule since 2026-08-25), so a param called
`emg` can be fed by any variable dropped on its `in__emg` handle. N edges → N
params. Exactly **one** return value: glue feeds a single parameter of a single
consumer, so a second output would have nothing to bind to.

### 4a. What may feed a glue node

The head of a chain may be **any bindable source**, and which one it is decides
where scidb applies the chain:

| Head | Applied | Identity |
|---|---|---|
| Variable | The Step-10/11 fusion point (§1) | Virtual glue record (§2) |
| Parameter / plain constant | `apply_constant_glue`, before the version keys | The glued value in `__constants` |
| PathInput | Refused (`GlueUnsupportedInputError`) | — |

`edge_resolver.resolve_source_binding` is the single place that decides which,
shared with the direct-edge path so the two cannot disagree. **They did.** Until
2026-09-10 `resolve_glue_chain` resolved its head with `node_id_to_var_label`,
which returns `None` for anything that is not a `variableNode` — so a
`Parameter → glue → fn` chain resolved to nothing, `resolve_function_edges`
skipped the binding, and the parameter **vanished from the function's inputs
entirely**. In a MATLAB run the remaining arguments then bound by POSITION:
`filterDelsys(loaded_data, config, Fs)` ran as `filterDelsys(loaded_data, Fs)`.

The GUI now binds the head honestly and lets **scidb** decide what is runnable.
That is the important half: a refusal names the problem, whereas a dropped
binding is indistinguishable from a forgotten edge.

**The column names are the genuinely non-obvious part**, because they depend on
how the variable stores its data (`_load_var_type_all` assembly modes):

| Variable's stored data | Columns the glue sees |
|---|---|
| A DataFrame | **The user's own DataFrame columns, under their own names.** There is no column named after the class |
| A scalar or array | Schema keys plus **one data column named after the class** — `df["RawEMG"]`, from `view_name()` |

Since this is not visible from the canvas, the **editing panel** shows the wired
input's real column list beside the editor, read from `_variables.dtype` (`mode:
single_column | multi_column | dataframe` plus column types). Deliberately not
scaffolded into the file as a comment: a comment goes stale on rewire, the panel
re-reads on every open.

---

## 5. A glue node is not a step

Glue has no run button and no run state (D5). It is transient by construction,
so a standalone run would produce nothing and a state badge would describe
nothing.

The rule that must be enforced in code: **`build_backend_pipeline` must never
emit a `StepSpec` for a glue node.** Glue is a property of the consuming step's
*input binding* — a `glue_chains` entry in the target dict alongside
`path_input_params` / `parameter_params` — not a node in the compiled pipeline.

```
Run analyze_emg:
    load RawEMG                     (Step 10, bulk)
    df = glue_drop_baseline(df)     (fusion, in-memory, not saved)
    analyze_emg(df)                 (Steps 11-19, saved as normal)
```

Whole-pipeline runs topologically order **function nodes only**; each glue node
collapses into the binding of whatever it feeds. A glue node feeding nothing is
never executed — that is not an error and must not red the canvas.

Two places currently assume "a node on the canvas is a step," and both need to
learn otherwise: `execution_service.build_backend_pipeline` /
`derive_fn_targets`, and `api/run.py`'s per-node Run route (which must *refuse*
a glue node id rather than compiling an empty pipeline and reporting a
successful run that did nothing).

---

## 6. Diagnostics

The predicted confusing failure is **"I edited my glue and nothing recomputed."**
The log trail is designed to make that a two-minute read:

1. `[glue] '<param>': chain = [glue_a, glue_b] (hashes …)` — INFO, every run.
2. `[glue] '<param>': applied <name> (+col, -col, dtype changes) in Ns` — DEBUG.
3. `[glue] virtual record <rid> for input <rid> (chain <hash>)` — DEBUG.
4. `[skip] '<fn>': binding '<param>' changed (<old rid> → <new rid>)` — the line
   that proves a glue edit propagated into the invocation graph.
5. `[glue] '<param>': no glue chain` — DEBUG, when wiring was expected to carry
   one; catches an edge drawn to the wrong handle.
6. `[edge_resolver] parameter '<p>' is fed through glue [<chain>] from <kind>
   '<ref>'` — INFO, at graph build and before every run. The **kind** is the
   thing to read: it decides which of §4a's three application sites applies.
7. `[glue] '<param>': applied N node(s) to the constant value` — INFO, the
   constant-fed path, stating that the glued value is what gets recorded.
8. `[edge_resolver] glue node '<g>' has N incoming edges [<id>:<handle>…]` —
   WARNING listing the **handles**, because the usual cause is a renamed glue
   parameter leaving a stale edge that is still the one followed.

---

## 7. Deliberately out of scope

- **Row-changing glue** — use a function node with a saved output.
- **Cross-language glue chains** (§1).
- **Glue on a PathInput-fed param** — a `PathInput` is always a
  `PerComboLoader`, so no table exists at the fusion point. Refused.
- **MATLAB glue on a constant-fed param** — §1's exception; refused, with the
  reason and the alternatives in the message. (Glue on a constant-fed param is
  otherwise *supported*, in Python. It used to be neither supported nor
  refused, just silently dropped — see §4a.)
- **Glue → variable node wiring** — there is no saved output to land.
- **Persisting glue data** in any form. Only the provenance node is written.
- **Two storage forms** (a one-liner in GUI state *plus* files for multi-line
  glue). Two constructs for one concept is exactly the Constant/Sweep split that
  D6 deleted; it would fork the hashing and MATLAB paths permanently.

## A note on the writable surface

`glue_dir` **widens the GUI's writable surface** from `entities_file` alone to
`entities_file + glue_dir` — a deliberate amendment to
`entity-editability-model.md`'s confinement rule, which said GUI writes are
confined to the entities file so the GUI never edits a hand-written module. The
spirit is preserved: still one designated, GUI-owned location, still never a
user's own module. But it is a change to a stated contract, not an oversight.

`glue_dir` must be threaded through **all four `_render_scistack_toml` call
sites** or it is silently dropped on the next Paths-popup save — the documented
trap in `code-discovery-categories.md`.

## See also

- `.claude/plan-free-code-glue-nodes-26-09-03.md` — the plan, with stages,
  tests and open risks.
- `scidb-for-each-internals.md` — Steps 10/11/12/16/19, the fusion point and
  everything it must not disturb.
- `database-model.md` — the canonical provenance schema (`archive/bipartite-provenance.md`
  is the older as-built note and is partly stale).
- `function-input-resolution.md` — the edges-only binding rule glue inherits.
- `entity-editability-model.md` — the confinement rule this feature amends.
- `code-discovery-categories.md` — how the six existing kinds are discovered;
  glue would be a seventh, classified by name prefix in `scidb.discover`.
- `matlab-run-database-ownership.md`, `matlab-path-resolution.md` — the MATLAB
  execution constraints behind the language rule in §1.
