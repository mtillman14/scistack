# Computation Caching

<!-- Ground truth (tests win over prose). Verified against:
     scihist/tests/test_cache_hit.py (reloaded lineage var -> cache hit, fn not re-called;
       call_count stays 1 across reload; chained intermediates also hit);
     scihist/tests/test_skip_computed.py (skip_computed default; unchanged->skip;
       input change->recompute only affected combo; scalar/dict input change->recompute;
       same content->skip; different __fn -> new variant runs (not skip/recompute);
       constants factor=2/3 independent variants each skip; new variant computes;
       output saved without lineage -> always recompute; pipeline propagation);
     scihist/tests/test_state.py::test_function_hash_change_stale_via_lineage (a function's
       OWN code change -> stale via check_node_state);
     scihist/src/scihist/database.py:30 (scihist wires the scidb database as the
       scilineage cache backend via configure_backend);
     scidb/tests/test_call_id.py (call_id disambiguates same fn across call sites).
     Do NOT restate the deferred content-staleness internals — see node-states.md and
     the project's content-staleness decision; describe currency at the node-state level. -->

Caching in SciStack means **identity-based reuse**: a computation is identified by
*what it is*, so if that identity already has a result, the work is reused instead
of repeated. Nothing is recomputed unless something it depends on actually
changed.

## The identity of a computation

A computation's identity is built from three things:

- the **function**, identified by a hash of its compiled bytecode (reformatting
  doesn't change it — see [Versioning & Content Hashing](hashing.md));
- its **variable inputs**, identified by their content-addressed `record_id`s
  (see [Variables & Storage](variables.md));
- its **constants**, which act as *variant discriminators* (below).

Combined, these form the **lineage hash** that [lineage](lineage.md) computes.
Two calls with the same identity are the same computation; a difference in any
part is a different computation with its own cache entry.

## Two places caching happens

### 1. Lineage cache hits — per function call

Re-running a `@lineage_fcn` with the same inputs returns the stored result
*without executing the function again*. Crucially, this holds **across save and
reload**: a variable saved from a tracked result keeps its lineage hash, so
feeding the reloaded variable back into the function is recognized as the same
computation:

```python
@lineage_fcn
def double(x):
    ...  # call_count incremented inside

ArrayValue.save(np.array([1, 2, 3]), subject=1)
result1 = double(ArrayValue.load(subject=1))   # executes — call_count == 1
save(ScalarValue, result1, subject=1, trial=1)

reloaded = ArrayValue.load(subject=1)
result2  = double(reloaded)                    # cache hit — call_count STILL 1
```

This works because `scihist` registers the `scidb` database as the lineage cache
backend, so lineage hashes are looked up against everything already stored (see
[Internals — scicanonicalhash](../internals/scicanonicalhash.md)).

### 2. `skip_computed` — per combo in batch processing

`scihist.for_each` runs `skip_computed=True` by default. Before iterating, it
filters out every combination whose output already exists and is current, so your
function is never called for already-done work. Each run reports `[skip]` and
`[recompute]` lines so the decision is visible:

```python
for_each(double, inputs={"x": RawSignal}, outputs=[Filtered],
         subject=[1], trial=[1])   # first run: computes
for_each(double, inputs={"x": RawSignal}, outputs=[Filtered],
         subject=[1], trial=[1])   # second run: skipped, function not called
```

Pass `skip_computed=False` to force every combo to run.

## What triggers recomputation

The behavior below is exactly what the test suite pins down:

- **An input changed → that combo recomputes.** Re-saving an input with different
  data gives it a new `record_id`; only the combos using the changed input
  recompute, and unchanged combos still skip. This is true for array, scalar, and
  dict-of-arrays inputs — sameness is judged by *content*, not by re-saving.
- **Nothing changed → skip.** Re-running with identical inputs and the same
  function object skips every combo.
- **A change propagates downstream.** In a multi-step pipeline, a changed raw
  input makes step 1 recompute, which produces a new intermediate `record_id`,
  which makes step 2 recompute — the cascade follows the data.
- **An output saved without lineage always recomputes.** If an output row was
  written by a plain `.save()` (no lineage record), `for_each` cannot prove it is
  current, so it recomputes.

## Constants are variant discriminators, not invalidators

Changing a constant does **not** invalidate the previous result — it defines a
*new variant* that coexists with the old one:

```python
for_each(scale, inputs={"signal": Raw, "factor": 2.0}, outputs=[Out], ...)
for_each(scale, inputs={"signal": Raw, "factor": 3.0}, outputs=[Out], ...)
# factor=2.0 and factor=3.0 are independent branches; re-running either one skips.
# A brand-new factor=7.0 has no prior output, so it computes.
```

Each constant value gets its own cache entry, so sweeping a parameter never
overwrites earlier runs.

## Caching vs. currency (node states)

There are two questions built on the same identity, and it helps to keep them
apart:

- **The run decision** — *"should I execute this combo now?"* — answered by
  `skip_computed` when a pipeline runs.
- **Currency** — *"is an existing output still up to date?"* — answered by
  [Node States](node-states.md), which reads stored outputs and classifies each as
  up-to-date, stale, or missing.

They use the same ingredients. In particular, a function's **own code change** is
detected at the node-state level: comparing the current bytecode hash against the
hash stored with the output marks that function's outputs stale. See
[Node States](node-states.md) for the full currency model, including how staleness
propagates through a pipeline graph.

## The same function at multiple call sites

Reusing one function from two different `for_each` call sites would naively let
the second run's expected-combo bookkeeping clobber the first's. SciStack avoids
this with a **`call_id`** — a stable hash of the output's version keys excluding
the function hash — so each call site's expected set is recorded independently and
the two coexist.

**Next:** [Node States](node-states.md) ·
[Versioning & Content Hashing](hashing.md) · [Lineage & Provenance](lineage.md) ·
[Guide: Caching Computations](../guide/caching.md)
