# Lineage & Provenance

<!-- Ground truth (tests win over prose). Verified against:
     scilineage/tests/test_core.py (@lineage_fcn -> LineageFcnResult; .data/.invoked/.hash;
       unpack_output multi-output + .output_num; invoked.inputs binds positional args to
       param names; compute_lineage_hash deterministic; classify_input kinds; manual());
     scilineage/tests/test_lineage.py (extract_lineage -> function_name/function_hash[64 SHA-256]/
       inputs[source_type,source_function]/constants; get_upstream_lineage chain + max_depth;
       to_dict/from_dict);
     scilineage/tests/test_hashing.py (canonical_hash 16-char content hash; numpy by content+
       dtype+shape; dict key-order-independent; list order matters);
     scilineage/src/scilineage/inputs.py (InputKind: LINEAGE_RESULT/SAVED_VARIABLE/
       UNSAVED_RESULT/RAW_DATA/CONSTANT);
     scidb/tests/test_optional_lineage_dependency.py (HAS_LINEAGE; scidb works without
       scilineage; version_keys __fn/__fn_hash/__inputs/__constants + namespaced branch_params).
     NOTE: lineage is a scihist feature — lineage_fcn/LineageFcn are RE-EXPORTED by scihist
       (import from scihist); manual() is re-exported by scidb; extract_lineage/
       get_upstream_lineage live only in the internal scilineage package. Python-only. -->

**Lineage** is the record of *how a value was produced*: which function ran, what
its inputs were, and a version stamp of the function itself. SciStack captures
this automatically as your code runs (this is what [`scihist`](architecture.md)
adds on top of `scidb`), building a provenance graph that doubles as a cache key —
the same lineage means the same result, so work can be reused instead of repeated.

This page explains the model. Lineage is **Python-only**; you reach it through
`scihist`, which re-exports the `lineage_fcn` decorator and wraps your functions
for you. (The engine itself is the internal `scilineage` package — see
[Internals](../internals/scicanonicalhash.md).)

## Functions that remember their inputs

Decorate a function with `@lineage_fcn` and each call returns a
`LineageFcnResult` instead of the raw value. The result carries both the computed
value and the provenance of the computation:

```python
from scihist import lineage_fcn

@lineage_fcn
def process(x, factor):
    return x * factor

result = process(5, factor=2)

result.data            # 10  — the computed value
result.invoked.inputs  # {"x": 5, "factor": 2}  — captured inputs
result.hash            # deterministic id for this (function, inputs) computation
```

A few properties worth knowing:

- The value lives on `.data`; the result also acts transparently in comparisons
  (`result == 10` is true), and `str(result)` shows the data.
- Positional arguments are bound to their **parameter names**, so `process(5, 2)`
  records `{"x": 5, "factor": 2}` — the lineage is readable regardless of call
  style.
- `result.hash` is deterministic: the same function and inputs always produce the
  same hash, and different inputs produce a different one. This is the basis for
  caching.

Multi-output functions opt in with `unpack_output=True`, and each output is its
own tracked result:

```python
@lineage_fcn(unpack_output=True)
def split(x):
    return x, x * 2

a, b = split(5)
a.data, a.output_num   # 5, 0
b.data, b.output_num   # 10, 1
```

## Inputs vs. constants: how an input is classified

The fidelity of lineage comes from correctly distinguishing *what kind of thing*
each input is. Every input is classified into one of five kinds:

| Kind | Meaning |
|---|---|
| `LINEAGE_RESULT` | A result from another tracked computation (a live result, *or* a saved variable that carries a lineage hash) |
| `SAVED_VARIABLE` | A database-backed variable with a `record_id` but no lineage |
| `UNSAVED_RESULT` | A variable wrapping a tracked result that hasn't been saved yet |
| `RAW_DATA` | A variable holding raw data with no lineage |
| `CONSTANT` | A literal value — an `int`, `float`, `str`, etc. |

Constants and variable inputs are recorded separately, so `extract_lineage`
reports `inputs` (provenance-bearing) apart from `constants` (literal
parameters). This is also why [constants](variables.md) are wrapped distinctly:
a `factor` of `2.0` is a tuning parameter, not a data dependency, and the lineage
reflects that.

## Lineage survives save and reload

The defining property of the system: **provenance is preserved across the
database boundary.** When a tracked result is saved as a variable, it keeps a
lineage hash. Reloaded later and fed into a downstream function, it classifies as
a `LINEAGE_RESULT` and produces the *same* downstream lineage hash as if you had
chained the live results in memory:

```python
@lineage_fcn
def step1(x): return x + 1

@lineage_fcn
def step2(x): return x * 2

# In-memory chain and save→reload chain compute the SAME downstream hash
out2_live     = step2(step1(5))
out2_reloaded = step2(reloaded_variable_with_step1s_hash)
# out2_live.invoked.compute_lineage_hash() == out2_reloaded...compute_lineage_hash()
```

Because of this, a pipeline split across runs — compute today, reload and extend
tomorrow — has identical provenance and cache behavior to one run end to end.

## Two hashes: function identity and content identity

Lineage rests on two distinct deterministic hashes:

- **Function hash** — a 64-character SHA-256 of the function's *compiled
  bytecode*. Because it hashes bytecode, reformatting or re-commenting a function
  does **not** change its hash; changing what it computes does.
- **Content hash** (`canonical_hash`) — a 16-character hash of a *value*. It is
  structural: lists hash in order, dicts hash independent of key order, and numpy
  arrays hash by content **plus dtype and shape** (so `int32` and `int64` of the
  same numbers differ).

A computation's **lineage hash** combines the function hash with the
classified inputs, giving the stable cache key that
[Computation Caching](caching.md) keys on.

## Reading the provenance graph

Two helpers turn a result into inspectable lineage. They live in the lineage
engine itself (see [Internals](../internals/scicanonicalhash.md)):

```python
from scilineage import extract_lineage, get_upstream_lineage

record = extract_lineage(result)
record.function_name   # "step2"
record.function_hash   # 64-char SHA-256
record.inputs          # [{"source_type": "thunk", "source_function": "step1", ...}]
record.constants       # [{"name": ..., "value_repr": ...}, ...]

chain = get_upstream_lineage(result, max_depth=10)  # walk the full ancestry
[r["function_name"] for r in chain]                 # ["step2", "step1", ...]
```

Lineage records round-trip through `to_dict()` / `from_dict()`, which is how they
are persisted alongside saved variables.

## Manual interventions

Real pipelines sometimes need an out-of-band correction — removing a sensor
artifact, say. Rather than breaking the provenance chain, re-enter the pipeline
with `manual()`, which records the edit as a first-class lineage step:

```python
from scidb import manual

corrected = manual(edited_data, label="outlier_removal",
                   reason="amplitude < 0.1 in trial 3 is a sensor artifact")
```

The result is a normal `LineageFcnResult` whose function name is `"manual"`, with
the `label` and `reason` captured in its lineage — so the intervention is
documented and reproducible, not a silent hand-edit.

## Where lineage fits in the stack

Lineage is what `scihist` adds on top of `scidb`. `scidb` itself runs without the
lineage engine (guarded by a `HAS_LINEAGE` flag); even for plain functions it
still records function and input identity (`__fn`, `__fn_hash`, `__inputs`,
`__constants`, and namespaced `branch_params`) for its own version tracking. The
full provenance graph and automatic function wrapping come from `scihist`, which
decorates your functions for you. (Implementation:
[Internals — scicanonicalhash](../internals/scicanonicalhash.md).)

**Next:** [Computation Caching](caching.md) ·
[Versioning & Content Hashing](hashing.md) · [Node States](node-states.md) ·
[API: Lineage](../api/lineage.md) · [Guide: Tracking Lineage](../guide/lineage.md)
