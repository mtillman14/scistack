# Glue fed by a Parameter node — diagnosis and fix

> **STATUS: all four stages implemented 2026-09-10, uncommitted, Python tests
> not yet run.** Two deliberate departures from the plan below, both found
> while implementing — see "As built" at the end.

**Date:** 2026-09-10
**Symptom (user):** first GUI test of a glue node. `filterDelsys` received only
2 of its 3 arguments; the glue node's value never arrived.

## What the log proves

Wiring the user built (`scidb.log`, 16:48–16:51):

```
param__delsys_config ──> glue_config_filter ──> fn__filterDelsys.in__config
RawEMG               ──────────────────────────> fn__filterDelsys.in__loaded_data
param__Fs            ──────────────────────────> fn__filterDelsys.in__Fs
```

The four lines that tell the whole story:

| Line | Message |
|---|---|
| 2800 | `WARN [edge_resolver] parameter 'config' is fed by glue glue_config_filter, but the chain does not start from a variable — wire the first glue node's input` |
| 3749 | `WARN generate_matlab_command: filterDelsys: parameter(s) ['config'] have no wiring, but later parameter(s) do — MATLAB binds by position … Emitting ['loaded_data', 'Fs'] for signature ['loaded_data','config','Fs']` |
| 3778 | `INFO [matlab] inputs: {loaded_data: <table 2x14>, Fs: 2000}` |
| 3779 | `WARN [matlab] filterDelsys: the inputs struct has 2 field(s) but the function declares 3 argument(s)` |

`config` was never bound at all — not mis-glued, *absent*. The MATLAB
positional-binding guard then correctly flagged the gap.

## Root causes (four, in order of impact)

### C1 — the glue chain head must be a Variable (the reported bug)

`scistack-gui/scistack_gui/domain/edge_resolver.py:199` `resolve_glue_chain`
resolves the head of the chain with `node_id_to_var_label`, which returns
`None` for anything that is not a `variableNode`. `resolve_function_edges`
(line 357-377) then warns and `continue`s **without binding the parameter at
all**.

So `Parameter → glue → fn` silently loses the parameter. Every *other* source
kind reaching a function directly (PathInput, Parameter) is handled a few lines
below — the glue branch just never learned about them.

`docs/claude/free-code-glue-nodes.md` §7 scopes out glue on a *PathInput*-fed
param. It says nothing about Parameters, and reshaping a config struct before
it reaches a function is a legitimate case.

### C2 — scidb cannot apply glue to a Parameter-fed input

`scidb.Parameter` **is** a `scifor.EachOf` (`scidb/parameter.py:60`) — a fan-out
axis, not data. It passes through `_convert_inputs` untouched
(`foreach.py:3104`, the "constant — pass through unchanged" branch), so
`fuse_glue` would find it in `loaded_inputs`, see no frame, and call the glue on
the **`EachOf` object itself** instead of on each value.

So even with C1 fixed, the value arriving at `filterDelsys` would be wrong.

### C3 — the MATLAB run path never delivers glue

`scistack_gui/services/matlab_command_service.py` and
`scistack_gui/api/matlab_command.py` have **no glue support whatsoever** — no
`glue` parameter, no emission into the generated `scidb.for_each(...)` call.
Only the Python run path (`api/run.py:449`, `build_run_glue`) passes it.

`+scidb/for_each.m` *does* accept and apply `glue` (line 42, 259, 440) — the
MATLAB half was built; the GUI just never fills it in. A MATLAB run therefore
drops glue silently, which is precisely the failure mode §2 of the design doc
exists to prevent.

### C4 — a stale glue input edge is kept, not reported usefully

At 16:48:35 the user wired `param__delsys_config → glue.in__value`; after
renaming the glue's parameter they wired `param__delsys_config → glue.in__config`
at 16:49:33. Both survive, and `resolve_glue_chain` follows only
`incoming[0]` — the *stale* one. Harmless here (same source), wrong in general.

## The fix

### Stage 1 — a glue chain may start from any binding kind *(fixes the symptom)*

`edge_resolver.py`:

* Extract `resolve_source_binding(source_node_id, manual_nodes,
  existing_node_labels) -> dict | None` — the "what kind of thing is this source
  node, and what is its declared ref" decision, which currently lives inline and
  three times over in `resolve_function_edges`.
* `resolve_glue_chain` returns `(chain, binding | None)` instead of
  `(chain, var_label | None)`.
* The glue branch of `resolve_function_edges` binds whatever comes back. Only a
  genuinely unresolvable head (an unwired glue node) warns — and the warning
  says *unwired*, not "not a variable".

Each layer keeps its own rule: the GUI binds the head honestly; **scidb** decides
what is runnable (a PathInput head still raises `GlueUnsupportedInputError` from
`refuse_pathinput_glue`, which is a loud, correct refusal rather than today's
silent drop).

Update the `ResolvedEdges.glue_chains` docstring — the chain no longer "still
names the upstream VARIABLE"; it interposes on whatever binding feeds it.

### Stage 2 — scidb: glue on a Parameter is applied before fan-out

New `scidb.glue.apply_parameter_glue(inputs, chains) -> set[str]`, called from
`for_each` right after `refuse_pathinput_glue` and **before** `_convert_inputs`.
For each glued param whose input is an `EachOf`, it maps the chain over
`.alternatives` and replaces the input with `EachOf(*glued)`. The params it
consumed are removed from the chains handed to `fuse_glue`.

Why this site rather than the Step-10/11 fusion point:

* The fanned-out value is what lands in `__constants`, so the **recorded
  constant is the glued value**. Editing the glue body changes the constant
  content hash, so `skip_computed` invalidates downstream **with no virtual
  record needed** — §2's staleness hole is closed exactly here, not merely
  warned about (as it is for `Merge` / `PerComboLoader`).
* `apply_glue_chain` already skips the row contract for non-DataFrames
  (`glue.py:396-405`), so scalars, dicts and structs work unchanged.
* `for_each_prepare` is Python on **both** run paths, so this one site serves
  Python and MATLAB runs identically.

**Documented exception to the language rule (§1).** Parameter-fed glue runs
Python-side before fan-out in every run, so it must be **Python** glue even in a
MATLAB run. `check_run_language` gains that carve-out with the reason inline; a
MATLAB glue body on a Parameter-fed param is refused with a message that says
why and points at the alternative (glue the variable, or reshape inside the
function).

### Stage 3 — the MATLAB run path carries glue

Thread `glue` through `matlab_command_service.generate_matlab_command` (built via
the existing `execution_service.build_run_glue`, which already emits MATLAB
source text) into `api/matlab_command.generate_matlab_command`, and emit
`'glue', struct(...)` in the generated `scidb.for_each` call — the shape
`+scidb/for_each.m:1355 build_glue_chains` already expects.

Log at INFO which params carry glue in the generated command, so a MATLAB run's
glue is visible in `scidb.log` the way a Python run's is.

### Stage 4 — diagnostics and guards

* `resolve_glue_chain`: when a glue node has >1 incoming edge, log the edge ids
  **and handles**, so a stale rename is identifiable from the log alone (C4).
* Graph-build validation: a Python glue node feeding a MATLAB function on a
  **variable** binding is a run-time error today with no canvas signal — surface
  it as a node-level warning at build time.
* `[glue] '<param>': head is a <kind> binding (<ref>)` at INFO on every run —
  §6's diagnostic trail currently assumes the head is always a variable.

## Tests

| Where | Test |
|---|---|
| `scistack-gui/tests/test_glue_nodes.py` | a Parameter-fed glue binds the parameter *and* records the chain (the regression) |
| " | a PathInput-fed glue binds and leaves scidb to refuse it |
| " | an unwired glue node still binds nothing, and says *unwired* |
| " | the generated MATLAB command contains the glue struct |
| `scidb/tests/test_glue.py` | glue maps over a multi-value Parameter's alternatives, not the `EachOf` |
| " | a single-value Parameter yields the glued value |
| " | MATLAB glue on a Parameter-fed param is refused with the stated reason |
| `scidb/tests/test_glue_identity.py` | editing a Parameter glue's body changes `__constants` and does **not** skip |

Run one package at a time (`project_pytest_one_package_at_a_time`).

## Docs

`docs/claude/free-code-glue-nodes.md`: new §4a "What feeds a glue node" (the
three head kinds and where each is applied), the §1 language-rule carve-out, and
§7 amended — Parameter heads move from unstated to supported.

---

## As built — two departures, and a fifth defect

### 1. The application site is "constant", not "Parameter", and it is earlier

The plan said "map the chain over `Parameter.alternatives` before fan-out".
That site does not exist on the Python path: **`for_each`'s Step 1 expands every
`EachOf` input by RECURSING**, one `for_each` call per alternative, long before
`_for_each_prepare` runs. A `Parameter` is therefore already a concrete scalar
by the time any glue code sees it.

So the rule is stated over the *value*, not the *declaration*: a chain whose
param holds a plain constant at prepare time is applied there
(`glue.apply_constant_glue`), before Step 8 builds the version keys. The
multi-valued case then needs no special handling at all — the fan-out and the
glue compose, each call gluing and recording its own value.

`glue.is_constant_axis` covers the one path with no Step 1 in front of it:
`scimatlab.bridge` calls `_for_each_prepare` directly, so an unexpanded
`Parameter` can arrive there and is mapped over its alternatives.

Validation was also split out (`split_constant_chains` +
`check_constant_glue_language`) so **both** halves are language-checked before
any body runs — a run that will be refused should be refused before it reshapes
anything.

### 2. Stage 3 needed a new vehicle, not just a new argument

The plan assumed threading `build_run_glue`'s output into the MATLAB command
generator. That covers MATLAB glue. It does not cover the user's actual case —
**Python** glue on a **constant**, which by §1's carve-out must stay Python even
in a MATLAB run, and which MATLAB cannot express as a function handle.

Added: a chain element may now be a struct
(`struct('name', …, 'language', 'python', 'source_file', …)`) alongside a
handle. `+scidb/for_each.m:build_glue_chains` accepts both and contributes **no
MATLAB handle** for a Python element, so `apply_matlab_glue` cannot run it
twice; `scimatlab.bridge._reconstruct_glue_chains` honours the per-entry
language and loads the body via the new `scidb.glue.resolve_python_glue`
(MATLAB's interpreter has no function registry, so the *path* is what travels).

`GlueSpec.__post_init__` now also accepts `source_text` as the third way to
have an identity, and `_compute_glue_hash` falls back to the text hash when a
Python body could not be loaded — a degraded but stable hash beats crashing at
spec construction.

### 3. A fifth defect, found while implementing

**`for_each`'s EachOf recursion never forwarded `glue=`** (`foreach.py:487`).
Any glue on a function with a multi-valued Parameter — or any multi-type
variable input — was silently dropped in every alternative: nothing reshaped,
no virtual glue record written, and a run that reported success. Fixed, with a
regression test (`TestGlueSurvivesEachOfExpansion`).

### Not done

The MATLAB half of Stage 3 (`+scidb/for_each.m`, `bridge.py`) **cannot be
exercised here** — no MATLAB, and no Python either. It needs a real run.
