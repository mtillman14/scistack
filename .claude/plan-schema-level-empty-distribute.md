# Plan: `schema_level = []` (all levels deselected) is silently rewritten to "all keys"

*2026-09-15. Diagnosed from `/workspace/scidb.log` run_id=ss7yr32h
(`loadDemographics`, MATLAB, `distribute=true`).*

## Symptom

A MATLAB function node with **every schema level deselected** and
`distribute=true` runs **once per subject** (952 combos:
17 subject x 7 session x 2 speed x 4 trial) instead of **once for the
dataset**, distributing the returned table's rows to `subject`.

## Root cause

`scistack-gui/scistack_gui/api/matlab_command.py` decides which schema keys to
emit as `for_each` name/value pairs with a **truthiness** test:

```python
iterate_keys = schema_level if schema_level else schema_keys   # :979
... _format_schema_kwargs(schema_level if schema_level else schema_keys, ...)  # :713
```

`schema_level` is tri-state by contract (`RunRequest.schema_level`,
`FunctionSettingsPanel.tsx:71`):

| value | meaning |
|---|---|
| `None` | not specified -> iterate ALL schema keys |
| `["subject", ...]` | iterate exactly these |
| `[]` | **deselect all** -> iterate nothing, one dataset-level call |

`[]` is falsy, so the third state collapses into the first: the generator
emits `'subject', [], 'session', [], ...` and the run fans out over the whole
grid. `domain/variant_resolver.py:254` already gets this right
(`if schema_level is not None`), and the Python run path
(`api/run.py:370`, `is None` + `scidb.for_each(schema_keys=[])`) does too —
only the MATLAB command generator is wrong.

Evidence chain in `scidb.log`:

- `:65277` `[server] Parsed request: ... schema_level=[], run_options={... 'distribute': True ...}` — the GUI asked correctly.
- `:65290` `emitting non-default run option(s) {'distribute': True}` — the option survived.
- `:65308` `[bridge] for_each_prepare returning: full_combos=952, extended_metadata_iterables keys=['subject','session','speed','trial']` — the generator put the keys back.

With the keys present, `+scifor/for_each.m:402` takes the *else* branch:
deepest iterated = `trial`, so distribute targets `cycle` — not `subject`.
With no keys emitted it takes the "nothing iterated" branch and targets
`real_schema_keys(1)` = `subject`, which is what the user asked for.

## Why it was invisible

Python's `scifor.for_each` logs `resolve_distribute_target: '<key>'` at INFO
precisely so a mis-resolved distribute names itself (see
`docs/claude/gui-run-options-flow.md`). **MATLAB's `+scifor/for_each.m` has the
same resolution logic and no such log line**, so the run said only
`options: distribute=true` and never which key it picked.

## Changes

### 1. Fix — `api/matlab_command.py` (GUI layer; this is a GUI translation bug)

- `:713` (never-run / template branch) and `:979` (`_for_each_call_lines`):
  `schema_level if schema_level is not None else schema_keys`.
- Log at INFO when an explicitly empty `schema_level` suppresses all schema
  kwargs, so a dataset-level run is distinguishable in `scidb.log` from a
  generator that simply forgot the keys.

### 2. Observability — `+scifor/for_each.m`

Emit `resolve_distribute_target: '<key>' (top of schema; nothing iterated)` /
`'<key>' (one level below '<deepest>')` at INFO, matching the Python wording
so both languages grep identically.

### 3. Tests — `scistack-gui/tests/test_matlab.py`

- `schema_level=[]` + variants -> **no** `'subject', []` pair in the generated
  command (the regression).
- `schema_level=[]` on the never-run/template branch -> same.
- `schema_level=None` -> all keys still emitted (pins the tri-state).
- `schema_level=[]` + `distribute=True` -> `'distribute', true` still emitted
  and no schema pairs (the exact reported combination).
- pipeline-step path (`generate_matlab_pipeline_command`) honours `[]` too.

## Not changed

- `services/execution_service.py:1293` (`build_backend_pipeline`) iterates the
  full grid unconditionally and passes no run options at all — a separate,
  already-documented limitation of the backend Pipeline path, out of scope.
