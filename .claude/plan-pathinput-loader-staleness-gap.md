# Finding: a partially re-run PathInput loader reads green

> Status: **diagnosed 2026-09-11, deferred by the user. Not implemented.**
> Found while building the variant-span banner
> (`.claude/plan-default-variant-selection.md`). Lives in `scidb.state` /
> `scidb.provenance_query`, not in the plotting layer.

## The gap

A zero-DB-input function — a `PathInput`-only loader — cannot be detected as
partially run. It reports **green** as soon as *one* combo exists under the
current source hash, and **red** only when *none* does.

`check_node_state` builds its expected set from
`expected_invocations_for_function`, which for such a function has no upstream
table to predict locations from and falls back to clause (a)
(`provenance_query.py:1826`):

```python
realized = realized_inputless_invocations(duck, fn_name, fn_hash)
expected |= realized
```

Expected becomes *what it has already produced under the current hash* — a
subset of what is present by construction, so `counts["missing"]` can never
exceed zero. The docstring states it plainly: "there is still no live source for
the set of combos it *should* produce, so un-run combos cannot be detected."

This is documented behaviour, not an oversight, and the restriction to the
current `fn_hash` was itself a fix (without it a loader could never go red
however much its code changed). What has changed is the **requirement**: the
user asked, 2026-09-11, that a function body differing between schema locations
be surfaced very prominently. This is the one case where the canvas actively
says the opposite.

## The scenario

With `loadDelsysEMGOneFile` (the real one, in the aging-well project):

1. It loads each Delsys file via `PathInput`, producing `RawEMG` at every trial
   location under hash `H1`. Canvas green.
2. The body is edited — a units conversion, a header parse. Current hash `H2`.
3. Node goes **red**: nothing realized under `H2`. Correct.
4. Run covers only *some* locations — a `where=` filter to check one trial
   first, a malformed file, or a run that died partway.
5. `realized_inputless_invocations(H2)` returns the handful that succeeded.
   Expected = those, all present → **node turns green.**
6. Every other trial still holds only its `H1` record, which at its own schema
   location is the newest thing there, so `CodeIsLatest` is True for it.
7. `FilteredEMG` now spans `Code:loadDelsysEMGOneFile` v1/v2 — half the subjects
   loaded with the bug, half without — and the canvas says green.

Step 4 deserves weight here: a MATLAB run that fails *after* producing some
outputs is exactly this shape, and per
`project_matlab_terminal_run_tracking` a real failure after connect is still an
open gap in run tracking, so a half-completed run may not announce itself.

**Sibling symptom, same root cause, no versions involved:** add three new EMG
files and never run, and the loader also reads green — un-run combos leave no
trace to be missing.

## Why it looks fixable rather than architectural

A `PathInput`'s expected set *is* enumerable. The discovery machinery that
resolves and expands `PathInput` patterns already exists in `scifor`
(`PathInput.apply_discovery`; see `project_pathinput_resolution_split` —
scifor/MATLAB resolves and discovers, Python scifor stays pure). Clause (a) does
not consult it. The work is wiring an existing live source into the expected-set
derivation, not inventing one.

Care required, in rough order of risk:

- **Discovery cost on every graph build.** `check_node_state` runs on every
  canvas refresh; filesystem globbing per loader node is a very different cost
  profile from a DuckDB query. Likely needs the same caching treatment the
  discovery harness already has.
- **Discovery is MATLAB-side for MATLAB loaders.** The Python half cannot
  enumerate what the MATLAB half would match, so a MATLAB `PathInput` loader may
  need a different answer than a Python one — or an honest "cannot tell".
- **A file that legitimately produces nothing** (excluded, filtered by `where=`,
  or skipped by the function itself) must not redden the node forever.
- **Windows path separators** in `scistack.toml` already mis-resolve on POSIX
  (`project_windows_config_paths`); a stale-set derived from globbing inherits
  that.

## Relationship to the variant-span banner

They are correlated but neither implies the other, and the banner is **not**
made redundant by fixing this:

| | canvas red | span banner |
|---|---|---|
| question | "is there work to do in this pipeline?" | "is the data in THIS figure heterogeneous, and which location is on which version?" |
| partial re-run of a normal function | yes | yes |
| partial re-run of a PathInput loader | **no (this gap)** | yes |
| never run / new data | yes | no |
| deliberately pinned to v1 | possibly | no |

Ship the banner first. In the scenario above it is the **only** signal, which is
an argument for it rather than against fixing this.

## Suggested first step

Diagnostics before behaviour, as with every other stage here: log, for each
zero-input function, the expected-set size, the realized-under-current-hash
count, and what `PathInput` discovery *would* enumerate — without acting on the
difference. That measures the gap on a real project before anyone changes what
turns a node red.
