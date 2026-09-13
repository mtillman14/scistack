# Finding: a partially re-run PathInput loader reads green

> Status: **CLOSED 2026-09-13.** Diagnosed 2026-09-11, deferred, then fixed in
> two halves by the schema location picker work
> (`.claude/plan-schema-location-picker.md`):
>
> - **(A) the picker's denominator** — Stage 1a. `scidb.locations` derives an
>   inputless function's expected set from `check_pathinput_node_state`'s
>   discovery set, so a never-loaded file reads red in the pane.
> - **(B) the canvas badge** — Stage 1c. `state._discovery_gate` brings the same
>   rule to `check_node_state`. How each risk below was answered is recorded at
>   the end of this document.
>
> Everything below is the original diagnosis, kept because the scenario and the
> risk list are what the fix was built against.

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

---

## How it was actually fixed (2026-09-13)

`state._discovery_gate`, called from `check_node_state` after the
invocation-membership answer. It **only ever adds** missing combos: nothing in
it can turn a red node green.

Each risk above, answered:

| risk | answer |
|---|---|
| **Discovery cost on every graph build** | A TTL cache (`state.DISCOVERY_CACHE_SECONDS`, 5s) keyed on the PathInput spec set. A canvas refresh arrives in bursts — a scope switch redraws every node — and the TTL collapses a burst into one walk while keeping "drop a file in, refresh, see red" responsive. Deliberately not event-invalidated: the filesystem changes behind our back by definition, so any hook would be a guess about when, and a stale green is the bug being fixed. `clear_discovery_cache()` is exposed for tests and post-run use. |
| **Windows separators / unreachable root** | The **credibility guard**, and the most important line in the change: if discovery finds *zero* combos while the function has realized outputs, the walk is broken here — not "every location vanished". The gate stands down, logs a warning naming the likely cause, and the node keeps its previous answer. Turning a whole study red because a path failed to resolve would be far worse than the stale green. Tested by deleting the data root under a working fixture. |
| **MATLAB-side discovery** | Subsumed by the guard. Discovery is a filesystem walk over the stored `PathInput.to_key()` template, which is language-agnostic; where the Python walk genuinely cannot see what MATLAB would, it sees *nothing*, and the guard treats that as "cannot tell". |
| **A file that legitimately produces nothing** | `exclusions.exclude_schema(reason, …)`, which `check_pathinput_node_state` already subtracts — and which forces the user to write down why. Deliberately the ONLY escape hatch: pure discovery is used because there is no recorded grid to consult (`_run.where_clause` is display-only by design), and the same exclusion already drops the location from the picker's denominator, so the badge and the pane agree by construction. Tested. |

**The suggested first step (diagnostics before behaviour) was folded in rather
than staged separately**: the gate logs the on-disk count, the never-run count
and the cache timing every time it runs, so the measurement the original plan
asked for is available on any real project — but it is emitted *by* the working
fix rather than ahead of it. The credibility guard is what made that safe to do
in one step; without it, measuring first would have been mandatory.

**What this did NOT fix.** Staggered processing — subjects 1-3 loaded, the body
improved, subjects 4-6 loaded — leaves no file unloaded, so the gate has nothing
to report while records genuinely span two versions. The variant-span banner
(`plot-variant-rows.md` §3) remains the only signal for that, which is what the
comparison table above always said.
