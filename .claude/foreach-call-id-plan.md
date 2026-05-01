# Plan: Add `call_id` to disambiguate `for_each` call sites

## Problem

`_for_each_expected` is keyed by `function_name` alone:

- `_persist_expected_combos` (scidb/foreach.py:1342) does
  `DELETE FROM _for_each_expected WHERE function_name = ?` before each insert,
  so the second `for_each` call for a reused function wipes the first call's
  expected set.
- Every inserted row hardcodes `branch_params = "{}"` (foreach.py:1332, 1349),
  so the `branch_params` column of the PK is unused.
- Readers in `scihist/state.py:534, 590` query
  `WHERE function_name = ?` — they silently return whichever call wrote last.

Net effect: if `bandpass` is invoked from two `for_each` calls (different
`subjects`, different `where`, different `Fixed` overrides, etc.), the GUI's
`check_node_state` will report missing outputs for combos the *other* call site
was responsible for.

## Design: what is a `call_id`?

A `call_id` identifies a specific `for_each` invocation site. Same site
re-run = same `call_id`. Different site = different `call_id`.

Source material is `ForEachConfig.to_version_keys()` (foreach_config.py:52),
which already captures everything that varies between call sites:

| version_key   | Include in call_id? | Reason                                                                     |
|---------------|---------------------|----------------------------------------------------------------------------|
| `__fn`        | yes                 | Cheap collision insurance; same hash across two functions is unlikely but free to guard against. |
| `__inputs`    | yes                 | Different input types/wrappers = different call site.                      |
| `__constants` | yes                 | Different constant values = different call site (matches today's `branch_params` semantics). |
| `__where`     | yes                 | Different filter = different call site.                                    |
| `__distribute`| yes                 | Affects which combos exist.                                                |
| `__as_table`  | yes                 | Affects what's saved.                                                      |
| `__fn_hash`   | **NO** (default)    | See tradeoff below — needs your call.                                      |

`call_id` = first 16 hex chars of SHA-256 of canonical JSON of the included
keys. Same truncation as `record_id` / `content_hash` / `__fn_hash` — collision
math is already familiar in this codebase.

### The `__fn_hash` tradeoff (decision needed)

**Exclude `__fn_hash` (recommended):**
- Cosmetic edits to the function don't fork the call_id, so the previous
  expected set stays linked to the same call site.
- Matches `branch_params`' stability story (which also excludes `__fn_hash`).
- Re-running with edited source overwrites the expected set in place, which
  is what you usually want.

**Include `__fn_hash`:**
- Editing the function body forks the call_id → old expected set becomes
  orphaned → `check_node_state` flags every combo as needing recompute,
  forcing the user to acknowledge the change.
- More aggressive staleness signal, but worse UX for trivial edits.

Per the existing memory `feedback_defer_content_staleness` — function-hash
mismatches have already been judged "traceability-only, not stale" elsewhere
in the codebase, which favors **exclude**.

## Schema change

`_for_each_expected` becomes:

```sql
CREATE TABLE IF NOT EXISTS _for_each_expected (
    function_name  VARCHAR NOT NULL,
    call_id        VARCHAR NOT NULL,
    schema_id      INTEGER NOT NULL,
    branch_params  VARCHAR DEFAULT '{}',
    PRIMARY KEY (function_name, call_id, schema_id, branch_params)
)
```

Migration: this table is purely diagnostic — every active `for_each` call
re-populates its rows on the next run. Cleanest path is **drop and recreate**
on first open of an old DB. Alternative (back-fill `call_id = ''`) keeps stale
rows alive forever; not worth the complexity.

Add an `ALTER`/migrate step in `_ensure_for_each_expected_table`
(database.py:614): if the table exists without `call_id`, drop and recreate.
Log the drop so it's visible.

## Code changes

### 1. `scidb/foreach_config.py`

Add:

```python
def to_call_id(self) -> str:
    """Stable hash identifying this for_each call site.
    Excludes __fn_hash so cosmetic source edits don't fork the call site."""
    keys = self.to_version_keys()
    keys.pop("__fn_hash", None)
    payload = json.dumps(keys, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]
```

Log the computed call_id at debug level (per CLAUDE.md NOTE 2 — observability).

### 2. `scidb/foreach.py`

- Compute `call_id = config.to_call_id()` next to `config_keys` (around line
  280, Step 8).
- Pass `call_id` into `_persist_expected_combos(...)` (line 461 site, function
  at line 1294).
- In `_persist_expected_combos`:
  - Change DELETE to `WHERE function_name = ? AND call_id = ?`.
  - INSERT `(function_name, call_id, schema_id, "{}")`.
  - Log how many rows were replaced vs. inserted.

### 3. `scidb/database.py`

- Update `CREATE TABLE` (line 622) to include `call_id`.
- Add migration in `_ensure_for_each_expected_table` (line 614):
  if existing schema lacks `call_id`, drop and recreate; log it.

### 4. `scihist-lib/src/scihist/state.py`

This is the biggest API question. Two reader sites at lines 534 and 590 do
`WHERE function_name = ?`. Options:

**Option A — read-time aggregation (low blast radius):**
Keep the current `WHERE function_name = ?` query; just `UNION` across all
call_ids. Equivalent to today's behavior but now correctly *unioned* across
call sites instead of *clobbered*. `check_node_state` answers "is this
function complete across all its call sites?".

**Option B — per-call-site reporting (richer, more work):**
Have `check_node_state` accept the same inputs/where/etc. as the original
`for_each` (or accept a `call_id`), so it can answer "is *this specific call
site* complete?". This is what the GUI probably wants long-term, but it
requires plumbing call_id through the GUI's node-state queries.

**Recommendation: ship Option A first** (preserves current observable
behavior, fixes the silent overwrite), open a follow-up ticket for B.

### 5. Tests (per CLAUDE.md NOTE 2)

New tests in `scidb/tests/`:

1. **Two call sites, disjoint subjects** — call `bandpass` once with
   `subject=[1,2,3]`, then again with `subject=[4,5,6]`. Assert
   `_for_each_expected` has 6 rows after both runs (today: only 3 survive).
2. **Two call sites, different constants** — same subjects, different
   `low_hz`. Assert distinct `call_id`s, both expected sets persist.
3. **Same call site, re-run** — call twice with identical args. Assert no
   row growth (DELETE-then-INSERT replaces in place).
4. **Migration smoke** — open a DB created against the old schema; verify
   `_ensure_for_each_expected_table` recreates it without raising.

Update existing `test_state_pathinput.py` / `test_state_matlab_pathinput.py`
expectations for the new column.

## Open questions

1. **Include `__fn_hash` in call_id?** Recommend no; flagged above.
2. **Drop-and-recreate migration acceptable?** Or do you need to preserve
   existing rows during the transition?
3. **`check_node_state` Option A vs B?** Recommend A now, B later.

## Out of scope

- Unifying `branch_params` and `version_keys` (Part 1 discussion). They stay
  separate; this plan only adds a third concept (`call_id`) for call-site
  identity, which is genuinely missing from the current model.
- Changing how `branch_params` accumulates downstream.
- GUI-side changes to expose call_id (only relevant if Option B is chosen).
