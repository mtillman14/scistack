# Permanent Filters & Data Exclusions: Design

## Problem

User has filter conditions that are semantically "this data is not part of
the analysis" and that they would otherwise have to retype at every
`for_each` call site. Two real-world flavors:

1. **Value-based per-variable/column**: `GAITRiteLoadedCycle["StepLengths_GR"] != 0`
   — a zero step length means the GAITRite missed the step, not a real
   measurement. This is a predicate on *values* of a specific column.
2. **Identifier-based global**: `subject=1, trial=2` was a failed recording
   session and should be excluded from every analysis. This is a predicate
   on *schema keys*, not on any column's value.

These two flavors look superficially similar ("data we want to ignore") but
have different storage, merge semantics, and lineage implications. They are
treated as two distinct features below.

---

## Value-based filters: the named-Filter idiom (no new code)

The existing `Filter` system already supports this use case directly. Filter
expressions are first-class values — assign them to a variable, reuse them
in any `where=` clause, compose them with `&` / `|` / `~`.

### Python idiom

```python
# Define once
clean_gr = GAITRiteLoadedCycle["StepLengths_GR"] != 0   # this IS a ColumnFilter

# Use anywhere — visible at the call site, composable with one-off filters
for_each(
    mean_change_from_reference,
    inputs={
        "baseline": Fixed(GAITRiteLoadedCycle["StepLengths_GR"], session="BL"),
        "value":    GAITRiteLoadedCycle["StepLengths_GR"],
    },
    outputs=[DeltaStepLength],
    where=clean_gr & (UAStartFoot() == "A"),
)

# Compose named filters with each other
clean_and_unilateral = clean_gr & (UAStartFoot() == "U")
for_each(..., where=clean_and_unilateral)
```

### MATLAB idiom

```matlab
clean_gr = GAITRiteLoadedCycle("StepLengths_GR") ~= 0;

scidb.for_each(@meanChangeFromReference, ...
   struct('baseline', scidb.Fixed(GAITRiteLoadedCycle("StepLengths_GR"), session="BL"), ...
          'value',    GAITRiteLoadedCycle("StepLengths_GR")), ...
   {DeltaStepLength()}, ...
   where=clean_gr & (UAStartFoot() == "A"));
```

### Why no `scidb.View` class

Earlier in the design exploration a `scidb.View` wrapper was considered.
It collapsed to a marker class with no behavior — every method would have
delegated to the underlying `Filter`. Dropped in favor of the existing
named-Filter pattern, which already provides:

- ✅ Visibility at the call site (the name appears in `where=`).
- ✅ Composition via `&` / `|` / `~`.
- ✅ Reuse across call sites.
- ✅ Lineage hash uniqueness via the existing `Filter.to_key()`.
- ✅ Zero new code, zero new concepts to learn.

If a future feature needs a marker (e.g. a global registry of named
filters, IDE introspection, linting) a wrapper can be added then. For now,
YAGNI.

### Composition with existing wrappers

No new behavior — the named filter is just a `Filter`, used through the
existing `where=` plumbing.

| Wrapper | Behavior |
|---|---|
| `Fixed` | Unchanged. Filters in `where=` don't interact with `Fixed`. |
| `Merge` | Unchanged. Existing rule at `foreach.py:1385-1391` — outer `where=` is NOT propagated to Merge constituents — still applies. Inner-join across constituents at `foreach.py:1411` produces the final result, as the user specified ("Merge always inner-joins"). |
| `EachOf` | Unchanged. Each variant iterates over the same outer `where=`. |
| `ColumnSelection` | Unchanged. ColumnSelection's existing operator overloads (`__eq__`, `__ne__`, `__lt__`, etc.) already build the Filter objects users assign and reuse. |
| Coverage validator | Unchanged. `_validate_filter_coverage` at `filters.py:761` operates on the Filter regardless of how it was constructed. |

### Documentation deliverable

Add a short section to `scidb/README.md` (or a new `docs/guide/filters.md`)
illustrating the idiom: "assign a Filter to a variable, reuse it via `&`/`|`
in `where=`". The user's `meanChangeFromReference` example is a good
motivating snippet.

---

## Filter canonicalization for load-time recall

### Why this matters

`for_each(where=F)` stores `F.to_key()` as the `__where` version key on
every output record (`foreach_config.py:106-118`). Two `for_each` calls
that share schema keys but use different `where=` filters produce
multiple records at the same schema location, distinguished only by
`__where`. Concrete case:

```matlab
deltaSL_A  = scidb.for_each(..., where=UAStartFoot()=="A" & GR("StepLengths_GR")~=0);
deltaSL_U  = scidb.for_each(..., where=UAStartFoot()=="U" & GR("StepLengths_GR")~=0);
deltaSL_UA = scidb.for_each(..., where=                     GR("StepLengths_GR")~=0);
```

All three sets of `DeltaStepLength` records live at the same
`(subject, session, speed)` schema locations.

`Variable.load(where=F)` already has a code path for this:
`_load_with_where` (`database.py:2076-2146`) serializes the load-time
`where=` the same way the save path did and matches it as a string
against the stored `__where`. When the match succeeds, the user gets
exactly the variant they asked for. When it fails, the call falls back
to `where.resolve()`, which filters at the **schema_id** level — and
since all three variants above share schema_ids, the fallback cannot
disambiguate them; it returns all three (or raises
`AmbiguousVersionError`).

So recall hinges on the Strategy 1 string match. Today that match is
literal: `CompoundFilter.to_key` emits `f"({left.to_key()}) {op}
({right.to_key()})"` (`filters.py:594`), preserving the user's operand
order and parenthesization. Trivially-equivalent expressions like
`A & B` vs `B & A` produce different `__where` strings and miss.

### Proposal: `Filter._canonical()`

Each `Filter` subclass implements `_canonical()` returning a normalized
sub-tree. `Filter.to_key()` calls `self._canonical()._raw_key()` so the
save path and the `_load_with_where` augmentation agree.

The canonical form does three things, in order:

1. **Flatten right-nested chains of the same AC operator.**
   `A & (B & C)` → `AND([A, B, C])`. Likewise for OR.
2. **Sort children of `&` / `|` by their canonical key** (alphabetical).
   Handles commutativity.
3. **Normalize value reprs to JSON form.** `ColumnFilter` value
   serialization switches from Python `repr()` (`'U'`, `True`, `None`)
   to `json.dumps()` (`"U"`, `true`, `null`). This is also what the
   MATLAB-side canonicalizer must emit for cross-language parity.

Optional extensions (deferred until evidence of need):

- **De Morgan push-down**: `~(A & B)` → `~A | ~B`, plus double-negation
  cancellation. Resolves a real equivalence class, but obscures the
  canonical form when humans debug `__where` values.
- **Idempotent dedupe**: drop equal adjacent siblings under sorted `&`
  / `|`. Cheap once children are sorted.

### When does canonicalization happen?

Canonicalize **eagerly at construction time**, not lazily at `to_key()`
time. Both strategies produce the same canonical form (canonicalization
is idempotent and deterministic), but eager wins operationally.

Invariant: **every `Filter` instance is in canonical form.** `__init__`
on each subclass — and `__and__` / `__or__` / `__invert__` on the base
class — applies canonicalization before storing children.

Concrete walkthrough for a compounded named-Filter case:

```python
Filter1 = MyVar1 & ~MyVar2     # stored as CanonicalAnd(sorted([MyVar1, ~MyVar2]))
Filter2 = MyVar3 > 5           # already a leaf, trivially canonical
Combined = Filter1 & Filter2
```

- **Lazy variant**: `Combined` would store the literal binary tree
  `CompoundFilter(Filter1, Filter2, "AND")`. `to_key()` would then
  recurse top-down: re-canonicalize `Filter1` (sort `MyVar1` against
  `~MyVar2`), re-canonicalize `Filter2`, flatten the outer AND, sort the
  three leaves. Full work, every time.
- **Eager variant**: `__and__` sees both operands are canonical and
  `Filter1` is itself an AND. It does one merge-sort step — flatten
  `Filter1`'s children with `Filter2` into a single sorted three-leaf
  list — and stores the result. `to_key()` is a flat walk.

Why eager is the right call here:

1. **Composition is incremental.** Building `A & B & C & D` one
   operator at a time touches each leaf O(1) times. Lazy re-walks the
   growing tree on each new operator (or defers all the work to
   `to_key()`, which still does the full walk).
2. **Reuse pays off.** The named-Filter idiom explicitly encourages
   assigning a Filter to a variable and using it in many `where=`
   clauses. Eager canonicalizes that filter once at definition; lazy
   redoes the work on every `to_key()` (i.e. once per `for_each` and
   once per matching `load`).
3. **Clean invariant for the implementation.** `__and__` / `__or__`
   can rely on children being flat sorted lists, so the new compound
   is a merge-sort step rather than a full recursive canonicalization.
4. **`Filter.__eq__` becomes meaningful.** Two structurally-equal
   canonical trees compare equal directly — useful for tests, dedupe,
   and the `branch_params` alternative below.

Costs and caveats:

- ⚠ **Filters must be immutable.** If any code mutates `filter.value`
  or similar after construction, the canonical form goes stale. The
  current `Filter` subclasses already look effectively immutable;
  this becomes a stated invariant and should be tested.
- ⚠ **Keep `__repr__` separate from the canonical form.** Eager
  destroys the original tree shape, so error messages and debugging
  output need their own `__repr__` (which today already differs from
  `to_key()`). Do not let canonicalization leak into user-facing
  display.
- ⚠ **Construction-time work for one-off filters that are never
  serialized.** Negligible compared to running a `for_each`, but worth
  noting if a profile ever flags Filter construction in a hot path.

### What `_canonical()` does *not* attempt

This is forgiveness for syntactic noise, not a semantic equivalence
engine. It will **not** unify any of the following — users who write
filters this way will see Strategy 1 miss and fall through to the
schema-level fallback:

| Not handled | Example |
|---|---|
| Op duality | `A != 0` vs `~(A == 0)`; `A < 5` vs `~(A >= 5)` |
| OR-of-equals ↔ `isin` | `A==1 \| A==2` vs `A.isin([1, 2])` |
| Range coalescing | `A > 5 & A > 3` ≡ `A > 5`; `A >= 0 & A <= 10` vs a hypothetical `A.between(0, 10)` |
| Tautology / contradiction | `A==1 & A==2` ≡ false; `A==1 \| A!=1` ≡ true |
| Numeric / string coercion | `A == 1` vs `A == "1"` |
| `RawFilter` SQL contents | `raw_sql("A=1")` vs `raw_sql("A = 1")` — opaque to the canonicalizer |
| Mixed `RawFilter` ↔ structured | `raw_sql("StepLengths_GR != 0")` vs `GR["StepLengths_GR"] != 0` |
| Schema-version drift | The `__where` string does not encode the referenced variable's `schema_version`; if it bumps between save and load, the canonical key is unchanged but the semantics may differ. If invalidation on schema bump is desired, fold `schema_version` into the atom's canonical form (e.g. `GR@v2['StepLengths_GR']`). |

The rule of thumb the docs should communicate: **the filter you save is
the filter you load**. Canonicalization only forgives operand-order
and value-repr noise; deeper rewrites are out of scope.

### Tradeoffs

- ✅ Fixes the realistic miss cases (AC reorderings, parenthesization,
  Python/MATLAB value-repr divergence).
- ✅ Self-contained in `filters.py` plus the MATLAB-side serializer.
- ⚠ **Migration**: existing `__where` values in users' DBs are in the
  pre-canonical form. After the change, those records won't match
  new-style keys. Either leave legacy records strandable (Strategy 2
  fallback still works at the schema_id level) or write a one-shot
  migration that re-serializes `__where` for every record. Recommend
  the migration — it's a single pass over `_record_metadata`.
- ⚠ **MATLAB parity burden**: the MATLAB filter serializer must emit
  the same canonical strings as Python. Requires a written contract
  (JSON value form, sort order, operator spellings, AC operator names)
  and a cross-language round-trip test.
- ⚠ Does not help users who want a *short, stable* label for a variant
  (e.g. `"U"` instead of the full filter expression). See alternative
  below.

### Alternative: `branch_params` instead of canonicalization

A much smaller change: let `for_each` accept a `branch_params={"start_foot":
"U"}` kwarg, stored on the record alongside `__where`. Then
`load(subject=..., start_foot="U")` disambiguates with no string-match
plumbing at all — `branch_params` already participates in the existing
load disambiguation path (`variable.py:389-409`).

Loses the "filter expression IS the identity" elegance and asks users to
maintain a label separately from the filter, but trades that for:

- No canonicalization code, no MATLAB mirror, no migration.
- A human-readable disambiguator that survives filter-expression edits
  (you can refactor the `where=` clause without orphaning records, as
  long as the label stays the same).

Pick canonicalization if filter-expression-as-identity is the goal.
Pick `branch_params` if the goal is just "let me name and recall the
three variants." They are not mutually exclusive — both could exist —
but if only one ships, `branch_params` is the lower-effort answer to
the user's actual recall problem.

---

## Global Schema-ID Exclusions

A persistent registry of "schema-key combinations to skip in every
analysis." This is **not** a value-predicate — it's a set of identifiers,
stored in the database, consulted by `for_each` before loading any inputs.

This is the one genuinely new mechanism, because the existing `Filter`
system has no good way to express "this trial is bad, full stop" without
the user manually inverting a list of bad trials into a predicate.

### Python API

```python
# Mark — persisted in the DB, audited
scidb.exclude_schema(subject=1, trial=2,
                     reason="equipment malfunction during recording")
scidb.exclude_schema(subject=3,
                     reason="participant withdrew")

# Inspect — returns currently-excluded combos (latest row per combo,
# filtered to status=0)
exclusions_df = scidb.list_exclusions()
#   subject  trial  reason                              changed_at
#   1        2      equipment malfunction during ...    2026-05-26 10:14
#   3        NaN    participant withdrew                2026-05-26 10:14

# Re-include (logged, not silently removed)
scidb.include_schema(subject=1, trial=2,
                     reason="re-reviewed video, recording was valid")

# Per-call override for sanity checks
for_each(..., include_excluded=True)
```

### No-op guard

Both `exclude_schema` and `include_schema` raise an error if the
**exact same keyset** is already on record with the same `status`. The
check is on the literal keys the user passed, not on effective state —
so a more-specific assertion is always allowed, even if a wildcard row
already covers it. This lets users record finer-grained reasons without
the system second-guessing them, while still blocking truly redundant
rows.

Exact-keyset match means: same set of specified columns, same values at
those columns, and NULL at every other schema-key column. The check
finds the most recent row matching that exact keyset (if any) and
compares its `status` to the requested one.

The default (no row with that exact keyset on file) counts as included,
so `include_schema` on a never-touched keyset raises; `exclude_schema`
on the same keyset succeeds.

```python
scidb.exclude_schema(subject=1, trial=2, reason="...")   # ok
scidb.exclude_schema(subject=1, trial=2, reason="...")   # raises: same keyset already excluded
scidb.include_schema(subject=1, trial=2, reason="...")   # ok (flips status)
scidb.include_schema(subject=1, trial=2, reason="...")   # raises: same keyset already included

# Wildcard + specific: both rows allowed, both kept in the log
scidb.exclude_schema(subject=3, reason="participant withdrew")             # ok
scidb.exclude_schema(subject=3, trial=2, reason="recording corrupted")     # ok — different keyset

# Most-specific-wins illustration
scidb.include_schema(subject=3, reason="re-reviewed video for whole subject")
# Effective state now:
#   (subject=3, trial=2)   → excluded   (row at (subject=3, trial=2)
#                                        is more specific than the
#                                        (subject=3, NULL) include)
#   (subject=3, trial=1)   → included   (only the (subject=3, NULL)
#                                        rows match; the latest of
#                                        those is the include)
# To re-include trial 2 as well, the user must restate it specifically:
scidb.include_schema(subject=3, trial=2, reason="trial 2 recording was OK after all")
```

### How it works internally

1. New sciduck table `__scidb_schema_overrides` (renamed from
   `__scidb_exclusions` — the table records both directions of state
   change, so the neutral name is more accurate):
   - One column per schema key (NULL = wildcard at that level — so
     `subject=3` with NULL trial overrides every trial of subject 3).
   - `status` (BOOLEAN NOT NULL — `1` = included, `0` = excluded).
   - `reason` (TEXT NOT NULL)
   - `changed_at` (TIMESTAMP)
   - (`changed_by` removed 2026-10-08: no user identity, D-2026-10-08-1)
   - Every write is an INSERT; rows are never updated or deleted, so
     the table is its own audit trail. The default state of any combo
     (no matching rows) is implicitly *included*. `exclude_schema(...)`
     inserts a row with `status=0`; `include_schema(...)` inserts a
     row with `status=1`. Both raise if the combo is already in the
     requested state (see the "No-op guard" subsection above).
   - **Conflict resolution:** if multiple rows match a combo, the
     **most specific** row wins; ties on specificity are broken by
     **most recent** `changed_at`. Specificity = number of non-NULL
     schema-key columns in the row (a row with `subject=3, trial=2` is
     more specific than `subject=3, trial=NULL`). Rationale: a
     fine-grained assertion like `exclude(subject=3, trial=2)` should
     not be silently erased by a later broader assertion like
     `include(subject=3)`; to undo the specific row, the user must
     restate it at the same specificity.
2. In `for_each`, after the existing `distinct_schema_combinations()` step
   and before iterating, filter the combo list against the currently-
   excluded combos. To resolve "currently excluded" for a combo: collect
   all rows in `__scidb_schema_overrides` whose non-NULL columns equal
   the combo's values at those keys (NULL = wildcard); among those,
   pick the one with the most non-NULL columns (most specific), breaking
   ties by most recent `changed_at`; the combo is excluded iff that
   row's `status=0`. A combo with no matching row is included.
3. **Exclusions narrower than the iteration schema** (e.g. exclusion
   names `subject=3` but iteration is over `[subject, trial]`): handled
   by combo-level filtering above — the NULL-wildcard on `trial`
   matches every trial of subject 3.
4. **Exclusions wider than the iteration schema** (e.g. exclusion names
   `subject=3` but iteration is only over `[trial]`, so subject is not
   in the combo keys): the exclusion still applies, but cannot be
   resolved at the combo level — subject is not present there. Instead,
   the exclusion is pushed down to the data-load step as an implicit
   row-level filter (`subject != 3`) applied to every input read inside
   the iteration. This requires the exclusion table's schema keys to be
   resolvable as columns on the loaded data, which is the normal case
   since schema keys are columns.
5. Lineage: hash the full contents of `__scidb_schema_overrides` (all
   rows, both `status=0` and `status=1`) and store it as a version key
   (`__schema_overrides_hash`) so that any change to the table — new
   exclusion, new re-inclusion, even a row that only applies via
   row-level pushdown — invalidates caches.

### MATLAB API

Mirror the Python — same function names on the `scidb` namespace.

### Pros / Cons

- ✅ Centralized — one place to look up "is this data excluded?".
- ✅ Audited — never silent deletion; re-inclusion preserves history.
- ✅ Different mental model from value filters — fits the use case of
  "this session is bad, full stop" cleanly.
- ⚠ Lineage hash invalidation is global — adding one exclusion invalidates
  every cached `for_each` result that touched any data. This is correct
  but potentially expensive; users should batch their exclusion edits.
- ⚠ Wildcard matching (NULL = "any trial of this subject") needs careful
  semantics — document in the exclusion table's docstring. See points 3
  and 4 above for the narrower-vs-wider iteration cases.

---

## Lineage requirements

The existing `Filter.to_key()` already serializes composed filters (via
`&`/`|`) into version_keys correctly, so the named-Filter idiom needs no
lineage work. If filter canonicalization is adopted, `to_key()`'s output
changes but the lineage role is unchanged.

For global exclusions: hash the full `__scidb_schema_overrides` table
contents and add it as a version key under a reserved name like
`__schema_overrides_hash`.

---

## Implementation order

1. **Documentation only — named-Filter idiom.** Add a short section
   illustrating the pattern to `scidb/README.md` or a new
   `docs/guide/filters.md`. ~1 commit.

2. **Global schema-id exclusion.** New file
   `scidb/exclusions.py` with `exclude_schema`, `include_schema`,
   `list_exclusions`. Migration to add `__scidb_schema_overrides` table.
   Modification to `for_each` to skip excluded combos and include the
   `__schema_overrides_hash` in version_keys. Tests for NULL-wildcard
   semantics, the no-op guard on both `exclude_schema` and
   `include_schema`, re-inclusion audit trail, and cache invalidation
   when overrides change.

3. **MATLAB mirror for global exclusions.** Same function names on the
   `scidb` namespace.

4. **Filter canonicalization (optional, only if load-time recall of
   named-Filter variants turns out to matter in practice).** Add
   `Filter._canonical()` to each subclass in `scidb/filters.py`; route
   `to_key()` through it; mirror the canonicalization rules in the
   MATLAB filter serializer; write a one-shot migration that
   re-serializes `__where` for existing `_record_metadata` rows. Tests
   for AC-equivalence (`A & B` ≡ `B & A`), nested-AC flattening, and
   cross-language round-trip. Consider `branch_params` as the
   lower-effort alternative before committing to this work.
