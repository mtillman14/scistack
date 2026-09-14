# Plan: Case A PathInput discovery in MATLAB `scifor.for_each`

Date: 2026-09-14
Status: BUILT 2026-09-14 (Stages 1-3). Key order: template-placeholder, approved.

## Symptom

All three tests in `scimatlab/tests/matlab/scifor/TestSciforEachOf.m` fail.
`scifor.for_each` returns a **0-row table**, so:

- `test_two_pathinputs_concatenate_both_locations` — `height(result)` is 0, not 4,
  then `result.output` throws `MATLAB:table:UnrecognizedVarName`.
- `test_single_alternative_matches_direct_pathinput` — `height(direct)` and
  `height(wrapped)` agree (both 0, so that check passes), then `direct.output`
  throws.
- `test_mismatched_placeholder_keys_errors_clearly` — expects
  `scifor:for_each:EachOfColumnMismatch` and nothing is thrown: two EMPTY branch
  tables have identical (zero) columns, so `vertcat_each_of_results` finds no
  mismatch to complain about.

One cause, three presentations. EachOf is not implicated — `scidb`'s EachOf test
passes on the same fixtures once its unrelated orientation bug is fixed.

## Root cause

`scimatlab/src/scimatlab/matlab/+scifor/for_each.m:242`:

```matlab
pi = find_pathinput(inputs);
if ~isempty(pi) && any(cellfun(@isempty, meta_values))
```

With no schema keys declared and no `key=[]` kwargs, `meta_keys`/`meta_values`
are **empty**. `cellfun` over an empty cell returns `[]`, and `any([])` is
`false` — so `pi.apply_discovery` is never called, no combos are produced, and
the result is an empty table. No warning is logged; the run looks successful.

This is Python's `apply_discovery` **Case A**, documented in
`scifor/src/scifor/pathinput.py:682`:

> **Case A — no metadata iterables at all**: adopt every discovered key with all
> of its discovered values and return the discovered combos so they drive
> iteration directly.

MATLAB implements only Case B. The gate above is the Case B precondition ("at
least one declared key is empty and needs filling"), used as though it were the
precondition for discovery as a whole.

### Why the passing PathInput tests do not catch it

Both `scifor.for_each`-with-`PathInput` tests that pass declare a schema **and**
pass the keys explicitly:

- `TestPathInput.m:569,576` — `set_schema(["subject","session","speed"])` then
  `subject=[], session=[], speed=[]`
- `TestPathInput.m:634,639` — `set_schema(["subject","trial"])` then
  `subject=[], trial=[]`

Both are Case B. `TestSciforEachOf` does neither — it is the only MATLAB test
that exercises Case A.

### The gate is not the whole fix

Entering the block is necessary but not sufficient. The fill loop immediately
after (`:253-257`) only writes back keys that are **already** in `meta_keys`:

```matlab
for i = 1:numel(meta_keys)
    if isempty(meta_values{i}) && isfield(filled, char(meta_keys(i)))
        meta_values{i} = filled.(char(meta_keys(i)));
    end
end
```

In Case A `meta_keys` is empty, so this loop does nothing regardless. The keys
`apply_discovery` adopted have to be **added**, and everything downstream that
is derived from `meta_keys` has to see them:

- the `all_combos` projection (`:261`, `project_combos(discovered_combos, meta_keys)`)
- the placeholder-coverage check (`:260`, `all(ismember(meta_keys, placeholder_keys))`)
- the unresolved-key pass (`:278-300`), which must not treat a freshly adopted
  key as unresolved
- the result table's metadata columns and their types
  (`build_single_output_table`, `restore_schema_column_types`)

## Decision needed before building

**Key order.** Case A has no declared schema to order by. Python preserves the
order `apply_discovery` returns, which follows the template's placeholder order
(`{subject}/{session}/data.txt` → `subject`, `session`). MATLAB should use
`pi.placeholder_keys()` order for the same reason, so the two layers put the
result table's columns in the same order and
`cartesian_product`/`project_combos` iterate identically. Confirm before I
build — the alternative (alphabetical) is stable but diverges from Python.

## Stage 1 — adopt discovered keys when none were declared

`scimatlab/src/scimatlab/matlab/+scifor/for_each.m`

1. Widen the gate at `:242` to
   `~isempty(pi) && (isempty(meta_values) || any(cellfun(@isempty, meta_values)))`.
2. After `apply_discovery` returns, append every field of `filled` that is not
   already in `meta_keys`, in `pi.placeholder_keys()` order, with its discovered
   values. Keep existing keys in their declared positions so Case B ordering is
   untouched.
3. Make the placeholder-coverage check and `project_combos` operate on the
   extended `meta_keys`, so Case A takes the `all_combos` path (disk combos
   drive iteration) rather than a Cartesian product that would re-invent
   non-existent combinations.
4. Leave the unresolved-key pass alone: adopted keys arrive non-empty, so they
   never reach it.

## Stage 2 — logging (CLAUDE.md note 2)

5. INFO when Case A adopts keys, naming them and the combo count — this is the
   decision that determines what executes, and its silence is why an empty
   result read as "no matching files" rather than "discovery never ran".
6. WARN when a `PathInput` is present, no keys are declared, and discovery still
   yields nothing. A `for_each` that silently iterates zero times is the exact
   failure this bug produced, and it should never again be indistinguishable
   from success.

## Stage 3 — tests

7. `scimatlab/tests/matlab/scifor/TestSciforEachOf.m` — already covers Case A
   through EachOf; it should pass unmodified once Stage 1 lands. Do not add a
   `set_schema` call to make it pass: that converts it to Case B and loses the
   only Case A coverage in the suite.
8. `scimatlab/tests/matlab/scifor/TestPathInput.m` — add a direct (non-EachOf)
   Case A test: `scifor.for_each(fn, struct('filepath', pi))` with no schema and
   no key kwargs, asserting the discovered keys appear as columns in template
   order and that `height` equals the file count, not a Cartesian product.
9. `scifor/tests/test_foreach_standalone.py` — assert Python's Case A column
   order matches what Stage 1 adopts, so the parity claim in the Decision above
   is pinned by a test rather than by this document.

Test runs are the user's to invoke; one package per pytest invocation
(`project_pytest_one_package_at_a_time`).

## Not in scope

- The `cartesian_product` iteration-order fix (first key was varying fastest,
  Python's `itertools.product` varies the last fastest). Fixed separately; it
  changes Case B row order too and is independent of this gap.
- Whether `scidb.for_each` should expose Case A. It already works there: Python
  owns that path's discovery and `prep{'full_combos'}` arrives pre-built.
