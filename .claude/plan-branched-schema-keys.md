# Plan: branched schema keys

Drafted 2026-11-15. Status: approved 2026-11-15, decisions confirmed; BUILT 2026-11-15 (see "Build record" at the end).
All stages are written; only `tree_owner` and earlier have been run by the user.

## The problem

A dataset's schema keys are one ordered list, read as one hierarchy. Real
datasets branch: `brand > category > item` beside `brand > location > person`.
Two databases would duplicate `brand` and lose lineage, variants and plots
across the boundary.

## The one idea

**The tree is a default. The key set is the truth.**

- *Truth:* a record's location is the SET of keys it names. Any set is legal,
  including one that names keys from two branches (a `Purchase` at
  `brand, category, item, location, person`). Whether two things can be
  combined is always decided by comparing key sets, never by the tree.
- *Default:* the tree (`parent_of`) is only used to choose sensible defaults
  and to decide what to offer first: which level checkboxes to show, where
  `distribute` goes, what an empty variable is expected at, how a dataset-wide
  tree is drawn.

So the tree simplifies the common case and never forbids anything. There is
no firewall between branches: a variable that carries keys from both is a
first-class citizen in runs, Plot Studio and Stat Studio.

## What stays exactly as it is

- `dataset_schema_keys` is still a flat ordered list and still the single
  owner of key ORDER (column order, sort order, default nesting order of a
  plot axis). A branched schema lists parents before children.
- `_schema` is still one wide table, one column per key, NULL where a record
  does not name the key. No storage change, no identity change.
- A project that declares a plain list gets `parent_of[k] = the previous key`,
  which is today's behaviour. Every existing test must pass unchanged at every
  stage; that is the regression net.

## Rules (each gets one owner and one row group in the cases table)

Let `S(x)` be the key set of a variable or record.

| Rule | Statement |
|---|---|
| broadcast | A attaches to B iff `S(A) ⊆ S(B)`. Replaces "A's keys are a list-prefix of B's". |
| coherent | A key set is coherent iff it lies on one chain of the tree, OR it is a subset of `S(input)` for some bound input (the data shows the combination exists). |
| default level | Union of the inputs' key sets if that union is coherent; otherwise the keys EVERY input carries. Item + person inputs default to `[brand]`. Item + person + Purchase default to all five. |
| offered keys | A checkbox is offered iff ticking it alone is coherent with every input. Item input: brand, category, item. Person input: brand, location, person. Brand-only input: all five. Any cross-branch input: every key it carries. |
| disabled tick | An offered key whose tick would make the current selection incoherent is disabled, with a tooltip naming the reason. Brand-only input: ticking `category` disables `location` and `person`. |
| code is never refused | `for_each(..., schema_keys=[...])` and explicit `key=[...]` iterables accept any key set. An incoherent one logs a WARN naming the rule; it is not an error. |
| distribute target | The single child of the deepest iterated key. Two children: raise and ask for the target by name. No child: today's error. |
| empty variable's keys | `chain(registered level)`: the level and its ancestors, not a list prefix. |

Parity table: `docs/claude/schema-branch-cases.json` (same pattern as
`location-filter-cases.json`), read by the scifor tests and by the frontend
test for the checkbox state.

## Declaration

One list per branch, the same form in every surface:

```python
configure_database("app.duckdb", dataset_schema_keys=[
    ["brand", "category", "item"],
    ["brand", "location", "person"],
])
```

```matlab
scidb.configure_database("app.duckdb", {["brand","category","item"], ["brand","location","person"]})
```

- A plain list of strings means one branch (today's call, unchanged).
- Normalized once into `(keys, parent_of)`: keys in first-appearance order.
- Refused with a message naming the key: a key with two different parents
  across paths, a repeated key inside one path, an empty path.
- No edge list, no NetworkX: neither carries display order, both need a
  second structure beside them, and NetworkX has no MATLAB twin.

Text form, for the places that only have one text box (the VS Code "Create
database" and bundle-import prompts, `extension.ts` ~89 and
`bundleImport.ts` ~109): commas within a branch, semicolons between branches.

```
brand, category, item; brand, location, person
```

No semicolon means one branch, so what people type today is unchanged. The
prompt's live validation line shows the mistake (a key with two parents) or
the parsed branches. The split is trivial; the rules stay in
`SchemaTree.from_paths`, which the backend runs on the result.

Stored in the database beside the keys (new table `_schema_key_parent`,
registered in `scidb/store/tables.py`). With data present the tree can only
be EXTENDED (stage `extend_tree`); every other change goes through
`change_schema_keys` while the dataset is empty.
Reopening with a different tree raises `SchemaKeysMismatchError`, as a
different key list does today. Not in `scistack.toml`.

## Stages

One stage = one commit; the user runs the suites between stages, one package
at a time.

### characterize

No production code. Find out what really breaks today.

- `scidb/tests/test_branched_schema.py`: flat list
  `[brand, category, item, location, person]`; an item-level variable, a
  person-level variable, a brand-level variable, and a cross-branch
  `Purchase`. Save, load, `for_each` on each branch, aggregate both branches
  to brand, a run over Purchase with item-level and person-level inputs
  broadcast onto it, `distribute` from brand and from item, a `where=` filter
  from the other branch, expected locations of an empty person-level variable.
- `scistackplotdb/tests/test_branched_schema_joins.py`: brand onto item,
  person-level attribute onto Purchase, item against person.
- Failures are recorded in this file under "Findings" and marked
  `xfail(strict=True)` with the stage that fixes each, so a stage that fixes
  one must remove its mark.

### tree_owner

`scifor/src/scifor/schema.py` (already the owner of the scifor key list).

- `SchemaTree` (frozen): `keys`, `parent_of`; `from_paths(paths)`,
  `linear(keys)`; `ancestors(k)`, `chain(k)`, `children(k)`,
  `leaves(key_set)`, `is_chain(key_set)`, `coherent(key_set, input_sets)`,
  `default_level(input_sets)`, `offered(input_sets, ticked)`.
- `set_schema(keys_or_paths)` accepts both forms; `get_schema()` still returns
  the flat list; `get_schema_tree()` returns the tree.
- Pure, no I/O, scifor stays standalone. One INFO line when a tree is set:
  keys, parents, whether it is linear.
- Tests: `scifor/tests/test_schema_tree.py`, driven by the cases JSON.

### declare_and_store

- `configure_database` / `DatabaseManager.__init__`: accept paths, normalize
  through `SchemaTree.from_paths`, keep `self.dataset_schema_keys` (flat) and
  add `self.schema_tree`. This is a two-line change in the constructor, not a
  new feature inside `DatabaseManager`.
- `_schema_key_parent` table; written at create, checked at open.
- `change_schema_keys(db, keys, ...)` accepts paths and rewrites the parents.
- `SchemaKeysMismatchError` reports a parent mismatch.
- MATLAB `+scidb/configure_database.m` and the bridge wire: a cell of string
  arrays crosses as a list of lists.
- Dataset intent in bundles (`project_dataset_intent_bundle`): parents travel
  with the key types.
- Tests: create, reopen, mismatch, change-while-empty, refuse-with-data,
  bundle round trip, MATLAB `TestConfigureDatabase`.

### extend_tree

Add a branch (or a deeper level) to a dataset that already holds data.

Why it is safe: a NULL in a key column already means "this record does not
name this key", which is exactly true of every existing record with respect
to a key that did not exist. So nothing existing changes meaning.

- `scidb.store.schema_keys_change.extend_schema_keys(db, paths, reason=)`.
- Allowed: new keys only, each attached under an existing key or under
  another new key, appended to the END of the flat list. The existing keys,
  their order and their parents must be unchanged.
- Refused with data present: rename, delete, re-parent, re-order, and
  inserting a key BETWEEN two existing keys of a chain (existing records
  would silently become records that skip a level).
- One transaction, re-using the existing machinery: drop the variable views,
  add the columns to every table in `schema_keyed_tables()` (or rebuild and
  copy, as the intent tables already do, if DuckDB refuses the ALTER),
  append the `_schema_key_parent` rows, re-create the views from
  `variable_view_sql`.
- **Unused keys are free; used keys are fixed** (added 2026-11-15). The same
  argument run backwards: a key no record names can be renamed, re-parented
  or deleted without changing the meaning of any record.
  - Owner: `scidb.store.emptiness.key_usage(db)` returns, per key, how many
    data records name it (non-internal `_record` rows joined to a `_schema`
    row where the key is non-NULL), plus what else names it: run levels
    (`_invocation.iteration_level`), exclusions, expectation marks. Rolled up
    per branch for display: "brand, category, item: in use (1,240 records);
    location, person: unused".
  - A key is USED if any record or any recorded run level names it. A used
    key keeps its name, parent and position.
  - An unused key can be renamed or re-parented. It can be deleted only
    together with every key below it, all unused. Intent rows naming it
    (exclusions, expectation marks) follow the existing rule: carried on a
    rename, and a delete is refused until they are removed, never dropped
    silently. Leftover history naming it (failed-run outcomes, `_schema`
    rows with no record) is removed with it and counted in the log line.
  - `is_empty(db)` becomes the special case "every key is unused", so the
    existing change-while-empty path and this one are one rule with one
    check, re-run inside the transaction.
  - Tests: extend then delete restores the original database (same ids,
    same frames); delete refused once one record is saved at `person`;
    delete of `location` alone refused while `person` stays; rename of an
    unused key carries its expectation marks.
- Afterwards the session's key list is stale in three places that hold a
  copy: `scifor.set_schema`, the GUI session, any open MATLAB session. The
  GUI half reopens the database as the change-keys service does today; a
  second process gets `SchemaKeysMismatchError` naming the added keys.
- Invariant test (the point of the stage): snapshot every `record_id`,
  `schema_id`, `invocation_id`, node state and each variable's loaded frame;
  extend; all identical. Then save at the new branch and run on it.
- GUI: an "Add a branch" action in the Schema Keys dialog, which today is
  only reachable while the dataset is empty.

### swap_order_sites

Each site trades list position for a call to the tree. With a linear tree the
result is identical, so existing suites must stay green.

| Site | Today | Becomes |
|---|---|---|
| `scifor/foreach.py` `resolve_distribute_target` (~1051) | next key in the list | single child, or raise |
| `scifor/distribute.py` `resolve_target` | walks the list downward | walks the chain below `start_key` |
| `scidb/provenance/expectation.py` (~865) | `keys[: index(level) + 1]` | `chain(level)` |
| `scidb/model/filters.py` (~347, ~1177, `_get_variable_level_idx`) | prefix up to the deepest populated index | the populated key set itself |
| `scidb/model/exceptions.py` (~378) | index of level | chain |
| `scidb/run/foreach_load.py` (~727) deepest populated index | index | leaves of the populated set |
| `scidb/inspect/render.py` schema tree | nests by list order | nests by `parent_of` |

MATLAB reads `distribute_key` from the Python run plan
(`scifor/matlab_loop.py`), so it follows without a second implementation;
the stage confirms that with a MATLAB test rather than assuming it.

Each swap logs nothing new on the linear path and one INFO line when the tree
changes the answer.

### level_default

Owner: `scidb/run/schema_level.py` (already the owner of "which keys does
this run iterate" for the GUI run, the compiled pipeline and both MATLAB
routes).

- Rule 3 of `resolve_schema_level` (inputs imply the level) calls
  `tree.default_level(levels)` instead of `finest_schema_keys`' plain union.
  The recorded-level narrowing uses the same call.
- `SchemaLevel.to_json` gains `offered` and `disabled: {key: reason}`.
- `FunctionNode.tsx` renders its level checkboxes from that payload; it holds
  no rule of its own. Frontend test reads the cases JSON.
- New rule strings (`RULE_INPUTS_COMMON`) so the panel says why it chose
  `[brand]` for an item + person node.
- Tests: `scidb/tests/test_schema_level_branches.py`,
  `scistack-gui/tests/test_schema_level_service.py`.

### subset_joins

Where "no firewall" is delivered for Plot Studio and Stat Studio.

- `scistackplotdb/hierarchy.py` `join_kind`: `broadcast` iff key-set subset.
  The merge is on the smaller set (unchanged mechanics).
- `scistackplotdb/source.py` (~939) factor-variable attach: same subset test.
  A person-level `AgeGroup` then groups a cross-branch `Purchase` plot, and
  `category` and `person` can be used together as grouping factors because
  the Purchase table carries both.
- Refusal message for the remaining case (item-level measure, person-level
  factor, nothing linking them): say that no record names both an item and a
  person, and that a variable saved at both is what would link them.
- Nested axis order keeps using the flat key order, so a cross-branch figure
  has a defined default nesting and the user can still reorder.
- Stat Studio shares `LongTable` and `factor_depths`
  (`scistackstats/tables.py`); the stage audits its nesting assumptions with
  one cross-branch test and records anything found here before changing it.
- Tests: the characterize join tests lose their xfail marks; one Plot Studio
  spec and one stats table over Purchase grouped by category and person.

### gui_declaration

- `SchemaKeysDialog.tsx`: one editable row per branch, read-only indented
  tree beside it, validation messages from the backend owner.
- `ProjectBootstrapWizard.tsx`: same row editor (one component, two users).
- Dataset-wide trees (inventory, coverage popup, location picker): build from
  the VARIABLE's key set, which is a chain or a cross-branch set in flat
  order. Audit each; change only those found drawing `location` under `item`.
- Update `docs/guide/gui-manual-testing` with the branched checks.

### docs

- `docs/claude/branched-schema-keys.md`: the one idea, the rules table, the
  owners.
- Fix `docs/claude/schema-hierarchy-contiguity.md` (it still describes
  `_schema_key_columns`, which no longer exists).
- `README.md`, `docs/api/scidb.md`, `docs/api/scifor.md`, `scifor/README.md`.

## Owners after the change

| Concept | Owner |
|---|---|
| key order | `dataset_schema_keys` (flat list), unchanged |
| parents, ancestry, coherence, default level, offered keys | `scifor.schema.SchemaTree` |
| which keys a run iterates | `scidb.run.schema_level` (calls the tree) |
| can A attach to B | `scistackplotdb.hierarchy.join_kind` (key-set subset) |
| stored parents | `_schema_key_parent`, written only by create and `change_schema_keys` |

## Not in this plan (backlog)

- A key with two parents. `parent_of` stays single-valued; a record naming
  keys from two branches already covers the use.
- Iterating the cross product of two branches when no input carries both
  (every item against every person of a brand).
- Joining two branches THROUGH a linking variable (item measure grouped by
  person via Purchase) without the measure itself being cross-branch.
- A node-and-edge diagram editor for the schema.

## Decisions (confirmed by the user, 2026-11-15)

- **Coherent rule.** A key set is coherent iff it lies on one chain, or some
  bound input carries all of it. It decides the default level and which
  checkboxes are enabled; it never refuses code.
- **`distribute` from a key with two children** raises and asks for the
  target by name. It does not guess the first branch.
- **Used keys are fixed, unused keys are free.** New branches and deeper
  levels may be added to a dataset that holds data (`extend_tree`). A key no
  record or run names can be renamed, re-parented or deleted (with
  everything below it). A key that is in use keeps its name, parent and
  position; mid-chain insert between two used keys stays refused.
- **No firewall between branches.** Any combination of keys from different
  branches stays usable, in runs and in Plot Studio / Stat Studio.
- **Declaration is one list per branch**; the one-text-box form uses commas
  within a branch and semicolons between branches.

No decisions are open.

## Findings

(filled in by the characterize stage)

### characterize run, 2026-11-15

Run by the user. Tests: `scidb/tests/test_branched_schema.py`,
`scistackplotdb/tests/test_branched_schema_joins.py`. Passing today: save/load
on every branch, aggregation of both branches to brand, brand/item and
item/person `join_kind`, the refusal for item measure + person factor.

| Finding | Test | Fixed in |
|---|---|---|
| `distribute` from `brand` takes the next list key (`category`) silently instead of raising | `TestDistribute::test_distribute_from_brand_is_ambiguous` | swap_order_sites |
| `distribute` from `item` does not raise (next list key is `location`, the wrong branch) | `TestDistribute::test_distribute_from_item_has_no_child_today` | swap_order_sites |
| Empty person-level variable expects the list prefix (brand, category, item, location, person) | `TestExpectedLocationsOfEmptyVariable` | swap_order_sites |
| (retracted: see the diagnosis below) | | |
| `join_kind` calls person branch onto five-key set `incompatible` | `test_person_branch_onto_purchase_broadcasts_by_key_set` | subset_joins |
| Plot Studio factor attach refuses `PersonAge` on `Purchase` ("not a prefix") | `test_person_level_factor_reaches_purchase_measure` | subset_joins |

Test bug found and fixed: the first per-branch `for_each` tests passed no
iterables, which aggregates (existing rule), not a finding.

### Broadcast onto `Purchase`: diagnosed, not a finding (2026-11-15)

The "empty frame" was a test bug. With no iterables `for_each` aggregates, so
`float(price)` received an `(8, 1)` array and the call failed. With all five
keys iterated, an item input and a person input both broadcast onto `Purchase`
correctly (one call at B1/c1/i1/l1/p1, scalars in).

Why it works today: `foreach_load._drop_unpopulated_schema_columns` drops a
loaded input's all-NULL key columns (`[coarse-input]` log line), so the input
filters only on keys it names. That is already the key-set rule, not a list
prefix. Consequences for the plan:

- **No `swap_order_sites` row is needed for input broadcast.** The `broadcast`
  rule is already true in `for_each`; only `scistackplotdb` (`subset_joins`)
  still uses list prefixes.
- Open: the NULL-drop only drops COLUMNS THAT ARE ENTIRELY NULL. A variable
  saved at mixed granularity keeps a partially populated key. Unchanged, but
  `tree_owner` tests should cover a variable that names item for some records
  and item+person for others.
- With no iterables the default level is "iterates nothing". Choosing the
  five keys (or `[brand]` for item + person inputs) is stage `level_default`.

## Build record (2026-11-15)

All eight stages written in one session; pytest was run by the user through
`tree_owner` (green). Everything after it is **written and statically checked
(pyright: no undefined names) but not run**. The user runs the suites at the end.

| Stage | Where |
|---|---|
| characterize | `scidb/tests/test_branched_schema.py`, `scistackplotdb/tests/test_branched_schema_joins.py` (xfails removed as stages landed) |
| tree_owner | `scifor/schema.py` (`SchemaTree`, `as_tree`, `tree_for`, `parse_schema_text`), `scifor/tests/test_schema_tree.py`, `docs/claude/schema-branch-cases.json` |
| declare_and_store | `scidb/store/schema_tree_store.py`, `_schema_key_parent` (tables.py), `DatabaseManager`/`configure_database`, bundle manifest `schema_parents`, MATLAB `configure_database.m` |
| extend_tree | `schema_keys_change.extend_schema_keys` + `_change_in_place`, `emptiness.key_usage` / `usage_by_branch`, `scidb/tests/test_extend_schema_keys.py` |
| swap_order_sites | `scifor/foreach.py` `_distribute_target`, `scifor/distribute.py` `resolve_target`, `expectation._variable_keys`, `filters.schema_id_key_closure`, `foreach_load` merge levels, `DuplicateLocationError.distribute_target` |
| level_default | `schema_level.py` (`RULE_INPUTS_COMMON`, `level_choices`, `to_json(offered)`), `query.finest_schema_keys`, `execution_service`, `FunctionSettingsPanel` + `schemaLevelBoxes.ts` |
| subset_joins | `scistackplotdb/hierarchy.py` `join_kind`, `source.py` factor attach + depth |
| gui_declaration | wizard + `SchemaKeysDialog` ("Add a schema key"), `RemedyDialog`, VS Code prompts (`schemaKeysText.ts`), CLI text form |
| docs | `docs/claude/branched-schema-keys.md`, rewritten `schema-hierarchy-contiguity.md`, API docs, manual-testing entry |

### Deviations from the plan, and things found while building

- **Level checkboxes live in `FunctionSettingsPanel.tsx`**, not `FunctionNode.tsx`.
- **`distribute` with several children**: raises and names the children and the
  way out (iterate down one of them first), but there is no `distribute="key"`
  to name the target. That would be a new kwarg in five places (grugbrain);
  left in the backlog.
- **Broadcast in `for_each` needed no change** (see "Broadcast onto Purchase").
- **Inspector's data tree** (`inspect/api.py schema_tree`) already nests by the
  populated keys only, so a person row never sits under `category`; a cross-branch
  row is listed in flat order. No change.
- **Dataset-wide trees audited** (location picker, coverage, inventory): all built
  from the variable's own populated keys in flat order. No change.
- **Stat Studio audit**: covariate joins already match on "every key it names is a
  column of the rows". The shared factor depth is now the flat position of the
  last pinning key (was `len(keys)`; identical on a prefix).
- **Key-set rule for filters**: a variable's level is the closure of its populated
  keys (`schema_id_key_closure`), identical to the old index on a linear schema.
  `where=` from an unrelated branch now raises with the reason.
- **Text-form parity**: the VS Code prompt re-checks the three typing mistakes in
  TypeScript (no server to ask); `schemaKeysText.test.ts` holds it to the `refused`
  cases in the shared JSON. The webview wizard only splits; the backend refuses.
- **Stale remedy wording**: the duplicate-location remedy no longer says a dataset
  with data "can't be changed"; it offers "Add a schema key…".
- **Unverified DuckDB behaviour**: `_change_in_place` uses `ALTER TABLE ... ADD / DROP
  / RENAME COLUMN` on `_schema` (which has a primary key) after dropping the views.
  If DuckDB refuses the ALTER, the plan's fallback applies: rebuild-and-copy as the
  intent tables are rebuilt. The transaction rolls back, so nothing is left half-done.
- **Regenerate the API types** once: `python scistack-gui/tools/gen_ts_types.py`
  (`ChangeSchemaKeysRequest.keys` now accepts branches).
