# Branched schema keys

Plan: `.claude/plan-branched-schema-keys.md`. Rules as data:
`docs/claude/schema-branch-cases.json` (read by `scifor/tests/test_schema_tree.py`,
`frontend/.../schemaLevelBoxes.test.ts`, `schemaText.test.ts` and
`extension/src/schemaKeysText.test.ts`).

## The one idea

**The tree is a default. The key set is the truth.**

Real datasets branch: `brand > category > item` beside `brand > location > person`.
Two databases would lose lineage, variants and plots across the boundary, so one
database holds both.

- *Truth:* a record's location is the SET of keys it names. Any set is legal,
  including one from two branches (a `Purchase` at brand, category, item,
  location, person). Whether two things combine is always decided by comparing
  key sets, never by the tree.
- *Default:* the tree (`parent_of`) only picks sensible defaults and what to
  offer first: which level checkboxes to show, where `distribute` goes, what an
  empty variable is expected at.

The tree never forbids anything. There is no firewall between branches.

## Declaring it

One list per branch, the same form everywhere:

```python
configure_database("app.duckdb", [
    ["brand", "category", "item"],
    ["brand", "location", "person"],
])
```

```matlab
scidb.configure_database("app.duckdb", {["brand","category","item"], ["brand","location","person"]})
```

A plain list is one branch (what it always was). Where there is one text box
(VS Code prompts, the GUI wizard, `--schema-keys`) commas separate keys and
semicolons separate branches: `brand, category, item; brand, location, person`.

Refused, naming the key: a key with two different parents, a key repeated in a
branch, an empty branch. Keys are ordered by first appearance, so a parent always
precedes its children.

## Owners

| Concept | Owner |
|---|---|
| key ORDER (columns, sort, default nesting) | `dataset_schema_keys`, the flat list, unchanged |
| parents, ancestry, coherence, default level, offered keys | `scifor.schema.SchemaTree` |
| the text form (split / format) | `scifor.parse_schema_text` / `format_schema_text` |
| stored parents | `_schema_key_parent`, via `scidb.store.schema_tree_store` (written only by create, `change_schema_keys`, `extend_schema_keys`) |
| which keys a run iterates | `scidb.run.schema_level` (calls the tree) |
| what key set a variable lives at | `scidb.model.filters.schema_id_key_closure` |
| which keys are used / unused | `scidb.store.emptiness.key_usage` |
| can A attach to B (plots, stats) | `scistackplotdb.hierarchy.join_kind` (key-set subset) |

`_schema` is still one wide table, a column per key, NULL where a record does not
name the key. No storage change, no identity change. Reopening with a different
tree raises `SchemaKeysMismatchError` naming the keys whose parent differs. A
bundle carries the tree as `project.schema_parents` in its manifest; the table
itself is not bundled (an import's key map could rename a parent out from under
it).

## The rules

`S(x)` is the key set of a variable. A *chain* is a key and its ancestors.

| Rule | Statement |
|---|---|
| broadcast | A attaches to B iff `S(A)` is a subset of `S(B)`. A coarse input's all-NULL key columns are dropped (`[coarse-input]` in the log), so it filters only on keys it names. |
| coherent | A key set is coherent iff it lies on one chain, or is a subset of some bound input's key set. |
| default level | The union of the inputs' key sets if coherent; otherwise the keys EVERY input carries. Item + person inputs default to `[brand]`; adding a Purchase input gives all five. A dataset-level input (no keys) neither adds nor narrows. |
| offered keys | A checkbox is offered iff adding it to each input's key set stays coherent. |
| disabled tick | An offered key whose tick would make the current selection incoherent is disabled, with a reason. |
| code is never refused | `schema_keys=[...]` and explicit key iterables accept any set. |
| distribute target | The single child of the deepest iterated key. Several children: raise, naming them. No child: the old error. In a returned table, the table's own columns pick among several children. |
| empty variable's keys | `chain(registered level)`, not a list prefix. |

## Changing the tree of a dataset that holds data

**Unused keys are free; used keys are fixed.** A NULL in a key column already
means "this record does not name this key", so:

- a NEW key (a branch, a deeper level) changes the meaning of nothing that
  exists: `extend_schema_keys(db, paths)`;
- a key no record and no run names can be renamed, re-parented or deleted
  (with everything below it), by `change_schema_keys` (`key_usage` says which);
- a used key keeps its name, parent and position. New keys go after the
  existing ones; none can be inserted between.

An empty dataset is the case where every key is unused and takes the full
rebuild. Intent rows naming a deleted key (exclusions, expectation marks) block
the delete until removed; they follow a rename. Both calls close the database:
the caller's key list is stale, so reopen it (the GUI reopens itself).

## Where it shows

- **Level checkboxes** (`FunctionSettingsPanel`): rendered from the panel payload's
  `offered` and `disabled`; no rule in the frontend.
- **Plot Studio / Stat Studio**: any key from any branch can be a grouping factor
  of a table that carries it. A factor attaches when its keys are a subset of the
  table's; its nesting depth is its position in the flat key order. Stat covariate
  joins already matched on "every key it names is a column of the rows".
- **Location trees** (picker, coverage, inventory): built from the VARIABLE's own
  populated keys in flat order, so a person-level variable never shows `category`.

## Not in the plan (backlog)

- A key with two parents (`parent_of` stays single-valued).
- Iterating the cross product of two branches when no input carries both.
- Joining two branches THROUGH a linking variable without the measure itself
  carrying both.
- A node-and-edge diagram editor for the schema.
- Choosing a `distribute` target by name when a key has several children.
