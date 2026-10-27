# Plan: Exclude / Include actions in the Data inventory popup

Date: 2026-10-26. Status: implemented 2026-10-26 (all four stages); tests not yet run.

## Goal

From the 📋 Inventory popup, a user can exclude a row (a schema-key
combination, or a collapsed block) with a reason, and re-include an excluded
row with a reason. The writes go through `scidb.exclude_schema` /
`scidb.include_schema`. Today the popup only displays the `excluded` count and
status.

## Findings that shape the plan

1. **A row is not a keyset yet.** A row holds the Cross + Within key values, a
   `status` cell in the All view, and `(all)` (`ALL_LEVELS`) in collapsed
   deeper Cross columns. Off keys are absent. Turning a row into the
   `**schema_keys` for `exclude_schema` means dropping `status` and every
   `(all)` cell. A collapsed block `subject=C03, session=(all)` becomes
   `{subject: C03}`, which `exclusion_status` already treats as a wildcard over
   every session (and every Off-key value). That interpretation belongs in
   `scidb.inventory` (it owns `all_marker` and the columns), not in the TSX.
2. **Re-including is blocked in a common case.** `include_schema` raises
   "has no exclusion record" unless the exact keyset has a row on file. If
   `subject=S01` was excluded, the inventory row `S01/BL` shows as excluded,
   but `include_schema(subject=S01, session=BL)` raises, even though inserting
   that more-specific include row is exactly how most-specific-wins is meant
   to work. Because the most specific matching row for a keyset is always its
   exact row when one exists, the guard can become "raise iff the keyset's
   **effective** status is already included". That is the same answer today
   whenever an exact row exists, and it unblocks the partial re-include.
3. **Nobody can say *which* override governs a combo.** `exclusion_status`
   computes the winning row and throws it away. The popup needs it to show
   "excluded by subject=S01 — 'participant withdrew' (2026-10-03)".
4. Plot caches already self-invalidate on `__scidb_schema_overrides` changes
   (`provenance_query.py` variable fingerprint). Handlers have
   `notify_dag_updated=True` to refresh the canvas.

## Stage 1: scidb (owner of all the rules)

`scidb/src/scidb/exclusions.py`
- New `Override` dataclass (`keys: dict[str, str]`, `included: bool`,
  `reason: str`, `changed_at`) and `governing_overrides(combos,
  schema_keys_order, db) -> list[Override | None]`: **the one owner** of the
  match + most-specific + latest-wins rule. The SELECT gains `reason`.
- `exclusion_status` becomes a consumer:
  `[o is not None and not o.included for o in governing_overrides(...)]`.
- `include_schema` guard: raise iff the keyset's effective status (from
  `governing_overrides([keyset], dataset_schema_keys)`) is included. The
  message names the governing row, or says "never excluded". `exclude_schema`
  keeps its literal exact-keyset guard (finer reasons stay allowed).
- DEBUG log: `include_schema: keyset=… governed by …=… (status, reason)`.

`scidb/src/scidb/inventory.py`
- `row_keyset(columns, row, all_marker=ALL_LEVELS) -> dict[str, str]`: drops
  `status` and `(all)` cells. Raises `ValueError` on an empty result or a
  length mismatch.

Tests (`scidb/tests/test_exclusions.py`, `test_inventory.py`):
- `governing_overrides`: no row; wildcard; specific beats wildcard; tie broken
  by latest; a deeper row doesn't match a coarser combo; the reason is returned.
- Include under a coarser exclusion succeeds and the inventory flips only that
  combo. Include of a never-excluded keyset still raises. Double include still
  raises.
- `row_keyset` on a plain row, a collapsed row, an All-view row, and a Within
  stratum row.
- Regression: every existing `exclusion_status` test passes unchanged.

## Stage 2: GUI backend (`scistack-gui/scistack_gui/api/schema.py`)

Three handlers, thin wrappers only:
- `inventory_row_override` (read): `{columns, row}` → `{keys, effective:
  "excluded"|"included", governed_by: Override|null}`, using `row_keyset` +
  `governing_overrides`. It fills the confirm panel.
- `exclude_schema_combo` / `include_schema_combo` (write): `{columns, row,
  reason}` → `{keys}`. An empty reason gives 400. `ValueError` gives 400.
  `undoable=False` (a database fact, like `set_schema_key_type`), and
  `notify_dag_updated=True`.
- INFO log: `[schema] exclude_schema_combo keys=… reason=…`.

Tests: extend `scistack-gui/tests/test_schema_inventory_api.py`. Exclude a
row, then the inventory counts change. Exclude a collapsed row, then the whole
block is excluded. Include under a coarser exclusion. Missing reason gives 400.
A duplicate exclude gives 400 with scidb's message.

## Stage 3: Frontend (`InventoryPopup.tsx`)

- An action column at the end of each row: **Exclude…** for missing or present
  rows, **Include…** for excluded rows. In the Missing-only view, every row is
  Exclude.
- Clicking it opens an inline panel under the table (no `window.prompt`). The
  panel shows:
  - the exact keyset in words, e.g. `subject=S01, session=BL — every trial`.
    Omitted keys are spelled out so a wildcard is never a surprise.
  - for Include, the governing exclusion and its reason, and whether this
    re-includes only this row (a more specific include) or lifts the
    exclusion itself (same keyset).
  - a reason textbox (required), and Confirm / Cancel.
- On success, show a green message, re-run the inventory and close the panel.
  On error, show scidb's message in the panel.
- Rebuild the bundle (`npm run build`). See the frontend-bundle-rebuild trap.

## Stage 4: Docs

- `docs/claude/data-inventory.md`: Entry points (GUI actions), Owners table
  (`governing_overrides`, `row_keyset`).
- `docs/claude/permanent-filters-design.md` and `docs/guide/filters.md`: the
  new include guard.
- `docs/gui-manual-testing-todo.md`: steps for exclude, collapsed exclude,
  partial re-include, and the error path.
- MATLAB `+scidb/include_schema.m` wraps the Python function, so it picks up
  the guard change automatically. Add a note to its help text.

## Out of scope (possible follow-ups)

- Multi-row selection or batch exclude.
- An "exclusion history" view of every row on file (`list_exclusions` shows
  only the current ones).
- Excluding by an arbitrary keyset not shown as a row.

## Decisions

- 2026-10-26: Include… on a row always targets **that row's keyset**. When a
  coarser exclusion governs, it inserts a more-specific include and the rest of
  the block stays excluded. This needs the include guard change (finding 2).
  To lift the coarser exclusion, the user clicks Include… on the collapsed row
  for that block, or uses Python/CLI.
