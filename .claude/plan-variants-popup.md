# Plan: one Variants popup (replaces the Provenance + Variants panels)

*Drafted 2026-09-30, revised the same day after user feedback (rev 2).
Approved 2026-09-30. Status: Stages 1-4 built + tests pass 2026-09-30. Stage 5 built
2026-09-30 (tsc + npm test pass, both bundles rebuilt; GUI pytest pending; not
visually checked, GUI item 0zzr). Stage 6 docs done the same day. Deviation: removing the
old `variable_provenance` / `variable_topologies` RPCs moved from Stage 4 to
Stage 5, together with the panels that call them.*

## What the user asked for

Right-click a Variable node → **Variants** → a popup of collapsible cards.

- **One card = one variant**: a distinct combination of settings, code versions
  and run options along the WHOLE upstream chain. The runs that produced it are
  listed inside the card.
- **Indicator** for which variant is current (the default).
- **Two buttons per card:**
  - **Make current** marks it as the variable's **default**. The default is what
    anything gets when it loads the variable *without naming a variant*:
    downstream processing, Plot Studio's opening choice, stats. Other variants
    are **not** hidden. Anything that names a variant (`Variant(X, low_hz=10)`,
    Plot Studio variant rows) still gets it, so variants can be plotted against
    each other. The pin sticks until the user changes it.
  - **Delete** really deletes the variant, for erroneous runs that shouldn't be
    part of the record. This is the one sanctioned exception to "never delete"
    (see memory `feedback_never_delete_mark_hidden`).
- Expanding a card shows:
  - the entire upstream pipeline, as a read-only mini DAG with every setting at
    every step;
  - every schema location the variant exists at;
  - the runs that produced it.
- Remove the 🔍 Provenance and 🧬 Variants header buttons and their panels. The
  CLI commands stay.
- Backend: the same Python code the CLI uses (`Inspector` / `Mutator`), passed
  as structured data. No parsing of CLI text, and no subprocess.

## Decisions (user, 2026-09-30)

| # | Question | Decision |
|---|---|---|
| D1 | What is a card? | One variant; its runs are listed inside |
| D2 | What does "current" mean? | The **default** selection. Exactly one per variable when pinned. It never hides anything |
| D3 | Pinned variant missing at a location | **Strict:** the default load returns nothing there, so downstream skips those locations. Other variants are still reachable by naming them |
| D4 | Downstream of a pin | Follows automatically (see "Default = a filter in coordinate space") |
| D5 | A downstream pin that disagrees with an upstream pin | **Allowed; the downstream pin wins** for that variable and everything below it. The card notes the disagreement |
| D6 | GUI run that writes to a pinned variable | **Ask:** Keep pin (new output is not the default) / Move pin to the new output / Cancel |
| D7 | Delete: what goes | The variant **and everything computed from it**, previewed per variable with counts first |
| D8 | Delete: trace | A **tombstone** audit row (variable, variant settings, counts, who, when, reason). No data is kept |
| D9 | Deleted settings still on the canvas | **Offer to remove the value** from the canvas Parameter. If accepted, **every variant of every variable** built from that value is deleted too, so nothing inconsistent survives. The full list appears in the confirmation |
| D10 | Backend | The shared Python API, not CLI text |
| D11 | Old panels | Remove both buttons and panels; keep the CLI |

## Why the existing `variants` output can't be used as-is

`Inspector.variants` / `topologies` groups records by the **last step's**
settings only (`VariantSummary.constants` is one hop). Two variants that differ
two steps upstream (e.g. `filter.low_hz`) come out as one row. Cards must be
full-chain variants, so the grouping has to be extended **in scidb**, and the
CLI has to render the extended version.

## Default = a filter in coordinate space (why downstream follows for free)

A pin is stored as the card's `selection`, written in the same keys as every
record's coordinate (`filter.low_hz`, `__code__.detect`, `__run__.detect`, per
`docs/claude/variant-space.md`). Downstream records carry their upstream
constants in their own coordinate. So the default for StepLength is:

    effective_default(StepLength) =
        merge(pins on every variable in StepLength's upstream chain,
              StepLength's own pin)          # nearer pin wins per key (D5)

The result is applied as a `branch_params_filter`, through
`records_for_variant` so the collapse trap (`pin_loads_uncollapsed`) is
respected. Nothing is hidden and no record is touched.

**One owner:** `scidb.variant_pins.effective_default(db, variable)`. Every
default-reading surface consumes it. None of them re-derives it.

**Consumers**, where an unnamed load becomes "the default":

- the for_each input loader: an input with no `Variant(...)`;
- `load()` with no variant kwargs;
- the node-state / expected-invocation predictor. **Must** consume it, or
  downstream nodes would stay red waiting for variants the default skips.
  Today it cross-products every coexisting input variant;
- Plot Studio's default variant selection (`project_default_variant_selection`);
- stats (`scistackstats`) default input resolution.

**Not consumers** (they keep seeing everything):

- `version_id="all"`, record_id lookups, the Inspector, and explicit
  `Variant(...)`;
- `AcrossVariants(X)`, which explicitly pools every variant, so a pin must not
  narrow it.

**No pin → today's behaviour** (every current variant flows).

**Performance:** the pins table is tiny. Cache it per `DatabaseManager` and
invalidate on pin writes. Log the cache hit/miss at DEBUG, and time the extra
filter in the for_each `[timing]` table.

## Stages

### Stage 1: scidb read side, `Inspector.variant_cards(variable)`

Returns `VariantCards(variable, cards, pin, effective_default)`. Each
`VariantCard` has:

- `selection`: the canonical dict naming exactly this variant.
  **Invariant (tested):** `records_for_variant(card.selection)` ≡
  `card.record_ids`.
- `distinguishing`: the axes whose value differs from a sibling card, with this
  card's values. The collapsed header shows these, and scidb decides them.
- `is_default` (from `effective_default`), `is_pinned`, `default_source` (this
  variable's pin or an upstream one), and `conflicts_with_upstream_pin` (D5).
- `verdict`: the existing `variant_verdict` vocabulary (a code/run re-run
  supersedes an older one).
- `record_count`, `first_saved`, `last_saved`, and `locations` (**all** of them,
  plus the ordered key names).
- `runs`: `RunRef`s, newest first.
- `upstream`: nodes and edges of the full chain, aggregated across the
  variant's records. Function nodes carry fn, code version, constants, run
  options and PathInput specs. A non-uniform setting reports every value with
  its count and logs a WARN.

CLI: `scidb variants X --cards [--json]`.

Logging (INFO): card count, records per card, distinguishing axes, and timing
per batched phase.

Tests are in `scidb/tests/test_variant_cards.py`:

- an upstream-only difference yields two cards;
- the selection round-trip;
- `distinguishing` lists only the differing axes;
- code-version and run-option splits;
- a raw-saved variable;
- bounded query count;
- CLI `--json` matches the Python object.

### Stage 2: scidb pins (the default)

- New table `_variant_pin(variable, selection_json, reason, pinned_by,
  pinned_at, released_at)`. Rows are never removed: moving or releasing a pin
  sets `released_at`, which leaves a history. This is new storage, and there's
  no way around it: a durable default has to live somewhere, and no existing
  column means "default".
- `scidb/variant_pins.py`: `pin_variant`, `release_pin`, `active_pins`,
  `effective_default`.
- `Mutator.pin_variant` / `release_pin` (with `@_mutation`) and
  `Inspector.variant_pins()`.
- CLI: `scidb pin X --variant K=V … --reason …` and `scidb unpin X --reason …`.
- Wire every consumer listed above.
- Terminal for_each saving new records to a pinned variable: log an INFO note
  that the new output is not the default. The GUI asks instead (Stage 5).
- Tests:
  - after a pin, an unnamed for_each input / `load()` gets only the default,
    and a named `Variant(...)` still gets the others;
  - `AcrossVariants` is unaffected;
  - strict gaps (D3);
  - an upstream pin narrows downstream defaults;
  - a downstream pin overrides (D5);
  - the predictor agrees with the loader, so a pinned chain can plan green;
  - release restores today's behaviour;
  - the pin history is kept;
  - the lock-error path.

### Stage 3: scidb delete

- `scidb/variant_delete.py`:
  - `delete_plan(db, targets)`: a **dry run**. `targets` is a list of
    selections, so D9 can pass "every record built with `fn.param = value` for
    every port that Parameter feeds". It returns the matched records plus their
    downstream closure, per variable with counts, and the locations each
    variable loses.
  - `delete_variant(db, targets, reason)` executes a plan in **one
    transaction**. It deletes:
    - the `<Type>_data` rows, `_record_save` and `_record`;
    - the `_invocation_output` edges;
    - the `_invocation_input` edges that point at a deleted record;
    - any `_invocation` left with no output, and its `_run_invocation` rows;
    - any `_run` left with no invocation.

    It keeps shared `__constant__` / `__pathinput__` records and files on disk
    (`generates_file`). It then writes a `_variant_tombstone` row (D8) and
    releases any pin that pointed at a deleted variant.
- `Mutator.delete_variant` with a required reason. CLI:
  `scidb delete-variant X --variant … --reason … [--yes]`. Without `--yes` it
  prints the plan and stops.
- Logging: the plan summary at INFO before executing, counts per table deleted
  at INFO, and timing.
- Tests:
  - the plan matches what's actually deleted;
  - downstream is included;
  - shared constants survive;
  - a run that also produced surviving records is kept;
  - the tombstone is written;
  - a pin on a deleted variant is released;
  - a failure mid-way rolls back (nothing deleted);
  - the D9 multi-variable scan.

### Stage 4: GUI backend

- RPCs, through the shared `DatabaseManager`:
  - `variable_variants`;
  - `pin_variant` / `release_pin`;
  - `delete_variant_plan` / `delete_variant`;
  - `run_pin_conflicts(node_ids)`.
- D9 needs "which (function, port) pairs does this Parameter feed". **Verify at
  build time who owns that.** If scidb knows it (Parameter entities /
  `declared_name` on constant edges), it goes in scidb. Otherwise the GUI
  resolves it from canvas edges and passes the ports down. Removing the value
  from the Parameter reuses the existing Parameter-edit path, not a new write.
- Remove the `variable_provenance` / `variable_topologies` RPCs, routes and
  services.
- Tests: RPC output matches `Inspector.variant_cards`; the pin, delete-plan and
  delete round-trips; conflict detection; both transports.

### Stage 5: GUI frontend

- `VariantsPopup` opens from a Variable node's right-click → **Variants**.
- **Collapsed card:** a default indicator (★ pinned here, ☆ default via an
  upstream pin), the distinguishing settings, record and location counts, last
  saved, **Make current** / **Release**, and **Delete**.
- **Expanded card:** a mini DAG (read-only, reusing the DAG node components the
  way `VariantDagPopup` does), a location tree grouped by schema level with
  counts, and the runs.
- **Delete confirmation** from `delete_variant_plan`: records per variable,
  "cannot be undone", a required reason field, and the D9 checkbox "also
  remove low_hz=10 from the Parameter". Ticking it re-plans and lists every
  affected variable.
- **Run dialog** (D6), before a GUI run that writes to a pinned variable.
- After a pin or delete, refresh node states and the canvas.
- React-free helpers in `variantsPopup.ts`, with `node --test` tests added to
  `tsconfig.test.json`'s include list.
- Delete `ProvenancePanel.tsx`, `TopologiesPanel.tsx` and `topologies.ts`, and
  the header buttons.
- Rebuild **both** vite bundles.

### Stage 6: docs

- New `docs/claude/variant-pins-and-deletion.md`:
  - default vs hide;
  - the coordinate-space filter and its consumer list;
  - D3/D5;
  - the delete cascade and what survives;
  - tombstones;
  - D9;
  - the one-hop gap in `variants`.
- Mark the panel sections of `variant-provenance-introspection.md` as removed.
- Add items to `docs/gui-manual-testing-todo.md`.

## Risks to watch

- **The predictor must follow the default.** If it doesn't, pinned chains show
  red forever. There's a test for exactly this in Stage 2.
- **Delete is the first destructive write in scidb.** It uses one transaction,
  requires a dry-run plan first, requires a reason, and writes a tombstone.
- **D9 can delete far more than the one card**, so the confirmation must show
  every affected variable before the button is enabled.

## Commands the user runs (no Python here)

```
cd /workspace/scidb && python -m pytest tests/test_variant_cards.py tests/test_variant_pins.py tests/test_variant_delete.py -q
cd /workspace/scistack-gui && python -m pytest tests/test_variants_popup_service.py -q
```

(One package per pytest run.) Frontend `npm test` and both builds run directly.
