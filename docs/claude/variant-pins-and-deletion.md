# Variant pins (the default) and variant deletion

*Written 2026-09-30, before implementation, from `.claude/plan-variants-popup.md`
(rev 2). Sections marked **(planned)** describe agreed behaviour that is not
built yet; update them as stages land.*

The GUI's **Variants** popup (right-click a Variable node → Variants) shows one
card per variant. Each card has two actions: **Make current** and **Delete**.
They are opposites in every way that matters, and this doc exists so they are
never confused:

| | Make current (pin) | Delete |
|---|---|---|
| what it changes | which variant an **unnamed** load gets | whether the records exist |
| other variants | untouched, still loadable by name | untouched (unless downstream of the deleted one) |
| reversible | yes: release or move the pin | **no** |
| storage | `_variant_pin` row (history kept) | rows removed; `_variant_tombstone` row written |
| uses `_record.excluded` | **no** | no |

## 1. What a "variant" is here (a card)

*Built 2026-09-30 (Stage 1): `scidb/inspect/variant_cards.py`,
`Inspector.variant_cards`, `scidb variants X --cards [--json] [--locations]`,
tests `scidb/tests/test_variant_cards.py` (not yet run).* One closure build
feeds both coordinate walks (`branch_params_batch` / `chain_batch` now take
`closure=`) and the per-card upstream DAG. "Current" on a card is judged per
location against the real `_find_record(version_id="latest")` load, never
re-derived. A selection that another card also satisfies is reported
(`selection_exact=False`, `overlaps_with`), never hidden.

A card is a **full-chain** variant: a distinct combination of upstream constants,
per-function code versions and run options along the WHOLE upstream chain. In
`variant-space.md` terms, it is one point in the coordinate space
(`branch_params` + `__code__.*` + `__run__.*`).

**The one-hop gap this fixes.** `Inspector.variants` / `topologies` (the
`scidb variants` command) groups by the producing call (`call_id`) and reports
only that call's direct constants. Two records that differ only two steps
upstream (`filter.low_hz=10` vs `20`) are one row there. Cards group by the full
coordinate instead. `variants`/`topologies` stay as they are, because they
answer the topology question ("how many node shapes produced this?").

Each card carries a `selection`: the canonical pin dict that names it. The
invariant is `records_for_variant(variable, card.selection) == card.record_ids`.
The GUI never builds a selection itself. It forwards the card's.

`distinguishing` is computed in scidb: the axes whose value differs from at
least one sibling card. The collapsed card header shows only these.

**Order: newest first** (user, 2026-10-01), meaning the most recently SAVED card
on top, the same "newest" as `variant_pins.pin_newest`. Cards saved at the same
instant (one run's batch) keep a stable order by producer, then selection text.
scidb owns the order (`build_variant_cards`); the popup and `scidb variants
--cards` show it as given.

## 2. Pins: "current" means the DEFAULT, never "the only one"

*Built 2026-09-30 (Stage 2), pytest not yet run. `scidb/variant_pins.py`
(`_variant_pin` table created at DB init; `pin_variant`, `release_pin`,
`active_pins`, `pin_history`, `effective_default`, `default_load_args`,
`default_records_by_schema`). `Mutator.pin_variant` / `release_pin`,
`Inspector.variant_pins`, and the CLI commands `scidb pin X --variant K=V --reason`,
`scidb unpin X --reason` and `scidb pins [X] [--history]`. Wired consumers:
`foreach._load_var_type_as_spread` (via `_load_input(use_default=)`, which
`AcrossVariants` turns off), `BaseVariable.load` with no variant kwargs, and
`expected_invocations_for_function`. Plot Studio and stats are wired in a
later stage. Which pins reach a variable is decided by the variable-TYPE
ancestry (`_type_parents`, one join, cached on the connection and keyed by
invocation count). A pin whose selection spans two cards is refused. Cards
carry `is_default`, `is_pinned` and `pin_conflict`. A terminal for_each that
saves to a pinned variable logs `[default]` at INFO.*

**Known limit:** an input that NAMES a variant (`Variant(X, ...)`) is still
predicted over the default, or over every variant when there is no pin. This is
the older xfail in `test_variant_pin_node_state.py`, and pins neither cause nor
fix it.

"Current" answers *which variant do I get when I don't say?* It does **not**
hide the others. Plotting variant A against variant B, or running a step on a
named `Variant(X, low_hz=10)`, must keep working when B is pinned.

### Why not `_record.excluded`

`exclude_variant` sets `_record.excluded`, and `_find_record` skips excluded
records on **every** load, named or not. That is hiding. Using it for pins
would make non-default variants unreachable, which is exactly what the user
ruled out.

### The default is a filter in coordinate space

A pin stores the card's `selection`. Selection keys are coordinate keys, and a
downstream record's coordinate contains its upstream constants. So a pin on an
upstream variable narrows downstream defaults with no extra machinery:

```
effective_default(V) = merge over the pins on V's upstream chain and V itself;
                       per key, the pin NEAREST to V wins.
```

It is applied as a `branch_params_filter` through `records_for_variant`, so the
collapse trap (`variant.pin_loads_uncollapsed`) is respected.

**One owner:** `scidb.variant_pins.effective_default(db, variable)`.

**Consumers**, where an unnamed load gets the default:

- the for_each input loader, for an input with no `Variant(...)`;
- `load()` with no variant kwargs;
- the node-state / expected-invocation predictor. **It must consume the
  default**, or a pinned chain plans red forever while waiting for variants the
  default skips;
- Plot Studio's default variant selection;
- stats' default input resolution.

**Not consumers:**

- `version_id="all"`, record_id lookups, `Inspector`, and explicit `Variant(...)`;
- `AcrossVariants(X)`, which pools every variant on purpose.

### Rules the user decided (2026-09-30)

- **One default per variable** once pinned. With no pin, today's behaviour
  applies: every current variant flows.
- **Strict gaps.** If the pinned variant does not exist at a location, the
  default load returns nothing there and downstream skips that location. It
  never falls back to another variant, because a figure mixing settings across
  subjects is the worse failure.
- **A downstream pin may disagree with an upstream one**, and the nearer pin
  wins for that variable and below. The card flags the disagreement.
- **Sticky.** A new run never moves a pin. Before a GUI run that would write to
  a pinned variable, the user chooses Keep pin / Move pin to the new output /
  Cancel. Terminal runs log an INFO note instead.
- **History.** `_variant_pin(variable, selection_json, reason,
  pinned_at, released_at)`. Moving or releasing a pin sets `released_at` and
  never removes the row.

## 3. Delete: real deletion, the one sanctioned exception

*Built 2026-09-30 (Stage 3), pytest not yet run. `scidb/variant_delete.py`
(`delete_plan`, `delete_variant`, `tombstones`; the `_variant_tombstone` table is
created at DB init). `Mutator.delete_variant(targets, reason,
expect_fingerprint=)`, `Inspector.delete_plan` / `tombstones`, and the CLI
`scidb delete-variant [X] --card ID | --variant K=V | --constant FN.PARAM=V |
--parameter NAME=V --reason R [--yes] [--fingerprint F]` (a dry run unless
`--yes`) and `scidb tombstones [X]`. Targets are unioned. Card and selection
targets take WHOLE cards, not `records_for_variant`, so older saves of the
variant go too and cannot be promoted back to "latest". `delete_variant`
re-plans inside its transaction and refuses a changed fingerprint. Records
hidden with `exclude_variant` are on no card and are not deleted by a card
target.*

The project never deletes data. The exception is this button, for **erroneous
runs that shouldn't be part of the record** (user, 2026-09-30). Do not
generalise it to other surfaces.

- **Scope:** the variant's records **and everything computed from them**
  (downstream closure). Deleting only the card would leave records whose inputs
  no longer exist.
- **Dry run first:** `delete_plan(targets)` → records per variable with counts,
  and the locations each variable loses. The GUI enables the button only once
  the plan is shown.
- **What is removed:**
  - `<Type>_data` rows, `_record_save`, `_record`;
  - `_invocation_output` edges;
  - `_invocation_input` edges into deleted records;
  - `_invocation`s left with no output, and their `_run_invocation` rows;
  - `_run`s left with no invocation.
- **What survives:**
  - shared `__constant__` / `__pathinput__` records, which other variants use;
  - files written to disk (`generates_file`);
  - runs and invocations that also produced surviving records.
- **Atomic:** one transaction, so a failure deletes nothing.
- **Tombstone:** `_variant_tombstone` row: variable, selection, counts per
  variable, reason (required), who, when. The database can then answer "where
  did those go?".
- A pin that pointed at a deleted variant is released, with reason "variant
  deleted".

### Removing the value from the canvas Parameter

If the deleted variant's constants still sit in a canvas Parameter, the next
Run all recomputes it. The confirmation therefore offers "also remove
`low_hz=10` from the Parameter".

Deleting one card does **not** by itself retire a value, because another card
may still use `low_hz=10` with different code. So ticking the box widens the
deletion to **every variant of every variable** built with that value through
any port the Parameter feeds. `delete_plan` takes a list of targets for this
reason, and the confirmation lists every affected variable. Removing the value
reuses the existing Parameter-edit path.

**Resolved 2026-09-30 (Stage 4): scidb owns it.** `parameter.parameter_node_name`
(the recorded `declared_name`, else the argument name) is the canvas Parameter
node's identity, so the delete target `{"parameter": N, "value": X}` matches
edges by that rule and needs no canvas wiring. `UpstreamStep.parameter_names`
exposes the same mapping per card. The GUI adds only source facts: whether the
Parameter still declares the value, and whether its file is editable
(`entity_editability`).

## 4. Where things live

| | owner |
|---|---|
| cards (full-chain grouping, selection, distinguishing) | `Inspector.variant_cards` (`scidb/inspect/api.py`), helpers in `scidb/inspect/variant_cards.py` |
| pins, `effective_default` | `scidb/variant_pins.py` (planned) |
| delete plan / execute / tombstone | `scidb/variant_delete.py` (planned) |
| write entry points | `Mutator.pin_variant` / `release_pin` / `delete_variant` (planned) |
| CLI | `scidb variants X --cards`, `scidb pin`, `scidb unpin`, `scidb delete-variant` |
| GUI backend | `scistack_gui/services/variant_cards_service.py` + `api/variants.py`: RPCs `variable_variants`, `pin_variant`, `release_pin`, `pin_newest_variant`, `delete_variant_plan`, `delete_variant`, `run_pin_conflicts` (built Stage 4) |
| GUI | `components/Variants/VariantsPopup.tsx` (right-click a Variable → Variants), helpers `variantCards.ts`, before-run gate `RunPinGate.tsx` (node Run + pipeline Run). The old Provenance/Variants panels, their header buttons and the `variable_provenance` / `variable_topologies` RPCs were removed (Stage 5) |

## See also

- `variant-space.md`: the coordinate vs the selector, and the three axes
- `variant-provenance-introspection.md`: `trace`, `records_for_variant`, the
  collapse trap
- `function-version-variants.md`: code-version ordinals
