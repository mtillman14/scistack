# Plan: Variable node shows the CURRENT variant's count, and which variant it is

Status: rev 2, draft for approval (2026-10-16).

Rev 2 drops rev 1's new "pinned else newest" rule. Per the user, it re-created
logic that already exists. The node now consumes the Variants popup's
"current" marking unchanged.

## What already decides "current" (reused, not re-implemented)

- `scidb.inspect.variant_cards._mark_defaults` sets `VariantCard.is_default`:
  - **With a pin** reaching the variable (its own or upstream, via
    `variant_pins.effective_default`): the cards matching the pin's selection,
    minus superseded ones unless the pin loads uncollapsed.
  - **Without a pin:** every card a plain `latest` load still returns
    (`verdict != "superseded"`).
- The popup's chip (`variantCards.ts statusBadge`) is just a display of
  `is_pinned` / `is_default` / `verdict`: "★ current (pinned)", "☆ current (via
  an upstream pin)", "current".
- The header text is `variantCards.ts cardHeading(card)` (the `distinguishing`
  axes, or "the only variant"). The count is `card.record_count`
  ("N records · M locations").
- Card order is newest-saved first (`build_variant_cards`).

## The node, built only from those

- **current cards** = `[c for c in cards if c.is_default]`, in the cards' own
  order. No new rule.
- **Exactly one current:** `★ V3 of 3 · 48 records` / `low_hz = 30`. It uses
  `cardHeading` and the same chip glyph as the popup.
- **Several current** (a sweep with no pin, e.g. low_hz 10/20/30 all live):
  show the first of them, i.e. the newest, which is the popup's top current
  card, plus `+2 more current`. The tooltip lists them. This is display order
  only; whether they are current is still scidb's call.
- **None current** (everything superseded): fall back to "empty", as today.
- **Only one variant at all:** `48 records`, with no "V1 of 1".
- **Tooltip:** "V3 is current (pinned). 3 variants, 312 records in all.
  Right-click → Variants to see them."

## The one new thing: the V-number (user-approved)

`VariantCard.ordinal` is assigned inside `build_variant_cards` by first save
(oldest = V1). Ties within one batch use the existing stable tie order. Both
the popup header and the node read it, so they cannot disagree. Deleting a
variant renumbers the later ones (documented).

## Making it cheap enough for the canvas

The node needs the cards, not their details. `build_variant_cards` gains
`detail: bool = True`. With `detail=False` it skips `_card_upstream`, runs,
wiring and per-location lists. It keeps grouping, `latest_load` (the verdict
needs it) and `_mark_defaults`, so "current" is computed by the same code
path. It is the same function, not a second one.

`variant_cards_service` / `api/pipeline.py` call it once per variable during
the canvas build, through one scidb helper,
`current_variants(db, names) -> {name: VariantCards}`. That helper is a loop
with timing, not a rule.

Logging:
- INFO `[current_variants] N variable(s) in X ms (slowest: Foo Y ms)`;
- `build_variant_cards` already records `timings_ms` per stage;
- DEBUG per variable: chosen card ordinal, how many are current, pinned or not.

Measure the canvas build on the aim2 dataset before and after. If the per-
variable cost is too high, batch the closure across variables in a later
stage. Measure first.

## Tests

- **scidb:**
  - `detail=False` gives the same `card_id` / `is_default` / `is_pinned` /
    `record_count` / `ordinal` as `detail=True` (parity: proves no divergent
    path);
  - ordinals follow first save; batch ties are stable;
  - deleting a variant renumbers.
- **scistack-gui:** the variable node payload carries the current card(s). A
  pin moves the node's card; a sweep reports several current.
- **frontend:** a pure `variableNodeText.ts` covering one / several / pinned /
  none / single-variant. It imports `cardHeading` and the badge rule from
  `variantCards.ts`.

## Docs

- `docs/claude/variant-pins-and-deletion.md`: ordinals, and "the canvas node
  shows the is_default card(s)".
- `docs/gui-manual-testing-todo.md`: new item.

## Stages

1. scidb: `ordinal`, `detail=False`, `current_variants`, tests.
2. GUI backend payload, test, timing measurement.
3. Frontend node + popup "V{n}"; rebuild both bundles.
4. Docs, manual-test item.
