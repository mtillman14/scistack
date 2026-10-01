# The iteration level is part of a call's identity

*Written 2026-10-01 with `.claude/plan-iteration-level-identity.md`. Read this before
touching invocation identity, run options, `current_run_options`, or the skip gate.*

## The bug that made it necessary

`DemographicsTable`, loaded from one CSV by the built-in `pandas.read_csv` node
(a PathInput-only loader):

| run | how it was called | what it saved |
|---|---|---|
| 09-29 09:49 | iterated **[subject]**, no `distribute` | the whole 16-row CSV at each of 18 subjects |
| 09-29 11:51 | **one call** over the whole dataset | the CSV flattened into 16 one-row records |

The Variants popup showed **one** card for 34 records, and a one-subject plot drew
every subject's row. `COUNT(DISTINCT invocation_id) = 1`.

Invocation identity was: the function hash, the edges (variable inputs, constants,
PathInput **name**), and the run options `as_table` / `distribute` /
`across_variants`. A loader with no variable inputs has no edge that records WHERE
the call iterated, so both runs were one invocation, one coordinate and one variant.
The per-location rule "newest save of one invocation wins" then kept the two subjects
missing from the CSV (SS02, SS04) on their stale whole-table records.

## The rule

**The schema keys a call iterates are identity, like `distribute`.** Running the same
function at a different level is a different computation.

| | where |
|---|---|
| definition (the final iterated keys, after PathInput discovery, in dataset order; `[]` = one call) | `foreach._iteration_level_of` |
| version key `__level` (so it reaches the record id via `__invocation_id`) | `ForEachConfig(level=)` → `to_version_keys` |
| folded into `invocation_id` whenever not None | `provenance.compute_invocation_id(iteration_level=)` |
| read on the save side (one reader) | `provenance_save._iteration_level` |
| stored | `_invocation.iteration_level VARCHAR[]` (NULL for glue hops and `__save__`) |
| shown | `run_options_label(..., iteration_level=)` → `distribute=false, level=subject/trial` or `level=(one call)` |

There is no migration (memory `beta-no-deprecation`). The column is in CREATE TABLE,
and a database created before 2026-10-01 must be rebuilt by re-running.

## What it is NOT

- **Not a call-site key.** `__level` is not in `_CALL_ID_INCLUDED_KEYS`. A call site
  is a canvas node; running that node at another level makes a new *variant* of it,
  not a new node. Canvas node identity, `config_call_id` and node-state scoping are
  unchanged.
- **Not in `pipeline_variants` / `Inspector.variants`.** That is the call-site view,
  which groups by call site and keeps its labels without the level. The full-chain view
  is `Inspector.variant_cards`, where two levels are two cards.

## The consequences, each with one owner

1. **Two levels give two cards** (`variant_cards`): the `__run__.fn` axis now differs.
2. **The older level is superseded everywhere**, including at locations the newer run
   never wrote (SS02/SS04), through the run-option currency rule.
3. **Currency is per (function, OUTPUT variable)** (`current_run_options` →
   `{(fn, output_type): label}`). Before this it was per function name. That was
   harmless while different run options were rare, but with the level in the label,
   one generic function used by two nodes at two levels (writing two variables) would
   have marked one node's every record superseded. The chain-wide display rule in
   `variant_identity_batch` uses `chain_batch`'s new `run_type` map (each hop's output
   variable) for the same scope. `_build_upstream_closure` now also returns `rec_type`.
4. **Run pins.** A pin naming no `level` matches any level; otherwise matching is
   exact (`provenance_query.run_options_label_matches`, used by the loader filter and
   the cards' overlap check). So `Variant(X, run_options="distribute=true")` keeps
   working, and `"distribute=false"` still does not match `"distribute=false,
   as_table=[df], …"`.
5. **The skip gate** compares labels, so a re-run at a new level is not skipped. The
   hook is built before the iterables resolve, so `_for_each_prepare` hands the level to
   it through `_should_skip._iteration_level_ref` (the same mechanism as
   `_agg_binding_ref`). This covers the Python and the MATLAB bridge path alike.
6. **The predictor** rebuilds ids with each history config's recorded level
   (`function_variant_configs` → `cfg["iteration_level"]`). A config's
   `iterated_keys` now prefers the recorded level over inferring it from the outputs.

## Related fixes made at the same time

- **Plot Studio's unnamed default.** A spec with no variant rows used to draw every
  generation. `scistackplot.variants._apply_default_pin` now applies the source's
  `default_pin` (the per-location latest flag) and logs the selection it used. Pinned
  defaults (`variant_pins`) are not yet applied in Plot Studio.
- **Per-record delete.** `variant_delete` target `{"variable", "record_ids"}`;
  `VariantCard.location_record_ids` (aligned with `locations`); the popup's
  location-tree leaves have a 🗑.
- **`sys.executable` in the `configure_database` log line**, beside the Python version.

## Tests

`scidb/tests/test_iteration_level_identity.py` (two levels give two cards; superseded
everywhere; same level skipped; new level not skipped; not in call_id; currency scope;
pin matching; predictor). Updated: `test_identity_parity.py` (rebuilds ids with the
stored level), `test_provenance_identity.py`, `test_run_option_variants.py`,
`test_variant_cards.py`, `test_variant_provenance.py`, `test_variant_delete.py`,
`scistackplotdb/tests/test_run_option_variants.py`,
`scistackplot/tests/test_default_variant_pin.py`, and
`scistack-gui/tests/test_variants_popup_service.py`.
