# Plan: P5 stage 5(b): pydantic wire models for PlotSpec / StatsSpec

User decision (2026-11-02): **pydantic wire models**, not a hand walker that
mirrors `to_dict`.

## Where they live

`scistack_gui/api/spec_wire.py`. Reasons:

- scistack-gui is the only package that depends on pydantic and the only wire
  boundary (design bar 4: "pydantic models at wire boundaries").
- scistackplot and scistackstats stay pydantic-free, so their import cost and
  dependency list are unchanged.

## The rule

- `PlotSpec.to_dict()` / `StatsSpec.to_dict()` stay the one producer of the
  wire dict, and `from_dict` the one reader.
- The wire models are the typed declaration of that dict (enums as text, nulls
  dropped, nested `to_dict`s, `FactorVariable.variant` a dict). TypeScript is
  generated from them.
- Choice vocabularies come from their Python owners: the enum classes (`Role`,
  `PlotKind`, ...) and the tuples (`FAMILIES`, `GUIDE_STEPS`, `Y_TITLES`,
  `TEXT_TARGETS`, ...). The models never spell a value.
- Defaults are NOT repeated. An optional field's wire default is "absent",
  which means "the dataclass's default". The dataclass stays the one owner of
  defaults.
- A field is required on the wire only where `from_dict` requires it
  (`measures`, `Filter.column`, `LevelGroup.name/source`, `Comparison.layer/level`,
  `FactorVariable.variable`, `DV.variable`, `StatsSpec.format/design`).
- A field is typed for what `from_dict` ACCEPTS where that is wider than what
  `to_dict` emits. Example: `HistogramOptions.exact_values` is `number[] |
  string`, because the GUI sends the typed text.

## Drift guards (`scistack-gui/tests/test_spec_wire.py`)

1. For every (dataclass, wire model) pair, the field names are equal. Adding a
   spec field fails until the wire model has it.
2. A maximal PlotSpec (every field off its default) and the default one:
   `to_dict()` validates with `extra="forbid"`, and the json dump
   (`exclude_unset`) equals `to_dict()` exactly.
3. The same for StatsSpec, with a maximal analysis, overlay, guide and
   formatting.
4. `from_dict(wire dump) == spec` (round trip through the model).
5. `api_json_schema()["$defs"]` holds `PlotSpec` and `StatsSpec`. The TS
   names come from the model titles.

Logging: `api_json_schema` logs DEBUG `[wire_schema] %d request models, %d
spec models`.

## Steps

1. `spec_wire.py` + tests + schema inclusion (this session).
2. **User**: `python -m pytest scistack-gui/tests -x -q`. Because the JSON
   changes, first run `python scistack-gui/tools/gen_ts_types.py`, which
   regenerates `api-schema.json` and `api-types.ts`.
3. Frontend (needs the generated file):
   - `statsSpec.ts` interfaces (`StatsSpec`, `DV`, `Analysis`,
     `CovariateBinding`, `Formatting`, `Overlay`, `GuideState`) become
     re-exports of the generated types;
   - `CustomPlotSpec` in `PlotStudio.tsx` becomes the generated `CustomPlot`;
   - `npx tsc --noEmit`, `npm test`, rebuild both bundles.
4. Later (not this stage): type the handlers' `spec: dict` request params as
   the wire models, so dispatch validates them. That changes runtime
   behaviour, so it needs a GUI check first. Migrate `PlotSpecFields` and
   the per-section TS types (`HistogramOptions`, `Comparison`, ...).
5. Then 5(c): the typed `callBackend` overload.

## Progress

- **Step 1 built (2026-11-02, untested; pyright clean).**
  `scistack_gui/api/spec_wire.py` (29 wire models, `SPEC_MODELS`);
  `wire_schema.api_json_schema` adds them as `$defs` and logs the counts;
  `tests/test_spec_wire.py` (field parity for all 29 pairs, maximal + default
  round trips for both specs, every-field coverage, choices from owners).
  User: GUI pytest green (2026-11-02).
- **Step 3 built (2026-11-02).** Findings that changed the design:
  - `"default": null` next to a `$ref` made json-schema-to-typescript emit
    numbered copies (`Aggregation1`). The "absent" marker now keeps the
    default out of the schema (`_optional`). Test:
    `test_spec_fields_carry_no_null_default`.
  - Tried and reverted: making the always-written keys `required` in the
    schema. The webview HOLDS accept-side values: `addAnalysis` and
    `asGuideSpec` build parts with only a few keys, and nothing replaces them
    with the backend's full spec. So the types stay accept-side (optional),
    and the readers fall back: `analyses(spec)` in statsSpec.ts,
    `guideState()` fills mode/done.
  - `GuideState.step`/`done` are NOT narrowed to GUIDE_STEPS: Plot Studio's
    step-by-step view keeps the same state with PLOT_GUIDE_STEPS
    ("figure").
  - Frontend: the statsSpec.ts interfaces are re-exports and aliases of the
    generated types (new `FactorKind`, `DescriptiveCell`, `Role`
    re-export); `CustomPlotSpec` became the generated `CustomPlot`;
    `<select>` values are cast to the generated unions (their options are
    the backend catalog's). Guard: `generatedTypes.test.ts` "the spec types
    are not hand-written again".
  - tsc clean (both configs), npm test 701/701, both bundles rebuilt.
    `api-schema.json` was hand-mirrored in node for drafting. The user's
    `gen_ts_types.py` run writes the real one.
  User: all suites green (2026-11-02).
- **Step 4 built (2026-11-02, untested in Python).**
  - Dispatch validates specs: `scistack_gui/api/spec_params.py`
    (`PlotSpecParam`, `StatsSpecParam`, and the parts `DifferenceBarParam`,
    `VariantSetParam`, `FactorVariableParam`). They are `dict`s at runtime;
    an after-validator checks them against the wire model and keeps the dict
    as sent. A refusal is a 422 naming the field path, with a WARNING
    `[spec_params] <Model> refused: ...`. The JSON schema references the wire
    model.
  - Lazy import (D8): `spec_wire` imports scistackplot + scistackstats, which
    the GUI keeps off its startup path. `spec_params._wire_model` is the one
    lazy import, and `lazy_baseline` went 1377 -> 1378. Test:
    `test_the_wire_models_load_on_first_use_not_at_startup`.
  - Every `spec`/`plot_spec`/`stats_spec`/`seed`/`derived_bars`/
    `variant_sets`/`covariates` request field is typed. `stats_plot_guide`
    got its own `PlotGuideRequest`: it shared `ComputeRequest`, a table's
    spec, while it takes a figure's.
  - Behaviour change: `test_plot_saved` legacy spec (`x_layers`) is now 422
    at the boundary (it was 400 from `LegacySpecError`).
  - `wire_schema`: a request model's free-form `dict` is TS `object`, so
    named interfaces fit it, and an `Any` field is TS `unknown`. The Wire
    models keep their index signatures. Test:
    `test_free_form_and_untyped_fields_have_usable_ts_types`.
  - Frontend: Plot Studio's `Spec` IS the generated `PlotSpec`.
    `PlotSpecFields`, `Matcher`, `FacetOptions`, `YAxis`, `StylePatch`,
    `HistogramOptions`/`KdeOptions`/`RegressionOptions` and their enums,
    `Comparison`/`CompareMode`, `DifferenceBar`, `PanelOverride`/`YTitles`,
    `LevelGroup`, `AliasEntry`/`SpecAliases`, `SpecColors`,
    `TextSizesValue`, `FactorVariable`, `Filter`, `VariantSet`, `Role` and
    `TileRole` are generated or derived from generated types. `KindInfo`
    now extends `KindTile` (it was a copy). Readers treat absent `match`/
    `a`/`b` as `{}` (`from_dict`'s reading). `textSizes.ts` is the one
    place element keys index `TextSizes` (`textSizeOf`).
- **5(c) built (2026-11-02).** `callBackend<M>(method, ...BackendArgs<M>)`:
  a method in `ApiParams` must pass its generated request type, with params
  required when the model has a required field. Any other method (no model,
  registered outside the table, or a runtime name) is untyped as before.
  Found and fixed by it: `node_location_tree`/`plot_location_tree` built one
  union payload; `import_pipeline` sent an unchecked `JSON.parse` result
  (now refused unless it is an object); exports/saves passed `outputSpec`
  typed nullable. Compile-time guard: `src/apiParams.typecheck.ts`
  (`@ts-expect-error` lines, checked by the `tsc` in `npm run build`).
  Twin guard extended to every migrated file.
- tsc clean (both configs), npm test 701/701, both bundles rebuilt.
  `api-schema.json` hand-mirrored again; the user's `gen_ts_types.py` run
  writes the real one.
- **Stage 5 is complete** once the user's run is green.
