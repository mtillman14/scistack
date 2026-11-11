# docs/claude index

Engineering notes written to fill conceptual gaps (CLAUDE.md). Grouped by
layer, mechanically from file names (2026-11-10, grug stage
`knowledge_consolidation`; see [grugbrain-principles.md](grugbrain-principles.md)).

Rules:

- **One live doc per concept.** When two docs below cover the same concept,
  merge them, or move the superseded one to `archive/` and say so at the top
  of the live one.
- **Superseded docs go to `archive/`**, never deleted.
- Regenerate this index after adding a doc (it is a list, not prose): each
  entry is the doc's first `# ` heading.

## Principles and reviews

- [Architecture review — 2026-09-20](architecture-review-2026-09-20.md) — `architecture-review-2026-09-20.md`
- [Cleanup audit](cleanup-audit.md) — `cleanup-audit.md`
- [Decisions — one page each, newest first](decisions.md) — `decisions.md`
- [Design review (pointer)](design-review.md) — `design-review.md`
- [Grugbrain principles, applied to SciStack](grugbrain-principles.md) — `grugbrain-principles.md`
- [Intent and fact](intent-and-fact.md) — `intent-and-fact.md`
- [Layer Interaction Analysis: scifor ↔ scidb ↔ scihist](layer-friction-analysis.md) — `layer-friction-analysis.md`
- [Preview/export paper parity (and the Plot Studio's light mode)](preview-paper-parity.md) — `preview-paper-parity.md`

## Plotting (scistackplot, Plot Studio)

- ["Not plotted": what a figure leaves out](not-plotted.md) — `not-plotted.md`
- ["Save data" — the long table a plot is drawn from](plot-data-export.md) — `plot-data-export.md`
- ["Show sample" — the collapsed keys' data drawn on top of the marks](show-sample-overlay.md) — `show-sample-overlay.md`
- [Auto text size: the layout rules](auto-text-layout-rules.md) — `auto-text-layout-rules.md`
- [Categorical (text) measures in Plot Studio](categorical-measures.md) — `categorical-measures.md`
- [Compare to reference](compare-to-reference.md) — `compare-to-reference.md`
- [Custom plot kinds: a `plot_` function drawing in Plot Studio](custom-plot-kinds.md) — `custom-plot-kinds.md`
- [Datetimes in plots](plot-datetimes.md) — `plot-datetimes.md`
- [Difference bars](difference-bars.md) — `difference-bars.md`
- [Endpoint leaf nodes: `plot_` and `stat_` functions, and drafts](plotting-leaf-nodes.md) — `plotting-leaf-nodes.md`
- [Facet Layout: the Grid, the Slots, and the Placement Passes](facet-layout-grid.md) — `facet-layout-grid.md`
- [Figure size and aspect ratio](figure-size.md) — `figure-size.md`
- [Grouping and Collapse — how a plot's factors are read](grouping-and-collapse.md) — `grouping-and-collapse.md`
- [Histogram and regression: the continuous-x built-in kinds](histogram-and-regression.md) — `histogram-and-regression.md`
- [Horizontal bar: BAR transposed](horizontal-bar.md) — `horizontal-bar.md`
- [How a function node's colour is decided](function-node-colour.md) — `function-node-colour.md`
- [Input Markers: ColName, PathInput, PathOutput](input-markers-colname-pathinput-pathoutput.md) — `input-markers-colname-pathinput-pathoutput.md`
- [Measure shape, and collapsing a 1-D measure to a scalar](measure-shape-and-collapse.md) — `measure-shape-and-collapse.md`
- [Named Variant Rows, and the Variant-Selection DAG](plot-variant-rows.md) — `plot-variant-rows.md`
- [Per-panel overrides](per-panel-overrides.md) — `per-panel-overrides.md`
- [Plot Studio controls: where each one lives and what it writes](plot-studio-controls.md) — `plot-studio-controls.md`
- [Plot colours: pinned mark colours per level](plot-colors.md) — `plot-colors.md`
- [Plot panel spacing: margins, cell gaps and bracket rows](plot-panel-spacing.md) — `plot-panel-spacing.md`
- [Plot presets](plot-presets.md) — `plot-presets.md`
- [Plot text: sizes and display labels](plot-text-and-labels.md) — `plot-text-and-labels.md`
- [Saved plots](saved-plots.md) — `saved-plots.md`
- [Spaghetti plot: repeated-measures trajectories on a nested x axis](spaghetti-plot.md) — `spaghetti-plot.md`
- [The plotting layer: `scistackplot` / `scistackplotdb`](plotting-library-design.md) — `plotting-library-design.md`
- [Y-limit scoping](y-limit-scoping.md) — `y-limit-scoping.md`

## Statistics

- [Endpoints: Data Visualization and Statistics — Design](endpoints-viz-and-stats-design.md) — `endpoints-viz-and-stats-design.md`
- [SciStack-GUI: canonical-id sharing across pipeline scopes, and scoped hidden state](scistack-gui-scoped-hidden-state.md) — `scistack-gui-scoped-hidden-state.md`
- [StatStudio Guided ("Step by step") — how it works](statstudio-guided.md) — `statstudio-guided.md`
- [StatStudio — control map](stat-studio-controls.md) — `stat-studio-controls.md`
- [Statistics — Design](statistics-design.md) — `statistics-design.md`
- [Statistics — how it works (owners, data flow, traps)](statistics.md) — `statistics.md`
- [Testing csv-stats' mixed models against R (lme4 / lmerTest)](lmm-r-oracle-testing.md) — `lmm-r-oracle-testing.md`
- [What "green" means, and at what granularity](schema-location-status.md) — `schema-location-status.md`
- [csv-stats v1 — Requirements from scistack](csv-stats-v1-requirements.md) — `csv-stats-v1-requirements.md`

## MATLAB

- [Knowing when a MATLAB run ends, and whether it worked](matlab-run-completion.md) — `matlab-run-completion.md`
- [MATLAB Bridge Transfer Paths](matlab-bridge-transfer-paths.md) — `matlab-bridge-transfer-paths.md`
- [MATLAB Load Performance Optimizations](matlab-load-performance.md) — `matlab-load-performance.md`
- [MATLAB Output Names vs Variable Types](matlab-output-name-vs-variable-type.md) — `matlab-output-name-vs-variable-type.md`
- [MATLAB Path Resolution — `matlab.functions` vs `matlab.variables`](matlab-path-resolution.md) — `matlab-path-resolution.md`
- [MATLAB Support in the SciStack VS Code Extension — Implementation Reference](matlab-gui-implementation.md) — `matlab-gui-implementation.md`
- [MATLAB Support in the SciStack VS Code Extension](matlab-gui-support-analysis.md) — `matlab-gui-support-analysis.md`
- [MATLAB ↔ Python Type Round-Trip (to_python / from_python)](matlab-type-roundtrip.md) — `matlab-type-roundtrip.md`
- [The MATLAB Output-Handle Contract](matlab-output-handle-contract.md) — `matlab-output-handle-contract.md`
- [Who owns the DuckDB file during a MATLAB run](matlab-run-database-ownership.md) — `matlab-run-database-ownership.md`
- [Why the first MATLAB "Run" click doesn't run anything](matlab-connection-gate.md) — `matlab-connection-gate.md`

## GUI (scistack-gui)

- [Data inventory (missing schema-key combinations)](data-inventory.md) — `data-inventory.md`
- [Debugging a run from the GUI](gui-debugging.md) — `gui-debugging.md`
- [Free-Code "Glue" Nodes](free-code-glue-nodes.md) — `free-code-glue-nodes.md`
- [GUI Readiness Assessment](gui-readiness.md) — `gui-readiness.md`
- [GUI notification routing: which channel reaches which webview](gui-notification-routing.md) — `gui-notification-routing.md`
- [GUI → plain .py export (future feature): notes on the disconnected-wiring mechanism](gui-export-to-plain-python.md) — `gui-export-to-plain-python.md`
- [How run options reach a run (GUI -> Python, GUI -> MATLAB)](gui-run-options-flow.md) — `gui-run-options-flow.md`
- [Manual edges onto history nodes (RETIRED 2026-10-22)](manual-edges-on-history-nodes.md) — `manual-edges-on-history-nodes.md`
- [Migrating scistack-gui to a VS Code Extension](gui-vscode-extension.md) — `gui-vscode-extension.md`
- [Multi-session GUI: one tab per database](gui-multi-session.md) — `gui-multi-session.md`
- [Node identity: which ids are computed and which are allocated](node-identity.md) — `node-identity.md`
- [Pending Constants in scistack-gui](scistack-gui-pending-constants.md) — `scistack-gui-pending-constants.md`
- [Schema Hierarchy and Non-Contiguous Key Support](schema-hierarchy-contiguity.md) — `schema-hierarchy-contiguity.md`
- [SciStack Project Setup Guide](scistack-gui-project-setup-guide.md) — `scistack-gui-project-setup-guide.md`
- [SciStack-GUI: per-handle manual-edge coverage clears "disconnected" wiring](scistack-gui-disconnected-wiring-reconnect.md) — `scistack-gui-disconnected-wiring-reconnect.md`
- [Submodule ports](submodule-ports.md) — `submodule-ports.md`
- [The canvas as a document, explained from the ground up](canvas-as-document-explained.md) — `canvas-as-document-explained.md`
- [The edge model: one id, one view, one port per argument](edge-model.md) — `edge-model.md`
- [Undo / redo in the GUI](undo-redo.md) — `undo-redo.md`
- [VS Code Extension: Server Startup Path and Failure Diagnostics](gui-extension-startup-path.md) — `gui-extension-startup-path.md`
- [Which node supplies a variant axis — three names that look like one](variant-axis-node-binding.md) — `variant-axis-node-binding.md`
- [`input_params` — how a function node's input handles are assembled](function-node-input-params.md) — `function-node-input-params.md`
- [scistack-gui frontend architecture](scistack-gui-frontend-architecture.md) — `scistack-gui-frontend-architecture.md`

## Iteration (scifor / for_each)

- [A node's recorded shape vs the edges you draw](hidden-path-input-identity.md) — `hidden-path-input-identity.md`
- [A selection below the iterated level is a variant](selection-variant-axis.md) — `selection-variant-axis.md`
- [A variable keeps the columns of its first save](variable-column-set.md) — `variable-column-set.md`
- [Alternate PathInput templates](alternate-pathinput-templates.md) — `alternate-pathinput-templates.md`
- [Coarse-level inputs and unpopulated schema columns](coarse-level-inputs.md) — `coarse-level-inputs.md`
- [Column Selection for BaseVariable](column-selection.md) — `column-selection.md`
- [DuckDB Column Types for Table (DataFrame) Variables](duckdb-column-types.md) — `duckdb-column-types.md`
- [EachOf — Generalized Variant Expansion](each-of-variant-expansion.md) — `each-of-variant-expansion.md`
- [Expected locations](expected-locations.md) — `expected-locations.md`
- [How `scifor.for_each()` Works — A Detailed Walkthrough](scifor-for-each-internals.md) — `scifor-for-each-internals.md`
- [Multi-Column Save: Schema Inference, Skip-on-Mismatch, and the Length-1 Unwrap](multi-column-save-schema.md) — `multi-column-save-schema.md`
- [PathInput spelling aliases](pathinput-folder-aliases.md) — `pathinput-folder-aliases.md`
- [PathInput: disambiguating adjacent placeholders with `key_regex`](pathinput-key-regex.md) — `pathinput-key-regex.md`
- [PathInput: zero-padded numeric filename matching](pathinput-zero-padded-matching.md) — `pathinput-zero-padded-matching.md`
- [Three identity layers, and how a PathInput-fed step moves through them](identity-layers-pathinput.md) — `identity-layers-pathinput.md`
- [Variant Selection](variant-selection.md) — `variant-selection.md`
- [What a variant row selects](variant-row-selection.md) — `variant-row-selection.md`
- [`__where` Provenance and Merge Constituent Loading](where-provenance-and-merge.md) — `where-provenance-and-merge.md`
- [`distribute` vs the spread rule — two ways rows become records](distribute-vs-spread.md) — `distribute-vs-spread.md`
- [as_table Column Type Normalization](as-table-column-types.md) — `as-table-column-types.md`
- [for_columns — Column-Wise Iteration + Reassembly](for-columns-iteration.md) — `for-columns-iteration.md`
- [for_each Caching / skip_computed](for-each-caching.md) — `for-each-caching.md`
- [for_each Input Unwrapping and as_table Behavior](for-each-input-unwrapping.md) — `for-each-input-unwrapping.md`
- [for_each kwarg parity: scifor <-> scidb, Python + MATLAB](for-each-kwarg-parity.md) — `for-each-kwarg-parity.md`
- [for_each() Kwargs Reference](for-each-kwargs.md) — `for-each-kwargs.md`
- [scifor/scidb Modifier-Class Unification](scifor-scidb-modifier-unification.md) — `scifor-scidb-modifier-unification.md`

## Storage, identity, provenance (scidb / sciduckdb)

- [A Parameter value is recorded as declared](declared-parameter-spelling.md) — `declared-parameter-spelling.md`
- [Code Discovery: Functions, Submodules, Variables, Parameters, PathInputs](code-discovery-categories.md) — `code-discovery-categories.md`
- [Config File Format: `scistack.toml`](config-file-formats.md) — `config-file-formats.md`
- [Constants are not necessarily scalars](constants-are-not-scalars.md) — `constants-are-not-scalars.md`
- [Database Model — Canonical Reference](database-model.md) — `database-model.md`
- [Datetime round-trip (naive, tz-aware, NaT)](datetime-roundtrip.md) — `datetime-roundtrip.md`
- [DuckDB Transaction Safety in save()](duckdb-transaction-safety.md) — `duckdb-transaction-safety.md`
- [Entity declaration surfaces](entity-declaration-surfaces.md) — `entity-declaration-surfaces.md`
- [Entity editability: how the GUI and script authoring modes cohere](entity-editability-model.md) — `entity-editability-model.md`
- [Error surfacing: failure records, on_error, and where failures show up](error-surfacing.md) — `error-surfacing.md`
- [Function-Version Variants](function-version-variants.md) — `function-version-variants.md`
- [Identity table: which facts of a call reach which id](identity-table.md) — `identity-table.md`
- [Library Function Name Identity (`pandas.read_csv` vs `read_csv`)](library-function-name-identity.md) — `library-function-name-identity.md`
- [Lineage / Provenance Design](lineage-design.md) — `lineage-design.md`
- [Lineage Simplification — Bipartite Provenance Model](lineage-simplification.md) — `lineage-simplification.md`
- [Lineage Wrapper Types and Cross-Layer Data Flow](lineage-wrapper-types.md) — `lineage-wrapper-types.md`
- [Logging architecture (scistacklog)](logging-architecture.md) — `logging-architecture.md`
- [Multi-Database Workflow](multi-database-workflow.md) — `multi-database-workflow.md`
- [Multi-Source Pipeline Discovery](multi-source-discovery.md) — `multi-source-discovery.md`
- [NULL ≡ NaN: how a missing number survives the round trip](null-nan-roundtrip.md) — `null-nan-roundtrip.md`
- [Pipeline Import/Export Identity Model](pipeline-import-identity.md) — `pipeline-import-identity.md`
- [Portability: exporting and importing whole projects](portability.md) — `portability.md`
- [Project & Library Structure](project-library-structure.md) — `project-library-structure.md`
- [Run options (`distribute` / `as_table`) as the third variant dimension](run-option-variants.md) — `run-option-variants.md`
- [Schema ID Allocation](schema-id-allocation.md) — `schema-id-allocation.md`
- [Schema key types: numeric/string declaration + canonicalization](schema-key-types.md) — `schema-key-types.md`
- [The entities file: declaring Variables, Parameters and PathInputs in TOML](entities-toml-format.md) — `entities-toml-format.md`
- [The iteration level is part of a call's identity](iteration-level-identity.md) — `iteration-level-identity.md`
- [Variant pins (the default) and variant deletion](variant-pins-and-deletion.md) — `variant-pins-and-deletion.md`
- [Variant provenance introspection — "which run produced the records I am looking at?"](variant-provenance-introspection.md) — `variant-provenance-introspection.md`
- [Variant space](variant-space.md) — `variant-space.md`
- [Variant — Per-Input branch_param Pinning](variant-branch-param-pinning.md) — `variant-branch-param-pinning.md`

## Other

- [DataFrame Conversion: Factoring Out as a Standalone Library](dataframe-conversion-factoring.md) — `dataframe-conversion-factoring.md`
- [Docs Ground-Truth: Package → Test Map](docs-ground-truth-test-map.md) — `docs-ground-truth-test-map.md`
- [Duplicate location in one batch vs. a stale value across runs](duplicate-location-vs-stale.md) — `duplicate-location-vs-stale.md`
- [Endpoint-First Pipelines — Concept](endpoint-first-pipelines.md) — `endpoint-first-pipelines.md`
- [How a Function Node's Inputs Get Built](function-input-resolution.md) — `function-input-resolution.md`
- [Linked vs independent copies of a pipeline](linked-vs-independent-copies.md) — `linked-vs-independent-copies.md`
- [Location filters: one meaning, four implementations](location-filter-semantics.md) — `location-filter-semantics.md`
- [Observability API — question → primitive mapping](observability-api-design.md) — `observability-api-design.md`
- [Output order: one owner (2026-10-28)](output-order.md) — `output-order.md`
- [Package Distribution: Versioning, Dependencies & the Dev-Install Contract](package-distribution.md) — `package-distribution.md`
- [Permanent Filters & Data Exclusions: Design](permanent-filters-design.md) — `permanent-filters-design.md`
- [Phase 8 — GUI Startup Diagnostics](phase-8-startup-diagnostics.md) — `phase-8-startup-diagnostics.md`
- [Placement-qualified ids vs. bare ids: which layer sees which](placement-qualified-ids.md) — `placement-qualified-ids.md`
- [Release process: one tag → PyPI + VS Code Marketplace + Open VSX](release-process.md) — `release-process.md`
- [Studio unification: one concept, one owner, across Process, Plot and Stats](studio-unification.md) — `studio-unification.md`
- [Synthetic factors in scistackplot](synthetic-factors.md) — `synthetic-factors.md`
- [Table/DataFrame Concatenation Optimization](table-concat-optimization.md) — `table-concat-optimization.md`
- [The input binding, and its round trip](input-binding-round-trip.md) — `input-binding-round-trip.md`
- [Variable Groups](variable-groups.md) — `variable-groups.md`
- [`where=` Filter System](where-filter-system.md) — `where-filter-system.md`

## Archive

Superseded notes: [archive/](archive/). Superseded plans: `.claude/archive/`.
