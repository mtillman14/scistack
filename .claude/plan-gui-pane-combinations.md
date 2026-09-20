# Plan — GUI pane combinations (what tests/integration still does not reach)

Drafted 2026-09-19. Status: **built 2026-09-19 (all four stages), unrun.**

## What is NOT tested today, by Plot Studio pane

| pane | covered | not covered |
|---|---|---|
| **Variants** | two Parameter variants as a colour axis; collapsing the axis refused | named variant ROWS (`variant_sets` with labels, two rows of one variable, a row pinning `latest`); the `latest`/`CodeIsLatest` default pin after a code edit; a row whose selection matches nothing; a row for a variable with no variants; the variant provenance panel (`Inspector.provenance` by pin); run-option variants (`distribute`/`as_table` as an axis); code-version rows after two edits |
| **Schema keys** (location tree) | `LocationFilter` prefixes / exclude / ragged / nothing-matches | the picker's own state machine (`location_tree`, `node_location_tree`, problems-only view); a prefix on a non-contiguous record; prefix + per-column `Filter` together; prefix + variant pin; the tree over a variable saved at two levels |
| **Filters** | level include; measure range not at all | `minimum`/`maximum` on the measure (and on a cell-collapsed 1-D); include + exclude on one column; a filter on a variant column; a filter on a joined factor column (`Demographics.group`); filters that leave one level (legend rule) |
| **Grouping** | GROUP lists up to the cap; colour on a layer; nested x (bar) | colour on the OUTER layer vs inner; grouping by a joined factor variable AND a schema key; `level_groups` (derived buckets) in the grouping and as colour; grouping order changed by hand vs by depth; spaghetti with a coloured first layer; series/dash styles on a 1-D with 2–3 uncoloured layers (past the 6-dash cycle) |
| **Factors** (roles) | the sweep, schema keys only | roles on `ColName`, `Variant`, `Variable`, a `level_groups` bucket and a joined factor — the sweep fixes `ColName=FACET` and never varies the synthetic factors |
| **Plot type** | every kind once, both renderers | `x_measure` (relational scatter) with every grouping; heatmap on a real 2-D record (the dataset has none); kind switch that re-defaults roles then a manual override (the "never overwrite a role the user set" rule) through the service |
| **Layout** | default grid | `n_rows`/`n_cols`; row/col MATCHERS (`starts_with` …) on the joints; a rule matching nothing (the "other" row); rules + a variant axis as facet |
| **Y axis** | limits exist | `scope` per key vs global; manual min/max; `log_y` with zero/negative values; scope × pooled × show-sample (the extent modes) |
| **Summary** | mean ± SD, pooled | median + IQR, SEM, CI95, `ErrorBand.NONE`; `cell_statistic` median/max on the 1-D; pooled × each |
| **Show sample** | one bar case | on box/violin/scatter/strip; `join_sample` override both ways; show the sample key itself vs a deeper key; with colour; with a nested x; with a variant axis |
| **Figure size / style** | — | font size, custom aspect, labels, title — mostly cosmetic, but `render_matplotlib` must not raise for any preset |
| **Save / export** | image, CSV, endpoint parity for one bar | export of every kind the sweep draws (codegen coverage!); image save of a fan-out into a folder; export with a variant pin, a location filter, level groups, a joined factor — each is a codegen path |

## Not tested on the DAG side at all

| surface | what it is |
|---|---|
| **ColumnSelection in node config** | `columnSelections` (columns / `iterate`) applied by the run service — the integration suite uses `Var["col"]` in Python only |
| **Run options in node config** | distribute / as_table / where filters / schemaSelection stored per node and read back at run time |
| **Run service** | executing a node from its config (`run_service`), cancel, the failure message for a missing input |
| **Node state** | up-to-date / missing / stale after each of the D-scenarios (`check_node_state`, the discovery gate) |
| **Entities file** | create a Variable / Parameter / PathInput from the GUI and run with it (`entities_file`, `scistack_entities.toml`) |
| **Manual edges on history nodes**, **glue nodes**, **layout write race**, **placement ids** | GUI-graph internals; only reachable through the JSON-RPC handlers with a fake webview |

## The approach: pairwise generation through the SERVICE layer

Hand-writing the cross product is hopeless (kinds x grouping x colour x
variants x sample x location x filters x summary x y-axis x layout is
millions). Two things make it tractable:

1. **All-pairs coverage.** Every PAIR of axis values appears in at least one
   case. For ~12 axes of 2–5 values that is ~40–60 cases instead of
   millions, and pairwise interactions are where these bugs live (colour x
   variant, sample x pooled, location x filter).
2. **Run them as the webview does** — JSON spec dicts through
   `plot_service.capabilities_for` / `resolve_figures` / `save_plot_data` /
   `export_code`, not `PlotSpec` objects. The dict shape (nulls, missing
   keys, legacy names) is a whole class of GUI breakage the object API
   never sees.

Contract per case, same as the edge suite: a result or a typed message;
plus, when a figure is drawn, the CSV parity check and a codegen export
that compiles and runs.

### Axes (first cut)

kind {bar, box, scatter, line, band, spaghetti} · grouping {1 key, 2 keys,
key+joined factor, key+level_group} · colour {none, inner, outer, variant}
· variants {none, 1 row, 2 rows of one variable, 2 variables} · show_sample
{off, sample key, deeper key, +join override} · location {none, prefix,
prefix+exclude} · filters {none, level, measure range} · summary
{mean/SD, median/IQR, pooled, NONE} · cell_statistic {mean, max} (1-D only)
· y_axis {auto, scoped, manual, log} · layout {flow, n_cols, matchers}

### Stages

1. **Generator + runner** (`tests/integration/pairwise.py`): a small
   all-pairs implementation (no new dependency), axis definitions that build
   a JSON spec dict for the example's `CycleSymmetry` / `CycleWaveform`, and
   one parametrised test through the service layer with the contract.
2. **DAG-side**: node-config → run-service tests (column selection, run
   options, where filters, schema selection) on the scratch database, and
   node state after each D-scenario.
3. **Pane specifics** that pairwise cannot express (variant rows, the
   location tree state, matchers) as targeted tests.
4. **Codegen coverage**: export + exec for every case the generator draws.

Estimated size: ~150 generated cases plus ~40 targeted; a few minutes.

## Built 2026-09-19

* `tests/integration/pairwise.py` — `all_pairs()` (greedy, deterministic) and
  the two axis sets with their JSON-spec builders (`scalar_spec`,
  `series_spec`).
* `tests/integration/test_pairwise_specs.py` — every generated case through
  `capabilities_for` / `resolve_figures` / `save_plot_data` / `export_code`
  as a dict; CSV parity for drawn scalar cases; the generator's own
  every-pair guarantee.
* `tests/integration/test_dag_runs.py` — `api/run._run_in_thread` on the
  test thread: re-run from history, no-history message, dry_run / save=False,
  schema selection (prefix and exclude), where filters, node-config column
  selections (plain and for_columns), node state after an upstream edit.
  Recomputes are driven by editing files on a copied data root and
  re-running the LOADER, never by a direct save (that is A11's ambiguity).
* `tests/integration/test_pane_specifics.py` — variant rows (two of one
  variable, one matching nothing, two variables, the pin after a code edit),
  the location-tree service, layout matchers and grid shapes, every summary
  statistic and y-axis mode, show sample x every summative kind x join
  modes, the 2-D heatmap, relational scatters, export of every kind executed
  on the pipeline's own frame.
* Dataset: `JointCoupling`, a 3 x 3 matrix per trial (72 files), loaded by
  `load_joint_coupling` — the heatmap path.

## Open findings (xfail, 2026-09-19)

Two column-selection gaps the DAG suite found, left as `strict=True` xfails so
they fail loudly when fixed:

1. **`for_columns` records no selector.** Its reassembly save path writes the
   invocation's input edges from `__upstream` (selector-less) rather than
   `__graph_var_bindings`, so `pipeline_variants[].selectors` is `{}` and a
   GUI re-run binds the whole variable. A plain `Var["col"]` does record one
   and now re-runs correctly. The simple `for_columns` shape DOES record it
   (`scidb/tests/test_reload_supersedes_changed_file.py`), so the gap is
   specific to the reassembly path.
2. **A node-config selection never reaches the run.** No `[column_selection]`
   line is logged and the function gets every column — either the node id in
   `_node_config` is not matched by `column_selections_for_nodes`, or the
   targets it stamps are not the ones the run uses.

Both are in `scistack-gui`/`scidb`, not the tests.
