// PlotStudio: the wire types and the small pure helpers over them.
// Moved verbatim out of PlotStudio.tsx (grug stage plotstudio_split,
// docs/claude/grugbrain-principles.md); PlotStudio.tsx is the consumer.
import { type Role as PlotSpecRole, type FactorVariable as GeneratedFactorVariable, type Filter as GeneratedFilter, type VariantSet as GeneratedVariantSet, type PlotSpec } from '../../generated/api-types'
import { type KindTile } from './kindTiles'
import { type SampleOverlay } from './showSample'
import { type CompareCapability } from './compare'
import { type TimeZoneCapability } from './timeZone'
import { type HistogramCapability, type RegressionCapability, type KdeCapability } from './continuousKinds'
import { type MatchOp } from './plotSpecReducer'
import { type PathStep } from './SchemaLocationPicker'
import { rolesAfterPick } from './locationSelection'
import { type AspectPreset } from './figureSize'
import { type CustomPreviewPayload } from './CustomFigure'
import { type OmissionReport, type NotPlottedTotal } from './notPlotted'

/** The dpi `plot_save` renders a raster at when the request names none
 *  (`api/plot.py` SaveRequest). Only the readout uses it; the save itself
 *  takes the backend's default. */
export const SAVE_DPI = 200

/** Four roles, two panes (docs/claude/grouping-and-collapse.md):
 *  `group` is the Grouping section's — an ordered list, innermost first, one
 *  entry optionally coloured — and the Factors dropdown offers the other
 *  three: Separate figures, Separate panels, Collapse. */
export type Role = PlotSpecRole

/** One entry of a factor's role dropdown, as the backend reports it.
 *
 *  There is deliberately no constant list here. The panel used to offer every
 *  role for every factor while `roles.validate` refused several of them, so
 *  picking one could produce an error instead of a plot. Both label and
 *  legality are answered by `capability.role_options`, which derives them by
 *  asking `validate` itself, so this component renders whatever it is given
 *  (CLAUDE.md NOTE 3). */
export interface RoleOption {
  role: Role
  label: string
  hint: string
  available: boolean
  /** validate's own message, naming the one-line fix. Null when available. */
  reason: string | null
}

/** The dropdown's contents when the backend has not reported any — which only
 *  happens before the first capability report lands. One entry, the role the
 *  spec already holds, so the control shows the truth instead of being empty.
 *  Never a guess at what else is legal: that is exactly the question this
 *  stage moved to the backend. */
export function fallbackRoles(current: Role | undefined): RoleOption[] {
  const role = current ?? 'iterate'
  return [{ role, label: role, hint: '', available: true, reason: null }]
}

export const KIND_LABELS: Record<string, string> = {
  scatter: 'Scatter',
  strip: 'Strip (jittered)',
  spaghetti: 'Spaghetti (paired lines)',
  line: 'Lines',
  box: 'Box',
  violin: 'Violin',
  bar: 'Bar + error',
  hbar: 'Horizontal bar + error',
  band: 'Mean + error band',
  heatmap: 'Heatmap',
  histogram: 'Histogram',
  kde: 'Density (KDE)',
  regression: 'Regression (fit line)',
}

export interface FactorInfo {
  name: string
  display: string
  levels: (string | number)[]
  level_count: number
  is_variant: boolean
  /** Levels are the measure's own struct/dict fields, not a condition. */
  is_field: boolean
  /** A dataset schema key (subject, session, …) rather than a variant or a
   *  struct field. Reported by the backend so the panel never works it out by
   *  intersecting two lists. */
  is_schema_key?: boolean
  /** Levels surviving `spec.filters`, measured with the same function the
   *  figure uses. Absent on the pre-filter (`describe`) view. */
  selected?: (string | number)[]
  /** What this factor's dropdown offers, and what it refuses. */
  roles?: RoleOption[]
  /** Whether this factor may join the grouping — the Grouping section's
   *  question, reported apart from `roles` because `group` is not in that
   *  menu. */
  group_available?: boolean
  group_reason?: string | null
  /** How many schema keys pin one of this factor's values — 1 for `subject`
   *  and for a subject-level grouping column, 2 for `session`. A combine sits
   *  just above its source (e.g. 1.5). Null for a variant axis, which is not a
   *  place in the hierarchy. The sort key for the grouping list; see `groups.ts`. */
  depth?: number | null
  /** On a combine: the factor it combines (and whose slot it holds). */
  combined_from?: string | null
  /** On a source replaced by an active combine: that combine. Such a factor
   *  is collapsed by the backend and listed only through the dropdown. */
  combined_into?: string | null
  /** The slot this factor stands in (its source, or itself) and every choice
   *  for it — the Grouping / Factors dropdown. One choice: plain text. */
  slot?: string
  alternatives?: string[]
}

/** A variable joined in as a factor, or one column of it.
 *
 *  `column: null` is the single-data-column case (a per-subject Condition). A
 *  named column is how a wide sheet groups a figure: Demographics as a whole
 *  holds Age, Sex and InterventionGroup at once and so has no value to group
 *  by. The factor lands in the table under the COLUMN's own name, which is
 *  what `name` carries — roles, filters and y-scoping all key on that. */
export type FactorVariable = GeneratedFactorVariable

/** A variable that MAY group this figure — one entry per variable, never per
 *  column.
 *
 *  `kind` says which question comes next. A single-data-column variable
 *  ("categorical"/"numeric") is offerable as it stands and arrives with its
 *  ready-made `offer`. A wide sheet ("columns") needs `plot_grouping_columns`
 *  to say which of its columns qualify — asked when the user opens it, because
 *  answering for every wide variable on every panel open cost ~8 s on a real
 *  project (two EMG variables that offer nothing). */
export interface GroupableVariableInfo {
  variable: string
  kind: 'categorical' | 'numeric' | 'columns'
  label: string
  column_count: number
  offer?: GroupableInfo
  /** A column of the plotted variable itself — see `GroupingCandidate.own`. */
  own?: boolean
}

/** One offer in the Grouping section: a FactorVariable plus what it looks like. */
export interface GroupableInfo extends Omit<FactorVariable, 'column'> {
  /** The offer always names it: null is the variable itself. */
  column: string | null
  /** Qualified, for display: `Demographics.InterventionGroup`. */
  label: string
  /** The factor name it will occupy in the table. */
  name: string
  levels: string[]
  level_count: number
}

/** A row filter, applied before anything else is reduced (generated). */
export type Filter = GeneratedFilter

export interface TableInfo {
  factors: FactorInfo[]
  measures: { name: string; shape: string }[]
  row_count: number
}

/** One kind of the plot-type grid: a tile (`kindTiles.KindTile`, the one owner
 *  of the tile fields) plus what the grid shows about it. */
export interface KindInfo extends KindTile {
  reason: string | null
  /** A plot_ function's name, for its one entry (custom plot kinds). */
  label?: string
  /** Whether picking this kind reduces a 1-D measure to one value per record
   *  first. The kind IS the request to collapse; there is no separate toggle. */
  collapses?: boolean
}

/** One variant factor as the picker renders it. Built entirely by
 *  `capability.variant_summary` — the GUI decides nothing about variants. */
export interface VariantFactorInfo {
  name: string
  levels: string[]
  /** Levels surviving the current selection, measured against the frame. */
  selected: string[]
  /** A code-version axis (`Code:bandpass`) rather than an experimental one. */
  is_code: boolean
  /** A run-options axis (`Run:loader`): which distribute/as_table the function
   *  ran under. Presented like a code axis; levels are flag labels. */
  is_run?: boolean
}

/** One row of the Variants section, as the backend reports it back. */
export interface VariantSetInfo {
  /** What to show: the user's name, or the auto label when they haven't typed. */
  name: string
  auto_label: string
  /** null until the user names it — that is what keeps the label following the
   *  selection while it is still being edited. */
  explicit_name: string | null
  selection: Record<string, unknown>
  /** False for a row added but not filled in yet. Such a row is inert — it
   *  changes nothing about the figure until it says something. */
  defined: boolean
  /** Rows this variant contributes to the figure. Zero is the number worth
   *  showing: a variant selecting a combination nobody ran looks exactly like a
   *  working one until its series fails to appear. */
  row_count: number
  /** Code axes this variant left open and whose versions its rows disagree on.
   *  Reported here because the code columns are no longer offered as factors —
   *  the fix is on this row, not in Factors.
   *
   *  Carries WHICH LOCATIONS hold WHICH VERSION, not just a count: this is now
   *  reported for the ordinary per-location "latest" state too (reversed
   *  2026-09-11), and "pools 2 versions" on the most common state in the system
   *  would be noise. "v2 is only subject 01" is a sentence you can act on. */
  spans: Record<string, SpanInfo>
  /** What the selection resolved to — `latest` becomes the per-location flag,
   *  so this is not the same thing as `selection`. */
  resolved: Record<string, unknown>
  /** Variant combinations that DO exist, populated ONLY when `row_count` is 0.
   *  The pin is applied blindly, so an empty figure comes from controls that
   *  look correctly filled in; this is what makes it self-explaining. */
  available: Record<string, string>[]
}

export interface SpanInfo {
  /** The function whose versions disagree, without the `Code:` prefix. */
  function: string
  /** Version -> how many rows of this variant it built. */
  versions: Record<string, number>
  /** Version -> the schema locations holding it, capped backend-side. */
  locations: Record<string, string[]>
  /** What a location string means, outermost first (`["subject","session"]`),
   *  so `01/pre` can be read without guessing. */
  schema_levels: string[]
  /** True when some version's location list was cut short. */
  truncated: boolean
}

/** One span as a sentence. Mirrors `scistackplot.variants.describe_span` — the
 *  backend log, this tag and the figure banner must not word the same span
 *  three different ways. */
export function describeSpan(span: SpanInfo): string {
  const parts = Object.entries(span.versions).map(([version, count]) => {
    const where = span.locations?.[version] ?? []
    if (where.length === 0) return `${version} (${count} row(s))`
    const listed = where.join(', ')
    return `${version} (${span.truncated ? `${listed}, …` : listed})`
  })
  return `${span.function}: ${parts.join('; ')}`
}

export interface VariantSummary {
  sets: VariantSetInfo[]
  factors: VariantFactorInfo[]
  /** Combinations present in the data — measured, not level counts multiplied,
   *  because real data is ragged and the default selection is on a non-factor
   *  flag. */
  total_combinations: number
  selected_combinations: number
}

/** What the title badge says about the measure.
 *
 *  While a collapse is in effect it names BOTH — `1d → scalar (mean)` — because
 *  a violin of trial means and a violin of raw samples look identical, and a
 *  figure must never claim to be drawing the samples it summarized. */
export function shapeBadge(capabilities?: Capabilities | null): string | undefined {
  if (!capabilities) return undefined
  const collapse = capabilities.cell_collapse
  if (!collapse?.active) return capabilities.shape
  return `${capabilities.raw_shape ?? '1d'} → ${capabilities.shape} (${collapse.statistic})`
}

/** One choice in the "Save data" depth picker (`scistackplot.export.DataDepth`):
 *  the deepest key kept as a column (null = nothing collapsed, the rows as
 *  plotted), what is averaged away first, and the header the file will have. */
export interface DataDepth {
  key: string | null
  label: string
  averaged: string[]
  columns: string[]
  /** The header with one column per struct field, or null when this depth
   *  has no field factor to spread. */
  wide_columns: string[] | null
  /** What the file's header READS (compared value columns named after what
   *  they hold); shown instead of `columns` / `wide_columns`. */
  header?: string[]
  wide_header?: string[] | null
}

/** "Save data": the plot's long table as CSV — whether it can be written
 *  (scalar plots only), why not, and the depths, default first. Decided by
 *  `scistackplot.data_export_options`; the panel only displays it. */
export interface DataExport {
  available: boolean
  reason: string | null
  depths: DataDepth[]
  default: string | null
  chain: string[]
  sample: string[]
  pooled: boolean
  /** The struct field factor (`ColName`) the "one column per field"
   *  checkbox spreads; null when no depth keeps one (no checkbox). */
  field_factor: string | null
}

/** `capabilities.custom` (scistackplot.custom.custom_capabilities). */
export interface CustomCapabilities {
  function: string
  level: 'panel' | 'figure'
  layout_input: string
  layout_kind: string
  reason: string | null
  accepts_context: boolean
  inputs: { param: string; variable: string; shape: string; keys: string[]; as_table: boolean }[]
  constants: string[]
  notes?: string[]
}

/** `describe_function`'s `plot_node`: the node Plot Studio opened on. */
export interface PlotNodeInfo {
  node_id: string
  function: string
  level: 'panel' | 'figure'
  inputs: Record<string, string>
  layout_input: string | null
  notes: string[]
}

export interface Capabilities {
  /** Set when a plot_ function draws the figure. */
  custom?: CustomCapabilities
  /** What the FIGURE is: `scalar` while a 1-D measure is being collapsed. The
   *  scalar-only controls key off this, and they apply to the collapsed value. */
  shape: string
  /** What the DATA is. Differs from `shape` only during a collapse. */
  raw_shape?: string
  /** Whether this 1-D measure's CELLS can be collapsed to one value each,
   *  whether they are being, and by which statistic. Not the collapse chain. */
  cell_collapse?: { applies: boolean; active: boolean; statistic: string }
  /** The collapse chain as the figure runs it: `order` deepest first, the
   *  last one the `sample` — what error bars, boxes and bands are drawn over
   *  — and whether the spec pools instead. */
  collapse?: { order: string[]; sample: string | null; pooled: boolean }
  /** "Show sample": the collapsed keys as checkboxes, what one point is, and
   *  the join decision — all decided by `roles`, displayed here. */
  sample_overlay?: SampleOverlay
  /** Whether this plot type can carry difference bars, and why not
   *  (`scistackplot.diffbars.unavailable`). */
  difference_bars?: { available: boolean; reason: string | null }
  /** Compare to reference: the layers + levels it may name, the default,
   *  and the current comparison's state (`scistackplot.compare`). */
  comparison?: CompareCapability
  /** Data > Time zone: whether a datetime is in play, the mode, the zones the
   *  data holds (`scistackplot.temporal.time_zone_summary`). */
  time_zone?: TimeZoneCapability
  /** Chart > Histogram / Regression (capability.histogram_summary / regression_summary). */
  histogram?: HistogramCapability
  regression?: RegressionCapability
  /** Chart > Density curve (capability.kde_summary). */
  kde?: KdeCapability
  /** Chart > Categories: a text measure's mode, its categories and why it
   *  cannot be drawn (`capability.categorical_summary`). */
  categorical?: {
    applies: boolean
    mode?: string | null
    stat?: string
    level?: string | null
    level_applies?: boolean
    levels?: string[]
    problem?: string | null
  }
  /** Chart > Bars: stacking and what it turns off (`capability.bar_summary`). */
  bar?: { applies: boolean; stack: boolean; note: string | null }
  /** Data > X variable: the current x measure and whether it is a datetime. */
  x_measure?: { current: string | null; temporal: boolean }
  /** Whether any factor is collapsed, i.e. whether there is a sample. */
  has_sample: boolean
  /** "Save data (CSV)": availability and the depth picker's choices. */
  data_export?: DataExport
  default: string
  available: string[]
  kinds: KindInfo[]
  /** Factors of the RESOLVED table — the synthetic `Variant` factor included,
   *  and the columns its selections consumed excluded. The panel renders these
   *  rather than `describe.table.factors`, which is the pre-selection view. */
  factors?: FactorInfo[]
  grouping?: GroupingInfo
  variants?: VariantSummary
}

/** The Grouping section's data model: which factors get one mark per level
 *  combination, in what order, and how the current kind reads that list.
 *
 *  The list is innermost first. For a categorical-x kind the layers are
 *  nested ticks (the coloured one dodges, labelled by the legend); for a 1-D
 *  measure or an x-y plot they are series; for a spaghetti the first entry is
 *  the lines. The backend decides; this panel renders the answer. */
export interface GroupingInfo {
  available: boolean
  /** Why nothing may group (a 2-D measure). Null when it may. */
  reason: string | null
  /** The grouping layers, innermost first — membership and order already
   *  reconciled the same way the figure does it. */
  layers: string[]
  /** The layer labelled by the legend, or null. */
  color?: string | null
  /** How this kind reads the list: tick layers in drawing order, series ids. */
  ticks?: string[]
  series?: string[]
  /** Tick layers labelled below the axis (the coloured one is not), and the
   *  cap on them. */
  labelled_layers?: number
  max_labelled_layers: number
  /** What the list means for this kind, in the figure's own words. */
  hint?: string
  /** The role a grouping ticked right now would take (`roles.role_for_new_
   *  grouping`). Published per SPEC, not per grouping: the factor is not in
   *  the table yet, so the answer does not depend on which one — which is what
   *  keeps a checkbox from costing a round trip. */
  new_grouping_role?: Role
}


export const MATCH_OPS: { value: MatchOp; label: string }[] = [
  { value: 'starts_with', label: 'starts with' },
  { value: 'ends_with', label: 'ends with' },
  { value: 'contains', label: 'contains' },
  { value: 'not_contains', label: 'does not contain' },
  { value: 'equals', label: 'is exactly' },
  { value: 'regex', label: 'matches regex' },
]


/**
 * What the backend actually laid out, echoed back on every figure.
 *
 * The panel never computes a grid itself: `rows`/`cols` here are the EFFECTIVE
 * numbers (so setting one of them visibly fills in the other), and
 * `layout_notes` carries anything the layout had to do that the rules did not
 * ask for — a panel spilled out of a claimed cell, a grid grown to fit.
 */
export interface GridMeta {
  rows?: number
  cols?: number
  panels?: number
  layout_notes?: string[]
  /** The factors a y-limit scope may name, AS RESOLVED by the backend: the
   *  figure's ITERATE keys then its FACET factors, including a schema key
   *  promoted to ITERATE or a facet the table defaulted — neither of which
   *  `spec.roles` mentions. */
  panel_factors?: string[]
  y_scope?: string[]
  /** The plot's x levels this figure has no data for
   *  (ResolvedPlot.missing_x); the "Show missing levels" box appears only
   *  when this is non-empty. */
  missing_x?: string[]
  /** Which side of each panel the logical x / y axes are drawn on
   *  (scistackplot render.base.axis_sides): a horizontal bar puts x (the
   *  categories) at the left and y (the values) along the bottom. */
  axis_sides?: { x: AxisSide; y: AxisSide }
}

export type AxisSide = 'bottom' | 'left'

export type VariantSet = GeneratedVariantSet

/** The figure's spec: `PlotSpec.to_dict()` (GENERATED from the backend's wire
 *  model, scistack_gui.api.spec_wire; each field is documented there). */
export type Spec = PlotSpec

/**
 * Show one schema location and nothing else.
 *
 * Module-level and pure because two callers need it and must not drift: the
 * picker inside this panel, and a location handed over from the canvas, which
 * is applied to the opening spec before the first resolve.
 *
 * Two things happen together, and the second is the one worth explaining.
 * Picking `subject=01` leaves `trial` unanswered, and a trial factor sitting on
 * AGGREGATE would collapse the very thing the user opened the picker to look at
 * — every trial's own values. So any schema key the selection does NOT name is
 * moved off AGGREGATE onto FREE (replicates), and one with no role at all gets
 * FREE too.
 *
 * Deliberately NOT a blanket reassignment: a key already on X, COLOR or FACET
 * is drawing its levels individually, which is what was asked for, and stamping
 * over a chosen x axis because a location was clicked would be astonishing.
 */
export function applyPickedLocation(spec: Spec, path: PathStep[], schemaKeys: string[]): Spec {
  return {
    ...spec,
    roles: rolesAfterPick(spec.roles, path, schemaKeys),
    // A picked row answers "which places", and says nothing about which
    // levels are omitted — so the standing rule survives the pick.
    location_filter: {
      keys: schemaKeys,
      include: [path],
      exclude_levels: spec.location_filter?.exclude_levels ?? {},
    },
  }
}

export interface DescribeResponse {
  catalog: {
    measures: {
      name: string
      shape: string
      plottable: boolean
      columns?: string[]
      /** On a "Var (text)" entry: the variable whose text fields it holds. */
      text_of?: string
    }[]
  }
  variable: string | null
  eligible?: boolean
  reason?: string | null
  table?: TableInfo
  spec?: Spec
  capabilities?: Capabilities
  /** Variables that can supply an x axis (joined). */
  joinable_with?: string[]
  /** Variables that can be plotted as another series (stacked) — same shape,
   *  same schema level. What the Variants rows' variable dropdown offers. */
  stackable_with?: string[]
  /** Why each OTHER variable is not offered, `{name: reason}`. The variant
   *  picker draws every variable node on the canvas, so one it cannot offer has
   *  to say why in place — an un-clickable node with no explanation is
   *  indistinguishable from a broken dialog. */
  stackable_refused?: Record<string, string>
  /** VARIABLES usable as a grouping — one recorded at or above this variable's
   *  level, so each row gets exactly one of its values. One entry per variable,
   *  never per column: `describe` no longer enumerates any wide table's
   *  columns, which is what made opening the panel cost seconds. */
  groupable_variables?: GroupableVariableInfo[]
  /** Why a column of an otherwise eligible variable is not offered,
   *  `{label: reason}`. A user can see the column in their spreadsheet, so its
   *  absence from the list has to be explained where they look for it. */
  groupable_refused?: Record<string, string>
  /** File types this matplotlib can write. Asked of the backend rather than
   *  listed here, so the dropdown and the save cannot disagree. */
  image_formats?: string[]
  /** Aspect-ratio presets for the figure size, in dropdown order. scistackplot
   *  owns the list (`figsize.ASPECT_PRESETS`); the panel only does the
   *  arithmetic (`figureSize.ts`). */
  figure_presets?: AspectPreset[]
  /** Set when Plot Studio opened on a plot_ node. */
  plot_node?: PlotNodeInfo
}

export interface FigurePayload {
  /** Position in the whole fan-out — not in `figures`, which holds one entry
   *  while the navigator is showing a single figure. */
  index: number
  key: Record<string, unknown>
  label: string
  /** The plotly figure; null when a plot_ function drew it (`custom_preview`). */
  figure: { data: unknown[]; layout: Record<string, unknown> } | null
  /** A plot_ function's figure: a PNG or its own plotly figure. */
  custom_preview?: CustomPreviewPayload
  row_count: number
  downsampled_from: number | null
  /** What this figure leaves out (scistackplot.omissions), for the popup. */
  omissions?: OmissionReport | null
}

/** What `plot_resolve` returns. The fan-out is described in full (labels,
 *  count) while only the selected figure carries a payload — a 1-D measure
 *  across thirty subjects is megabytes per figure. */
export interface ResolveResponse {
  ok: boolean
  error: string | null
  figures: FigurePayload[]
  figure_labels: string[]
  figure_count: number
  figure_index: number
  /** Notes about the FIGURE SET: schema keys promoted to ITERATE because a
   *  nested key iterates. Never silently — a user who asked for one figure per
   *  trial and got one per subject-and-trial would think it was broken. */
  notes: string[]
  /** Problems with the DATA (e.g. every value NaN), each shown once as a
   *  VS Code popup — `scistackplot.cell.all_nan_warning`. */
  data_warnings?: string[]
  /** The "Not plotted" button for the figures sent (omissions.combine). */
  not_plotted?: NotPlottedTotal
}

