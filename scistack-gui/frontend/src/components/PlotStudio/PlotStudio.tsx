/**
 * Plot Studio — interactive plotting for a scidb variable.
 *
 * The panel is deliberately thin. It renders whatever `plot_describe` and
 * `plot_capabilities` return and sends back a spec; every decision about which
 * plot kinds are legal, what the defaults are, and how data is reduced lives in
 * scistackplot (CLAUDE.md NOTE 3). Adding a plot kind or a role should require
 * no change here beyond a label.
 *
 * The controls replace the four mutually-dependent checkbox groups of the
 * original R/Shiny app with one rule: every factor carries exactly one role.
 * That is a single <select> per factor, and the invariant is enforced by the
 * backend rather than by widgets updating each other's options.
 *
 * See docs/claude/plotting-library-design.md.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import createPlotlyComponent from 'react-plotly.js/factory'
import Plotly from 'plotly.js-cartesian-dist-min'
import { callBackend, isVSCodeMode } from '../../api'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import VariantDagPopup from './VariantDagPopup'
import GroupingDagPopup from './GroupingDagPopup'
import SchemaLocationPicker, { type PathStep } from './SchemaLocationPicker'
import {
  type LocationSelection,
  asSelection,
  describeSelection,
  rolesAfterPick,
} from './locationSelection'
import { orderGroups, placeGroupLayer } from './groups'
import {
  isLocked,
  isTicked,
  joinChoice,
  joinSetting,
  toggleShowSample,
  type JoinChoice,
  type SampleOverlay,
} from './showSample'
import {
  type AspectPreset,
  CUSTOM_ASPECT,
  FALLBACK_PRESETS,
  aspectName,
  heightFor,
  pixelReadout,
} from './figureSize'

/** The dpi `plot_save` renders a raster at when the request names none
 *  (`api/plot.py` SaveRequest). Only the readout uses it; the save itself
 *  takes the backend's default. */
const SAVE_DPI = 200

const Plot = createPlotlyComponent(Plotly)

/** Four roles, two panes (docs/claude/grouping-and-collapse.md):
 *  `group` is the Grouping section's — an ordered list, innermost first, one
 *  entry optionally coloured — and the Factors dropdown offers the other
 *  three: Separate figures, Separate panels, Collapse. */
type Role = 'group' | 'facet' | 'iterate' | 'collapse'

/** One entry of a factor's role dropdown, as the backend reports it.
 *
 *  There is deliberately no constant list here. The panel used to offer every
 *  role for every factor while `roles.validate` refused several of them, so
 *  picking one could produce an error instead of a plot. Both label and
 *  legality are answered by `capability.role_options`, which derives them by
 *  asking `validate` itself, so this component renders whatever it is given
 *  (CLAUDE.md NOTE 3). */
interface RoleOption {
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
function fallbackRoles(current: Role | undefined): RoleOption[] {
  const role = current ?? 'iterate'
  return [{ role, label: role, hint: '', available: true, reason: null }]
}

const KIND_LABELS: Record<string, string> = {
  scatter: 'Scatter',
  strip: 'Strip (jittered)',
  spaghetti: 'Spaghetti (paired lines)',
  line: 'Lines',
  box: 'Box',
  violin: 'Violin',
  bar: 'Bar + error',
  band: 'Mean + error band',
  heatmap: 'Heatmap',
}

interface FactorInfo {
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
   *  and for a subject-level grouping column, 2 for `session`. Null for a
   *  variant axis or a derived bucket, which are not places in the hierarchy.
   *  The sort key for the grouping list; see `groups.ts`. */
  depth?: number | null
}

/** A factor derived by bucketing another factor's levels. */
interface LevelGroup {
  /** The new factor's name, e.g. "Phase". */
  name: string
  /** The factor being bucketed, e.g. "session". */
  source: string
  /** `{level: group label}`. */
  mapping: Record<string, string>
  /** null DROPS rows the mapping does not name; a string buckets them. There is
   *  no "leave them unlabelled" — a NaN group becomes its own silent series. */
  unmatched: string | null
}

/** A variable joined in as a factor, or one column of it.
 *
 *  `column: null` is the single-data-column case (a per-subject Condition). A
 *  named column is how a wide sheet groups a figure: Demographics as a whole
 *  holds Age, Sex and InterventionGroup at once and so has no value to group
 *  by. The factor lands in the table under the COLUMN's own name, which is
 *  what `name` carries — roles, filters and y-scoping all key on that. */
interface FactorVariable {
  variable: string
  column: string | null
  /** Which VARIANT of the grouping variable supplies the labels — a selection
   *  keyed by frame column, exactly like a variant row's. Absent/empty means
   *  nothing was said: every variant contributes, and where that leaves two
   *  labels for one schema location the backend warns and takes the first. */
  variant?: Record<string, unknown>
}

/** A variable that MAY group this figure — one entry per variable, never per
 *  column.
 *
 *  `kind` says which question comes next. A single-data-column variable
 *  ("categorical"/"numeric") is offerable as it stands and arrives with its
 *  ready-made `offer`. A wide sheet ("columns") needs `plot_grouping_columns`
 *  to say which of its columns qualify — asked when the user opens it, because
 *  answering for every wide variable on every panel open cost ~8 s on a real
 *  project (two EMG variables that offer nothing). */
interface GroupableVariableInfo {
  variable: string
  kind: 'categorical' | 'numeric' | 'columns'
  label: string
  column_count: number
  offer?: GroupableInfo
}

/** One offer in the Grouping section: a FactorVariable plus what it looks like. */
interface GroupableInfo extends FactorVariable {
  /** Qualified, for display: `Demographics.InterventionGroup`. */
  label: string
  /** The factor name it will occupy in the table. */
  name: string
  levels: string[]
  level_count: number
}

/** A row filter, applied before anything else is reduced. */
interface Filter {
  column: string
  /** Keep only these levels. Absent means "every level" — an all-levels list is
   *  never stored, because it would rot as soon as new data arrived. */
  include?: (string | number)[]
  exclude?: (string | number)[]
  minimum?: number
  maximum?: number
}

interface TableInfo {
  factors: FactorInfo[]
  measures: { name: string; shape: string }[]
  row_count: number
}

interface KindInfo {
  kind: string
  available: boolean
  reason: string | null
  /** Whether picking this kind reduces a 1-D measure to one value per record
   *  first. The kind IS the request to collapse; there is no separate toggle. */
  collapses?: boolean
  /** The assignment (roles, grouping order, colour) this kind should open
   *  with, when selecting it should re-default them (untouched defaults only —
   *  `roles.roles_for_kind`). Carried on the option so `setKind` can apply it
   *  SYNCHRONOUSLY with the click: an RPC would race the resolve queue and
   *  could land after the user had set a role, overwriting the one thing the
   *  rule promises not to touch. */
  assignment?: { roles: Record<string, Role>; groups: string[]; color: string | null } | null
}

/** One variant factor as the picker renders it. Built entirely by
 *  `capability.variant_summary` — the GUI decides nothing about variants. */
interface VariantFactorInfo {
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
interface VariantSetInfo {
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

interface SpanInfo {
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
function describeSpan(span: SpanInfo): string {
  const parts = Object.entries(span.versions).map(([version, count]) => {
    const where = span.locations?.[version] ?? []
    if (where.length === 0) return `${version} (${count} row(s))`
    const listed = where.join(', ')
    return `${version} (${span.truncated ? `${listed}, …` : listed})`
  })
  return `${span.function}: ${parts.join('; ')}`
}

interface VariantSummary {
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
function shapeBadge(capabilities?: Capabilities | null): string | undefined {
  if (!capabilities) return undefined
  const collapse = capabilities.cell_collapse
  if (!collapse?.active) return capabilities.shape
  return `${capabilities.raw_shape ?? '1d'} → ${capabilities.shape} (${collapse.statistic})`
}

/** One choice in the "Save data" depth picker (`scistackplot.export.DataDepth`):
 *  the deepest key kept as a column (null = nothing collapsed, the rows as
 *  plotted), what is averaged away first, and the header the file will have. */
interface DataDepth {
  key: string | null
  label: string
  averaged: string[]
  columns: string[]
  /** The header with one column per struct field, or null when this depth
   *  has no field factor to spread. */
  wide_columns: string[] | null
}

/** "Save data": the plot's long table as CSV — whether it can be written
 *  (scalar plots only), why not, and the depths, default first. Decided by
 *  `scistackplot.data_export_options`; the panel only displays it. */
interface DataExport {
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

interface Capabilities {
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
interface GroupingInfo {
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

type MatchOp = 'starts_with' | 'ends_with' | 'contains' | 'not_contains' | 'equals' | 'regex'

const MATCH_OPS: { value: MatchOp; label: string }[] = [
  { value: 'starts_with', label: 'starts with' },
  { value: 'ends_with', label: 'ends with' },
  { value: 'contains', label: 'contains' },
  { value: 'not_contains', label: 'does not contain' },
  { value: 'equals', label: 'is exactly' },
  { value: 'regex', label: 'matches regex' },
]

interface Matcher {
  op: MatchOp
  value: string
  label?: string | null
}

interface FacetOptions {
  /** Grid size. null/absent means "compute me from the other one and the panel count". */
  n_rows?: number | null
  n_cols?: number | null
  /** One entry per row / column slot; a blank value is an unset slot. */
  rows?: Matcher[]
  cols?: Matcher[]
  share_x?: boolean
  // No `share_y` — it became `Spec.y_axis`, see YAxis below.
}

/**
 * What the y axis spans, and what separates spans.
 *
 * `scope` names the factors that get their OWN limits. Empty means one range
 * across the whole dataset; naming every panel factor means each panel scales
 * to itself; anything between is the useful middle ("one scale per subject").
 * Only factors that separate panels — iterate and facet — may appear, because
 * a colour or replicate factor lives inside a single panel.
 */
interface YAxis {
  scope?: string[]
  minimum?: number | null
  maximum?: number | null
}

/**
 * What the backend actually laid out, echoed back on every figure.
 *
 * The panel never computes a grid itself: `rows`/`cols` here are the EFFECTIVE
 * numbers (so setting one of them visibly fills in the other), and
 * `layout_notes` carries anything the layout had to do that the rules did not
 * ask for — a panel spilled out of a claimed cell, a grid grown to fit.
 */
interface GridMeta {
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
}

interface VariantSet {
  name: string | null
  selection: Record<string, unknown>
  /** Which variable this row draws from; null/absent means the primary measure.
   *  Rows over different variables stack into the same `Variant` factor, which
   *  is how "Raw vs Filtered" and "v1 vs v2" become one mechanism. */
  variable?: string | null
}

interface Spec {
  measures: string[]
  /** Variable supplying the x axis of a relational plot. Separate from
   *  `measures` because it JOINS (one x per y) where extra series STACK. */
  x_measure?: string | null
  roles: Record<string, Role>
  /* The grouping layers, INNERMOST FIRST. Membership is `roles`; this is only
     the order, so assigning a role can never make the spec invalid. */
  groups?: string[]
  /* The grouping layer labelled by the legend, or null. */
  color?: string | null
  kind: string
  /* Centre and spread over the SAMPLE — the outermost collapsed key's levels
     after the inner collapses. `pooled` drops every collapsed key in one
     groupby instead ("weight by N"). */
  aggregate?: { statistic: string; error: string; pooled?: boolean }
  /* How a 1-D measure's CELLS are reduced to one value each when a scalar
     kind is selected for it. Only meaningful while that is true — there is no
     "collapse on/off", the kind decides. */
  cell_statistic?: string
  /* "Show sample": the collapsed keys whose data is overlaid as points; a
     deeper key implies the shallower ones. `join_sample` null = automatic. */
  show_sample?: string[]
  join_sample?: boolean | null
  facet?: FacetOptions
  /* Which factors get their own y limits, plus manual overrides. */
  y_axis?: YAxis
  /* Variables — or single columns of them — joined in as FACTORS rather than
     plotted: a subject-level Condition holding stim/sham, or the
     InterventionGroup column of a wide demographics table. They take a role
     like any other factor. `column: null` means the variable itself. */
  factor_variables?: FactorVariable[]
  /* Factors derived by bucketing another factor's levels (session -> Phase). */
  level_groups?: LevelGroup[]
  /* Row filters. The Filters section writes these. Schema keys no longer do:
     they are the location picker's, which needs a shape `Filter` cannot hold
     (see `location_filter`). A cleared filter removes its entry entirely rather
     than storing every level. */
  filters?: Filter[]
  /* Which schema locations to draw, as hierarchy prefixes — the location
     picker's storage. Separate from `filters` because a tree of checkboxes
     means something RAGGED (all of subject 01, plus trials 1-3 of subject 02)
     and per-column include-lists can only express a Cartesian product.
     Empty `include` is inert: everything is drawn. */
  location_filter?: { keys: string[]; include: PathStep[][]; exclude_levels?: Record<string, string[]> }
  /* One entry per row of the Variants section. One row is a pin (the figure
     shows that variant); several are a comparison, and a `Variant` factor
     appears in Factors carrying whichever role the user gives it. */
  variant_sets?: VariantSet[]
  /* Cosmetics. `width`/`height` are INCHES and size the SAVED figure (and
     the exported code); the preview fills its pane regardless. */
  style?: { width?: number; height?: number; font_size?: number; [key: string]: unknown }
}

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
function applyPickedLocation(spec: Spec, path: PathStep[], schemaKeys: string[]): Spec {
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

interface DescribeResponse {
  catalog: { measures: { name: string; shape: string; plottable: boolean }[] }
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
}

interface FigurePayload {
  /** Position in the whole fan-out — not in `figures`, which holds one entry
   *  while the navigator is showing a single figure. */
  index: number
  key: Record<string, unknown>
  label: string
  figure: { data: unknown[]; layout: Record<string, unknown> }
  row_count: number
  downsampled_from: number | null
}

/** What `plot_resolve` returns. The fan-out is described in full (labels,
 *  count) while only the selected figure carries a payload — a 1-D measure
 *  across thirty subjects is megabytes per figure. */
interface ResolveResponse {
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
}

interface Props {
  /** Variable type (scidb) or column name (CSV). Empty picks the default. */
  variable: string
  /** Set to plot a CSV file instead of the project database. */
  csvPath?: string
  /**
   * True when this IS the whole webview (its own VS Code tab) rather than a
   * modal over the DAG: no backdrop, no rounded card, no close button — the
   * tab's own chrome does that.
   */
  embedded?: boolean
  /**
   * One schema location to open on — the canvas picker's row click, arriving
   * through the extension. Applied once, to the spec `describe` returns, so
   * "click a location, see that location" holds from the canvas exactly as it
   * does from the picker inside this panel.
   */
  initialLocation?: PathStep[]
  onClose: () => void
}

export default function PlotStudio({
  variable,
  csvPath,
  embedded = false,
  initialLocation,
  onClose,
}: Props) {
  // Threaded into every call: the backend picks CsvSource or ScidbSource from
  // it, and nothing else about the panel changes (one DataSource protocol).
  const sourceParams = useMemo(
    () => (csvPath ? { csv_path: csvPath } : {}),
    [csvPath]
  )
  const [describe, setDescribe] = useState<DescribeResponse | null>(null)
  const [spec, setSpec] = useState<Spec | null>(null)
  const [capabilities, setCapabilities] = useState<Capabilities | null>(null)

  // Factors of the resolved table (Variant included, selected-away columns
  // gone). `describe.table.factors` is the pre-selection view and would show
  // both a `Code:` column and the `Variant` factor that consumed it.
  //
  // Declared up here, beside the state it reads, because the role callbacks
  // below need `factorDepths` — a callback referring forward to a `const`
  // further down the body reads fine and is a trip hazard nobody should have
  // to check.
  const factors = capabilities?.factors ?? describe?.table?.factors ?? []

  // Depth per factor, from the backend (`FactorInfo.depth`). Nothing here works
  // out what "outer" means — it reads the number scistackplot publishes, so the
  // control and the figure nest a new layer the same way. See `groups.ts`.
  const factorDepths = useMemo(() => {
    const depths: Record<string, number | null | undefined> = {}
    for (const factor of factors) depths[factor.name] = factor.depth
    return depths
  }, [factors])

  // Policy, though, comes from the backend: whether the x axis can be grouped
  // at all for this measure's shape, why not when it cannot, and what role a
  // grouping ticked right now would take.
  const grouping = capabilities?.grouping

  const [figures, setFigures] = useState<FigurePayload[]>([])
  // Which figure of the ITERATE fan-out is on screen. The fan-out runs in
  // schema order (outermost key most significant), so stepping past subject
  // 1's last trial lands on subject 2's first — the backend orders it, this is
  // just a cursor into that list.
  const [figureIndex, setFigureIndex] = useState(0)
  const [figureLabels, setFigureLabels] = useState<string[]>([])
  const [fanoutNotes, setFanoutNotes] = useState<string[]>([])
  const [specError, setSpecError] = useState('')
  const [loadError, setLoadError] = useState('')
  const [busy, setBusy] = useState(false)
  const [code, setCode] = useState<string | null>(null)
  const [notice, setNotice] = useState('')
  // Reclaiming space happens at two levels: hiding the controls rail inside
  // the panel, and asking VS Code to enlarge the tab the webview lives in.
  const [controlsHidden, setControlsHidden] = useState(false)
  // Index of the variant row whose DAG popup is open, or null.
  const [variantEditor, setVariantEditor] = useState<number | null>(null)
  // "+ Add variant" is a two-step pick, not an immediate append — see
  // addVariantFromPicker for why the row is created on apply instead.
  const [addingVariant, setAddingVariant] = useState(false)
  // The schema location picker — the whole of the "Schema keys" section.
  const [locationPickerOpen, setLocationPickerOpen] = useState(false)
  // The figure-size dropdown's own choice, held as state rather than derived
  // from the size alone: derived-only, picking "Custom" over an 8 x 6 figure
  // would snap straight back to 4:3. Null means "say what the size is".
  const [aspectChoice, setAspectChoice] = useState<string | null>(null)
  const panelRef = useRef<HTMLDivElement>(null)
  const [canvasHeight, setCanvasHeight] = useState(0)
  const observerRef = useRef<ResizeObserver | null>(null)

  // A CSV has no variable type; name the file instead.
  const title = describe?.variable ?? variable ?? (csvPath ? csvPath.split('/').pop() ?? 'CSV' : '')

  // Plotly needs a definite pixel height, so measure the canvas rather than
  // hardcoding one: a maximized panel should give the figure the extra space.
  //
  // A callback ref, NOT a mount effect: on mount this component renders the
  // "Loading…" branch, which has no canvas, so an effect keyed on [] observed
  // nothing and left the height at 0 — the figure then sat at its 320px floor
  // in a full-height pane.
  const canvasRef = useCallback((node: HTMLDivElement | null) => {
    observerRef.current?.disconnect()
    if (!node || typeof ResizeObserver === 'undefined') return
    setCanvasHeight(node.getBoundingClientRect().height)
    const observer = new ResizeObserver(entries => {
      setCanvasHeight(entries[0].contentRect.height)
    })
    observer.observe(node)
    observerRef.current = observer
  }, [])

  useEffect(() => () => observerRef.current?.disconnect(), [])

  // --- open ---------------------------------------------------------------
  useEffect(() => {
    let cancelled = false
    setDescribe(null)
    setSpec(null)
    setFigures([])
    setAspectChoice(null)
    setLoadError('')
    callBackend('plot_describe', { variable: variable || undefined, ...sourceParams })
      .then(raw => {
        if (cancelled) return
        const response = raw as DescribeResponse
        setDescribe(response)
        // A location handed over from the canvas is applied to the OPENING
        // spec, not as a later edit: seeding it here means the first resolve
        // already draws that location, rather than drawing everything once and
        // then narrowing — which reads as a flicker and costs a full resolve of
        // the whole dataset on the way past.
        if (response.spec) {
          const keys = (response.table?.factors ?? [])
            .filter(f => f.is_schema_key)
            .map(f => f.name)
          setSpec(
            initialLocation?.length
              ? applyPickedLocation(response.spec, initialLocation, keys)
              : response.spec
          )
        }
        if (response.capabilities) setCapabilities(response.capabilities)
      })
      .catch(err => !cancelled && setLoadError((err as Error).message))
    return () => { cancelled = true }
    // `initialLocation` is deliberately NOT a dependency: it is what the panel
    // OPENED on, and re-running describe because it changed would throw away
    // every edit made since. PlotRoot remounts on retarget, which is how a
    // second location from the canvas arrives.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [variable, sourceParams])

  // Which factors fan the figure set out. Changing THAT is what invalidates a
  // cursor into the fan-out; changing a colour or a plot kind does not, and
  // resetting to figure 1 on every spec edit would make the panel unusable
  // while browsing. A fan-out that merely got shorter is handled by the
  // backend's clamp instead.
  const iterateSignature = useMemo(
    () =>
      Object.entries(spec?.roles ?? {})
        .filter(([, role]) => role === 'iterate')
        .map(([name]) => name)
        .sort()
        .join('|'),
    [spec?.roles]
  )
  useEffect(() => { setFigureIndex(0) }, [iterateSignature])

  // --- resolve on every spec change (debounced) ---------------------------
  const timer = useRef<number | null>(null)
  // The spec the capability report was last fetched for. Stepping the
  // navigator re-resolves (that is the point — only the shown figure is
  // serialized) but cannot change which plot kinds are legal, so it must not
  // cost a second RPC per arrow press.
  const capsSpecRef = useRef<string | null>(null)
  const specKey = useMemo(() => JSON.stringify(spec ?? null), [spec])
  // Monotonic id of the newest request. A reply whose id is not the current one
  // is stale and is DROPPED — see below for why that matters so much here.
  const generation = useRef(0)
  // --- single-flight ------------------------------------------------------
  // At most ONE resolve is in flight, and at most one waiting behind it.
  //
  // The generation counter below drops stale REPLIES, which was never the whole
  // problem: the backend still ran every superseded request to completion. The
  // 180 ms debounce only coalesces keystrokes, so a drag across role dropdowns
  // fired a resolve every 180 ms while each took 4-16 s, and the server spawns a
  // thread per request and never cancels (server.py). Measured 2026-09-10 and
  // again 2026-09-11: four to six full resolves in flight at once, each
  // re-exploding millions of rows and slowing the others down, with resolve
  // times climbing 3.3s -> 7.6s -> 15.2s -> 27.7s as they piled up — until one
  // crossed the fixed 30 s transport timeout and surfaced as "Request
  // plot_resolve timed out". Nothing had hung; the panel was competing with its
  // own abandoned work.
  //
  // Holding one back turns that drag into two resolves total: the one already
  // running, and the state the user actually stopped on. Intermediate specs are
  // never sent at all, which is the only way to not pay for them — the
  // transport has no cancel, so anything already dispatched runs to completion.
  const inFlight = useRef(false)
  const queued = useRef<{ spec: Spec; figureIndex: number; specKey: string } | null>(null)

  // Explicitly typed because it calls itself (to drain `queued`), and an
  // inferred self-referential const is a TS error rather than a cycle.
  const launchResolve: (spec: Spec, index: number, key: string) => void = useCallback(
    (nextSpec: Spec, nextIndex: number, nextKey: string) => {
      inFlight.current = true
      const mine = ++generation.current
      setBusy(true)
      const needCapabilities = capsSpecRef.current !== nextKey
      Promise.all([
        callBackend('plot_resolve', {
          spec: nextSpec,
          figure_index: nextIndex,
          ...sourceParams,
        }),
        needCapabilities
          ? callBackend('plot_capabilities', { spec: nextSpec, ...sourceParams }).catch(
              () => null
            )
          : Promise.resolve(null),
      ])
        .then(([resolved, caps]) => {
          if (mine !== generation.current) return
          const result = resolved as ResolveResponse
          setSpecError(result.ok ? '' : (result.error ?? 'Could not resolve this plot.'))
          setFigures(result.figures ?? [])
          setFigureLabels(result.figure_labels ?? [])
          setFanoutNotes(result.notes ?? [])
          // Adopt the index the backend actually rendered. It clamps a stale
          // cursor against a fan-out that shrank, and the arrows must never
          // disagree with the figure on screen. React bails out when the value
          // is unchanged, so this cannot loop.
          if (typeof result.figure_index === 'number') setFigureIndex(result.figure_index)
          if (caps) {
            setCapabilities(caps as Capabilities)
            capsSpecRef.current = nextKey
          }
        })
        .catch(err => {
          if (mine !== generation.current) return
          setSpecError((err as Error).message)
        })
        .finally(() => {
          inFlight.current = false
          const next = queued.current
          queued.current = null
          if (next) {
            // Someone changed the spec while this was running. Send the LAST
            // such state — everything before it is already superseded and
            // sending it would be exactly the pile-up this exists to stop.
            launchResolve(next.spec, next.figureIndex, next.specKey)
            return
          }
          // Only the newest request may say the panel is idle. A superseded one
          // finishing late would otherwise clear the spinner while the resolve
          // the user is actually waiting for is still running.
          if (mine === generation.current) setBusy(false)
        })
    },
    [sourceParams]
  )

  useEffect(() => {
    if (!spec) return
    if (timer.current) window.clearTimeout(timer.current)
    timer.current = window.setTimeout(() => {
      if (inFlight.current) {
        // Replaces any earlier waiter rather than queueing behind it.
        queued.current = { spec, figureIndex, specKey }
        // The panel is still working on the user's behalf, so it must still say
        // so — the running request may be for a spec they have already moved on
        // from, and clearing `busy` when it lands would be a lie.
        setBusy(true)
        return
      }
      launchResolve(spec, figureIndex, specKey)
    }, 180)
    return () => { if (timer.current) window.clearTimeout(timer.current) }
  }, [spec, specKey, figureIndex, sourceParams, launchResolve])

  // --- figure navigation --------------------------------------------------
  const figureCount = figureLabels.length
  const stepFigure = useCallback(
    (delta: number) =>
      setFigureIndex(current =>
        Math.max(0, Math.min(current + delta, Math.max(0, figureCount - 1)))
      ),
    [figureCount]
  )

  useEffect(() => {
    if (figureCount < 2) return
    const onKey = (event: KeyboardEvent) => {
      // Never steal the arrows from a field the user is typing in.
      const target = event.target as HTMLElement | null
      const tag = target?.tagName
      if (tag === 'INPUT' || tag === 'SELECT' || tag === 'TEXTAREA') return
      if (event.key === 'ArrowRight') stepFigure(1)
      else if (event.key === 'ArrowLeft') stepFigure(-1)
      else return
      event.preventDefault()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [figureCount, stepFigure])

  // --- spec edits ---------------------------------------------------------
  const setRole = useCallback((factor: string, role: Role) => {
    setSpec(prev => {
      if (!prev) return prev
      const roles = { ...prev.roles, [factor]: role }
      // Keep the grouping order in step with membership: a factor newly
      // grouped is placed by the data's own nesting, one leaving drops out —
      // and takes the colour with it, since colour names a grouping layer.
      //
      // PLACED, not appended. And because this writes `groups` on every
      // toggle, the backend's own depth rule (`ordered_groups`) never sees an
      // unplaced holder to place: this is the copy that decides what the user
      // gets, which is why it lives in a tested module (`groups.ts`) rather
      // than inline here.
      const current = prev.groups ?? []
      const groups =
        role === 'group'
          ? placeGroupLayer(current, factor, factorDepths)
          : current.filter(name => name !== factor)
      const color = role !== 'group' && prev.color === factor ? null : prev.color
      return { ...prev, roles, groups, color }
    })
  }, [factorDepths])

  /** Move a grouping layer one step inward (-1, towards the first entry) or
   *  outward (+1). */
  const moveGroupLayer = useCallback((factor: string, delta: number) => {
    setSpec(prev => {
      if (!prev) return prev
      const layers = [...(prev.groups ?? [])]
      const from = layers.indexOf(factor)
      const to = from + delta
      if (from < 0 || to < 0 || to >= layers.length) return prev
      layers.splice(to, 0, ...layers.splice(from, 1))
      return { ...prev, groups: layers }
    })
  }, [])

  /** Colour one grouping layer (null: none). Colour is a tag on a layer,
   *  not a role — it never splits data, it labels a split by legend. */
  const setColor = useCallback((factor: string | null) => {
    setSpec(prev => (prev ? { ...prev, color: factor } : prev))
  }, [])

  const setKind = useCallback((kind: string) => {
    // A kind change can also change what the measure IS: a scalar kind on a
    // 1-D variable collapses it, and the roles a 1-D variable opens with (every
    // key on "separate figures") leave no replicates, so box and violin would
    // stay greyed out and this click would appear to do nothing. The backend
    // decides whether to re-default and what to (`roles.roles_for_kind`); this
    // applies the answer it already sent.
    //
    // Only when that answer describes the spec on screen. The report is a
    // debounced echo, and a suggestion computed against an older spec could
    // overwrite a role the user has set since — the one thing the rule promises
    // never to do.
    const reportIsForThisSpec = capsSpecRef.current === specKey
    const suggested = reportIsForThisSpec
      ? (capabilities?.kinds ?? []).find(info => info.kind === kind)?.assignment ?? null
      : null
    setSpec(prev =>
      prev
        ? {
            ...prev,
            kind,
            ...(suggested
              ? { roles: suggested.roles, groups: suggested.groups, color: suggested.color }
              : {}),
          }
        : prev
    )
  }, [capabilities, specKey])

  const setCellStatistic = useCallback((statistic: string) => {
    setSpec(prev => (prev ? { ...prev, cell_statistic: statistic } : prev))
  }, [])

  const setAggregate = useCallback((patch: { statistic?: string; error?: string; pooled?: boolean }) => {
    setSpec(prev => prev && ({
      ...prev,
      aggregate: { statistic: 'mean', error: 'sd', ...(prev.aggregate ?? {}), ...patch },
    }))
  }, [])

  const setShowSample = useCallback((name: string, on: boolean) => {
    setSpec(prev => (prev ? { ...prev, show_sample: toggleShowSample(prev.show_sample, name, on) } : prev))
  }, [])

  const setJoinSample = useCallback((choice: JoinChoice) => {
    setSpec(prev => (prev ? { ...prev, join_sample: joinSetting(choice) } : prev))
  }, [])

  const setFacet = useCallback((patch: Partial<FacetOptions>) => {
    setSpec(prev => (prev ? { ...prev, facet: { ...(prev.facet ?? {}), ...patch } } : prev))
  }, [])

  const setYAxis = useCallback((patch: Partial<YAxis>) => {
    setSpec(prev => (prev ? { ...prev, y_axis: { ...(prev.y_axis ?? {}), ...patch } } : prev))
  }, [])

  const setStyle = useCallback((patch: { width?: number; height?: number; font_size?: number }) => {
    setSpec(prev => (prev ? { ...prev, style: { ...(prev.style ?? {}), ...patch } } : prev))
  }, [])

  /** Add or remove one factor from the y-limit scope, keeping panel order. */
  const toggleYScope = useCallback((name: string, on: boolean) => {
    setSpec(prev => {
      if (!prev) return prev
      const current = prev.y_axis?.scope ?? []
      const next = on
        ? [...current.filter(f => f !== name), name]
        : current.filter(f => f !== name)
      return { ...prev, y_axis: { ...(prev.y_axis ?? {}), scope: next } }
    })
  }, [])

  /**
   * Edit the rule in one grid slot, padding the array out to reach it.
   *
   * The slots ARE the grid — there is no add/remove, because the number of rows
   * and columns is the thing the user set. A slot left blank claims nothing and
   * takes whatever is left over, in order (scistackplot's Matcher.is_blank).
   */
  const setRuleAt = useCallback(
    (axis: 'rows' | 'cols', index: number, patch: Partial<Matcher>) => {
      setSpec(prev => {
        if (!prev) return prev
        const rules = [...(prev.facet?.[axis] ?? [])]
        while (rules.length <= index) rules.push({ op: 'contains', value: '' })
        rules[index] = { ...rules[index], ...patch }
        return { ...prev, facet: { ...(prev.facet ?? {}), [axis]: rules } }
      })
    },
    []
  )

  /**
   * Keep only `levels` of `column`, or drop the filter when `levels` is null.
   *
   * "Every level selected" is stored as NO filter rather than as a list of all
   * of them: a list would freeze today's levels into the spec, and the figure
   * would silently stop showing subject 13 the day it arrives.
   */
  const setLevelFilter = useCallback(
    (column: string, levels: (string | number)[] | null) => {
      setSpec(prev => {
        if (!prev) return prev
        const others = (prev.filters ?? []).filter(f => f.column !== column)
        const filters = levels === null ? others : [...others, { column, include: levels }]
        return { ...prev, filters }
      })
    },
    []
  )

  /**
   * Which schema locations to draw — the location picker's checkbox edits.
   *
   * `include: []` is inert (everything drawn), so an untouched picker and a
   * "select all" click store the same thing. `keys` travels with it as display
   * order; matching reads the keys named inside each prefix.
   */
  const setLocationSelection = useCallback(
    (selection: LocationSelection, keys: string[]) => {
      setSpec(prev =>
        prev
          ? {
              ...prev,
              location_filter: {
                keys,
                include: selection.include,
                exclude_levels: selection.exclude_levels,
              },
            }
          : prev
      )
    },
    []
  )

  /**
   * A row was clicked: show that location and nothing else.
   *
   * Two things happen together, and the second is the one worth explaining.
   * Picking `subject=01` leaves `trial` unanswered, and a trial factor sitting
   * on AGGREGATE would collapse the very thing the user opened the picker to
   * look at — every trial's own values. So any schema key the selection does
   * NOT name is moved off AGGREGATE onto FREE (replicates), and one with no
   * role at all gets FREE too.
   *
   * Deliberately NOT a blanket reassignment: a key already on X, COLOR or FACET
   * is drawing its levels individually, which is what was asked for, and
   * stamping over a chosen x axis because a location was clicked would be
   * astonishing.
   */
  const pickLocation = useCallback(
    (path: PathStep[], keys: string[]) => {
      setSpec(prev => (prev ? applyPickedLocation(prev, path, keys) : prev))
    },
    []
  )

  /** Numeric bounds on a measure. Either end may be null (open). */
  const setRangeFilter = useCallback(
    (column: string, minimum: number | null, maximum: number | null) => {
      setSpec(prev => {
        if (!prev) return prev
        const others = (prev.filters ?? []).filter(f => f.column !== column)
        if (minimum === null && maximum === null) return { ...prev, filters: others }
        const filter: Filter = { column }
        if (minimum !== null) filter.minimum = minimum
        if (maximum !== null) filter.maximum = maximum
        return { ...prev, filters: [...others, filter] }
      })
    },
    []
  )

  const addLevelGroup = useCallback((source: string, levels: (string | number)[]) => {
    setSpec(prev => {
      if (!prev) return prev
      // Every level starts in its own bucket, named after itself: the figure is
      // unchanged until the user actually merges two of them, so adding a group
      // can never silently redraw the plot.
      const mapping: Record<string, string> = {}
      levels.forEach(level => { mapping[String(level)] = String(level) })
      const group: LevelGroup = {
        name: `${source} group`,
        source,
        mapping,
        unmatched: null,
      }
      return { ...prev, level_groups: [...(prev.level_groups ?? []), group] }
    })
  }, [])

  const editLevelGroup = useCallback((index: number, patch: Partial<LevelGroup>) => {
    setSpec(prev => {
      if (!prev) return prev
      const groups = [...(prev.level_groups ?? [])]
      if (!groups[index]) return prev
      groups[index] = { ...groups[index], ...patch }
      return { ...prev, level_groups: groups }
    })
  }, [])

  const removeLevelGroup = useCallback((index: number) => {
    setSpec(prev =>
      prev
        ? {
            ...prev,
            level_groups: (prev.level_groups ?? []).filter((_, i) => i !== index),
          }
        : prev
    )
  }, [])

  /** Commit the row the "+ Add variant" picker built.
   *
   *  The row is created HERE, on apply — not by the "+" click. Clicking "+"
   *  used to append an empty row immediately, which meant the list grew before
   *  the user had said anything and a cancelled thought left "(not set)"
   *  behind. Now "+" asks two questions (which variable, which variant) and
   *  nothing enters the spec until both are answered.
   *
   *  The row arrives already pinned, because the picker seeded its selection
   *  from `variants.default_selection` — so a new row is ONE variant, the same
   *  way the panel opened on one. An empty selection would mean "every variant
   *  of this variable", which is never what clicking "+" meant. */
  const addVariantFromPicker = useCallback(
    (next: {
      selection: Record<string, unknown>
      name: string
      variable?: string
    }) => {
      setSpec(prev => {
        if (!prev) return prev
        // Row 0 already exists: `roles.default_spec` seeds it for every table,
        // variants or not. This used to seed it HERE, lazily, on the way to
        // adding the second — which meant the panel opened with an empty
        // Variants list saying nothing about the variable on screen, and put a
        // rule about what a figure opens on in TypeScript (CLAUDE.md NOTE 3).
        return {
          ...prev,
          variant_sets: [
            ...(prev.variant_sets ?? []),
            {
              name: next.name || null,
              selection: next.selection,
              variable: next.variable ?? null,
            },
          ],
        }
      })
      setAddingVariant(false)
    },
    []
  )

  const removeVariantSet = useCallback((index: number) => {
    setSpec(prev =>
      prev
        ? { ...prev, variant_sets: (prev.variant_sets ?? []).filter((_, i) => i !== index) }
        : prev
    )
  }, [])

  /** Adopt a combination the data actually holds, from the empty-state panel.
   *
   *  Replaces the selection outright rather than merging into it. The pin that
   *  produced the empty figure is exactly what has to go, and merging would
   *  keep the latest flag beside an explicit version — rows that are
   *  simultaneously the newest and an old version, which is nothing at all.
   *  Same rule as the popup's `withoutLatestFlag`: an explicit choice
   *  supersedes the shortcut. */
  const adoptCombination = useCallback(
    (index: number, combination: Record<string, string>) => {
      setSpec(prev => {
        if (!prev) return prev
        const sets = [...(prev.variant_sets ?? [])]
        if (!sets[index]) return prev
        sets[index] = { ...sets[index], selection: { ...combination } }
        return { ...prev, variant_sets: sets }
      })
    },
    []
  )

  const editVariantSet = useCallback((index: number, patch: Partial<VariantSet>) => {
    setSpec(prev => {
      if (!prev) return prev
      const sets = [...(prev.variant_sets ?? [])]
      if (!sets[index]) return prev
      sets[index] = { ...sets[index], ...patch }
      return { ...prev, variant_sets: sets }
    })
  }, [])

  // Schema keys get their own section: "plot subject 01, trial 2" is a
  // different question from "narrow this factor", asked far more often, and the
  // keys are the same few every time. Everything else filterable — a struct's
  // fields, a joined group variable — lands in Filters. Variant factors are
  // excluded: selecting variants is the Variants section's job, and offering it
  // twice is how the two answers start disagreeing.
  const schemaKeys = useMemo(
    () => factors.filter(f => f.is_schema_key),
    [factors]
  )
  const otherFilterable = useMemo(
    () => factors.filter(f => !f.is_schema_key && !f.is_variant),
    [factors]
  )
  const schemaKeyNames = useMemo(() => schemaKeys.map(f => f.name), [schemaKeys])

  const locationSummary = useMemo(
    () => describeSelection(asSelection(spec?.location_filter)),
    [spec?.location_filter]
  )

  // Which variant the picker is describing. One row is a pin (the panel opens
  // on exactly one), so its selection IS the variant in view; with several rows
  // the figure is a comparison and no single variant is "the" one, so the
  // picker falls back to the source's default the same way the canvas does.
  const pickerSelection = useMemo(() => {
    const sets = spec?.variant_sets ?? []
    return sets.length === 1 ? (sets[0].selection ?? null) : null
  }, [spec?.variant_sets])
  // Membership from roles, order from `groups` — the same reconciliation the
  // backend does, so the control shows what the figure will draw.
  //
  // Derived from the SPEC rather than read off `capabilities.grouping.layers`,
  // for the reason the Variants rows are: the spec updates on the click and
  // the capability report is a debounced echo, so a checkbox reading the echo
  // would visibly lag the tick. The backend's copy is what the FIGURE uses;
  // this one only has to agree with it, and `ordered_groups` is the shared
  // definition both are written from.
  const groupLayers = useMemo(() => {
    const holders = Object.entries(spec?.roles ?? {})
      .filter(([, role]) => role === 'group')
      .map(([name]) => name)
    return orderGroups(spec?.groups ?? [], holders, factorDepths)
  }, [spec?.roles, spec?.groups, factorDepths])

  const groupableVariables = describe?.groupable_variables ?? []
  const [groupPickerOpen, setGroupPickerOpen] = useState(false)

  /** Apply the picker's selection: replace `factor_variables` wholesale.
   *
   *  Wholesale because the popup shows every grouping at once and opens with
   *  the current ones ticked — so what comes back IS the answer, unticking
   *  included. Merging would make unticking impossible.
   *
   *  Each grouping still arrives with a role (`roles.role_for_new_grouping`,
   *  published on the capability report): a grouping ticked is a request for
   *  one mark per level, so it joins the grouping list — or becomes a facet
   *  when the tick layers are full. Never left to default. */
  const applyGroupings = useCallback(
    (groups: { variable: string; column: string | null; variant: Record<string, unknown> }[]) => {
      setGroupPickerOpen(false)
      setSpec(prev => {
        if (!prev) return prev
        const names = new Set(groups.map(g => g.column ?? g.variable))
        const gone = (prev.factor_variables ?? [])
          .map(f => f.column ?? f.variable)
          .filter(name => !names.has(name))
        const fresh = (capsSpecRef.current === specKey && grouping?.new_grouping_role) || 'group'
        const roles = { ...prev.roles }
        for (const name of gone) delete roles[name]
        let layers = (prev.groups ?? []).filter(name => !gone.includes(name))
        const color = prev.color && gone.includes(prev.color) ? null : prev.color
        for (const group of groups) {
          const name = group.column ?? group.variable
          if (roles[name]) continue // a decision already made is never overwritten
          roles[name] = fresh
          if (fresh === 'group') layers = placeGroupLayer(layers, name, factorDepths)
        }
        return { ...prev, factor_variables: groups, roles, groups: layers, color }
      })
    },
    [factorDepths, grouping?.new_grouping_role, specKey]
  )
  // Only real factors can be bucketed — not a factor this spec already derived
  // (bucketing a bucket answers nothing) and not a variant axis.
  const derivedNames = useMemo(
    () => new Set((spec?.level_groups ?? []).map(g => g.name)),
    [spec?.level_groups]
  )
  const bucketable = useMemo(
    () => factors.filter(f => !f.is_variant && !derivedNames.has(f.name)),
    [factors, derivedNames]
  )

  // Rows come from the SPEC, annotations from the backend. The spec is the
  // source of truth and updates on the keystroke; `capabilities` is a debounced
  // echo, so rendering rows from it would make "+ Add variant" appear to do
  // nothing for a moment — and, worse, would let a row index mean different
  // things in the list and in the popup during that window.
  const variantRows = useMemo(
    () =>
      (spec?.variant_sets ?? []).map((set, index) => {
        const info = capabilities?.variants?.sets?.[index]
        return {
          explicitName: set.name,
          autoLabel: info?.auto_label ?? '(not set)',
          // Naming a variable is saying something, so such a row is defined
          // even with nothing selected: "also plot FilteredEMG, all of it".
          defined:
            Object.keys(set.selection ?? {}).length > 0 || Boolean(set.variable),
          rowCount: info?.row_count,
          spans: info?.spans ?? {},
          variable: set.variable ?? null,
        }
      }),
    [spec?.variant_sets, capabilities]
  )
  // --- what the canvas has to report about the pin ------------------------
  // Both of these come off `capabilities.variants.sets`, measured by the same
  // mask the renderer applied, so the figure and the message about it cannot
  // disagree.
  // The index is carried, not looked up later: `variant_summary` reports one
  // entry per `spec.variant_sets` IN ORDER, so position is the only reliable
  // identity a row has — two rows can share a name, and a name can change while
  // a debounced capability report is still in flight.
  const definedSets = useMemo(
    () =>
      (capabilities?.variants?.sets ?? [])
        .map((set, index) => ({ set, index }))
        .filter(({ set }) => set.defined),
    [capabilities]
  )

  // Rows whose data was built by more than one version of some function. This
  // is now reported for the ordinary per-location "latest" state too (the
  // 2026-09-11 reversal), which is only tolerable because the message names
  // WHICH locations hold WHICH version — see SpanInfo.
  const spanningSets = useMemo(
    () => definedSets.filter(({ set }) => Object.keys(set.spans ?? {}).length > 0),
    [definedSets]
  )

  // The figure is empty AND the variants are why. Not "some row is empty": with
  // two rows and one of them matching, the figure is drawn and the per-row "no
  // data" tag is the right weight. Replacing a real figure with an explanation
  // would be worse than the tag.
  const emptyPin = useMemo(
    () =>
      definedSets.length > 0 &&
      definedSets.every(({ set }) => set.row_count === 0)
        ? definedSets
        : [],
    [definedSets]
  )

  const stackable = describe?.stackable_with ?? []
  // The section is reachable whenever there is anything for it to say: this
  // source has pipeline variants, the spec already holds rows, OR another
  // variable could be plotted alongside this one.
  //
  // That last clause was missing, and it made the whole multi-variable feature
  // unreachable: a project that never edited a function and swept no parameters
  // has no variant factors and no default row, so the section stayed hidden and
  // there was no way to add the second series.
  const hasVariants = Boolean(
    (capabilities?.variants?.factors ?? []).length > 0 ||
      variantRows.length > 0 ||
      stackable.length > 0
  )
  const summarizing = spec?.kind === 'bar' || spec?.kind === 'band'
  const faceted = Object.values(spec?.roles ?? {}).includes('facet')

  // The grid the backend actually built. Read back rather than recomputed: the
  // "set one, the other follows" arithmetic is grid_shape_for's, in
  // scistackplot, and a second copy of it here would be the thing that drifts.
  const gridMeta = (figures[0]?.figure?.layout?.meta ?? {}) as GridMeta
  const effRows = Math.max(1, gridMeta.rows ?? 1)
  const effCols = Math.max(1, gridMeta.cols ?? 1)
  const layoutNotes = gridMeta.layout_notes ?? []

  // Only factors that separate PANELS can separate y limits — a colour or
  // replicate factor lives inside one panel, so splitting on it would ask one
  // axis for two ranges. Offering only these is how the control cannot be put
  // into a state the backend has to refuse (it drops them with a warning).
  //
  // Read back off the figure, not derived from `spec.roles`: the backend
  // completes roles the spec never states (a schema key promoted to ITERATE
  // because a nested key iterates, a `Variable` facet by default), and a
  // panel factor with no checkbox was one the user could never scale apart.
  // `spec.roles` is only the fallback before the first figure arrives.
  const yScopeFactors = gridMeta.panel_factors ?? Object.entries(spec?.roles ?? {})
    .filter(([, role]) => role === 'iterate' || role === 'facet')
    .map(([name]) => name)
  const yScope = (spec?.y_axis?.scope ?? []).filter(n => yScopeFactors.includes(n))
  // The limits the backend actually applied, read back off the figure so the
  // number on screen and the number in the box cannot disagree.
  // On a log axis plotly's `range` is in log10 units (render.base.axis_range);
  // the box shows data units, as the backend computed them.
  const plotlyYRange = (
    figures[0]?.figure?.layout?.yaxis as { range?: [number, number]; type?: string } | undefined
  )
  const appliedYLimits: [number, number] | undefined =
    plotlyYRange?.range && plotlyYRange.type === 'log'
      ? [10 ** plotlyYRange.range[0], 10 ** plotlyYRange.range[1]]
      : plotlyYRange?.range

  // --- export -------------------------------------------------------------
  const handleExport = useCallback(() => {
    if (!spec) return
    setNotice('')
    callBackend('plot_export', { spec, ...sourceParams })
      .then(raw => setCode((raw as { source: string }).source))
      .catch(err => setNotice(`Export failed: ${(err as Error).message}`))
  }, [spec, sourceParams])

  // The background save's job id. A ref, not state: the first figure can
  // report progress before the render that set it has committed, and the
  // message handler has to be able to match that id immediately.
  const saveJob = useRef<string | null>(null)
  // The same fact as state, purely to re-render the button. The ref is the
  // one the handler reads; this only ever follows it.
  const [saving, setSaving] = useState(false)
  // Which kind of save the running job is — the progress wording differs
  // ("Resolving figure 1 of 2…" means nothing for a CSV).
  const saveKind = useRef<'image' | 'data'>('image')
  // "Save data": the inline depth chooser, and the depth picked in it.
  const [dataChooser, setDataChooser] = useState(false)
  const [dataDepth, setDataDepth] = useState<string | null>(null)
  // "One column per field" for a struct variable — on by default (user,
  // 2026-09-19); applies to the field factor (ColName) only.
  const [dataWide, setDataWide] = useState(true)
  const [imageFormat, setImageFormat] = useState('png')
  // What the backend says it can write. A short, ordered shortlist first —
  // these are the ones a paper needs — then whatever else is available.
  const allFormats = describe?.image_formats ?? ['png', 'svg', 'pdf', 'eps']
  const formatChoices = [
    ...['png', 'svg', 'pdf', 'eps'].filter(f => allFormats.includes(f)),
    ...allFormats.filter(f => !['png', 'svg', 'pdf', 'eps'].includes(f)),
  ]

  // --- figure size ----------------------------------------------------------
  // The size the SAVE draws at, in inches. The preview keeps filling its pane;
  // this is deliberately about the file, which is where the ratio matters.
  const presets = describe?.figure_presets ?? FALLBACK_PRESETS
  const figWidth = typeof spec?.style?.width === 'number' ? spec.style.width : 8
  const figHeight = typeof spec?.style?.height === 'number' ? spec.style.height : 6
  // Points in the export, px in the preview — one number, so a change is
  // visible before anything is saved. 14 is `StyleOptions.font_size`.
  const fontSize = typeof spec?.style?.font_size === 'number' ? spec.style.font_size : 14
  const aspect = aspectChoice ?? aspectName(figWidth, figHeight, presets)
  const onAspect = useCallback(
    (name: string) => {
      setAspectChoice(name)
      // A ratio keeps the width and moves the height; custom moves nothing.
      if (presets.find(p => p.name === name)?.ratio != null) {
        setStyle({ height: heightFor(figWidth, name, presets, figHeight) })
      }
    },
    [presets, figWidth, figHeight, setStyle]
  )
  const onWidth = useCallback(
    (width: number) => setStyle({ width, height: heightFor(width, aspect, presets, figHeight) }),
    [aspect, presets, figHeight, setStyle]
  )
  const onHeight = useCallback(
    (height: number) => {
      setStyle({ height })
      // Back to "say what it is": a typed height that lands on 16:9 reads
      // as 16:9, and anything else reads as custom.
      setAspectChoice(null)
    },
    [setStyle]
  )

  /** Save one figure, or the whole fan-out.
   *
   *  Two buttons because the costs differ, but ONE shape: both are background
   *  jobs that report progress (`saveJob` below). A save renders at FULL
   *  resolution with no downsampling, and that is minutes per figure — 1543 s
   *  for a two-figure fan-out (scidb.log 2026-09-11) — against a fixed 30 s
   *  transport timeout. "Save current figure" was a request/response on the
   *  theory that one figure is cheap; it timed out just as reliably as saving
   *  everything did, and left the backend rendering a file nobody was waiting
   *  for. No timeout value fixes that, so neither one asks for an answer. */
  const saveFigures = useCallback(
    async (scope: 'current' | 'all') => {
      if (!spec) return
      setNotice('')
      // A fan-out asks for a FOLDER. Its N filenames are the figure labels —
      // not the user's to choose — so asking for a name only raised the
      // question "which of the thirty is that?". One figure is one file, and
      // there the name is the whole point.
      const savingAll = scope === 'all'
      const defaultName = `${title || 'figure'}.${imageFormat}`.replace(
        /[^\w.-]+/g,
        '_'
      )
      try {
        let path: string | null = null
        if (isVSCodeMode) {
          const picked = await callBackend(
            savingAll ? 'pick_save_folder' : 'pick_save_path',
            // Only the CHOSEN format, not every one available: the dialog's
            // filter would otherwise let the user pick a second, different
            // answer to a question the dropdown already asked, and the backend
            // honours the dropdown.
            savingAll ? {} : { defaultName, formats: [imageFormat] }
          )
          path = (picked as { path: string | null }).path
          if (!path) return  // dialog cancelled
        } else {
          path = window.prompt(
            savingAll
              ? `Folder to save ${figureCount} figures into:`
              : 'Save the figure as:',
            savingAll ? '' : defaultName
          )
          if (!path) return
        }

        // The id is OURS, and it is adopted before the request leaves. The job
        // starts on a thread the moment the backend receives it, so a quick
        // figure can report progress — or complete — while this promise is
        // still settling; a handler that learned the id only from the response
        // would drop those messages and leave the panel saving forever.
        const job = `ps-${Math.random().toString(36).slice(2, 10)}`
        saveJob.current = job
        saveKind.current = 'image'
        setSaving(true)
        setNotice(
          savingAll
            ? `Saving ${figureCount} figures at full resolution…`
            : 'Saving this figure at full resolution…'
        )
        await callBackend('plot_save_start', {
          spec,
          path,
          job_id: job,
          image_format: imageFormat,
          // null, not omitted: the backend reads "which figure" from this one
          // field, and `undefined` would be dropped by JSON. It also decides
          // whether `path` is a file or the folder to fill.
          figure_index: savingAll ? null : figureIndex,
          ...sourceParams,
        })
      } catch (err) {
        // The id was adopted before the request; a request that never reached
        // the backend has no job to report, so release the button here or it
        // stays disabled until the panel is reopened.
        saveJob.current = null
        setSaving(false)
        setNotice(`Could not save: ${(err as Error).message}`)
      }
    },
    [spec, sourceParams, title, figureIndex, figureCount, imageFormat]
  )

  /** Save the plot's long table as CSV — the rows the figure is drawn from
   *  (`scistackplot.plot_data`), every figure of the fan-out in one file.
   *  `depth` is a key from `capabilities.data_export.depths`; null is the
   *  default (the plotted sample). Same job path and messages as an image
   *  save: the load alone can outlast the transport timeout. */
  const saveData = useCallback(
    async (depth: string | null, fieldsAsColumns: boolean) => {
      if (!spec) return
      setDataChooser(false)
      setNotice('')
      const defaultName = `${title || 'plot'}_data.csv`.replace(/[^\w.-]+/g, '_')
      try {
        let path: string | null = null
        if (isVSCodeMode) {
          const picked = await callBackend('pick_save_path', {
            defaultName,
            formats: ['csv'],
            filterName: 'CSV',
          })
          path = (picked as { path: string | null }).path
          if (!path) return  // dialog cancelled
        } else {
          path = window.prompt('Save the plot data as:', defaultName)
          if (!path) return
        }
        const job = `ps-${Math.random().toString(36).slice(2, 10)}`
        saveJob.current = job
        saveKind.current = 'data'
        setSaving(true)
        setNotice("Collecting the plot's data…")
        await callBackend('plot_save_start', {
          spec,
          path,
          job_id: job,
          what: 'data',
          // null, not omitted, for the same reason as figure_index above.
          depth,
          fields_as_columns: fieldsAsColumns,
          ...sourceParams,
        })
      } catch (err) {
        saveJob.current = null
        setSaving(false)
        setNotice(`Could not save the data: ${(err as Error).message}`)
      }
    },
    [spec, sourceParams, title]
  )

  const openDataSave = useCallback(() => {
    const info = capabilities?.data_export
    if (!info?.available) return
    // Nothing to ask — one depth and no struct fields to spread — so go
    // straight to the file dialog.
    if (info.depths.length <= 1 && !info.field_factor) {
      void saveData(info.default, true)
      return
    }
    setDataDepth(info.default)
    setDataChooser(open => !open)
  }, [capabilities, saveData])

  useBackendMessage(
    useCallback((msg) => {
      // Both transports, one handler: the WebSocket path delivers the dict as
      // sent (`type`, fields at the top level) and the JSON-RPC path wraps it
      // (`method` + `params`). Same normalization every other consumer does.
      const kind = (msg.type ?? msg.method) as string
      const params = (msg.params ?? msg) as Record<string, unknown>
      if (!kind?.startsWith('plot_save_')) return
      // Ignore another panel's job — two Plot Studio tabs can save at once.
      if (params.job_id !== saveJob.current) return

      if (kind === 'plot_save_progress') {
        // Two stages, reported apart, because they are not the same size:
        // resolving a figure at full resolution is minutes and writing it is
        // seconds. A panel that said "0 of 2 saved" for twelve minutes was
        // counting the cheap half and looked hung.
        if (saveKind.current === 'data') {
          setNotice(
            params.stage === 'resolving' ? "Collecting the plot's data…" : 'Writing the CSV…'
          )
          return
        }
        setNotice(
          params.stage === 'resolving'
            ? `Resolving figure ${params.done} of ${params.total} at full resolution…`
            : `Saving ${params.done} of ${params.total}…`
        )
      } else if (kind === 'plot_save_complete') {
        const files = (params.files ?? []) as string[]
        const directory = params.directory as string | undefined
        const elapsed = params.elapsed as number | undefined
        saveJob.current = null
        setSaving(false)
        // One file: name it. Many: name the FOLDER and the count — thirty full
        // paths sharing one directory is not a message anyone reads, and it
        // pushed the one fact that matters (it finished) off the end.
        const where =
          files.length === 1
            ? files[0]
            : `${files.length} files in ${directory ?? 'the chosen folder'}`
        const rows = params.rows as number | undefined
        setNotice(
          `Saved ${where}` +
            (typeof rows === 'number' ? ` — ${rows} row(s)` : '') +
            (elapsed ? ` in ${elapsed.toFixed(1)}s` : '')
        )
      } else if (kind === 'plot_save_failed') {
        saveJob.current = null
        setSaving(false)
        setNotice(`Could not save: ${params.error}`)
      }
    }, [])
  )

  const handleAddToPipeline = useCallback(() => {
    if (!spec) return
    setNotice('Writing endpoint…')
    callBackend('plot_add_to_pipeline', { spec })
      .then(raw => {
        const result = raw as { ok?: boolean; error?: string; function_name?: string; file?: string }
        if (result.error) setNotice(`Could not add: ${result.error}`)
        else setNotice(`Added ${result.function_name} to ${result.file}. Wire it up on the canvas.`)
      })
      .catch(err => setNotice(`Could not add: ${(err as Error).message}`))
  }, [spec])

  // --- render -------------------------------------------------------------
  if (loadError) {
    return (
      <Shell variable={title} shape={shapeBadge(capabilities)} onClose={onClose} embedded={embedded}>
        <div style={styles.error}>Could not open the plot panel: {loadError}</div>
      </Shell>
    )
  }
  if (!describe) {
    return (
      <Shell variable={title} shape={shapeBadge(capabilities)} onClose={onClose} embedded={embedded}>
        <div style={styles.note}>Loading…</div>
      </Shell>
    )
  }
  if (describe.eligible === false) {
    // The empty state the design doc insists on: say why, never draw blank axes.
    return (
      <Shell variable={title} shape={shapeBadge(capabilities)} onClose={onClose} embedded={embedded}>
        <div style={styles.note}>{describe.reason}</div>
      </Shell>
    )
  }

  const gridRows = (figures[0]?.figure?.layout?.meta as { rows?: number } | undefined)?.rows ?? 1
  // The navigator strip and any fan-out note sit above the figure and take
  // their height out of it.
  const chrome = (figureCount > 1 ? 34 : 0) + fanoutNotes.length * 30
  const available = figures.length > 1
    ? Math.round((canvasHeight - chrome) / 2) - 28
    : canvasHeight - chrome - 16
  // Fill the pane, but never squeeze a tall facet grid: each row needs room for
  // its own tick labels and the next row's title.
  const figureHeight = Math.max(320, gridRows * 240, available)

  return (
    <Shell
      variable={title}
      // The measure's shape used to head its own sidebar section, which spent a
      // whole block restating the title. It belongs to the title.
      shape={shapeBadge(capabilities)}
      onClose={onClose}
      embedded={embedded}
      panelRef={panelRef}
      controlsHidden={controlsHidden}
      onToggleControls={() => setControlsHidden(v => !v)}
      sidebar={
        <div
          style={{
            ...styles.controls,
            ...(controlsHidden ? styles.controlsHidden : null),
          }}
        >
          {hasVariants && (
            <Section title="Variants">
              <div style={styles.hint}>
                Each row is one variant: a variable, narrowed to one version of
                the pipeline that produced it. Two or more become a factor you
                can colour or facet by.
              </div>
              <VariantRows
                rows={variantRows}
                primary={spec?.measures?.[0] ?? title}
                stackable={stackable}
                onRename={(index, name) => editVariantSet(index, { name })}
                onSetVariable={(index, variable) =>
                  editVariantSet(index, { variable })
                }
                onEdit={index => setVariantEditor(index)}
                onRemove={removeVariantSet}
                onAdd={() => setAddingVariant(true)}
              />
              <VariantReadout summary={capabilities?.variants} />
            </Section>
          )}

          {schemaKeys.length > 0 && (
            <Section title="Schema keys">
              {/* One button, and nothing else. The flat per-key LevelPickers
                  that used to live here are gone, not hidden: they could only
                  express a Cartesian product, they said nothing about whether
                  the data at a location was any good, and keeping them beside
                  the picker would be two controls answering one question — the
                  way the Variants/Factors duplication went wrong. */}
              <div style={styles.hint}>
                Which records to plot, and whether each location's data is
                sound. Omit a level everywhere (by key), or pick locations one
                by one. Everything is included until you say otherwise.
              </div>
              <button
                type="button"
                style={styles.locationButton}
                onClick={() => setLocationPickerOpen(true)}
              >
                {locationSummary}
              </button>
            </Section>
          )}

          <Section title="Filters">
            <div style={styles.hint}>
              Narrow what is drawn without changing what anything means.
            </div>
            {otherFilterable.map(factor => (
              <LevelPicker
                key={factor.name}
                factor={factor}
                onChange={levels => setLevelFilter(factor.name, levels)}
              />
            ))}
            {spec && capabilities?.shape === 'scalar' && (
              <RangeFilter
                measure={spec.measures[0]}
                filter={(spec.filters ?? []).find(f => f.column === spec.measures[0])}
                onChange={(minimum, maximum) =>
                  setRangeFilter(spec.measures[0], minimum, maximum)
                }
              />
            )}
          </Section>

          {/* Grouping: what groups EXIST, then which of them group the x axis.
              These were two sections with near-identical names ("Groups" and
              an "X grouping" list that only appeared once two factors already
              held X, making it unreachable until the user found the role
              dropdown). One question, one place, read top to bottom. */}
          <Section title="Grouping">
            {/* Refusals count towards showing this: if every column of a sheet
                was rejected, the reasons are the only thing that explains an
                otherwise empty section. */}
            {(groupableVariables.length > 0 ||
              Object.keys(describe?.groupable_refused ?? {}).length > 0 ||
              (spec?.level_groups ?? []).length > 0) && (
              <>
                <div style={styles.hint}>
                  Grouping the data already records, and buckets you define.
                  Both become factors you can colour, facet, or group the x
                  axis by.
                </div>
                {/* One button, and the groupings it produced. The flat list of
                    every column of every wide variable that used to live here
                    did not scale — a demographics sheet is 18 checkboxes and
                    ~100 queries on every panel open — and it could not express
                    WHICH version of the sheet supplies the labels. Both are the
                    picker's job now, on the canvas the user already knows. */}
                <button
                  type="button"
                  style={styles.locationButton}
                  onClick={() => setGroupPickerOpen(true)}
                  disabled={groupableVariables.length === 0}
                  title={
                    groupableVariables.length === 0
                      ? 'No variable is recorded at or above this data’s level.'
                      : 'Choose what stratifies this figure'
                  }
                >
                  {(spec?.factor_variables ?? []).length === 0
                    ? 'Group by…'
                    : `Group by: ${(spec?.factor_variables ?? [])
                        .map(f => f.column ?? f.variable)
                        .join(', ')}`}
                </button>
                {(spec?.factor_variables ?? []).map(group => {
                  const name = group.column ?? group.variable
                  return (
                    <div key={name} style={styles.kindRow}>
                      <span style={styles.factorName}>{name}</span>
                      <span style={styles.groupRole}>
                        {groupingPlacement(spec, groupLayers, factors, name)}
                      </span>
                    </div>
                  )
                })}
                {/* Variables refused in place, for the reason the variant
                    picker shows them: one a user expected to group by, absent
                    with no explanation, is indistinguishable from a bug. */}
                {Object.entries(describe?.groupable_refused ?? {}).map(
                  ([label, reason]) => (
                    <div key={label} style={styles.refusedRow} title={reason}>
                      {label} — {reason}
                    </div>
                  )
                )}
                {(spec?.level_groups ?? []).map((group, index) => (
                  <LevelGroupEditor
                    key={index}
                    group={group}
                    onEdit={patch => editLevelGroup(index, patch)}
                    onRemove={() => removeLevelGroup(index)}
                  />
                ))}
                <BucketAdder factors={bucketable} onAdd={addLevelGroup} />
              </>
            )}

            {/* The grouping list: one mark per level combination, innermost
                first, one layer optionally coloured. What a "mark" is — a
                bar, a line, a spaghetti line — is the kind's reading, and the
                backend says it in `hint`. A 2-D measure refuses with a reason. */}
            {grouping?.available ? (
              <GroupingList
                factors={factors}
                layers={groupLayers}
                color={spec?.color ?? null}
                hint={grouping.hint}
                labelled={grouping.labelled_layers ?? groupLayers.length}
                maxLabelled={grouping.max_labelled_layers}
                onToggle={(name, on) => setRole(name, on ? 'group' : 'iterate')}
                onMove={moveGroupLayer}
                onColor={setColor}
              />
            ) : (
              grouping?.reason && (
                <div style={styles.hint}>
                  <strong>Grouping:</strong> {grouping.reason}
                </div>
              )
            )}
          </Section>

          <Section title="Factors">
            <div style={styles.hint}>
              Everything not grouped: separate figures, separate panels, or
              collapsed (averaged). The last collapsed key is the sample the
              error bars are drawn over.
            </div>
            {factors.filter(factor => spec?.roles?.[factor.name] !== 'group').map(factor => (
              <label key={factor.name} style={styles.factorRow}>
                <span style={styles.factorName}>
                  {factor.display}
                  {factor.is_variant && <span style={styles.variantTag} title="A pipeline variant, not a replicate">variant</span>}
                  {factor.is_field && <span style={styles.fieldTag} title="The fields of this struct/dict variable — one subplot each by default">fields</span>}
                  <span style={styles.levelCount}>{factor.level_count}</span>
                </span>
                <select
                  value={spec?.roles?.[factor.name] ?? 'iterate'}
                  onChange={e => setRole(factor.name, e.target.value as Role)}
                  style={styles.select}
                >
                  {/* `factors` falls back to describe's raw table, which
                      carries no role report — an empty <select> would be a
                      dead control, so show at least what it is set to. */}
                  {(factor.roles ?? fallbackRoles(spec?.roles?.[factor.name])).map(option => (
                    <option
                      key={option.role}
                      value={option.role}
                      // Disabled rather than hidden: a role that is unavailable
                      // BECAUSE of another choice ("something else already has
                      // Color") has to stay visible, or the control silently
                      // shrinks and the user cannot see what to undo.
                      disabled={!option.available}
                      title={option.reason ?? option.hint}
                    >
                      {option.label}
                      {option.available ? '' : ' —'}
                    </option>
                  ))}
                </select>
              </label>
            ))}
          </Section>

          <Section title="Plot type">
            {(capabilities?.kinds ?? []).map(info => (
              <label
                key={info.kind}
                style={{ ...styles.kindRow, opacity: info.available ? 1 : 0.45 }}
                title={info.reason ?? ''}
              >
                <input
                  type="radio"
                  name="plot-kind"
                  checked={spec?.kind === info.kind}
                  disabled={!info.available}
                  onChange={() => setKind(info.kind)}
                  style={{ marginRight: 6 }}
                />
                {KIND_LABELS[info.kind] ?? info.kind}
              </label>
            ))}
            {/* Shown only for a 1-D measure, because only there is there
                anything to reduce. No on/off switch beside it: the KIND says
                whether the vectors are drawn or summarized, so "collapse on,
                kind = line" is a state that cannot be expressed rather than one
                the panel has to adjudicate. */}
            {capabilities?.cell_collapse?.applies && (
              <>
                <label style={styles.factorRow}>
                  <span style={styles.factorName}>Per-record value</span>
                  <select
                    value={spec?.cell_statistic ?? 'mean'}
                    onChange={e => setCellStatistic(e.target.value)}
                    style={styles.select}
                  >
                    <option value="mean">Mean</option>
                    <option value="median">Median</option>
                  </select>
                </label>
                <div style={styles.hint}>
                  {capabilities.cell_collapse.active
                    ? `Each vector is reduced to its ${
                        spec?.cell_statistic ?? 'mean'
                      } — one value per record — and plotted like any scalar.`
                    : 'Applies to scatter, strip, spaghetti, box, violin and bar: each vector becomes one value per record.'}
                </div>
              </>
            )}
          </Section>

          {faceted && (
            <Section title="Layout">
              <div style={styles.hint}>
                Set rows or columns — the other follows from the number of
                subplots. Then name what belongs in each one; blank takes
                whatever is left, in order.
              </div>
              <div style={styles.gridSizeRow}>
                <GridSizeInput
                  label="N rows"
                  value={spec?.facet?.n_rows ?? null}
                  effective={effRows}
                  onChange={n => setFacet({ n_rows: n })}
                />
                <GridSizeInput
                  label="N columns"
                  value={spec?.facet?.n_cols ?? null}
                  effective={effCols}
                  onChange={n => setFacet({ n_cols: n })}
                />
              </div>

              <RuleSlots
                title="Rows"
                count={effRows}
                rules={spec?.facet?.rows ?? []}
                onEdit={(i, patch) => setRuleAt('rows', i, patch)}
              />
              <RuleSlots
                title="Columns"
                count={effCols}
                rules={spec?.facet?.cols ?? []}
                onEdit={(i, patch) => setRuleAt('cols', i, patch)}
              />

              {/* Never let the layout quietly disobey a rule: if a subplot had
                  to move, or the grid had to grow, say which and why. */}
              {layoutNotes.map((note, index) => (
                <div key={index} style={styles.layoutNote}>{note}</div>
              ))}
            </Section>
          )}

          <Section title="Y axis">
            <div style={styles.hint}>
              Which plots share one y scale. Nothing ticked means every plot in
              the dataset gets the same limits; ticking everything autoscales
              each subplot to its own data.
            </div>
            {yScopeFactors.length === 0 ? (
              <div style={styles.hint}>
                One plot, so there is nothing to separate — give a factor the
                <em> separate figures</em> or <em>subplot</em> role to scale
                them apart.
              </div>
            ) : (
              yScopeFactors.map(name => (
                <label key={name} style={styles.factorRow}>
                  <input
                    type="checkbox"
                    checked={yScope.includes(name)}
                    onChange={e => toggleYScope(name, e.target.checked)}
                  />
                  <span style={styles.factorName}>{name}</span>
                </label>
              ))
            )}
            <div style={styles.gridSizeRow}>
              <LimitInput
                label="Min"
                value={spec?.y_axis?.minimum ?? null}
                onChange={v => setYAxis({ minimum: v })}
              />
              <LimitInput
                label="Max"
                value={spec?.y_axis?.maximum ?? null}
                onChange={v => setYAxis({ maximum: v })}
              />
            </div>
            {/* The rule AND the numbers it produced: "why is this 0.61?" has to
                have an answer the user can read off the panel. */}
            <div style={styles.layoutNote}>
              {yScope.length === 0
                ? 'Same limits everywhere'
                : `Separate limits per ${yScope.join(', ')}`}
              {appliedYLimits
                ? ` — ${appliedYLimits[0].toPrecision(3)} to ${appliedYLimits[1].toPrecision(3)}`
                : ''}
            </div>
          </Section>

          {summarizing && (
            <Section title="Summary">
              <label style={styles.factorRow}>
                <span style={styles.factorName}>Centre</span>
                <select
                  value={spec?.aggregate?.statistic ?? 'mean'}
                  onChange={e => setAggregate({ statistic: e.target.value })}
                  style={styles.select}
                >
                  <option value="mean">Mean</option>
                  <option value="median">Median</option>
                </select>
              </label>
              <label style={styles.factorRow}>
                <span style={styles.factorName}>Spread</span>
                <select
                  value={spec?.aggregate?.error ?? 'sd'}
                  onChange={e => setAggregate({ error: e.target.value })}
                  style={styles.select}
                >
                  <option value="sd">SD</option>
                  <option value="sem">SEM</option>
                  <option value="ci95">95% CI</option>
                  <option value="iqr">IQR</option>
                  <option value="none">None</option>
                </select>
              </label>
              {/* The chain, as the figure runs it: nested and unweighted by
                  default, each subject counting once however many trials it
                  has; pooled is the deliberate alternative (weight by N). */}
              <label style={styles.factorRow} title="Drop every collapsed factor in one step, so a subject with more trials weighs more">
                <span style={styles.factorName}>Weight by N</span>
                <input
                  type="checkbox"
                  checked={spec?.aggregate?.pooled ?? false}
                  onChange={e => setAggregate({ pooled: e.target.checked })}
                />
              </label>
              <div style={styles.hint}>{sampleNote(capabilities, spec)}</div>
            </Section>
          )}

          {capabilities?.sample_overlay && (
            <Section title="Show sample">
              {/* The collapsed keys' data drawn on top of the marks — for the
                  distribution behind a bar, and for the raw data behind it.
                  Python decides what a tick means (a deeper key implies the
                  shallower ones: a trial is a trial OF a subject), whether the
                  points are joined and why; this section displays the answer. */}
              {!capabilities.sample_overlay.available ? (
                <div style={styles.hint}>{capabilities.sample_overlay.reason}</div>
              ) : (
                <>
                  <div style={styles.hint}>
                    Overlay the collapsed keys' data as points: tick a key to see one
                    point per level of it. Deeper keys imply the shallower ones.
                  </div>
                  {capabilities.sample_overlay.factors.map(factor => (
                    <label
                      key={factor.name}
                      style={styles.factorRow}
                      title={isLocked(factor) ? `Implied by a deeper key — part of every point's identity` : undefined}
                    >
                      <span style={styles.factorName}>{factor.name}</span>
                      <input
                        type="checkbox"
                        checked={isTicked(factor)}
                        disabled={isLocked(factor)}
                        onChange={e => setShowSample(factor.name, e.target.checked)}
                      />
                    </label>
                  ))}
                  {capabilities.sample_overlay.shown.length > 0 && (
                    <>
                      <label
                        style={styles.factorRow}
                        title="Auto joins the points when they are repeated measures: the shown key sits above the x axis's key in the schema, so each has a value at every position"
                      >
                        <span style={styles.factorName}>Join points</span>
                        <select
                          value={joinChoice(spec?.join_sample)}
                          onChange={e => setJoinSample(e.target.value as JoinChoice)}
                          style={styles.select}
                        >
                          <option value="auto">
                            Auto ({capabilities.sample_overlay.join.automatic
                              ? capabilities.sample_overlay.join.join ? 'lines' : 'points'
                              : '…'})
                          </option>
                          <option value="lines">Lines</option>
                          <option value="points">Points</option>
                        </select>
                      </label>
                      <div style={styles.hint}>{capabilities.sample_overlay.granularity}</div>
                    </>
                  )}
                  {capabilities.sample_overlay.ignored.length > 0 && (
                    <div style={styles.hint}>
                      Not collapsed right now, so not shown: {capabilities.sample_overlay.ignored.join(', ')}
                    </div>
                  )}
                </>
              )}
            </Section>
          )}

          <Section title="Figure size">
            <div style={styles.hint}>
              Applies to saved images, exported code and the pipeline step. The
              preview fills the pane regardless. Pick a ratio and set the width
              (journal columns: 3.5 in single, 7.2 in double); the height follows.
            </div>
            <label style={styles.factorRow}>
              <span style={styles.factorName}>Aspect</span>
              <select
                value={aspect}
                onChange={e => onAspect(e.target.value)}
                style={styles.select}
              >
                {presets.map(preset => (
                  <option key={preset.name} value={preset.name} title={preset.hint}>
                    {preset.label}
                  </option>
                ))}
              </select>
            </label>
            <div style={styles.gridSizeRow}>
              <PositiveNumberInput label="Width (in)" value={figWidth} onChange={onWidth} />
              <PositiveNumberInput
                label="Height (in)"
                value={figHeight}
                onChange={onHeight}
                title={
                  aspect === CUSTOM_ASPECT
                    ? 'Custom: width and height are independent'
                    : 'Typing a height here switches the ratio to whatever it makes'
                }
              />
              <PositiveNumberInput
                label="Font (pt)"
                value={fontSize}
                onChange={font_size => setStyle({ font_size })}
                title="matplotlib font.size: ticks, labels, legend and title all scale with it"
              />
            </div>
            <div style={styles.layoutNote}>
              {pixelReadout(figWidth, figHeight, SAVE_DPI)} at {SAVE_DPI} dpi for a
              raster, before the whitespace trim takes a little off each edge.
            </div>
          </Section>

          <div style={{ ...styles.actions, flexWrap: 'wrap' }}>
            {/* Beside the save buttons, not in a menu: the format is part of
                the save, and the two buttons share it. The list comes from the
                backend (what this matplotlib can write), so a build without a
                PDF writer cannot be asked for one. */}
            <label style={styles.gridSizeField}>
              <span style={styles.gridSizeLabel}>Format</span>
              <select
                value={imageFormat}
                onChange={e => setImageFormat(e.target.value)}
                disabled={saving}
                style={{ ...styles.select, width: 80 }}
              >
                {formatChoices.map(fmt => (
                  <option key={fmt} value={fmt}>{fmt.toUpperCase()}</option>
                ))}
              </select>
            </label>
            <button
              type="button"
              style={styles.button}
              onClick={() => saveFigures('current')}
              // Same one-job-at-a-time rule as the button beside it: both are
              // background jobs now, and the progress readout follows one id.
              disabled={saving}
              title={
                saving
                  ? 'A save is already running'
                  : 'Render with matplotlib at full resolution — the same figure ' +
                    'the pipeline would produce. Runs in the background.'
              }
            >
              {saving
                ? 'Saving…'
                : figureCount > 1
                  ? 'Save current figure'
                  : 'Save image'}
            </button>
            {/* Only worth offering when there is more than one figure — and
                worth keeping separate, because it costs figureCount times as
                much as the button beside it. */}
            {figureCount > 1 && (
              <button
                type="button"
                style={styles.button}
                onClick={() => saveFigures('all')}
                // One job at a time. A second would write to the same files
                // from a second thread, and the progress readout can only
                // follow one id.
                disabled={saving}
                title={
                  saving
                    ? 'A save is already running'
                    : `Render all ${figureCount} figures at full resolution, ` +
                      `one file each — runs in the background`
                }
              >
                {saving ? 'Saving…' : `Save all ${figureCount} figures`}
              </button>
            )}
            {/* The rows the figure is drawn from, for statistics that must
                match the plot. Disabled with the backend's reason for a raw
                1-D / 2-D measure (scalar plots only). */}
            <button
              type="button"
              style={styles.button}
              onClick={openDataSave}
              disabled={saving || !capabilities?.data_export?.available}
              title={
                saving
                  ? 'A save is already running'
                  : capabilities?.data_export?.available
                    ? 'Save the long table this plot is drawn from as CSV — every ' +
                      'figure in one file, the figure keys as columns'
                    : capabilities?.data_export?.reason ?? 'Not available for this plot'
              }
            >
              Save data (CSV)
            </button>
            <button type="button" style={styles.button} onClick={handleExport}>
              Export code
            </button>
            <button type="button" style={styles.primaryButton} onClick={handleAddToPipeline}>
              Add to pipeline
            </button>
          </div>
          {dataChooser && capabilities?.data_export?.available && (
            <div style={styles.dataChooser}>
              {capabilities.data_export.depths.length > 1 && (
                <div style={styles.hint}>
                  Which rows? The default is exactly what the plot is drawn from;
                  deeper choices keep the lower levels instead of averaging them.
                </div>
              )}
              {capabilities.data_export.depths.length > 1 &&
                capabilities.data_export.depths.map(depth => (
                <label key={depth.key ?? '__plotted'} style={styles.dataDepthOption}>
                  <input
                    type="radio"
                    name="data-depth"
                    checked={dataDepth === depth.key}
                    onChange={() => setDataDepth(depth.key)}
                  />
                  <span>
                    {depth.label}
                    {depth.key === capabilities.data_export?.default ? ' (default)' : ''}
                    <span style={styles.dataColumns}>
                      {(dataWide && depth.wide_columns
                        ? depth.wide_columns
                        : depth.columns
                      ).join(', ')}
                    </span>
                  </span>
                </label>
              ))}
              {(() => {
                // The checkbox only means something at a depth that keeps the
                // struct's fields; elsewhere it is hidden, not greyed out.
                const chosen = capabilities.data_export.depths.find(d => d.key === dataDepth)
                if (!capabilities.data_export.field_factor || !chosen?.wide_columns) return null
                return (
                  <label style={styles.dataDepthOption}>
                    <input
                      type="checkbox"
                      checked={dataWide}
                      onChange={e => setDataWide(e.target.checked)}
                    />
                    <span>
                      One column per field ({capabilities.data_export.field_factor})
                      <span style={styles.dataColumns}>
                        {(dataWide ? chosen.wide_columns : chosen.columns).join(', ')}
                      </span>
                    </span>
                  </label>
                )
              })()}
              <div style={styles.actions}>
                <button
                  type="button"
                  style={styles.primaryButton}
                  onClick={() => void saveData(dataDepth, dataWide)}
                  disabled={saving}
                >
                  Save CSV…
                </button>
                <button type="button" style={styles.button} onClick={() => setDataChooser(false)}>
                  Cancel
                </button>
              </div>
            </div>
          )}
          {notice && <div style={styles.notice}>{notice}</div>}
        </div>
      }
    >
      <div style={styles.canvas} ref={canvasRef}>
        {specError && <div style={styles.specError}>{specError}</div>}
        {/* Say what the fan-out did that the spec did not literally ask for —
            iterating a nested key iterates its ancestors, so the figure count
            is larger than "one per trial" would suggest. */}
        {fanoutNotes.map((note, index) => (
          <div key={index} style={styles.layoutNote}>{note}</div>
        ))}
        {figureCount > 1 && (
          <FigureNavigator
            index={figureIndex}
            count={figureCount}
            label={figureLabels[figureIndex] ?? ''}
            onStep={stepFigure}
          />
        )}
        {/* The figure IS drawn, and its points were computed by more than one
            version of the same function. On the canvas rather than only on the
            sidebar row because that was the explicit requirement: a tag in a
            section the user may have collapsed is not prominent.
            Informational, not an error — per-location "latest" legitimately
            produces this, and it is the figure that cannot show it. The
            sentence names the locations so it can be acted on or dismissed at a
            glance.
            Deliberately says nothing about the pipeline canvas: a partially
            re-run PathInput loader reads GREEN there while producing exactly
            this state, so pointing at the canvas would mislead in the one case
            where this banner is the only signal
            (.claude/plan-pathinput-loader-staleness-gap.md). */}
        {spanningSets.map(({ set, index }) => (
          <div key={`span-${index}`} style={styles.spanBanner}>
            <strong>{set.name}</strong> mixes{' '}
            {Object.values(set.spans).map(describeSpan).join(' | ')}. Pin a
            version (or run options) on the row, or split it into one variant per
            version.
          </div>
        ))}
        {busy && <div style={styles.note}>Resolving…</div>}
        {/* Why the figure is empty, where the figure would have been.
            Reachable by design: the opening pin is applied blindly (latest
            code, first parameter value) because a rule that quietly picks a
            different value to avoid an empty figure is no longer predictable.
            The cost of that choice is paid here — the panel has to name what
            was attempted AND what exists, or the controls look correctly filled
            in and simply broken. */}
        {!specError && !busy && emptyPin.length > 0 && (
          <div style={styles.emptyPin}>
            <div style={styles.emptyPinTitle}>
              No records match {emptyPin.length > 1 ? 'these variants' : 'this variant'}.
            </div>
            {emptyPin.map(({ set, index }) => (
              <div key={index} style={styles.emptyPinBlock}>
                <div style={styles.emptyPinAttempt}>
                  <strong>{set.name}</strong> is asking for{' '}
                  <code style={styles.emptyPinCode}>
                    {Object.entries(set.resolved ?? {})
                      .map(([key, value]) => `${key}=${String(value)}`)
                      .join(', ') || 'everything'}
                  </code>
                </div>
                {set.available.length > 0 ? (
                  <>
                    <div style={styles.emptyPinHint}>
                      These combinations have records — click one to plot it:
                    </div>
                    <div style={styles.emptyPinOptions}>
                      {set.available.map((combination, position) => (
                        <button
                          key={position}
                          type="button"
                          style={styles.emptyPinOption}
                          onClick={() => adoptCombination(index, combination)}
                        >
                          {Object.entries(combination)
                            .map(([key, value]) => `${key}=${value}`)
                            .join(', ')}
                        </button>
                      ))}
                    </div>
                  </>
                ) : (
                  <div style={styles.emptyPinHint}>
                    This variable has no records at all yet — run the pipeline
                    that produces it.
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
        {!specError && figures.length === 0 && !busy && emptyPin.length === 0 && (
          <div style={styles.note}>Nothing to plot with these settings.</div>
        )}
        {figures.map((figure, index) => (
          <div
            key={figure.label || index}
            style={{
              ...styles.figureBlock,
              ...(figures.length === 1 ? { height: '100%' } : null),
            }}
          >
            {/* The navigator already names the figure it is showing. */}
            {figure.label && figureCount < 2 && (
              <div style={styles.figureLabel}>{figure.label}</div>
            )}
            {figure.downsampled_from && (
              <div style={styles.downsampleNote}>
                Showing a reduced view of {figure.downsampled_from.toLocaleString()} points —
                the exported figure uses every point.
              </div>
            )}
            <Plot
              data={figure.figure.data}
              layout={{
                ...figure.figure.layout,
                autosize: true,
                height: figureHeight,
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                // Colour only. The size is the renderer's (`layout.font.size` =
                // `StyleOptions.font_size`), so the setting shows before a save.
                font: { ...(figure.figure.layout.font as object), color: '#ccc' },
              }}
              config={{
                displaylogo: false,
                responsive: true,
                // A webview cannot download; "Save image" does it server-side.
                modeBarButtonsToRemove: isVSCodeMode ? ['toImage'] : [],
              }}
              style={{ width: '100%' }}
              useResizeHandler
            />
          </div>
        ))}
      </div>

      {code !== null && (
        <div style={styles.codeOverlay} onClick={() => setCode(null)}>
          <pre style={styles.code} onClick={e => e.stopPropagation()}>{code}</pre>
        </div>
      )}

      {variantEditor !== null && !csvPath && (
        <VariantDagPopup
          // The ROW's variable, not the panel's: a row plotting RawEMG must
          // edit RawEMG's axes, or the popup offers versions from a variable
          // this row does not draw.
          variable={
            variantRows[variantEditor]?.variable ?? describe?.variable ?? variable
          }
          selection={(spec?.variant_sets?.[variantEditor]?.selection ?? {}) as Record<string, unknown>}
          name={variantRows[variantEditor]?.explicitName ?? ''}
          placeholder={variantRows[variantEditor]?.autoLabel ?? ''}
          onCancel={() => setVariantEditor(null)}
          onApply={({ selection, name }) => {
            // An empty name stays null so the label keeps following the
            // selection; typing one pins it.
            editVariantSet(variantEditor, { selection, name: name || null })
            setVariantEditor(null)
          }}
        />
      )}

      {/* "+ Add variant": pick a variable on the canvas, then a variant of it.
          Any plottable variable, not only the one being plotted — that is what
          makes overlaying RawEMG on FilteredEMG reachable at all. */}
      {addingVariant && !csvPath && (
        <VariantDagPopup
          pick
          variable={describe?.variable ?? variable}
          pickable={stackable}
          refusals={describe?.stackable_refused ?? {}}
          selection={{}}
          name=""
          onCancel={() => setAddingVariant(false)}
          onApply={addVariantFromPicker}
        />
      )}

      {/* "Group by…": pick a variable on the canvas, then its columns and the
          variant of it that supplies the labels. Not offered on the CSV path —
          a flat table has no separately-recorded variable to join in. */}
      {groupPickerOpen && !csvPath && (
        <GroupingDagPopup
          measure={describe?.variable ?? variable}
          candidates={groupableVariables}
          refusals={describe?.groupable_refused ?? {}}
          selected={(spec?.factor_variables ?? []).map(f => ({
            variable: f.variable,
            column: f.column ?? null,
          }))}
          sourceParams={sourceParams}
          onCancel={() => setGroupPickerOpen(false)}
          onApply={applyGroupings}
        />
      )}

      {locationPickerOpen && (
        <SchemaLocationPicker
          variable={describe?.variable ?? variable}
          selection={pickerSelection}
          value={asSelection(spec?.location_filter)}
          onChange={selection => setLocationSelection(selection, schemaKeyNames)}
          onPick={path => {
            pickLocation(path, schemaKeyNames)
            setLocationPickerOpen(false)
          }}
          onClose={() => setLocationPickerOpen(false)}
          csvPath={csvPath}
        />
      )}
    </Shell>
  )
}

interface ShellProps {
  variable: string
  /** The measure's shape (`scalar`, `series_1d`, …), badged beside the title. */
  shape?: string
  onClose: () => void
  embedded?: boolean
  /** The controls rail. It sits under the header, in the same narrow column. */
  sidebar?: React.ReactNode
  /** The figure area — it owns the full height of the panel. */
  children: React.ReactNode
  panelRef?: React.RefObject<HTMLDivElement>
  controlsHidden?: boolean
  onToggleControls?: () => void
}

function Shell({
  variable,
  shape,
  onClose,
  embedded,
  sidebar,
  children,
  panelRef,
  controlsHidden,
  onToggleControls,
}: ShellProps) {
  return (
    // No overlay click-to-close: a stray click on the backdrop while dragging a
    // plotly selection would throw the panel away mid-exploration.
    <div style={embedded ? styles.embeddedRoot : styles.overlay}>
      {/* A row, not a column: the title bar caps the controls rail rather than
          spanning the panel, so the figure keeps the tab's full height. */}
      <div ref={panelRef} style={embedded ? styles.embeddedPanel : styles.panel}>
        <div style={{ ...styles.rail, ...(controlsHidden ? styles.railCollapsed : null) }}>
          <div style={styles.header}>
            {/* The toggle sits over the rail it collapses, so the control is
                where the thing it controls is. */}
            <div style={styles.headerLeft}>
              {onToggleControls && (
                <button
                  type="button"
                  style={styles.headerButton}
                  onClick={onToggleControls}
                  title={controlsHidden ? 'Show the controls' : "Give the figure the controls' width"}
                >
                  {controlsHidden ? '❯' : '❮'}
                </button>
              )}
              {/* A collapsed rail is only as wide as its buttons, so the title
                  would be the one thing keeping it wide. */}
              {!controlsHidden && (
                <span style={styles.title} title={`Plot — ${variable}`}>
                  Plot — {variable}
                  {shape && <span style={styles.shapeTag}>{shape}</span>}
                </span>
              )}
            </div>
            <div style={styles.headerActions}>
              {!embedded && (
                <button type="button" style={styles.close} onClick={onClose}>✕</button>
              )}
            </div>
          </div>
          {sidebar}
        </div>
        {children}
      </div>
    </div>
  )
}

interface GridSizeInputProps {
  label: string
  /** What the user pinned, or null when this dimension is computed. */
  value: number | null
  /** What the backend laid out — shown as a placeholder while unpinned. */
  effective: number
  onChange: (value: number | null) => void
}

/**
 * One dimension of the facet grid.
 *
 * An empty box is not "1", it is "you decide": the backend computes it from the
 * other dimension and the subplot count, and the result shows through as the
 * placeholder. That is what makes "I set 2 columns" answer "so, 3 rows".
 */
function GridSizeInput({ label, value, effective, onChange }: GridSizeInputProps) {
  return (
    <label style={styles.gridSizeField}>
      <span style={styles.gridSizeLabel}>{label}</span>
      <input
        type="number"
        min={1}
        value={value ?? ''}
        placeholder={String(effective)}
        onChange={e => onChange(e.target.value ? Number(e.target.value) : null)}
        title={value === null ? `Computed: ${effective}. Type a number to pin it.` : 'Clear to compute automatically'}
        style={{
          ...styles.select,
          width: 56,
          ...(value === null ? styles.gridSizeAuto : null),
        }}
      />
    </label>
  )
}

interface LimitInputProps {
  label: string
  value: number | null
  onChange: (value: number | null) => void
}

/**
 * One end of a manual y range. Blank means "compute it".
 *
 * Deliberately NOT `Number(e.target.value)` unguarded: an empty box gives
 * `Number('') === 0`, which would turn clearing the field into pinning the axis
 * at zero — the opposite of what clearing means. A half-typed "-" or "1e" is
 * held as-is too, rather than snapping the axis around while it is being typed.
 */
function LimitInput({ label, value, onChange }: LimitInputProps) {
  const [text, setText] = useState<string | null>(null)
  const shown = text ?? (value === null || value === undefined ? '' : String(value))
  return (
    <label style={styles.gridSizeField}>
      <span style={styles.gridSizeLabel}>{label}</span>
      <input
        type="text"
        inputMode="decimal"
        value={shown}
        placeholder="auto"
        onChange={e => {
          const next = e.target.value
          setText(next)
          if (next.trim() === '') onChange(null)
          else if (Number.isFinite(Number(next))) onChange(Number(next))
        }}
        onBlur={() => setText(null)}
        title="Blank computes the limit from the data"
        style={{
          ...styles.select,
          width: 72,
          ...(value === null || value === undefined ? styles.gridSizeAuto : null),
        }}
      />
    </label>
  )
}

interface PositiveNumberInputProps {
  label: string
  value: number
  onChange: (value: number) => void
  title?: string
}

/**
 * One dimension of the saved figure (inches) or its font size (points).
 *
 * Same typing discipline as `LimitInput`: the text is held while it is being
 * typed and only a positive, finite number is committed — `Number('')` is 0,
 * and a 0-inch figure (or 0 pt font) is a matplotlib error, not a size. Unlike
 * the limit, blank means nothing here: the field falls back to the value it had.
 */
function PositiveNumberInput({ label, value, onChange, title }: PositiveNumberInputProps) {
  const [text, setText] = useState<string | null>(null)
  return (
    <label style={styles.gridSizeField}>
      <span style={styles.gridSizeLabel}>{label}</span>
      <input
        type="text"
        inputMode="decimal"
        value={text ?? String(value)}
        onChange={e => {
          const next = e.target.value
          setText(next)
          const parsed = Number(next)
          if (next.trim() !== '' && Number.isFinite(parsed) && parsed > 0) onChange(parsed)
        }}
        onBlur={() => setText(null)}
        title={title}
        style={{ ...styles.select, width: 64 }}
      />
    </label>
  )
}

interface RuleSlotsProps {
  title: string
  /** Number of slots = the grid dimension. Comes from the rendered layout. */
  count: number
  rules: Matcher[]
  onEdit: (index: number, patch: Partial<Matcher>) => void
}

/**
 * One axis of the facet grid, one editor per slot.
 *
 * Fixed length, not a list with +/- buttons: the grid already has a known
 * number of rows and columns, so a rule is a property OF row 2, not an extra
 * row. Each slot claims the panels whose name it matches; a blank slot takes
 * whatever is left over, in order, and anything matching no slot at all lands
 * in a trailing "other" row/column rather than disappearing.
 */
function RuleSlots({ title, count, rules, onEdit }: RuleSlotsProps) {
  const singular = title.replace(/s$/, '')
  return (
    <div style={styles.ruleList}>
      <div style={styles.ruleTitle}>{title}</div>
      {Array.from({ length: count }, (_, index) => {
        const rule = rules[index] ?? { op: 'contains' as MatchOp, value: '' }
        return (
          <div key={index} style={styles.ruleRow}>
            <span style={styles.slotIndex}>{singular} {index + 1}</span>
            <select
              value={rule.op}
              onChange={e => onEdit(index, { op: e.target.value as MatchOp })}
              style={{ ...styles.select, flex: '0 0 88px' }}
            >
              {MATCH_OPS.map(op => (
                <option key={op.value} value={op.value}>{op.label}</option>
              ))}
            </select>
            <input
              value={rule.value}
              onChange={e => onEdit(index, { value: e.target.value })}
              placeholder="anything"
              style={{ ...styles.select, flex: 1, minWidth: 0 }}
            />
          </div>
        )
      })}
    </div>
  )
}

interface VariantRow {
  /** The user's name, or null while the label follows the selection. */
  explicitName: string | null
  /** What the selection says it is, used as the placeholder. */
  autoLabel: string
  /** False until the row selects something. Such a row changes nothing. */
  defined: boolean
  /** Rows it contributes, once the backend has measured it. */
  rowCount?: number
  /** Which variable this row draws from; null means the plot's primary measure. */
  variable: string | null
  /** Code axes it leaves open and disagrees on, with which locations hold
   *  which version. See `SpanInfo`. */
  spans: Record<string, SpanInfo>
}

interface VariantRowsProps {
  rows: VariantRow[]
  /** The plot's primary measure — what a row with no variable draws from. */
  primary: string
  /** Other variables a row may draw from (same shape, same schema level). */
  stackable: string[]
  onRename: (index: number, name: string | null) => void
  onSetVariable: (index: number, variable: string | null) => void
  onEdit: (index: number) => void
  onRemove: (index: number) => void
  onAdd: () => void
}

/**
 * The Variants section: a name and a "select on the DAG" button, per row.
 *
 * Shaped like Factors — two columns, many rows — because it is the same kind of
 * list: a thing, and what to do with it. The name is editable and lands in the
 * figure's legend, which is the whole reason a variant is *named* rather than
 * numbered: "baseline" and "20 Hz filter" survive being read a week later,
 * "variant 2" does not.
 *
 * The selection itself is not editable here. It is a point in a space whose
 * coordinates are pipeline nodes, and the pipeline canvas is already the
 * picture of that space — see VariantDagPopup.
 */
function VariantRows({
  rows,
  primary,
  stackable,
  onRename,
  onSetVariable,
  onEdit,
  onRemove,
  onAdd,
}: VariantRowsProps) {
  return (
    <div style={styles.variantPicker}>
      {rows.map((row, index) => (
        <div key={index} style={styles.variantSetRow}>
          <input
            value={row.explicitName ?? ''}
            placeholder={row.autoLabel}
            onChange={e => onRename(index, e.target.value || null)}
            style={styles.variantNameInput}
            title={
              row.explicitName === null
                ? `Named from its selection: ${row.autoLabel}. Type to override.`
                : 'The name this variant carries in the figure'
            }
          />
          {/* Which variable this row draws from — ALWAYS shown, as a dropdown
              when there is something to switch to and as a plain label when
              there is not.
              It used to appear only when another variable could be stacked, so
              the common single-variable case rendered a row that never said what
              it was plotting. With rows now able to name different variables,
              a row that does not state its own is unreadable: two rows called
              "current" and "v1" say nothing about which is EMG and which is
              force. The name is the subject of the row; the selection is only
              what narrows it. */}
          {stackable.length > 0 ? (
            <select
              value={row.variable ?? ''}
              onChange={e => onSetVariable(index, e.target.value || null)}
              style={styles.variantVariableSelect}
              title="The variable this variant plots"
            >
              <option value="">{primary}</option>
              {stackable.map(name => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          ) : (
            <span
              style={styles.variantVariableLabel}
              title="The variable this variant plots — nothing else here can be plotted alongside it"
            >
              {row.variable ?? primary}
            </span>
          )}
          <button
            type="button"
            style={styles.variantSelectButton}
            onClick={() => onEdit(index)}
            title="Choose this variant on the pipeline graph"
          >
            ⋔ Select
          </button>
          {/* One variant is the pin; there is nothing to remove down to zero. */}
          {rows.length > 1 && (
            <button
              type="button"
              style={styles.variantRemove}
              onClick={() => onRemove(index)}
              title="Remove this variant"
            >
              ✕
            </button>
          )}
          {/* An unfilled row is inert, not broken: it must not wear the same
              warning as a selection that genuinely matched nothing. */}
          {!row.defined && (
            <span style={styles.variantUnsetTag} title="Nothing chosen yet — this row does not affect the figure. Pick a variable, or Select a variant on the graph.">
              not set
            </span>
          )}
          {row.defined && row.rowCount === 0 && (
            <span style={styles.variantEmptyTag} title="This selection matched no records — check it against what has actually run">
              no data
            </span>
          )}
          {Object.keys(row.spans).length > 0 && (
            <span
              style={styles.variantEmptyTag}
              title={
                `This variant's rows were built by more than one version of the ` +
                `code, or more than one set of run options (distribute/as_table): ` +
                `${Object.values(row.spans)
                  .map(describeSpan)
                  .join(' | ')}. Pin a version on it, or split it into one ` +
                `variant per version.`
              }
            >
              pools {Object.keys(Object.values(row.spans)[0].versions).length} versions
            </span>
          )}
        </div>
      ))}
      <button
        type="button"
        style={styles.variantAdd}
        onClick={onAdd}
        title="Plot another variable, or another variant of this one, alongside"
      >
        + Add series
      </button>
    </div>
  )
}

/**
 * How much of the variant space is on screen.
 *
 * The readout matters as much as the rows. A canvas or a factor list shows
 * coordinates; neither shows the PRODUCT, and an exploding panel count is the
 * thing that actually catches people out.
 */
function VariantReadout({ summary }: { summary?: VariantSummary }) {
  if (!summary || summary.factors.length === 0) return null
  const { total_combinations, selected_combinations } = summary
  const none = selected_combinations === 0
  return (
    <div style={none ? styles.variantCountEmpty : styles.variantCount}>
      {none
        ? 'Nothing selected — the figure would be empty.'
        : `${selected_combinations} of ${total_combinations} variant combination${
            total_combinations === 1 ? '' : 's'
          }`}
    </div>
  )
}

/**
 * Step through an ITERATE fan-out one figure at a time.
 *
 * The order is the backend's (`scistackplot.roles.fanout_keys`): schema keys
 * outermost-first, each in its declared level order. So with a `[subject,
 * trial]` schema this walks subject 1's trials, then rolls over to subject 2's
 * first trial — the arrows are a plain cursor, and none of that ordering is
 * decided here.
 */
function FigureNavigator({
  index,
  count,
  label,
  onStep,
}: {
  index: number
  count: number
  label: string
  onStep: (delta: number) => void
}) {
  return (
    <div style={styles.navigator}>
      <button
        type="button"
        style={{ ...styles.navButton, ...(index === 0 ? styles.navButtonOff : null) }}
        onClick={() => onStep(-1)}
        disabled={index === 0}
        title="Previous figure (←)"
      >
        ◀
      </button>
      <span style={styles.navLabel}>{label || '(unlabelled)'}</span>
      <span style={styles.navCount}>
        {index + 1} of {count}
      </span>
      <button
        type="button"
        style={{
          ...styles.navButton,
          ...(index >= count - 1 ? styles.navButtonOff : null),
        }}
        onClick={() => onStep(1)}
        disabled={index >= count - 1}
        title="Next figure (→)"
      >
        ▶
      </button>
    </div>
  )
}

/**
 * One factor's levels, with a checkbox each and a "3 of 12" readout.
 *
 * Collapsed by default: a schema key can have thirty levels, and the common
 * case is not filtering at all. The count is the backend's — `selected` is
 * measured through the same `apply_filters` the figure uses, so this can never
 * claim a selection the figure disagrees with.
 */
function LevelPicker({
  factor,
  onChange,
}: {
  factor: FactorInfo
  onChange: (levels: (string | number)[] | null) => void
}) {
  const [open, setOpen] = useState(false)
  const levels = factor.levels ?? []
  const selected = factor.selected ?? levels
  const chosen = new Set(selected.map(String))
  const all = chosen.size === levels.length

  const toggle = (level: string | number) => {
    const next = new Set(chosen)
    if (next.has(String(level))) next.delete(String(level))
    else next.add(String(level))
    // Back to everything selected means "no filter", not a list of all levels.
    if (next.size === levels.length) onChange(null)
    else onChange(levels.filter(l => next.has(String(l))))
  }

  return (
    <div style={styles.levelPicker}>
      <button
        type="button"
        style={styles.levelPickerHead}
        onClick={() => setOpen(v => !v)}
        title={`Choose which ${factor.display} values to plot`}
      >
        <span style={styles.factorName}>{factor.display}</span>
        <span style={all ? styles.levelCount : styles.levelCountFiltered}>
          {all ? `all ${levels.length}` : `${chosen.size} of ${levels.length}`}
        </span>
        <span style={styles.levelChevron}>{open ? '▾' : '▸'}</span>
      </button>
      {open && (
        <div style={styles.levelList}>
          {levels.map(level => (
            <label key={String(level)} style={styles.levelRow}>
              <input
                type="checkbox"
                checked={chosen.has(String(level))}
                onChange={() => toggle(level)}
                style={{ marginRight: 6 }}
              />
              {String(level)}
            </label>
          ))}
          {/* Both bulk actions, so a 51-column ColName list can be cleared
              and then one or two columns ticked, instead of unticking 49.
              Deselect passes [] (an explicit empty filter), never null --
              null means "no filter" and would re-select everything. */}
          <div style={styles.levelBulkRow}>
            {!all && (
              <button type="button" style={styles.levelReset} onClick={() => onChange(null)}>
                Select all
              </button>
            )}
            {chosen.size > 0 && (
              <button type="button" style={styles.levelReset} onClick={() => onChange([])}>
                Deselect all
              </button>
            )}
          </div>
          {chosen.size === 0 && (
            <div style={styles.levelEmpty}>
              Nothing selected — the figure will be empty.
            </div>
          )}
        </div>
      )}
    </div>
  )
}

/**
 * Sort one factor's levels into named buckets.
 *
 * A text box per level, prefilled with the level's own name: typing the same
 * word into two of them merges those levels. That is the whole interaction —
 * "pre and post1 are both baseline" is exactly what the user says, and nothing
 * happens to the figure until they say it.
 */
function LevelGroupEditor({
  group,
  onEdit,
  onRemove,
}: {
  group: LevelGroup
  onEdit: (patch: Partial<LevelGroup>) => void
  onRemove: () => void
}) {
  const [open, setOpen] = useState(true)
  const buckets = Object.entries(group.mapping)
  return (
    <div style={styles.levelPicker}>
      <div style={styles.variantSetRow}>
        <input
          value={group.name}
          onChange={e => onEdit({ name: e.target.value })}
          style={styles.variantNameInput}
          title="The name this derived factor carries in the figure"
        />
        <button
          type="button"
          style={styles.variantSelectButton}
          onClick={() => setOpen(v => !v)}
          title={`Buckets of ${group.source}`}
        >
          {open ? '▾' : '▸'} {group.source}
        </button>
        <button type="button" style={styles.variantRemove} onClick={onRemove} title="Remove">
          ✕
        </button>
      </div>
      {open && (
        <div style={styles.levelList}>
          {buckets.map(([level, label]) => (
            <div key={level} style={styles.bucketRow}>
              <span style={styles.bucketLevel}>{level}</span>
              <span style={styles.bucketArrow}>→</span>
              <input
                value={label}
                onChange={e =>
                  onEdit({ mapping: { ...group.mapping, [level]: e.target.value } })
                }
                style={styles.bucketInput}
                placeholder="(drop)"
              />
            </div>
          ))}
          <label style={styles.levelRow} title="Levels with an empty bucket are dropped from the figure unless you name a catch-all">
            <input
              type="checkbox"
              checked={group.unmatched !== null}
              onChange={e => onEdit({ unmatched: e.target.checked ? 'other' : null })}
              style={{ marginRight: 6 }}
            />
            Keep the rest as
            <input
              value={group.unmatched ?? ''}
              disabled={group.unmatched === null}
              onChange={e => onEdit({ unmatched: e.target.value })}
              style={{ ...styles.bucketInput, marginLeft: 6 }}
            />
          </label>
        </div>
      )}
    </div>
  )
}

/** "+ Add group" — pick which factor's levels to bucket. */
function BucketAdder({
  factors,
  onAdd,
}: {
  factors: FactorInfo[]
  onAdd: (source: string, levels: (string | number)[]) => void
}) {
  if (factors.length === 0) return null
  return (
    <select
      value=""
      style={styles.variantAdd}
      onChange={e => {
        const factor = factors.find(f => f.name === e.target.value)
        if (factor) onAdd(factor.name, factor.levels)
      }}
    >
      <option value="">+ Group levels of…</option>
      {factors.map(factor => (
        <option key={factor.name} value={factor.name}>
          {factor.display}
        </option>
      ))}
    </select>
  )
}

/** Numeric bounds on the measure: the outlier trim, not a level picker. */
function RangeFilter({
  measure,
  filter,
  onChange,
}: {
  measure: string
  filter?: Filter
  onChange: (minimum: number | null, maximum: number | null) => void
}) {
  const parse = (text: string) => (text.trim() === '' ? null : Number(text))
  return (
    <div style={styles.rangeRow}>
      <span style={styles.factorName}>{measure}</span>
      <input
        type="number"
        value={filter?.minimum ?? ''}
        placeholder="min"
        style={styles.rangeInput}
        onChange={e => onChange(parse(e.target.value), filter?.maximum ?? null)}
      />
      <input
        type="number"
        value={filter?.maximum ?? ''}
        placeholder="max"
        style={styles.rangeInput}
        onChange={e => onChange(filter?.minimum ?? null, parse(e.target.value))}
      />
    </div>
  )
}

/**
 * What ticking a grouping did to the figure, in three words.
 *
 * Reads the spec, never decides anything: the role itself came from
 * `roles.role_for_new_grouping` and the nesting position from `groups.ts`.
 * The one that matters is "pooled" — a grouping that landed on FREE because
 * both channels were taken really does average its levels together, and a row
 * that looks applied while doing nothing is what this work set out to remove.
 */
function groupingPlacement(
  spec: Spec | null,
  layers: string[],
  factors: FactorInfo[],
  name: string
): string {
  const role = spec?.roles?.[name]
  if (role === 'group') {
    const at = layers.indexOf(name)
    const inside = layers[at - 1]
    const display = (n: string) => factors.find(f => f.name === n)?.display ?? n
    const where = inside ? `grouped, around ${display(inside)}` : 'grouped'
    return spec?.color === name ? `${where}, coloured` : where
  }
  if (role === 'facet') return 'separate panels'
  if (role === 'iterate') return 'separate figures'
  if (role === 'collapse') return 'collapsed'
  return 'separate figures'
}

/** What the Summary section says the error bars are over — the chain and
 *  its sample, read off the capability report (`collapse`), never derived. */
function sampleNote(capabilities: Capabilities | null | undefined, spec: Spec | null): string {
  const chain = capabilities?.collapse
  if (!chain || chain.order.length === 0) {
    return 'Nothing is collapsed, so there is no sample: each mark is one value and has no error bar.'
  }
  const error = spec?.aggregate?.error ?? 'sd'
  if (chain.pooled) {
    return `Pooled: every level of ${chain.order.join(' × ')} is one observation; ${error} across all of them.`
  }
  const inner = chain.order.slice(0, -1)
  const within = inner.length ? `${inner.join(', then ')} averaged within ${chain.sample} first; ` : ''
  return `${within}${error} across ${chain.sample}.`
}

/**
 * The grouping list: which factors get one mark per level combination, in
 * what order, and which one is coloured.
 *
 * Innermost first — the first row is the mark's own identity, each row below
 * wraps around it. The ↑ arrow moves a layer inward (towards the first row),
 * ↓ outward. One radio column tags the coloured layer: colour never splits
 * data, it labels a split by legend, which is why it is a tag on a row here
 * and not a role in the Factors dropdown.
 *
 * `maxLabelled` is the backend's cap on LABELLED tick layers (a fourth level
 * of nesting cannot be read off an axis); the coloured layer is labelled by
 * the legend and does not count, so colouring one makes room for another.
 */
function GroupingList({
  factors,
  layers,
  color,
  hint,
  labelled,
  maxLabelled,
  onToggle,
  onMove,
  onColor,
}: {
  factors: FactorInfo[]
  layers: string[]
  color: string | null
  hint?: string
  labelled: number
  maxLabelled: number
  onToggle: (factor: string, on: boolean) => void
  onMove: (factor: string, delta: number) => void
  onColor: (factor: string | null) => void
}) {
  const groupable = factors.filter(f => f.group_available || layers.includes(f.name))
  const full = labelled >= maxLabelled
  const display = (name: string) =>
    factors.find(f => f.name === name)?.display ?? name

  return (
    <>
      <div style={styles.hint}>
        {hint ?? 'One mark per combination of these — first entry innermost.'}
        {' '}Tick the colour to label a layer by legend instead.
      </div>
      {layers.map((name, index) => (
        <div key={name} style={styles.xLayerRow}>
          <span style={styles.xLayerDepth}>{index + 1}</span>
          <input
            type="checkbox"
            checked
            onChange={() => onToggle(name, false)}
            title="Stop grouping by this factor (it separates figures instead)"
            style={{ marginRight: 6 }}
          />
          <span style={styles.factorName}>{display(name)}</span>
          <label style={styles.colorTag} title="Label this layer by legend colour">
            <input
              type="radio"
              name="grouping-colour"
              checked={color === name}
              onChange={() => onColor(name)}
              onClick={() => { if (color === name) onColor(null) }}
              style={{ marginRight: 3 }}
            />
            colour
          </label>
          <button
            type="button"
            style={{
              ...styles.navButton,
              ...(index === 0 ? styles.navButtonOff : null),
            }}
            disabled={index === 0}
            onClick={() => onMove(name, -1)}
            title="Move inward"
          >
            ↑
          </button>
          <button
            type="button"
            style={{
              ...styles.navButton,
              ...(index === layers.length - 1 ? styles.navButtonOff : null),
            }}
            disabled={index === layers.length - 1}
            onClick={() => onMove(name, 1)}
            title="Move outward"
          >
            ↓
          </button>
        </div>
      ))}
      {groupable
        .filter(f => !layers.includes(f.name))
        .map(factor => (
          <label
            key={factor.name}
            style={{ ...styles.kindRow, opacity: full ? 0.45 : 1 }}
            title={
              full
                ? `At most ${maxLabelled} labelled tick layers fit on the x axis — colour one, or separate panels.`
                : factor.group_reason ?? 'One mark per level of this factor'
            }
          >
            <input
              type="checkbox"
              checked={false}
              disabled={full || !factor.group_available}
              onChange={e => onToggle(factor.name, e.target.checked)}
              style={{ marginRight: 6 }}
            />
            {factor.display}
          </label>
        ))}
      {layers.length === 0 && groupable.length === 0 && (
        <div style={styles.hint}>No factor here can group the marks.</div>
      )}
    </>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>{title}</div>
      {children}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
  },
  // Own-tab mode: fill the webview exactly, no card, no backdrop.
  embeddedRoot: { position: 'absolute', inset: 0, background: '#16162a' },
  embeddedPanel: {
    width: '100%', height: '100%', background: '#16162a',
    display: 'flex', flexDirection: 'row', overflow: 'hidden',
  },
  panel: {
    width: '96vw', height: '94vh', background: '#16162a',
    border: '1px solid #3a3a5a', borderRadius: 8,
    display: 'flex', flexDirection: 'row', overflow: 'hidden',
  },
  // The title bar and the controls share one column, so the figure column
  // starts at the very top of the panel.
  rail: {
    width: 260, flexShrink: 0, display: 'flex', flexDirection: 'column',
    minHeight: 0, borderRight: '1px solid #2a2a4a',
  },
  // Collapsed: only as wide as the toggle that brings it back.
  railCollapsed: { width: 'auto' },
  header: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    gap: 6, padding: '8px 12px', borderBottom: '1px solid #2a2a4a',
    background: '#1a1a2e', flexShrink: 0,
  },
  title: {
    color: '#eee', fontSize: 13, fontWeight: 600,
    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
  },
  close: {
    background: 'transparent', border: 'none', color: '#888',
    cursor: 'pointer', fontSize: 14,
  },
  // LONGHANDS only, and the same keys in both states — see controlsHidden.
  controls: {
    padding: 12, flex: 1, minHeight: 0, overflowX: 'hidden', overflowY: 'auto',
  },
  // Collapsed rather than unmounted: the control state survives the toggle.
  //
  // These two objects must name exactly the same overflow properties. React
  // removes whatever a re-render drops, so a collapsed state that added the
  // `overflow` SHORTHAND left React clearing `overflow` on reopen — and
  // clearing the shorthand clears overflow-y with it, while the unchanged
  // `overflowY: 'auto'` was skipped as "nothing to re-apply". The rail came
  // back unscrollable, and only after a collapse/expand cycle.
  controlsHidden: {
    width: 0, flex: '0 0 0', padding: 0, overflowX: 'hidden', overflowY: 'hidden',
  },
  headerActions: { display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0 },
  headerLeft: { display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 },
  ruleList: { marginTop: 6 },
  ruleTitle: { fontSize: 10, color: '#999', marginBottom: 3 },
  ruleRow: { display: 'flex', alignItems: 'center', gap: 4, marginBottom: 4 },
  slotIndex: { fontSize: 10, color: '#888', flex: '0 0 52px' },
  gridSizeRow: { display: 'flex', gap: 8, marginBottom: 4 },
  gridSizeField: { display: 'flex', alignItems: 'center', gap: 4 },
  gridSizeLabel: { fontSize: 11, color: '#bbb' },
  // An unpinned dimension reads as derived, not as something the user typed.
  gridSizeAuto: { color: '#8a8aa8', fontStyle: 'italic' },
  layoutNote: {
    fontSize: 10, color: '#e0b050', marginTop: 6, lineHeight: 1.4,
  },
  headerButton: {
    background: '#22223a', color: '#ccc', border: '1px solid #3a3a5a',
    borderRadius: 4, cursor: 'pointer', fontSize: 11, padding: '2px 8px',
  },
  // minWidth 0: a flex item defaults to its content's width, and a wide plotly
  // figure would then push the rail off the panel instead of scrolling.
  canvas: { flex: 1, minWidth: 0, padding: 12, overflowY: 'auto' },
  section: { marginBottom: 16 },
  sectionTitle: {
    fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.6,
    color: '#7b68ee', marginBottom: 6, fontWeight: 700,
  },
  hint: { fontSize: 10, color: '#777', marginBottom: 6, fontStyle: 'italic' },
  refusedRow: {
    fontSize: 10, color: '#6b6b7a', marginBottom: 4, paddingLeft: 18,
    fontFamily: 'monospace',
  },
  locationButton: {
    width: '100%',
    textAlign: 'left',
    background: '#12121f',
    border: '1px solid #2a2a4a',
    color: '#ddd',
    borderRadius: 4,
    padding: '5px 8px',
    fontSize: 12,
    cursor: 'pointer',
  },
  pinNote: {
    fontSize: 10, lineHeight: 1.45, color: '#d9c48f', marginBottom: 6,
    padding: '5px 7px', background: '#221c0c', borderLeft: '2px solid #d9b45f',
  },
  factorRow: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    gap: 6, marginBottom: 5,
  },
  factorName: {
    fontSize: 11, fontFamily: 'monospace', color: '#ccc',
    display: 'flex', alignItems: 'center', gap: 4, minWidth: 0,
  },
  levelCount: {
    fontSize: 9, color: '#666', background: '#22223a',
    borderRadius: 8, padding: '0 5px',
  },
  // Where a ticked grouping ended up. Muted: it is a confirmation, not a
  // warning — the one reading that IS a warning ("pooled") says so in words.
  groupRole: { fontSize: 9, color: '#7c7ca0', marginLeft: 6 },
  colorTag: { fontSize: 10, color: '#7c7ca0', marginLeft: 6, marginRight: 4, whiteSpace: 'nowrap' as const },
  // A wide variable, collapsed. Reads as a row rather than a button so the
  // section still scans as one list of groupings.
  variantTag: {
    fontSize: 8, color: '#fbbf24', border: '1px solid #6b5a1a',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
  },
  fieldTag: {
    fontSize: 8, color: '#67e8f9', border: '1px solid #1a5a6b',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
  },
  codeTag: {
    fontSize: 8, color: '#c4b5fd', border: '1px solid #4c3a8a',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
    marginLeft: 6,
  },
  variantPicker: {
    display: 'flex', flexDirection: 'column', gap: 6, marginTop: 8,
  },
  variantSetRow: {
    display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap',
  },
  variantNameInput: {
    flex: 1, minWidth: 0,
    background: '#22223a', color: '#ddd', border: '1px solid #3a3a5a',
    borderRadius: 4, fontSize: 11, padding: '3px 5px',
  },
  variantSelectButton: {
    flex: '0 0 auto', padding: '3px 8px', background: '#22223a', color: '#c4b5fd',
    border: '1px solid #4c3a8a', borderRadius: 4, cursor: 'pointer', fontSize: 11,
  },
  variantRemove: {
    flex: '0 0 auto', padding: '2px 5px', background: 'transparent', color: '#888',
    border: 'none', cursor: 'pointer', fontSize: 11,
  },
  variantEmptyTag: {
    fontSize: 9, color: '#fbbf24', border: '1px solid #6b5a1a',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
  },
  // Grey, not amber: "not yet said" is a state, not a problem.
  variantUnsetTag: {
    fontSize: 9, color: '#8a8aa8', border: '1px solid #3a3a5a',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
  },
  variantAdd: {
    alignSelf: 'flex-start', padding: '3px 10px', background: 'transparent',
    color: '#9d92f5', border: '1px dashed #4c3a8a', borderRadius: 4,
    cursor: 'pointer', fontSize: 11,
  },
  variantRow: { display: 'flex', flexDirection: 'column', gap: 2 },
  variantName: {
    fontSize: 10, color: '#9ca3af', fontFamily: 'monospace',
    display: 'flex', alignItems: 'center',
  },
  variantLevels: { display: 'flex', flexWrap: 'wrap', gap: 8 },
  variantLevel: {
    display: 'flex', alignItems: 'center', gap: 3,
    fontSize: 11, color: '#ddd', cursor: 'pointer',
  },
  variantCount: {
    fontSize: 10, color: '#9ca3af', marginTop: 4,
    borderTop: '1px solid #333', paddingTop: 4,
  },
  variantCountEmpty: {
    fontSize: 10, color: '#fbbf24', marginTop: 4,
    borderTop: '1px solid #333', paddingTop: 4,
  },
  shapeTag: { fontSize: 9, color: '#67e8f9', marginLeft: 6 },
  readonlyValue: { fontSize: 12, fontFamily: 'monospace', color: '#eee' },
  select: {
    background: '#22223a', color: '#ddd', border: '1px solid #3a3a5a',
    borderRadius: 4, fontSize: 11, padding: '2px 4px', maxWidth: 130,
  },
  kindRow: {
    display: 'flex', alignItems: 'center', fontSize: 11,
    color: '#ccc', marginBottom: 3, cursor: 'pointer',
  },
  actions: { display: 'flex', gap: 6, marginTop: 8 },
  button: {
    flex: 1, padding: '5px 8px', background: '#22223a', color: '#ccc',
    border: '1px solid #3a3a5a', borderRadius: 4, cursor: 'pointer', fontSize: 11,
  },
  primaryButton: {
    flex: 1, padding: '5px 8px', background: '#7b68ee', color: '#fff',
    border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 600,
  },
  notice: { fontSize: 10, color: '#67e8f9', marginTop: 6 },
  dataChooser: {
    marginTop: 8,
    padding: 8,
    border: '1px solid #3a3a3a',
    borderRadius: 4,
  },
  dataDepthOption: {
    display: 'flex',
    alignItems: 'flex-start',
    gap: 6,
    fontSize: 11,
    marginBottom: 6,
    cursor: 'pointer',
  },
  dataColumns: { display: 'block', fontSize: 10, color: '#888', fontFamily: 'monospace' },
  note: { fontSize: 12, color: '#777', fontStyle: 'italic', padding: 8 },
  error: { fontSize: 12, color: '#f87171', padding: 12 },
  specError: {
    fontSize: 11, color: '#fbbf24', background: '#2a2416',
    border: '1px solid #6b5a1a', borderRadius: 4, padding: 8, marginBottom: 8,
  },
  figureBlock: { marginBottom: 14 },
  figureLabel: {
    fontSize: 11, fontFamily: 'monospace', color: '#aaa', marginBottom: 2,
  },
  navigator: {
    display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6,
    padding: '3px 6px', background: '#16162a', border: '1px solid #2c2c4a',
    borderRadius: 4,
  },
  navButton: {
    background: '#22223c', color: '#ddd', border: '1px solid #3a3a5a',
    borderRadius: 3, cursor: 'pointer', fontSize: 12, lineHeight: 1,
    padding: '3px 8px',
  },
  navButtonOff: { opacity: 0.35, cursor: 'default' },
  navLabel: {
    flex: 1, fontSize: 11, fontFamily: 'monospace', color: '#ccc',
    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  navCount: { fontSize: 11, color: '#888', whiteSpace: 'nowrap' },
  levelPicker: { marginBottom: 4 },
  levelPickerHead: {
    display: 'flex', alignItems: 'center', gap: 6, width: '100%',
    background: 'transparent', border: 'none', color: '#ddd', cursor: 'pointer',
    fontSize: 11, padding: '3px 0', textAlign: 'left',
  },
  levelCountFiltered: {
    fontSize: 10, color: '#fbbf24', background: '#2a2416',
    border: '1px solid #6b5a1a', borderRadius: 8, padding: '0 6px',
  },
  levelChevron: { fontSize: 9, color: '#777' },
  levelList: {
    maxHeight: 160, overflowY: 'auto', padding: '2px 0 4px 10px',
    borderLeft: '1px solid #2c2c4a', marginLeft: 2,
  },
  levelRow: {
    display: 'flex', alignItems: 'center', fontSize: 11, color: '#bbb',
    padding: '1px 0', cursor: 'pointer',
  },
  levelReset: {
    background: 'transparent', border: 'none', color: '#7aa2f7',
    cursor: 'pointer', fontSize: 10, padding: '2px 0',
  },
  levelBulkRow: { display: 'flex', gap: 10 },
  levelEmpty: { fontSize: 10, color: '#fbbf24', paddingTop: 2 },
  xLayerRow: { display: 'flex', alignItems: 'center', gap: 4, padding: '2px 0' },
  xLayerDepth: {
    fontSize: 9, color: '#888', background: '#22223c', borderRadius: 8,
    padding: '0 5px', minWidth: 14, textAlign: 'center',
  },
  bucketRow: { display: 'flex', alignItems: 'center', gap: 4, padding: '1px 0' },
  bucketLevel: {
    fontSize: 10, color: '#bbb', fontFamily: 'monospace',
    minWidth: 54, overflow: 'hidden', textOverflow: 'ellipsis',
  },
  bucketArrow: { fontSize: 9, color: '#666' },
  bucketInput: {
    flex: 1, minWidth: 40, background: '#1a1a2e', color: '#ddd',
    border: '1px solid #3a3a5a', borderRadius: 3, fontSize: 10, padding: '1px 4px',
  },
  rangeRow: { display: 'flex', alignItems: 'center', gap: 6, marginTop: 4 },
  rangeInput: {
    width: 62, background: '#1a1a2e', color: '#ddd',
    border: '1px solid #3a3a5a', borderRadius: 3, fontSize: 10, padding: '2px 4px',
  },
  variantVariableSelect: {
    background: '#1a1a2e', color: '#ddd', border: '1px solid #3a3a5a',
    borderRadius: 3, fontSize: 10, padding: '2px 4px', maxWidth: 110,
  },
  // The same information as the dropdown, when there is nothing to switch to.
  // Styled as text rather than as a disabled control: a greyed-out select
  // invites clicking and then refuses, which is worse than a plain label.
  variantVariableLabel: {
    fontSize: 10, color: '#9a9ab8', fontFamily: 'monospace',
    maxWidth: 110, overflow: 'hidden', textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  // Informational blue, NOT the amber of specError/downsampleNote. Per-location
  // "latest" legitimately produces a spanning figure; this is a statement of
  // fact about what is drawn, not a warning that something broke. Colouring it
  // like an error would train the user to dismiss the one message that says
  // their subjects were computed by different code.
  spanBanner: {
    fontSize: 11, color: '#a9c7ff', background: '#161a2e',
    border: '1px solid #2f4172', borderRadius: 4, padding: 8, marginBottom: 8,
    lineHeight: 1.5,
  },
  emptyPin: {
    fontSize: 11, color: '#ddd', background: '#16162a',
    border: '1px solid #3a3a5a', borderRadius: 6, padding: 14, marginBottom: 8,
    lineHeight: 1.6,
  },
  emptyPinTitle: { fontSize: 12, color: '#fff', marginBottom: 8 },
  emptyPinBlock: { marginTop: 10 },
  emptyPinAttempt: { color: '#bbb' },
  emptyPinCode: {
    fontFamily: 'monospace', fontSize: 10, color: '#fbbf24',
    background: '#0e0e1a', borderRadius: 3, padding: '1px 4px',
  },
  emptyPinHint: { color: '#8a8aa8', fontSize: 10, marginTop: 6 },
  emptyPinOptions: {
    display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 6,
  },
  emptyPinOption: {
    background: '#1a1a2e', color: '#9d92f5', border: '1px solid #4c3a8a',
    borderRadius: 4, cursor: 'pointer', fontSize: 10, padding: '3px 8px',
    fontFamily: 'monospace',
  },
  downsampleNote: { fontSize: 10, color: '#fbbf24', marginBottom: 4 },
  codeOverlay: {
    position: 'absolute', inset: 0, background: 'rgba(0,0,0,0.75)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 40,
  },
  code: {
    background: '#0e0e1a', color: '#ddd', border: '1px solid #3a3a5a',
    borderRadius: 6, padding: 16, fontSize: 11, fontFamily: 'monospace',
    maxHeight: '80%', maxWidth: '80%', overflow: 'auto', whiteSpace: 'pre',
  },
}
