/**
 * The Text sizes block: which boxes to show, what an empty box says, and
 * how a typed size, ratio or bound is written into `style.text`.
 *
 * React-free so `npm test` can run it (tsconfig.test.json). The sizes
 * themselves are Python's: `scistackplot.textsize` owns the ratios, the
 * bands and the drawn sizes (one reference x ratio, clamped into each
 * element's floor/ceiling) and ships them as `layout.meta.text_sizes`.
 * Nothing here multiplies a reference by a ratio. An empty box shows the
 * number Python drew, so the placeholder cannot disagree with the saved file.
 * See docs/claude/plot-text-and-labels.md.
 */

/** How auto chose the reference (`autosize.AutoTextSize.to_dict()`). */
export interface AutoTextMeta {
  target: string
  /** The chosen reference in pt (the one dial). */
  reference: number
  /** `[bottom, top]` searched; null when every element is fixed. */
  span: [number, number] | null
  /** Drawn pt per element at the reference. */
  sizes: Record<string, number>
  /** Element -> 'floor' | 'ceiling' where held at a bound. */
  clamped: Record<string, string>
  /** The rules that failed one step up (why it is not larger). */
  limit: string[]
  /** The rules still failing at the bottom (the fitting handles them). */
  failing: string[]
  at_bottom: boolean
  layouts: number
  ms: number
}

/** `layout.meta.text_sizes`: `ResolvedSizes.to_dict()` plus the auto block
 *  (`autosize.text_sizes_meta`). */
export interface ResolvedTextSizes {
  /** Drawn points per element; `base` is the reference. */
  [element: string]:
    | number
    | null
    | string
    | string[]
    | AutoTextMeta
    | Record<string, unknown>
    | undefined
  pinned?: string[]
  /** Element -> 'floor' | 'ceiling' at this reference. */
  clamped?: Record<string, string>
  /** Every element's ratio (defaults + the spec's own). */
  ratios?: Record<string, number>
  /** Element -> [floor, ceiling] pt (auto only; empty when Font is fixed). */
  bounds?: Record<string, [number, number]>
  /** Null when `base` is fixed. */
  auto?: AutoTextMeta | null
  target?: string
  targets?: string[]
}

/** `style.text`'s per-element tables (`TextSizes.ratios/floors/ceilings`). */
export const TEXT_TABLES = ['ratios', 'floors', 'ceilings'] as const
export type TextTable = (typeof TEXT_TABLES)[number]

/** `StyleOptions.text` as the panel holds it: only the sizes that are set,
 *  `target` (a string), and the tables (absent when empty). */
export type TextSizesValue = Record<
  string,
  number | string | Record<string, number> | undefined
>

/** Meta keys that are not an element's box. */
const NOT_ROWS = new Set(['base', 'pinned', 'auto', 'target', 'targets', 'clamped', 'ratios', 'bounds'])
/** `style.text` keys that are not an element's size. */
const NOT_SIZES = new Set<string>(['base', 'target', ...TEXT_TABLES])

/** Before the first render: Python's `spec.TEXT_TARGETS`, for the toggle. */
const FALLBACK_TARGETS = ['print', 'slide']

export interface TextSizeRow {
  key: string
  label: string
  title: string
  /** What Python drew for this element (null before the first render). */
  resolved: number | null
  pinned: boolean
  /** Auto only: held at its bound ("held at its floor"), or null. */
  autoReason: string | null
}

/** Labels for the elements this build knows. An element Python adds later
 *  still gets a box, labelled by its key, because the rows come from the
 *  resolved sizes, not from this table. */
const KNOWN: Record<string, { label: string; title: string }> = {
  title: { label: 'Title', title: 'The figure title' },
  x_label: { label: 'X label', title: 'The x axis title, including one shared under several columns' },
  y_label: { label: 'Y label', title: "The y axis title, and each faceted panel's y title" },
  x_ticks: {
    label: 'X ticks',
    title: 'The x tick labels. Fixed, the fit may still rotate, wrap or thin them but never shrinks them',
  },
  y_ticks: { label: 'Y ticks', title: 'The y tick labels' },
  groups: { label: 'Groups', title: 'The label rows under a nested x axis (brackets)' },
  legend: {
    label: 'Legend',
    title: 'Legend entries. Fixed, the legend may move below the plot but is never shrunk',
  },
  legend_title: {
    label: 'Legend title',
    title: 'The legend title. Empty, it keeps its ratio to the entries, even when they shrink',
  },
  differences: { label: 'Differences', title: 'The label over each difference bar ("*")' },
}

/** Before the first render there is no meta; the boxes still show. */
const FALLBACK_ORDER = Object.keys(KNOWN)

/** One row per element other than `base`, in Python's order. */
export function textSizeRows(
  meta: ResolvedTextSizes | undefined | null,
  text: TextSizesValue | undefined,
): TextSizeRow[] {
  const keys = meta ? Object.keys(meta).filter(key => !NOT_ROWS.has(key)) : FALLBACK_ORDER
  return keys.map(key => {
    const value = meta?.[key]
    const known = KNOWN[key]
    // The panel's own spec says what is set; meta lags one render behind.
    const pinned = typeof text?.[key] === 'number'
    const held = !pinned && meta?.auto ? meta?.clamped?.[key] : undefined
    return {
      key,
      label: known?.label ?? key,
      title: known?.title ?? key,
      resolved: typeof value === 'number' ? value : null,
      pinned,
      autoReason: held ? `held at its ${held}` : null,
    }
  })
}

/** What an empty box says: the size it will be drawn at. */
export function placeholderFor(row: TextSizeRow): string {
  if (row.resolved === null) return 'auto'
  return `auto · ${formatPt(row.resolved)}`
}

/** A row's hover text: what it sizes, and for an auto size whether it is
 *  held at its floor or ceiling instead of following its ratio. */
export function rowTitle(row: TextSizeRow): string {
  return row.autoReason ? `${row.title}. Auto: ${row.autoReason}, not at its ratio` : row.title
}

/** Whether `base` is auto (unset) in the panel's spec. */
export function isAutoText(text: TextSizesValue | undefined): boolean {
  return typeof text?.base !== 'number'
}

/** The Font box's placeholder while auto: the chosen reference, if known. */
export function basePlaceholder(meta: ResolvedTextSizes | undefined | null): string {
  return typeof meta?.base === 'number' && meta?.auto ? `auto · ${formatPt(meta.base)}` : 'auto'
}

/** One line on what auto decided: the reference, its range, and the rule
 *  that limited it (or what still fails at the bottom). Null when fixed. */
export function autoSummary(meta: ResolvedTextSizes | undefined | null): string | null {
  const auto = meta?.auto
  if (!auto) return null
  const span = auto.span ? ` of ${formatPt(auto.span[0])}–${formatPt(auto.span[1])}` : ''
  const head = `Reference ${formatPt(auto.reference)} pt${span}`
  if (auto.failing.length) return `${head} (the bottom); still failing: ${auto.failing.join('; ')}`
  if (auto.limit.length) return `${head}; one step larger: ${auto.limit.join('; ')}`
  return `${head} (the top)`
}

/** The Font box's hover text: how auto works, and what it decided. */
export function baseTitle(meta: ResolvedTextSizes | undefined | null): string {
  const intro =
    'Empty = auto: one reference size, as large as the figure lays out cleanly; every ' +
    'element is the reference x its ratio, held inside its floor and ceiling. A number ' +
    'fixes the reference (no floors or ceilings)'
  const auto = meta?.auto
  const summary = autoSummary(meta)
  if (!auto || !summary) return intro
  return `${intro}. ${summary}. Chosen in ${auto.layouts} layout(s), ${auto.ms} ms`
}

/** The targets the toggle offers: Python's list once a render has sent it. */
export function textTargets(meta: ResolvedTextSizes | undefined | null): string[] {
  return meta?.targets?.length ? meta.targets : FALLBACK_TARGETS
}

/** The panel's target (`print` unless set). */
export function textTarget(text: TextSizesValue | undefined): string {
  return typeof text?.target === 'string' ? text.target : 'print'
}

/** `style.text` with the target set. Stored even when it is the default,
 *  because Python's `to_dict` always writes it (a deleted key would read
 *  as "modified" against a saved plot). */
export function withTextTarget(text: TextSizesValue | undefined, target: string): TextSizesValue {
  return { ...(text ?? {}), target }
}

/**
 * `style.text` with one size set or cleared. A cleared or non-positive size
 * DELETES the key rather than storing null: the saved spec drops nulls
 * (`PlotSpec.to_dict`), so a stored null would read as "modified" against it.
 */
export function withTextSize(
  text: TextSizesValue | undefined,
  key: string,
  value: number | null,
): TextSizesValue {
  const next: TextSizesValue = { ...(text ?? {}) }
  if (value === null || !Number.isFinite(value) || value <= 0) delete next[key]
  else next[key] = value
  return next
}

/** One table of `style.text` (`ratios` / `floors` / `ceilings`). */
export function textTable(text: TextSizesValue | undefined, table: TextTable): Record<string, number> {
  const value = text?.[table]
  return value && typeof value === 'object' ? value : {}
}

/**
 * `style.text` with one element's ratio, floor or ceiling set or cleared.
 * Cleared = the element's entry deleted, and an emptied table deleted too:
 * Python's `to_dict` drops empty tables, so a leftover `{}` would read as
 * "modified" against a saved plot.
 */
export function withTextTable(
  text: TextSizesValue | undefined,
  table: TextTable,
  element: string,
  value: number | null,
): TextSizesValue {
  const entries = { ...textTable(text, table) }
  if (value === null || !Number.isFinite(value) || value <= 0) delete entries[element]
  else entries[element] = value
  const next: TextSizesValue = { ...(text ?? {}) }
  if (Object.keys(entries).length) next[table] = entries
  else delete next[table]
  return next
}

/** `style.text` with every ratio, floor and ceiling back to the defaults. */
export function resetTextTables(text: TextSizesValue | undefined): TextSizesValue {
  const next: TextSizesValue = { ...(text ?? {}) }
  for (const table of TEXT_TABLES) delete next[table]
  return next
}

/** Whether any ratio, floor or ceiling is set. */
export function hasTextTables(text: TextSizesValue | undefined): boolean {
  return TEXT_TABLES.some(table => Object.keys(textTable(text, table)).length > 0)
}

/** One row of the Proportions grid: an element's stored intent and what
 *  Python drew from it. */
export interface ProportionRow {
  key: string
  label: string
  /** Stored overrides (null = the default, shown as the placeholder). */
  ratio: number | null
  floor: number | null
  ceiling: number | null
  /** Python's effective values (defaults included), for the placeholders. */
  effectiveRatio: number | null
  effectiveFloor: number | null
  effectiveCeiling: number | null
  /** Drawn pt, and 'floor' / 'ceiling' when held there. */
  drawn: number | null
  clamped: string | null
  /** A fixed pt size: outside the scaling, its ratio and bounds unused. */
  fixed: boolean
}

/** The Proportions grid, one row per element in Python's order. Bounds are
 *  absent while Font is fixed (a fixed reference is never clamped). */
export function proportionRows(
  meta: ResolvedTextSizes | undefined | null,
  text: TextSizesValue | undefined,
): ProportionRow[] {
  const ratios = textTable(text, 'ratios')
  const floors = textTable(text, 'floors')
  const ceilings = textTable(text, 'ceilings')
  return textSizeRows(meta, text).map(row => {
    const bounds = meta?.bounds?.[row.key]
    return {
      key: row.key,
      label: row.label,
      ratio: ratios[row.key] ?? null,
      floor: floors[row.key] ?? null,
      ceiling: ceilings[row.key] ?? null,
      effectiveRatio: meta?.ratios?.[row.key] ?? null,
      effectiveFloor: bounds ? bounds[0] : null,
      effectiveCeiling: bounds ? bounds[1] : null,
      drawn: row.resolved,
      clamped: row.pinned ? null : meta?.clamped?.[row.key] ?? null,
      fixed: row.pinned,
    }
  })
}

/** The Drawn cell: "12.6", "9.6 · floor" when held at a bound, "fixed". */
export function drawnLabel(row: ProportionRow): string {
  if (row.drawn === null) return '—'
  const drawn = formatPt(row.drawn)
  if (row.fixed) return `${drawn} · fixed`
  return row.clamped ? `${drawn} · ${row.clamped}` : drawn
}

/** `style.text` with every per-element size cleared; `base`, `target` and
 *  the proportions are kept. */
export function resetTextSizes(text: TextSizesValue | undefined): TextSizesValue {
  const kept: TextSizesValue = {}
  for (const key of NOT_SIZES) if (text?.[key] !== undefined) kept[key] = text[key]
  return kept
}

/** Whether any element (not `base`, `target` or a table) is set. */
export function hasFixedSizes(text: TextSizesValue | undefined): boolean {
  return Object.entries(text ?? {}).some(([key, value]) => !NOT_SIZES.has(key) && typeof value === 'number')
}

/** For the overlap notice: the fixed x tick size, if that is what overlaps. */
export function fixedTickNote(text: TextSizesValue | undefined): string | null {
  const size = text?.x_ticks
  return typeof size === 'number' ? `The x tick font is fixed at ${formatPt(size)} pt.` : null
}

/** 11.662 → "11.7", 14 → "14". */
export function formatPt(value: number): string {
  return String(Math.round(value * 10) / 10)
}
