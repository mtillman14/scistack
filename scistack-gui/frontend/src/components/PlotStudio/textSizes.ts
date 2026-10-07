/**
 * The Text sizes block: which boxes to show, what an empty box says, and
 * how a typed size is written into `style.text`.
 *
 * React-free so `npm test` can run it (tsconfig.test.json). The sizes
 * themselves are Python's: `scistackplot.textsize.resolve_sizes` decides every
 * derived size and ships them as `layout.meta.text_sizes`. Nothing here
 * multiplies `base` by a ratio. An empty box shows the number Python resolved,
 * so the placeholder cannot disagree with the saved file.
 * See docs/claude/plot-text-and-labels.md.
 */

/** How auto chose the sizes (`autosize.AutoTextSize.to_dict()`). */
export interface AutoTextMeta {
  target: string
  /** Points per element auto sized, plus `base`. */
  sizes: Record<string, number>
  /** Element -> why the next size up failed. Absent = at its ceiling. */
  binding: Record<string, string>
  /** Elements whose floor still needed fitting. */
  at_floor: string[]
  layouts: number
  ms: number
}

/** `layout.meta.text_sizes`: `ResolvedSizes.to_dict()` plus the auto block
 *  (`autosize.text_sizes_meta`). */
export interface ResolvedTextSizes {
  /** Points per element. `legend_title` is null when it follows the entries. */
  [element: string]: number | null | string | string[] | AutoTextMeta | undefined
  pinned?: string[]
  /** Null when `base` is fixed. */
  auto?: AutoTextMeta | null
  target?: string
  targets?: string[]
}

/** `StyleOptions.text` as the panel holds it: only the sizes that are set,
 *  and `target` (a string). */
export type TextSizesValue = Record<string, number | string | undefined>

/** Meta keys that are not an element's box. */
const NOT_ROWS = new Set(['base', 'pinned', 'auto', 'target', 'targets'])
/** `style.text` keys that are not an element's size. */
const NOT_SIZES = new Set(['base', 'target'])

/** Before the first render: Python's `spec.TEXT_TARGETS`, for the toggle. */
const FALLBACK_TARGETS = ['print', 'slide']

export interface TextSizeRow {
  key: string
  label: string
  title: string
  /** What Python resolved for this element; null = "follows the legend". */
  resolved: number | null
  pinned: boolean
  /** Auto only: why this element is not larger (Python's words), or null. */
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
  groups: {
    label: 'Groups',
    title: 'The label rows under a nested x axis (brackets). Empty, they scale with the x ticks',
  },
  legend: {
    label: 'Legend',
    title: 'Legend entries. Fixed, the legend may move below the plot but is never shrunk',
  },
  legend_title: { label: 'Legend title', title: 'Empty, it follows the legend entries' },
  differences: {
    label: 'Differences',
    title: 'The label over each difference bar ("*"). Empty, it is the font size',
  },
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
    return {
      key,
      label: known?.label ?? key,
      title: known?.title ?? key,
      resolved: typeof value === 'number' ? value : null,
      pinned,
      autoReason: (!pinned && meta?.auto?.binding?.[key]) || null,
    }
  })
}

/** What an empty box says: the size it will be drawn at. */
export function placeholderFor(row: TextSizeRow): string {
  if (row.resolved === null) return row.key === 'legend_title' ? '= legend' : 'auto'
  return `auto · ${formatPt(row.resolved)}`
}

/** A row's hover text: what it sizes, and for an auto size what stopped it
 *  growing ("x ticks rotate 45°" — it would at the next size up). */
export function rowTitle(row: TextSizeRow): string {
  return row.autoReason
    ? `${row.title}. Auto: the next size up fails (${row.autoReason})`
    : row.title
}

/** Whether `base` is auto (unset) in the panel's spec. */
export function isAutoText(text: TextSizesValue | undefined): boolean {
  return typeof text?.base !== 'number'
}

/** The Font box's placeholder while auto: the chosen font size, if known. */
export function basePlaceholder(meta: ResolvedTextSizes | undefined | null): string {
  return typeof meta?.base === 'number' && meta?.auto ? `auto · ${formatPt(meta.base)}` : 'auto'
}

/** The Font box's hover text: how auto works, and what it decided. */
export function baseTitle(meta: ResolvedTextSizes | undefined | null): string {
  const intro =
    'Empty = auto: each text element is as large as the figure lays it out cleanly, ' +
    'within the Print or Slide band. A number fixes the font and every empty size below follows it'
  const auto = meta?.auto
  if (!auto) return intro
  const limited = Object.entries(auto.binding)
  const floor = auto.at_floor.length ? `; still fitted at the floor: ${auto.at_floor.join(', ')}` : ''
  return (
    `${intro}. Chosen in ${auto.layouts} layout(s), ${auto.ms} ms` +
    (limited.length ? `; limited: ${limited.map(([e, why]) => `${e} (${why})`).join('; ')}` : '; all at the ceiling') +
    floor
  )
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

/** `style.text` with every per-element size cleared; `base` and `target` are kept. */
export function resetTextSizes(text: TextSizesValue | undefined): TextSizesValue {
  const kept: TextSizesValue = {}
  for (const key of NOT_SIZES) if (text?.[key] !== undefined) kept[key] = text[key]
  return kept
}

/** Whether any element (not `base`, not `target`) is set. */
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
