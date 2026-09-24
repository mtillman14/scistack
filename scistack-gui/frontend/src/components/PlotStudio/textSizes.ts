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

/** `layout.meta.text_sizes`, as `ResolvedSizes.to_dict()` writes it. */
export interface ResolvedTextSizes {
  /** Points per element. `legend_title` is null when it follows the entries. */
  [element: string]: number | null | string[] | undefined
  pinned?: string[]
}

/** `StyleOptions.text` as the panel holds it: only the sizes that are set. */
export type TextSizesValue = Record<string, number | undefined>

export interface TextSizeRow {
  key: string
  label: string
  title: string
  /** What Python resolved for this element; null = "follows the legend". */
  resolved: number | null
  pinned: boolean
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
}

/** Before the first render there is no meta; the boxes still show. */
const FALLBACK_ORDER = Object.keys(KNOWN)

/** One row per element other than `base`, in Python's order. */
export function textSizeRows(
  meta: ResolvedTextSizes | undefined | null,
  text: TextSizesValue | undefined,
): TextSizeRow[] {
  const keys = meta
    ? Object.keys(meta).filter(key => key !== 'base' && key !== 'pinned')
    : FALLBACK_ORDER
  return keys.map(key => {
    const value = meta?.[key]
    const known = KNOWN[key]
    return {
      key,
      label: known?.label ?? key,
      title: known?.title ?? key,
      resolved: typeof value === 'number' ? value : null,
      // The panel's own spec says what is set; meta lags one render behind.
      pinned: typeof text?.[key] === 'number',
    }
  })
}

/** What an empty box says: the size it will be drawn at. */
export function placeholderFor(row: TextSizeRow): string {
  if (row.resolved === null) return row.key === 'legend_title' ? '= legend' : 'auto'
  return `auto · ${formatPt(row.resolved)}`
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

/** `style.text` with every per-element size cleared; `base` is kept. */
export function resetTextSizes(text: TextSizesValue | undefined): TextSizesValue {
  return typeof text?.base === 'number' ? { base: text.base } : {}
}

/** Whether any element other than `base` is set. */
export function hasFixedSizes(text: TextSizesValue | undefined): boolean {
  return Object.entries(text ?? {}).some(([key, value]) => key !== 'base' && typeof value === 'number')
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
