/**
 * Difference bars in Plot Studio (Statistics > Difference bars): pure logic,
 * no React, so `npm test` can run it. Plan: .claude/plan-difference-bars.md;
 * docs/claude/difference-bars.md.
 *
 * The panel DISPLAYS what Python decided (`layout.meta.difference_bars`,
 * built by `scistackplot.diffbars.difference_meta`): each panel's `match`
 * (already text), the axis names a click reports, the ticks that can be
 * picked with the stretch of x each covers, and the bars drawn. This module
 * turns a click into one of those ticks, edits `spec.difference_bars`, and
 * builds the hover highlight. It never places a bar, never decides which
 * panel a bar belongs to, and never turns a level into text itself.
 */

/** `scistackplot.diffbars.DifferenceBar`. */
export interface DifferenceBar {
  /** The panel's separate-figure + subplot values, as text. */
  match: Record<string, string>
  /** The two ticks, `{x layer: level}`. Unordered. */
  a: Record<string, string>
  b: Record<string, string>
  label?: string
}

/** One tick a bar can end on (`difference_meta` panel `targets`). */
export interface DiffTarget {
  slot: number
  position: number
  /** The stretch of x that picks this tick: its mark and sample points. */
  x0: number
  x1: number
  values: Record<string, string>
  /** The category the trace data names this tick by. */
  x_text: string
  /** What the tick reads as. */
  label: string
}

/** One bar as drawn (`PlacedBar.to_dict`). */
export interface DiffPlaced {
  bar: DifferenceBar
  left: number
  right: number
  y: number
  left_foot: number
  right_foot: number
  label_y: number
  label_top: number
}

export interface DiffPanelMeta {
  index: number
  match: Record<string, string>
  display_title: string
  /** plotly axis names ("x", "x2"): what a click reports. */
  xaxis: string
  yaxis: string
  targets: DiffTarget[]
  bars: DiffPlaced[]
  unresolved: { bar: DifferenceBar; reason: string }[]
  /** Bars that do not fit under a typed Max. */
  unfit: DifferenceBar[]
  y_limits: [number, number] | null
}

export interface DifferenceMeta {
  panels: DiffPanelMeta[]
  /** Bars naming another figure (kept, drawn there). */
  not_in_figure: DifferenceBar[]
  /** Placed on the preview's estimate rather than the export's measurement. */
  estimated: boolean
}

export const DEFAULT_LABEL = '*'

/** A stable identity for a `{name: text}` dict, independent of key order. */
export function dictId(values: Record<string, string>): string {
  return JSON.stringify(Object.keys(values).sort().map(name => [name, values[name]]))
}

export function sameDict(a: Record<string, string>, b: Record<string, string>): boolean {
  return dictId(a) === dictId(b)
}

/** Same panel and the same two ends, in either order (`DifferenceBar.same_pair`). */
export function samePair(x: DifferenceBar, y: DifferenceBar): boolean {
  if (!sameDict(x.match, y.match)) return false
  return (
    (sameDict(x.a, y.a) && sameDict(x.b, y.b)) || (sameDict(x.a, y.b) && sameDict(x.b, y.a))
  )
}

/** The panel whose x axis is *xaxis* ("x", "x2"; plotly may report "x2" or an axis object id). */
export function panelForAxis(
  meta: DifferenceMeta | null | undefined,
  xaxis: string | undefined,
): DiffPanelMeta | undefined {
  if (!meta || !xaxis) return undefined
  return meta.panels.find(panel => panel.xaxis === xaxis)
}

/**
 * The tick a click or hover at *x* picks. A number (a numeric or positional
 * axis, including sample points at their offsets) picks the target whose
 * [x0, x1] holds it — the nearest centre if two touch at a boundary. A
 * string (a category axis names points by category) picks the target with
 * that category text.
 */
export function targetAt(
  panel: DiffPanelMeta | undefined,
  x: number | string | undefined | null,
): DiffTarget | undefined {
  if (!panel || x === undefined || x === null) return undefined
  if (typeof x === 'string') {
    const exact = panel.targets.find(t => t.x_text === x)
    if (exact) return exact
    const numeric = Number(x)
    if (x.trim() === '' || Number.isNaN(numeric)) return undefined
    x = numeric
  }
  const value = x
  const inside = panel.targets.filter(t => t.x0 <= value && value <= t.x1)
  if (!inside.length) return undefined
  return inside.reduce((best, t) =>
    Math.abs(t.position - value) < Math.abs(best.position - value) ? t : best,
  )
}

/** Bars naming this panel, in spec order. */
export function barsForPanel(
  bars: DifferenceBar[] | undefined,
  match: Record<string, string>,
): DifferenceBar[] {
  return (bars ?? []).filter(bar => sameDict(bar.match, match))
}

/**
 * `spec.difference_bars` with a new bar between *a* and *b* on the panel
 * *match*. Unchanged when both ends are one tick or the pair is already
 * there (in either order) — adding never duplicates.
 */
export function addBar(
  bars: DifferenceBar[] | undefined,
  match: Record<string, string>,
  a: Record<string, string>,
  b: Record<string, string>,
  label: string = DEFAULT_LABEL,
): DifferenceBar[] {
  const list = bars ?? []
  const next: DifferenceBar = { match: { ...match }, a: { ...a }, b: { ...b }, label }
  if (sameDict(a, b) || list.some(bar => samePair(bar, next))) return list
  return [...list, next]
}

/** Remove every entry for this pair (removes a setting, never data). */
export function removeBar(bars: DifferenceBar[] | undefined, bar: DifferenceBar): DifferenceBar[] {
  return (bars ?? []).filter(entry => !samePair(entry, bar))
}

/** Set the label of this pair's entries; an empty label becomes the default. */
export function setLabel(
  bars: DifferenceBar[] | undefined,
  bar: DifferenceBar,
  label: string,
): DifferenceBar[] {
  const text = label === '' ? DEFAULT_LABEL : label
  return (bars ?? []).map(entry => (samePair(entry, bar) ? { ...entry, label: text } : entry))
}

/** "pre ↔ post": each end by its tick's text in this panel when known. */
export function barText(bar: DifferenceBar, panel?: DiffPanelMeta): string {
  const name = (end: Record<string, string>) =>
    panel?.targets.find(t => sameDict(t.values, end))?.label ?? Object.values(end).join(' · ')
  return `${name(bar.a)} ↔ ${name(bar.b)}`
}

// ---------------------------------------------------------------------------
// Picking: "+ Add difference bar", then two clicks

export type Picking =
  | { phase: 'idle' }
  | { phase: 'first' }
  | { phase: 'second'; panelIndex: number; match: Record<string, string>; first: DiffTarget }

export const IDLE: Picking = { phase: 'idle' }

/** What one click does while picking: the next state, and the bar to add
 *  when the click completed a pair. A click on no tick changes nothing; a
 *  click on the first tick again changes nothing; a second click in ANOTHER
 *  panel starts over there (a bar joins two ticks of one panel). */
export function pickStep(
  state: Picking,
  panel: DiffPanelMeta | undefined,
  target: DiffTarget | undefined,
): { state: Picking; add?: { match: Record<string, string>; a: Record<string, string>; b: Record<string, string> } } {
  if (state.phase === 'idle' || !panel || !target) return { state }
  if (state.phase === 'first' || state.panelIndex !== panel.index) {
    return { state: { phase: 'second', panelIndex: panel.index, match: panel.match, first: target } }
  }
  if (sameDict(state.first.values, target.values)) return { state }
  return {
    state: IDLE,
    add: { match: state.match, a: state.first.values, b: target.values },
  }
}

/** The line under "+ Add difference bar" while picking. */
export function pickingPrompt(state: Picking): string {
  if (state.phase === 'first') return 'Click a mark: the first end of the bar. Esc cancels.'
  if (state.phase === 'second') return `First end: ${state.first.label}. Click the other mark. Esc cancels.`
  return ''
}

/** Band strengths: the hovered tick, and the first pick while choosing the second. */
export const HOVER_OPACITY = 0.18
export const PICKED_OPACITY = 0.35

/** One tick in one panel. */
export interface TickRef {
  panelIndex: number
  slot: number
}

/**
 * The translucent bands shown while picking: the hovered tick, and the first
 * pick (stronger), each covering its whole column of the panel so the bar,
 * box and every sample point in it read as selected. The colour is passed in
 * (the theme's accent, `PLOT_THEMES`), never literal here; plotly draws
 * the band as SVG, which cannot read a CSS variable. View-only: added
 * to the displayed layout, never to the figure.
 */
export function highlightShapes(
  meta: DifferenceMeta | null | undefined,
  hover: TickRef | null,
  picked: TickRef | null,
  color: string,
): Record<string, unknown>[] {
  const shapes: Record<string, unknown>[] = []
  const band = (ref: TickRef | null, opacity: number, name: string) => {
    if (!ref || !meta) return
    const panel = meta.panels.find(p => p.index === ref.panelIndex)
    const target = panel?.targets.find(t => t.slot === ref.slot)
    if (!panel || !target) return
    shapes.push({
      type: 'rect',
      xref: panel.xaxis,
      yref: `${panel.yaxis} domain`,
      x0: target.x0,
      x1: target.x1,
      y0: 0,
      y1: 1,
      fillcolor: color,
      opacity,
      line: { width: 0 },
      layer: 'below',
      name,
    })
  }
  band(picked, PICKED_OPACITY, 'difference-pick')
  if (!picked || !hover || picked.panelIndex !== hover.panelIndex || picked.slot !== hover.slot) {
    band(hover, HOVER_OPACITY, 'difference-hover')
  }
  return shapes
}

/** How many bars the spec holds (the Statistics group's summary). */
export function barCount(bars: DifferenceBar[] | undefined): number {
  return (bars ?? []).length
}
