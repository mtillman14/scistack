/**
 * The Colours section's edits to a plot's own mark colours
 * (`PlotSpec.colors` and `StyleOptions.mark_color`).
 *
 * React-free so `npm test` runs it (tsconfig.test.json). WHICH colour a mark
 * is drawn in is Python's (`render.base.palette_for`, with the pins merged by
 * `scistackplot.colors`), shipped per figure as `layout.meta.colorable` with
 * each colour's origin. The panel never parses colour text either: what is
 * typed goes to the spec as written, and Python canonicalises it (or drops
 * it with a WARN, which the swatch then shows by keeping the old colour).
 * This module only writes the plot layer, in the shape `PlotSpec.to_dict`
 * gives it back:
 *
 * - a cleared value DELETES its key, and a thing left empty is removed, so a
 *   reopened saved plot does not read as "modified" over a leftover `{}`;
 * - the `colors` object itself stays, even empty (`to_dict` always writes it).
 *
 * docs/claude/plot-colors.md.
 */

export type ColorOrigin = 'plot' | 'project' | 'palette'

/** One painted level as the figure draws it. */
export interface ColorableLevel {
  raw: string
  /** The level's display text (its alias, else raw). */
  text: string
  /** The colour DRAWN, `#rrggbb`. */
  hex: string
  origin: ColorOrigin
}

/** One entry of `layout.meta.colorable` (`render.base.colorable`). */
export interface Colorable {
  /** The painted factor; null for the single mark colour. */
  factor: string | null
  /** The key a pin is written under (`Variable.Column` for a grouping
   *  column); null for the single mark colour (`style.mark_color` /
   *  `[colors] default`). */
  key: string | null
  /** `colour` | `sample colour` | `marks`. */
  role: string
  name: string
  levels: ColorableLevel[]
  truncated: boolean
}

/** `PlotSpec.colors`: `{thing: {level: colour text}}`. */
export type SpecColors = Record<string, Record<string, string>>

/** The plot's own colour text for one level, or null when the plot sets none. */
export function plotColor(colors: SpecColors | undefined, key: string, raw: string): string | null {
  const text = colors?.[key]?.[raw]
  return typeof text === 'string' && text !== '' ? text : null
}

/** Set (text) or clear (null / "") the plot's colour for one level. */
export function withPlotColor(
  colors: SpecColors | undefined,
  key: string,
  raw: string,
  value: string | null,
): SpecColors {
  const next: SpecColors = { ...(colors ?? {}) }
  const entry: Record<string, string> = { ...(next[key] ?? {}) }
  if (value === null || value === '') delete entry[raw]
  else entry[raw] = value
  if (Object.keys(entry).length) next[key] = entry
  else delete next[key]
  return next
}

/** The plot's own single mark colour (`style.mark_color`), or null. */
export function plotMarkColor(style: Record<string, unknown> | undefined): string | null {
  const value = style?.mark_color
  return typeof value === 'string' && value !== '' ? value : null
}

/** What an empty box shows: the colour drawn without the plot's own pin,
 *  and where it comes from. */
export function colorPlaceholder(level: ColorableLevel): string {
  if (level.origin === 'project') return `${level.hex} (project)`
  if (level.origin === 'palette') return `${level.hex} (palette)`
  return level.hex
}

/** How a row's project button reads, or null for no button — the Labels
 *  section's rule:
 *  - the plot pins a colour: offer to make it the project's;
 *  - the project pins it and the plot does not: offer to remove it there. */
export function colorProjectAction(
  plotValue: string | null,
  level: ColorableLevel,
): 'save' | 'remove' | null {
  if (plotValue !== null) return 'save'
  if (level.origin === 'project') return 'remove'
  return null
}

/** How many of an entry's levels are pinned (plot or project) — for the
 *  collapsed header. */
export function pinnedCount(entry: Colorable): number {
  return entry.levels.filter(level => level.origin !== 'palette').length
}

/**
 * `fn`, called at most once per `ms` of quiet: the LAST value of a burst wins.
 *
 * The native colour picker fires an `input` event for every step of a drag.
 * Each commit is a spec edit and so a resolve round trip; unthrottled, a
 * drag queued dozens of them. `flush` commits a pending value at once (the
 * picker's `change`, when the dialog closes), `cancel` drops it (unmount).
 */
export function debounced<T>(
  fn: (value: T) => void,
  ms: number,
): { call: (value: T) => void; flush: () => void; cancel: () => void } {
  let timer: ReturnType<typeof setTimeout> | null = null
  let pending: { value: T } | null = null
  const fire = () => {
    timer = null
    if (pending) {
      const { value } = pending
      pending = null
      fn(value)
    }
  }
  return {
    call(value: T) {
      pending = { value }
      if (timer !== null) clearTimeout(timer)
      timer = setTimeout(fire, ms)
    },
    flush() {
      if (timer !== null) clearTimeout(timer)
      fire()
    },
    cancel() {
      if (timer !== null) clearTimeout(timer)
      timer = null
      pending = null
    },
  }
}

/** How long a picker drag must pause before the colour is committed. */
export const PICKER_DEBOUNCE_MS = 250

/** The formats a colour box takes, for its hint and tooltip. Kept here, not in
 *  the .tsx, because plotTheme.test.ts forbids colour literals in components
 *  (a chrome colour belongs to plotTheme.ts); these are help text, not paint. */
export const COLOR_FORMATS = '#rrggbb, rgb(r, g, b) or a colour name'
