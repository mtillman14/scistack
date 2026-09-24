/**
 * The Labels section's edits to a plot's own aliases (`PlotSpec.aliases`).
 *
 * React-free so `npm test` runs it (tsconfig.test.json). WHAT a figure's text
 * reads as is Python's (`scistackplot.aliases`), shipped per figure as
 * `layout.meta.labelable` with each text's origin. This module only writes
 * the plot layer, in the shape `PlotSpec.to_dict` gives it back:
 *
 * - a cleared value DELETES its key, and an entry left empty is removed, so a
 *   reopened saved plot does not read as "modified" over a leftover `{}`;
 * - the `aliases` object itself stays, even empty. `to_dict` always writes it,
 *   so dropping it would read as a change too.
 *
 * docs/claude/plot-text-and-labels.md.
 */

export type AliasOrigin = 'plot' | 'project' | 'raw'

/** One piece of text as the figure draws it. */
export interface LabelableText {
  raw: string
  text: string
  origin: AliasOrigin
}

/** One entry of `layout.meta.labelable` (`scistackplot.aliases.labelable`). */
export interface Labelable {
  factor: string
  /** The key an alias is written under: `Variable.Column` for a grouping
   *  column, else the factor or measure itself. */
  key: string
  role: string
  name: LabelableText
  levels: LabelableText[]
  truncated: boolean
}

/** One `PlotSpec.aliases` entry: `Alias(name, levels)`. */
export interface AliasEntry {
  name?: string | null
  levels?: Record<string, string>
}

export type SpecAliases = Record<string, AliasEntry>

/** The plot's own name alias for `key`, or null when the plot sets none. */
export function plotName(aliases: SpecAliases | undefined, key: string): string | null {
  const name = aliases?.[key]?.name
  return typeof name === 'string' && name !== '' ? name : null
}

/** The plot's own alias for one level, or null when the plot sets none. */
export function plotLevel(aliases: SpecAliases | undefined, key: string, raw: string): string | null {
  const text = aliases?.[key]?.levels?.[raw]
  return typeof text === 'string' && text !== '' ? text : null
}

function tidy(entry: AliasEntry): AliasEntry | null {
  const out: AliasEntry = {}
  if (typeof entry.name === 'string' && entry.name !== '') out.name = entry.name
  const levels = Object.fromEntries(
    Object.entries(entry.levels ?? {}).filter(([, text]) => typeof text === 'string' && text !== '')
  )
  if (Object.keys(levels).length) out.levels = levels
  return Object.keys(out).length ? out : null
}

function withEntry(aliases: SpecAliases | undefined, key: string, entry: AliasEntry): SpecAliases {
  const next: SpecAliases = { ...(aliases ?? {}) }
  const tidied = tidy(entry)
  if (tidied) next[key] = tidied
  else delete next[key]
  return next
}

/** Set (text) or clear (null / "") the plot's name alias for `key`. */
export function withPlotName(
  aliases: SpecAliases | undefined,
  key: string,
  value: string | null,
): SpecAliases {
  return withEntry(aliases, key, { ...(aliases?.[key] ?? {}), name: value ?? '' })
}

/** Set (text) or clear (null / "") the plot's alias for one level. */
export function withPlotLevel(
  aliases: SpecAliases | undefined,
  key: string,
  raw: string,
  value: string | null,
): SpecAliases {
  const current = aliases?.[key] ?? {}
  return withEntry(aliases, key, {
    ...current,
    levels: { ...(current.levels ?? {}), [raw]: value ?? '' },
  })
}

/** What an empty box shows: the text the figure draws without the plot's
 *  own alias — the project's alias, else the raw text. */
export function placeholderFor(item: LabelableText): string {
  if (item.origin === 'project') return `${item.text} (project)`
  return item.origin === 'raw' ? item.text : item.raw
}

/** How a row's project button reads, or null for no button.
 *  - the plot sets a value: offer to make it the project's;
 *  - the project sets it and the plot does not: offer to remove it there. */
export function projectAction(
  plotValue: string | null,
  item: LabelableText,
): 'save' | 'remove' | null {
  if (plotValue !== null) return 'save'
  if (item.origin === 'project') return 'remove'
  return null
}

/** How many of an entry's texts differ from raw — for the collapsed header. */
export function aliasedCount(entry: Labelable): number {
  return entry.levels.filter(level => level.origin !== 'raw').length
}
