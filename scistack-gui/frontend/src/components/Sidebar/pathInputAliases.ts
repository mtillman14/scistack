/**
 * PathInput aliases — several on-disk spellings of one schema-key value, e.g.
 * a `{session}` written "Baseline" in a folder and "BL" in a file name
 * (`scifor.PathInput(aliases=)`, match-only). Shape, exactly as the backend
 * serves and accepts it: `{key: {canonical: [spelling, ...]}}`.
 *
 * Pure edits only. VALIDATION is not here: an alias key that is not a
 * template placeholder, or a spelling claimed by two canonicals, is refused
 * by scifor's PathInput when the declaration is written, and the panel shows
 * that refusal — one owner for the rule (CLAUDE.md NOTE 4).
 */

export type PathInputAliases = Record<string, Record<string, string[]>>

export interface AliasRow {
  key: string
  canonical: string
  spellings: string[]
}

/** "Baseline, 1. Baseline" -> ["Baseline", "1. Baseline"]: comma-separated,
 * trimmed, blanks and repeats dropped. */
export function parseSpellings(text: string): string[] {
  const out: string[] = []
  for (const part of text.split(',')) {
    const s = part.trim()
    if (s && !out.includes(s)) out.push(s)
  }
  return out
}

/** *aliases* with *spellings* added under key/canonical (merged with any
 * already there). A spelling equal to the canonical is dropped: the canonical
 * always matches itself. Never mutates the input. */
export function withAlias(
  aliases: PathInputAliases,
  key: string,
  canonical: string,
  spellings: string[],
): PathInputAliases {
  const k = key.trim()
  const c = canonical.trim()
  const next = clone(aliases)
  const merged = [...(next[k]?.[c] ?? [])]
  for (const s of spellings) {
    if (s !== c && !merged.includes(s)) merged.push(s)
  }
  next[k] = { ...(next[k] ?? {}), [c]: merged }
  return next
}

/** *aliases* without key/canonical; a key left with no canonicals is dropped
 * too, so "remove the last alias" writes a plain template again. */
export function withoutAlias(
  aliases: PathInputAliases,
  key: string,
  canonical: string,
): PathInputAliases {
  const next = clone(aliases)
  if (!next[key]) return next
  delete next[key][canonical]
  if (Object.keys(next[key]).length === 0) delete next[key]
  return next
}

/** One display row per canonical, in declaration order. */
export function aliasRows(aliases: PathInputAliases | null | undefined): AliasRow[] {
  const rows: AliasRow[] = []
  for (const [key, canonMap] of Object.entries(aliases ?? {})) {
    for (const [canonical, spellings] of Object.entries(canonMap)) {
      rows.push({ key, canonical, spellings: [...spellings] })
    }
  }
  return rows
}

/** How many canonicals carry aliases — what the node's badge counts. */
export function aliasCount(aliases: PathInputAliases | null | undefined): number {
  return aliasRows(aliases).length
}

function clone(aliases: PathInputAliases): PathInputAliases {
  const out: PathInputAliases = {}
  for (const [key, canonMap] of Object.entries(aliases ?? {})) {
    out[key] = {}
    for (const [canonical, spellings] of Object.entries(canonMap)) {
      out[key][canonical] = [...spellings]
    }
  }
  return out
}
