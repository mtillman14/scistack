/**
 * The design held in memory by a Studio tab (plan-stats-on-figure.md): which
 * spec fields are design is Python's (`design_fields`); these functions only
 * consume that list. (The old `design_changed` broadcast between a linked
 * figure and table is gone: one tab, one design.)
 *
 * Pure (no React, no DOM): tested under node --test.
 */

// ---------------------------------------------------------------------------
// One tab, two views: the design held in memory (plan-stats-on-figure.md).
//
// The OWNER of which spec fields are design is Python
// (`scistackplot.model.design.design_paths`, sent as `design_fields` with
// `plot_describe`; `copy_design` is its Python copy). These functions only
// CONSUME that list, so the two views cannot disagree about it.

type Dict = Record<string, unknown>

/** The design fields of *spec*, as `{path: value}` (dotted paths into blocks). */
export function designOf(spec: Dict, fields: string[]): Dict {
  const out: Dict = {}
  for (const path of fields) {
    let at: unknown = spec
    for (const part of path.split('.')) {
      at = at !== null && typeof at === 'object' ? (at as Dict)[part] : undefined
    }
    out[path] = at === undefined ? null : at
  }
  return out
}

/** *target* with every design field of *source* copied over it. A field
 *  *source* does not have is removed from *target* (absent = default). */
export function mergeDesign(target: Dict, source: Dict, fields: string[]): Dict {
  const next: Dict = { ...target }
  const mine = designOf(source, fields)
  for (const path of fields) {
    const parts = path.split('.')
    const value = mine[path]
    if (parts.length === 1) {
      if (value === null) delete next[path]
      else next[path] = value
      continue
    }
    const block = { ...((next[parts[0]] as Dict | undefined) ?? {}) }
    if (value === null) delete block[parts[1]]
    else block[parts[1]] = value
    next[parts[0]] = block
  }
  return next
}

/** Whether two specs hold the same design. */
export function sameDesign(a: Dict, b: Dict, fields: string[]): boolean {
  return JSON.stringify(designOf(a, fields)) === JSON.stringify(designOf(b, fields))
}
