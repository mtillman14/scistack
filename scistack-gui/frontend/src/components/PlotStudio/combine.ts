/**
 * Combines — the panel's half. A combine maps a factor's levels into fewer
 * (stim1..stim4 -> STIM, sham -> SHAM) and, while active, REPLACES that factor:
 * it holds the source's slot (role, grouping position, colour) and the source
 * is collapsed away (user decision 2026-09-26,
 * `.claude/plot-studio-combine-section.md`).
 *
 * Python owns what a combine MEANS (`scistackplot/groups.py`: which apply,
 * their depth; `roles.complete_assignment`: the replaced source is collapsed).
 * This module owns the two spec EDITS the panel makes, and nothing else:
 *
 * - `switchSlot`: the slot dropdown in Grouping / Factors. The one place a
 *   role, a grouping position and the colour move from one name to another.
 * - the bucket model: the Combine section's editor reads a combine as ordered
 *   buckets of levels and writes it back as `LevelGroup.mapping`.
 *
 * React-free, so `npm test` runs it.
 */

export interface LevelGroup {
  /** The combined factor's name, e.g. "Stim". */
  name: string
  /** The factor whose levels are combined, e.g. "condition". */
  source: string
  /** `{level: bucket label}`; a level absent (or labelled "") is dropped. */
  mapping: Record<string, string>
  /** null DROPS rows in no bucket; a string is the catch-all bucket. */
  unmatched: string | null
  /** Whether this combine replaces its source right now. At most one per
   *  source; absent reads as true (the backend's default). */
  active?: boolean
}

/** The slice of a spec a slot switch touches. */
export interface SlotSpec {
  roles?: Record<string, string>
  groups?: string[]
  color?: string | null
  level_groups?: LevelGroup[]
}

export function isActive(group: LevelGroup): boolean {
  return group.active !== false
}

/** Which name holds `source`'s slot: its active combine, else the source. */
export function slotHolder(groups: LevelGroup[] | undefined, source: string): string {
  const active = (groups ?? []).find(g => g.source === source && isActive(g))
  return active ? active.name : source
}

/**
 * Hand `source`'s slot to `choice` — the source itself or one of its combines.
 *
 * The slot moves unchanged: the role, the place in the grouping list and the
 * colour tag go from the current holder to `choice`, and the holder is left
 * with nothing (a replaced source's role is Python's to supply — always
 * COLLAPSE). Exactly one combine of `source` is active afterwards, or none
 * when `choice` is the source. Round-tripping restores the spec exactly.
 */
export function switchSlot<S extends SlotSpec>(spec: S, source: string, choice: string): S {
  const groups = spec.level_groups ?? []
  const current = slotHolder(groups, source)
  if (current === choice) return spec
  if (choice !== source && !groups.some(g => g.source === source && g.name === choice)) {
    return spec
  }

  const level_groups = groups.map(g =>
    g.source === source ? { ...g, active: g.name === choice } : g
  )
  const roles = { ...(spec.roles ?? {}) }
  if (current in roles) {
    roles[choice] = roles[current]
    delete roles[current]
  }
  const layers = (spec.groups ?? []).map(name => (name === current ? choice : name))
  const color = spec.color === current ? choice : spec.color
  return { ...spec, level_groups, roles, groups: layers, color }
}

/**
 * Add a combine of `source` and give it the slot at once.
 *
 * Every level starts in its own bucket, named after itself, so the figure's
 * data is unchanged until levels are actually combined. The name is made
 * unique against `taken` (every factor and combine name), because the backend
 * refuses a combine named like an existing factor.
 */
export function addCombine<S extends SlotSpec>(
  spec: S,
  source: string,
  levels: (string | number)[],
  taken: string[]
): S {
  const used = new Set([...taken, ...(spec.level_groups ?? []).map(g => g.name)])
  let name = `${source} combined`
  for (let n = 2; used.has(name); n++) name = `${source} combined ${n}`
  const mapping: Record<string, string> = {}
  for (const level of levels) mapping[String(level)] = String(level)
  // Inactive first, then switched to: the switch is what moves the slot.
  const group: LevelGroup = { name, source, mapping, unmatched: null, active: false }
  const added = { ...spec, level_groups: [...(spec.level_groups ?? []), group] }
  return switchSlot(added, source, name)
}

/**
 * Remove the combine at `index`. If it holds the slot, the slot goes back to
 * the source first, so the role and grouping position survive the removal.
 */
export function removeCombine<S extends SlotSpec>(spec: S, index: number): S {
  const group = spec.level_groups?.[index]
  if (!group) return spec
  const restored = isActive(group) ? switchSlot(spec, group.source, group.source) : spec
  return {
    ...restored,
    level_groups: (restored.level_groups ?? []).filter((_, i) => i !== index),
  }
}

/**
 * Rename the combine at `index`, carrying its role, grouping position and
 * colour to the new name (they are keyed by name).
 */
export function renameCombine<S extends SlotSpec>(spec: S, index: number, name: string): S {
  const group = spec.level_groups?.[index]
  if (!group || group.name === name) return spec
  const level_groups = (spec.level_groups ?? []).map((g, i) => (i === index ? { ...g, name } : g))
  if (!isActive(group)) return { ...spec, level_groups }
  const roles = { ...(spec.roles ?? {}) }
  if (group.name in roles) {
    roles[name] = roles[group.name]
    delete roles[group.name]
  }
  const layers = (spec.groups ?? []).map(n => (n === group.name ? name : n))
  const color = spec.color === group.name ? name : spec.color
  return { ...spec, level_groups, roles, groups: layers, color }
}

// --- the bucket model ----------------------------------------------------------

export interface Bucket {
  label: string
  /** Member levels, in the source's own level order. */
  levels: string[]
}

/**
 * A combine's buckets, in the order they first appear in its mapping — which
 * is the order the backend gives the combined factor's levels (the legend
 * order). Empty labels are "no bucket", never a bucket.
 */
export function readBuckets(group: LevelGroup, sourceLevels: (string | number)[]): Bucket[] {
  const order: string[] = []
  const members = new Map<string, Set<string>>()
  for (const [level, label] of Object.entries(group.mapping)) {
    if (label === '' || label === null || label === undefined) continue
    if (!members.has(label)) {
      order.push(label)
      members.set(label, new Set())
    }
    members.get(label)!.add(level)
  }
  const levelOrder = sourceLevels.map(String)
  return order.map(label => {
    const set = members.get(label)!
    const known = levelOrder.filter(level => set.has(level))
    const unknown = [...set].filter(level => !levelOrder.includes(level))
    return { label, levels: [...known, ...unknown] }
  })
}

/**
 * Buckets back to a mapping, bucket by bucket, so the mapping's first
 * appearances — the legend order — are the bucket order. Buckets with no
 * levels cannot be written (a mapping holds levels) and are left out.
 */
export function writeBuckets(buckets: Bucket[]): Record<string, string> {
  const mapping: Record<string, string> = {}
  for (const bucket of buckets) {
    for (const level of bucket.levels) mapping[level] = bucket.label
  }
  return mapping
}

/** Source levels in no bucket — the rows the combine drops (unless a
 *  catch-all is set). */
export function unusedLevels(buckets: Bucket[], sourceLevels: (string | number)[]): string[] {
  const used = new Set(buckets.flatMap(b => b.levels))
  return sourceLevels.map(String).filter(level => !used.has(level))
}

/**
 * Put `levels` into the bucket `label` (a level belongs to one bucket, so it
 * leaves any other), or take them out of every bucket when `label` is null.
 * A bucket left empty disappears; `label` is appended when it is new.
 */
export function assignLevels(
  buckets: Bucket[],
  levels: string[],
  label: string | null,
  sourceLevels: (string | number)[]
): Bucket[] {
  const moving = new Set(levels)
  let next = buckets.map(b => ({ ...b, levels: b.levels.filter(l => !moving.has(l)) }))
  if (label !== null) {
    if (!next.some(b => b.label === label)) next.push({ label, levels: [] })
    const order = sourceLevels.map(String)
    next = next.map(b => {
      if (b.label !== label) return b
      const set = new Set([...b.levels, ...levels])
      const known = order.filter(level => set.has(level))
      const unknown = [...set].filter(level => !order.includes(level))
      return { ...b, levels: [...known, ...unknown] }
    })
  }
  return next.filter(b => b.levels.length > 0)
}

/** Rename a bucket in place (its position, and so the legend order, kept).
 *  Renaming onto an existing label merges the two, at the earlier position. */
export function renameBucket(buckets: Bucket[], from: string, to: string): Bucket[] {
  if (from === to || to === '') return buckets
  const merged: Bucket[] = []
  for (const bucket of buckets) {
    const label = bucket.label === from ? to : bucket.label
    const existing = merged.find(b => b.label === label)
    if (existing) existing.levels = [...existing.levels, ...bucket.levels]
    else merged.push({ label, levels: [...bucket.levels] })
  }
  return merged
}

/**
 * The levels a shift-click selects: every level between the last one clicked
 * and this one, inclusive, in the source's order. Without an anchor (or an
 * anchor no longer among the levels) it is just the clicked level.
 */
export function levelRange(
  sourceLevels: (string | number)[],
  anchor: string | null,
  clicked: string
): string[] {
  const order = sourceLevels.map(String)
  const to = order.indexOf(clicked)
  const from = anchor === null ? -1 : order.indexOf(anchor)
  if (from < 0 || to < 0) return [clicked]
  const [lo, hi] = from <= to ? [from, to] : [to, from]
  return order.slice(lo, hi + 1)
}

/** One-line summary for a collapsed combine row: `10 → 2`. */
export function combineSummary(group: LevelGroup, sourceLevels: (string | number)[]): string {
  const buckets = readBuckets(group, sourceLevels)
  const dropped = group.unmatched === null ? unusedLevels(buckets, sourceLevels).length : 0
  const count = buckets.length + (group.unmatched !== null && unusedLevels(buckets, sourceLevels).length ? 1 : 0)
  const base = `${sourceLevels.length} → ${count}`
  return dropped ? `${base}, ${dropped} dropped` : base
}
