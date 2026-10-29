/**
 * for_columns column groups in the Inputs picker: pure helpers between the
 * stored selection and the editable draft. No rules live here — whether a
 * draft is valid, combine or split, and how groups pair with outputs is the
 * backend's answer (`plan_column_groups`, which asks scifor's owners). This
 * file only shapes data.
 *
 * Stored shape (scidb.intent, scifor.groups_to_wire):
 *   { columns: [union], iterate: true, groups: [{ name?, columns }] }
 * `name` is present for combine (one output: the group's column name) and
 * absent for split (several outputs, paired in order).
 */

export interface ColumnGroup {
  name?: string
  columns: string[]
}

export interface ColumnSelectionWithGroups {
  columns: string[]
  iterate: boolean
  groups?: ColumnGroup[]
}

/** `plan_column_groups`' answer. */
export interface GroupPlan {
  ok: boolean
  mode: 'combine' | 'split' | null
  groups: { name: string; columns: string[] }[]
  overlaps: Record<string, string[]>
  error: string | null
  // The normalized selection the store keeps (when ok): what the picker saves.
  selection?: ColumnSelectionWithGroups | null
}

/** The editable groups a stored selection stands for: its groups, or one
 *  single-column group per picked column. */
export function groupsFromSelection(sel: ColumnSelectionWithGroups | undefined): ColumnGroup[] {
  if (!sel) return []
  if (sel.groups && sel.groups.length > 0) {
    return sel.groups.map(g => ({ ...(g.name !== undefined ? { name: g.name } : {}), columns: [...g.columns] }))
  }
  return sel.columns.map(c => ({ columns: [c] }))
}

/** The selection a draft stands for. One output names its groups (combine;
 *  a group without a name yet is named after its first column); several
 *  outputs leave them unnamed (split, paired in order). */
export function selectionFromGroups(groups: ColumnGroup[], nOutputs: number): ColumnSelectionWithGroups {
  const named = nOutputs <= 1
  const columns: string[] = []
  for (const g of groups) for (const c of g.columns) if (!columns.includes(c)) columns.push(c)
  return {
    columns,
    iterate: true,
    groups: groups.map(g => named
      ? { name: g.name ?? g.columns[0] ?? '', columns: [...g.columns] }
      : { columns: [...g.columns] }),
  }
}

/** Move group `i` by `delta` places (↑ = -1, ↓ = +1); out of range is a no-op. */
export function moveGroup(groups: ColumnGroup[], i: number, delta: number): ColumnGroup[] {
  const j = i + delta
  if (i < 0 || i >= groups.length || j < 0 || j >= groups.length) return groups
  const next = [...groups]
  ;[next[i], next[j]] = [next[j], next[i]]
  return next
}

export function removeGroup(groups: ColumnGroup[], i: number): ColumnGroup[] {
  return groups.filter((_, k) => k !== i)
}

export function addColumnToGroup(groups: ColumnGroup[], i: number, col: string): ColumnGroup[] {
  return groups.map((g, k) => (k === i && !g.columns.includes(col) ? { ...g, columns: [...g.columns, col] } : g))
}

export function removeColumnFromGroup(groups: ColumnGroup[], i: number, col: string): ColumnGroup[] {
  return groups.map((g, k) => (k === i ? { ...g, columns: g.columns.filter(c => c !== col) } : g))
}

export function renameGroup(groups: ColumnGroup[], i: number, name: string): ColumnGroup[] {
  return groups.map((g, k) => (k === i ? { ...g, name } : g))
}

/** The one line under the picker saying what the run will do. */
export function describePlan(plan: GroupPlan | null, outputNames: string[]): string {
  if (!plan) return ''
  if (!plan.ok) return plan.error ?? 'This grouping cannot run.'
  if (plan.mode === 'split') return 'Split: each group saves to its own output'
  if (plan.mode === 'combine') {
    const target = outputNames[0] ?? 'the output'
    return plan.groups.length === 0
      ? `Combine into ${target}: one column per data column`
      : `Combine into ${target}: one column per group`
  }
  return ''
}

/** The overlap warning, or '' when no column is in two groups. */
export function describeOverlaps(plan: GroupPlan | null): string {
  if (!plan || !plan.ok) return ''
  const parts = Object.entries(plan.overlaps).map(([c, names]) => `${c} (in ${names.join(', ')})`)
  return parts.length ? `Shared by more than one group — each still receives it: ${parts.join('; ')}` : ''
}

/** Same content? (The draft is re-sent only when it changed.) */
export function sameSelection(a: ColumnSelectionWithGroups | undefined | null, b: ColumnSelectionWithGroups | undefined | null): boolean {
  return JSON.stringify(a ?? null) === JSON.stringify(b ?? null)
}
