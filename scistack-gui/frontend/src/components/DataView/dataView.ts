/**
 * The data view's rules (GitHub #28): rows, cells, roll-up, filters.
 *
 * Every state is scidb's (`scidb.provenance.data_view`, through the
 * `data_view` handler, part `view`). A column is one variable at ITS keys; a
 * row is a location of the schema hierarchy (built from `any_data`). This
 * module only decides which cell a row shows for a column:
 *
 *   row as deep as the column's keys   that location's cell
 *   row deeper (a trial, a subject-level column)   the column's cell at the
 *                                       row's projection (a coarser variable
 *                                       is the same value for every trial)
 *   row shallower (a subject, a trial-level column)   the WORST state of the
 *                                       column's cells under it (roll-up)
 *
 * `locationKey` is the same string as `data_view.location_key` in Python.
 */

export type CellState =
  | 'present' | 'warning' | 'outdated' | 'failed'
  | 'missing_input' | 'new' | 'not_run' | 'excluded'

export interface Cell {
  state: CellState
  glyph: string
  detail: string
  record_ids: string[]
}

export interface Column {
  variable: string
  function_name?: string | null
  keys: string[]
  resolved: boolean
  cells: Record<string, Cell>
  counts: Record<string, number>
  footer: string
  present: number
  counted: number
  unchecked: number
  notes: string[]
  error?: string
}

export interface DropOff {
  variable: string
  key: string
  count: number
  lost: { value: string; state: CellState }[]
  lost_count: number
}

export interface DataViewReply {
  schema_keys: string[]
  glyphs: Record<CellState, string>
  labels: Record<CellState, string>
  any_data: string[]
  columns: Column[]
  dropoff: DropOff[]
}

export type Step = [string, string]

export interface Row {
  id: string
  key: string
  value: string
  /** The location this row stands for, outermost first. */
  path: Step[]
  depth: number
  children: Row[]
}

/** Worst first when rolling up (the Python side's `_SEVERITY`). */
export const SEVERITY: Record<CellState, number> = {
  excluded: 0, present: 1, warning: 2, new: 3, outdated: 4,
  not_run: 5, missing_input: 6, failed: 7,
}

/** THE text key of a location at some keys: `subject=01|trial=3`. */
export function locationKey(path: Step[] | Record<string, string>, keys: string[]): string {
  const loc: Record<string, string> = Array.isArray(path) ? Object.fromEntries(path) : path
  return keys.filter(k => loc[k] !== undefined && loc[k] !== null).map(k => `${k}=${loc[k]}`).join('|')
}

export function parseKey(key: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const part of key.split('|')) {
    const i = part.indexOf('=')
    if (i > 0) out[part.slice(0, i)] = part.slice(i + 1)
  }
  return out
}

function numericAware(a: string, b: string): number {
  const na = Number(a), nb = Number(b)
  if (!Number.isNaN(na) && !Number.isNaN(nb)) return na - nb
  return a < b ? -1 : a > b ? 1 : 0
}

/** The location hierarchy from every saved location (`any_data`), in schema
 *  order. A non-contiguous save hangs a deeper key directly under a shallower
 *  one; the row carries its key so it reads right. */
export function buildRows(anyData: string[], schemaKeys: string[]): Row[] {
  const roots: Row[] = []
  const index = new Map<string, Row>()
  for (const key of anyData) {
    const loc = parseKey(key)
    let parent: Row | null = null
    const path: Step[] = []
    for (const k of schemaKeys) {
      const v = loc[k]
      if (v === undefined) continue
      path.push([k, v])
      const id = JSON.stringify(path)
      let row = index.get(id)
      if (!row) {
        row = { id, key: k, value: v, path: [...path], depth: path.length - 1, children: [] }
        index.set(id, row)
        ;(parent ? parent.children : roots).push(row)
      }
      parent = row
    }
  }
  const sort = (rows: Row[]) => {
    rows.sort((a, b) => schemaKeys.indexOf(a.key) - schemaKeys.indexOf(b.key) || numericAware(a.value, b.value))
    rows.forEach(r => sort(r.children))
  }
  sort(roots)
  return roots
}

/** The cell a row shows for a column (see the module docstring), or null. */
export function cellFor(column: Column, path: Step[]): Cell | null {
  if (!column.resolved || column.error) return null
  const loc = Object.fromEntries(path)
  const has = column.keys.filter(k => k in loc)
  if (has.length === column.keys.length) {
    return column.cells[locationKey(loc, column.keys)] ?? null
  }
  // Shallower row: roll up the cells under it.
  const prefix = path.filter(([k]) => column.keys.includes(k))
  if (prefix.length === 0 && path.length > 0) {
    // The row names only keys the column does not have (non-contiguous): nothing to show.
    return null
  }
  let worst: Cell | null = null
  for (const [key, cell] of Object.entries(column.cells)) {
    const at = parseKey(key)
    if (!prefix.every(([k, v]) => at[k] === v)) continue
    if (worst === null || SEVERITY[cell.state] > SEVERITY[worst.state]) worst = cell
  }
  return worst
}

/** Rows (and their parents) where any of *columns* has one of *states*;
 *  every row when *states* is empty. */
export function filterRows(rows: Row[], columns: Column[], states: string[]): Row[] {
  if (states.length === 0) return rows
  const wanted = new Set(states)
  const keep = (row: Row): Row | null => {
    const children = row.children.map(keep).filter((r): r is Row => r !== null)
    const own = columns.some(c => {
      const cell = cellFor(c, row.path)
      return cell !== null && wanted.has(cell.state) && row.children.length === 0
    })
    if (!own && children.length === 0) return null
    return { ...row, children }
  }
  return rows.map(keep).filter((r): r is Row => r !== null)
}

/** "40 → 38 → 31": the drop-off line of the footer. */
export function dropOffText(dropoff: DropOff[]): string {
  if (dropoff.length === 0) return ''
  return `${dropoff[0].key}s: ` + dropoff.map(d => String(d.count)).join(' → ')
}

/** Every row id, for "expand all". */
export function allRowIds(rows: Row[]): string[] {
  return rows.flatMap(r => [r.id, ...allRowIds(r.children)])
}
