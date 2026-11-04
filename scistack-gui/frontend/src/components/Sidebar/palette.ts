/**
 * Pure (React-free) rules for the sidebar palette's lists: search, "By file"
 * grouping and the "Last saved" order (.claude/plan-palette-order-and-delete.md).
 * Run under `npm test` (tsconfig.test.json).
 *
 * The palette never sorts entity NAMES itself. The server sends every list
 * already in display order (scistack_gui/domain/display_order.py, the one
 * owner of A→Z), and everything here preserves that order: filtering keeps
 * it, grouping keeps it inside each group, and the time sort is stable, so
 * ties and never-saved items stay in the server's order.
 */

/** Where a palette function is declared (`get_registry().function_sources`). */
export interface FunctionSource {
  file: string | null
  line: number | null
  /** A library reference (numpy.mean) or MATLAB builtin: no file of the user's. */
  library: boolean
}

export interface FileGroup {
  /** Stable key: the full path, or LIBRARY_GROUP / UNKNOWN_GROUP. */
  key: string
  /** What the header shows: the file name, "Library", or "Source not found". */
  label: string
  /** Tooltip: the full path (or an explanation). */
  title: string
  items: string[]
}

export const LIBRARY_GROUP = '<library>'
export const UNKNOWN_GROUP = '<unknown>'

/** The file name of a POSIX or Windows path. */
export function baseName(path: string): string {
  return path.split(/[\\/]/).pop() || path
}

/**
 * Whether *name* matches the search *query*: case-insensitive substring of
 * the name, or of the file name of *file* (so typing `emg_utils` finds every
 * function declared in emg_utils.py). An empty or blank query matches all.
 */
export function matchesSearch(query: string, name: string, file?: string | null): boolean {
  const q = query.trim().toLowerCase()
  if (!q) return true
  if (name.toLowerCase().includes(q)) return true
  return !!file && baseName(file).toLowerCase().includes(q)
}

/** *items* that match *query*, in their given (server) order. */
export function filterBySearch<T>(
  items: readonly T[],
  query: string,
  nameOf: (item: T) => string,
  fileOf?: (item: T) => string | null | undefined,
): T[] {
  if (!query.trim()) return [...items]
  return items.filter(item => matchesSearch(query, nameOf(item), fileOf?.(item) ?? null))
}

/**
 * *names* grouped by the file that declares them, each group keeping the
 * server's order. Groups are ordered by file name (case-insensitive, then
 * full path so two `utils.py` in different folders stay apart and stable),
 * with "Library" and "Source not found" last. File names are not entity
 * names, so ordering them here does not compete with display_order.py.
 */
export function groupByFile(
  names: readonly string[],
  sources: Readonly<Record<string, FunctionSource | undefined>>,
): FileGroup[] {
  const groups = new Map<string, FileGroup>()
  for (const name of names) {
    const src = sources[name]
    let key: string
    let label: string
    let title: string
    if (src?.file) {
      key = src.file
      label = baseName(src.file)
      title = src.file
    } else if (src?.library) {
      key = LIBRARY_GROUP
      label = 'Library'
      title = 'Library references and built-ins: functions you did not write'
    } else {
      key = UNKNOWN_GROUP
      label = 'Source not found'
      title = 'The source file of these functions could not be located (see scistack.log)'
    }
    let group = groups.get(key)
    if (!group) {
      group = { key, label, title, items: [] }
      groups.set(key, group)
    }
    group.items.push(name)
  }
  const rank = (g: FileGroup) => (g.key === LIBRARY_GROUP ? 1 : g.key === UNKNOWN_GROUP ? 2 : 0)
  return [...groups.values()].sort((a, b) => {
    const r = rank(a) - rank(b)
    if (r !== 0) return r
    const byName = a.label.localeCompare(b.label, undefined, { sensitivity: 'base' })
    return byName !== 0 ? byName : a.key.localeCompare(b.key)
  })
}

/**
 * *names* newest-saved first. Never-saved names (absent, or null) go last.
 * Stable: equal times and every never-saved name keep the server's order.
 * ISO-8601 strings from one source compare correctly as Date values.
 */
export function sortByLastSaved(
  names: readonly string[],
  lastSaved: Readonly<Record<string, string | null | undefined>>,
): string[] {
  const time = (n: string): number | null => {
    const iso = lastSaved[n]
    if (!iso) return null
    const t = Date.parse(iso)
    return Number.isNaN(t) ? null : t
  }
  return names
    .map((name, index) => ({ name, index, t: time(name) }))
    .sort((a, b) => {
      if (a.t === null && b.t === null) return a.index - b.index
      if (a.t === null) return 1
      if (b.t === null) return -1
      return b.t !== a.t ? b.t - a.t : a.index - b.index
    })
    .map(x => x.name)
}

/** "just now" / "5 min ago" / "3 h ago" / "2 d ago" / "4 mo ago" / "1 y ago". */
export function relativeTime(iso: string | null | undefined, now: number = Date.now()): string {
  if (!iso) return 'never saved'
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return ''
  const s = Math.max(0, Math.round((now - t) / 1000))
  if (s < 60) return 'just now'
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min ago`
  const h = Math.round(m / 60)
  if (h < 24) return `${h} h ago`
  const d = Math.round(h / 24)
  if (d < 31) return `${d} d ago`
  const mo = Math.round(d / 30)
  if (mo < 12) return `${mo} mo ago`
  return `${Math.round(d / 365)} y ago`
}
