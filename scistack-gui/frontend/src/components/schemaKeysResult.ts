/**
 * The result of `change_schema_keys`, worded for SchemaKeysDialog's "Schema
 * keys changed" view. The backend decides what changed
 * (scidb.project.key_rename + each bundle section's rename_keys); this only
 * turns its `references` into lines. docs/claude/changing-schema-keys.md.
 */

/** What `change_schema_keys` returns on success. */
export interface ChangeResult {
  ok: true
  old_keys: string[]
  new_keys: string[]
  renames?: Record<string, string>
  /** {place: what changed there}: config tables, entities entries, canvas counts, … */
  references?: Record<string, unknown>
  leftovers?: string[]
}

/** One line per place a rename reached, from the backend's `references`
 * (`{config: [tables], entities: [entries], gui: {statements, bindings},
 * plots: {table: rows}, stats: {table: rows}}`). Places that changed
 * nothing are left out. */
export function referenceLines(r: Pick<ChangeResult, 'references'>): string[] {
  const refs = r.references ?? {}
  const lines: string[] = []
  const list = (v: unknown) => (Array.isArray(v) ? v.map(String) : [])
  const config = list(refs.config)
  if (config.length) lines.push(`scistack.toml: ${config.map(t => `[${t}]`).join(', ')}`)
  const entities = list(refs.entities)
  if (entities.length) lines.push(`Entities file: ${entities.join(', ')}`)
  const gui = refs.gui as { statements?: number; bindings?: number } | undefined
  if (gui?.statements) lines.push(`Node settings (level, locations, filters): ${gui.statements}`)
  if (gui?.bindings) lines.push(`Submodule bindings: ${gui.bindings}`)
  for (const section of ['plots', 'stats']) {
    const counts = refs[section] as Record<string, number> | undefined
    const n = Object.values(counts ?? {}).reduce((a, b) => a + (Number(b) || 0), 0)
    if (n) lines.push(`Saved ${section} (every version): ${n}`)
  }
  return lines
}
