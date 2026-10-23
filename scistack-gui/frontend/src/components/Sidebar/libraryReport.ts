/**
 * What a ⇪ Share / publish / ✎ Make my own copy did, as the dialogs show it —
 * the pure half (no React), so `npm test` runs it
 * (.claude/plan-library-fork-chain.md O24/O26).
 *
 * The report is `services/library_authoring.AuthoringReport.to_dict()`; the
 * terminal prints the same report (`scistack library ... --json`).
 */

export interface AuthoringReport {
  action: 'share' | 'publish' | 'copy'
  library: string
  folder: string
  pipeline: string | null
  source: string | null
  files: string[]
  functions: Record<string, string>
  kept_labels: string[]
  variables: string[]
  parameters: string[]
  path_inputs: string[]
  dependencies: string[]
  derived_from: string[]
  step_values: string[]
  carried: Record<string, number>
  not_carried: { function: string; reason: string }[]
  warnings: string[]
  install: string
}

/** The report as lines: what happened, every rename, and whether each
 *  renamed step's results carried over (R2) or why not. */
export function reportLines(r: AuthoringReport): string[] {
  const verb = r.action === 'publish' ? 'Published to' : r.action === 'copy' ? 'Copied as' : 'Shared as'
  const lines = [`${verb} ${r.library} (${r.files.length} file(s) written) in ${r.folder}`]
  if (r.source) lines.push(`From ${r.source}; it replaces ${r.source} in this project.`)
  for (const [oldName, newName] of Object.entries(r.functions)) {
    const n = r.carried[newName]
    const why = r.not_carried.find(x => x.function === newName)
    const outcome = n !== undefined
      ? `results carried (${n} record(s), nothing re-run)`
      : why ? `needs a run: ${why.reason}` : ''
    lines.push(`${oldName} → ${newName}${outcome ? ` — ${outcome}` : ''}`)
  }
  if (r.kept_labels.length) lines.push(`Kept as they are: ${r.kept_labels.join(', ')}`)
  if (r.step_values.length) {
    lines.push(
      `Step values no Parameter feeds (recipients set them per placement): ${r.step_values.join(', ')}`,
    )
  }
  if (r.dependencies.length) lines.push(`Depends on: ${r.dependencies.join(', ')}`)
  return lines
}

/** A default name for ✎ Make my own copy: `<lib>_mine`. */
export function suggestCopyName(library: string): string {
  return `${library}_mine`
}

/** The same rule the backend applies first (scidb.project.validate_project_name):
 *  a Python identifier, lower-case by convention. */
export function isLibraryName(name: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name.trim())
}
