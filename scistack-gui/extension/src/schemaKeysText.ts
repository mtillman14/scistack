/**
 * The schema keys text box of the "Create database" and bundle-import prompts
 * (vscode-free, so `npm test` runs it).
 *
 * One text box can only hold one string, so a branched schema is written with
 * commas within a branch and semicolons between branches:
 *
 *     brand, category, item; brand, location, person
 *
 * No semicolon is one branch, so what people type today is unchanged. The
 * rules are `scifor.schema.SchemaTree.from_paths` (the owner, which the server
 * runs on the result). A prompt has no server to ask, so the three mistakes a
 * typist can make are re-checked here for the live validation line, and
 * `schemaKeysText.test.ts` holds this copy to the shared refused cases in
 * docs/claude/schema-branch-cases.json.
 */

export interface ParsedSchema {
  /** One list per branch; empty when the text holds no keys. */
  branches: string[][]
  /** Why the text is not a valid declaration, or null. */
  error: string | null
}

export function parseSchemaText(text: string): ParsedSchema {
  const trimmed = text.trim()
  if (!trimmed) return { branches: [], error: 'Provide at least one schema key' }
  const branches = trimmed.split(';').map(part =>
    part.split(',').map(k => k.trim()).filter(Boolean),
  )
  const parent = new Map<string, string | null>()
  for (const branch of branches) {
    if (branch.length === 0) return { branches, error: 'A schema key branch cannot be empty.' }
    const seen = new Set<string>()
    let previous: string | null = null
    for (const key of branch) {
      if (seen.has(key)) {
        return { branches, error: `Schema key '${key}' is repeated within the branch ${branch.join(', ')}.` }
      }
      seen.add(key)
      if (parent.has(key) && parent.get(key) !== previous) {
        return {
          branches,
          error:
            `Schema key '${key}' has two different parents: '${parent.get(key)}' and ` +
            `'${previous}'. A key sits under one parent.`,
        }
      }
      if (!parent.has(key)) parent.set(key, previous)
      previous = key
    }
  }
  return { branches, error: null }
}

/** The text form (the inverse of `parseSchemaText`). */
export function formatSchemaText(branches: string[][]): string {
  return branches.map(b => b.join(', ')).join('; ')
}

/** Every key once, in first-appearance order. */
export function flatKeys(branches: string[][]): string[] {
  const out: string[] = []
  for (const b of branches) for (const k of b) if (!out.includes(k)) out.push(k)
  return out
}

/** The `--schema-keys` argument: no spaces, semicolons between branches. */
export function schemaArg(branches: string[][]): string {
  return branches.map(b => b.join(',')).join(';')
}

/** One line for the prompt's live validation: the parsed branches. */
export function describeSchema(branches: string[][]): string {
  if (branches.length === 1) return `${branches[0].length} key(s): ${branches[0].join(' → ')}`
  return `${branches.length} branches: ` + branches.map(b => b.join(' → ')).join('  |  ')
}

/** What a create-database call sends: a plain list for one branch, else one
 *  list per branch (so a one-branch schema is exactly what it always was). */
export type SchemaDeclaration = string[] | string[][]

export function declarationOf(branches: string[][]): SchemaDeclaration {
  return branches.length === 1 ? branches[0] : branches
}

/** The `--schema-keys` value of either form. */
export function declarationArg(d: SchemaDeclaration): string {
  return isBranched(d) ? schemaArg(d) : d.join(',')
}

/** A readable one-liner of either form, for logs. */
export function declarationText(d: SchemaDeclaration): string {
  return isBranched(d) ? formatSchemaText(d) : d.join(', ')
}

function isBranched(d: SchemaDeclaration): d is string[][] {
  return d.length > 0 && Array.isArray(d[0])
}
