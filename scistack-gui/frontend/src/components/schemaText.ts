/**
 * The one-text-box form of a schema declaration (React-free, so `npm test`
 * runs it): commas within a branch, semicolons between branches.
 *
 *     brand, category, item; brand, location, person
 *
 * No semicolon is one branch, so what people type today is unchanged. This is
 * only the SPLIT. Every rule (a key with two parents, a repeated key, an empty
 * branch) is `scifor.schema.SchemaTree.from_paths`, which the backend runs on
 * what is sent; its message is shown when the backend refuses. The Python
 * twin of this file is `scifor.parse_schema_text`; the shared cases are in
 * docs/claude/schema-branch-cases.json.
 */

/** `"a, b; a, c"` -> `[["a","b"],["a","c"]]`. Blank keys are dropped; an
 *  empty branch (`"a; ;b"`) is kept as `[]` so the backend can refuse it by
 *  name. Empty text is no branches. */
export function parseSchemaText(text: string): string[][] {
  if (!text.trim()) return []
  return text.split(';').map(part =>
    part.split(',').map(k => k.trim()).filter(Boolean),
  )
}

/** The inverse of `parseSchemaText`. */
export function formatSchemaText(branches: string[][]): string {
  return branches.map(branch => branch.join(', ')).join('; ')
}

/** What the backend takes: a plain list for one branch, else one list per
 *  branch (so today's single-branch requests are byte-for-byte unchanged). */
export function declarationOf(branches: string[][]): string[] | string[][] {
  return branches.length === 1 ? branches[0] : branches
}

/** Every key once, in first-appearance order (the flat key order). */
export function flatKeys(branches: string[][]): string[] {
  const seen: string[] = []
  for (const branch of branches) for (const key of branch) if (!seen.includes(key)) seen.push(key)
  return seen
}

/** One line for a live validation hint: the parsed branches. */
export function describeBranches(branches: string[][]): string {
  if (branches.length === 0) return ''
  if (branches.length === 1) return `keys: ${branches[0].join(' → ')}`
  return `${branches.length} branches: ` + branches.map(b => b.join(' → ')).join('  |  ')
}
