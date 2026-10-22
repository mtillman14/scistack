/**
 * Schema-key types — the "Numeric" checkbox in the PathInput panel's Schema
 * Keys section (.claude/plan-schema-key-types-gui.md Stage 5).
 *
 * The DATABASE owns the declaration (`scidb.schema_types`); the backend
 * serves it through `get_schema_key_types` and writes it through
 * `set_schema_key_type`. Nothing here decides a type or converts a value —
 * these are pure view helpers (checkbox state, the confirm text), so they
 * can be tested without React.
 *
 * The setting is PROJECT-WIDE (one per schema key, not per PathInput): every
 * PathInput panel shows the same state for `trial`.
 */

export type SchemaKeyType = 'numeric' | 'string'

export interface SchemaKeyTypeInfo {
  key: string
  key_type: SchemaKeyType | null
  /** How the key's stored values load today (declared, else inferred). */
  loads_as: 'numbers' | 'strings'
  n_values: number
}

export interface SchemaKeyTypesResponse {
  schema_keys: string[]
  key_types: Record<string, SchemaKeyType>
  keys: SchemaKeyTypeInfo[]
}

export interface SchemaKeyTypeImpact {
  key: string
  from: SchemaKeyType | null
  to: SchemaKeyType | null
  n_values: number
  respelled: [string, string][]
  merged: Record<string, string[]>
  invalid: string[]
}

export interface SetSchemaKeyTypeResult {
  ok: boolean
  needs_confirm?: boolean
  error?: string
  impact?: SchemaKeyTypeImpact
  key_types?: Record<string, SchemaKeyType>
}

/** What `update_path_input` reports after a digit-only key_regex save. */
export interface KeyRegexDeclarations {
  declared: string[]
  hints: { key: string; reason: string }[]
}

export interface NumericCheckbox {
  /** False for a placeholder that is not a schema key: no checkbox at all. */
  shown: boolean
  checked: boolean
  disabled: boolean
  title: string
}

/** The checkbox for *key*: shown only for schema keys; a declared `string`
 * is shown read-only (it is set by hand, `scidb set-key-type`). */
export function numericCheckbox(
  info: SchemaKeyTypeInfo | undefined,
  locked: boolean,
): NumericCheckbox {
  if (!info) return { shown: false, checked: false, disabled: true, title: '' }
  const base =
    `Project-wide: applies to '${info.key}' everywhere, not just this PathInput. ` +
    `Stored in the database.`
  if (info.key_type === 'string') {
    return {
      shown: true,
      checked: false,
      disabled: true,
      title:
        `'${info.key}' is declared string: spelling is identity and files match exactly. ` +
        `Change it with \`scidb set-key-type ${info.key} none\`. ${base}`,
    }
  }
  const now =
    info.key_type === 'numeric'
      ? `'${info.key}' is numeric: "001", "1" and 1 are one value.`
      : `'${info.key}' is undeclared: values are kept as spelled (loads as ${info.loads_as}).`
  return { shown: true, checked: info.key_type === 'numeric', disabled: locked, title: `${now} ${base}` }
}

const PREVIEW = 3

/** The confirm text for a change that re-spells stored values. */
export function impactSummary(impact: SchemaKeyTypeImpact): string {
  const parts: string[] = []
  if (impact.respelled.length) {
    const shown = impact.respelled
      .slice(0, PREVIEW)
      .map(([from, to]) => `'${from}' → '${to}'`)
      .join(', ')
    const more = impact.respelled.length > PREVIEW ? ', …' : ''
    parts.push(
      `${impact.respelled.length} of ${impact.n_values} stored value(s) will be spelled differently by new saves (${shown}${more}).`,
    )
  }
  const merged = Object.entries(impact.merged)
  if (merged.length) {
    const shown = merged
      .slice(0, PREVIEW)
      .map(([to, olds]) => `${olds.map(o => `'${o}'`).join(' + ')} → '${to}'`)
      .join('; ')
    parts.push(`${merged.length} location(s) will merge (${shown}).`)
  }
  parts.push(
    `Rows already saved keep their old spelling, so re-run the functions that save '${impact.key}' afterwards.`,
  )
  return parts.join(' ')
}

/** The note shown after a key-pattern save that declared keys numeric. */
export function declaredNote(declared: string[]): string {
  if (!declared.length) return ''
  const names = declared.map(k => `'${k}'`).join(', ')
  return `${names} declared numeric: the pattern only matches digits. Untick Numeric to undo.`
}
