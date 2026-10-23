/**
 * One row of a tunables panel (.claude/plan-library-fork-chain.md O26): the
 * ONE component both the placed node's panel and the project-wide view use.
 *
 * A Parameter or PathInput row is read-only here: its value is the
 * project's (edit it in the sidebar, where the project's other uses see the
 * same value). A step value row edits THIS placement's binding, or offers
 * "Make this a Parameter" (E5).
 */

import { useState } from 'react'
import {
  formatValue,
  isChanged,
  parseStepValue,
  suggestParameterName,
  type StepValue,
  type TunableParameter,
  type TunablePathInput,
} from './tunables'

type Props =
  | { kind: 'parameter'; item: TunableParameter }
  | { kind: 'path_input'; item: TunablePathInput }
  | {
      kind: 'step_value'
      item: StepValue
      onSet: (key: string, value: unknown) => void
      onPromote: (key: string, parameter: string) => void
    }

export default function TunableRow(props: Props) {
  const [draft, setDraft] = useState<string | null>(null)

  if (props.kind === 'parameter') {
    const p = props.item
    return (
      <div style={styles.row} title="A project Parameter: edit it in the sidebar">
        <span style={styles.name}>{p.name}</span>
        <span style={styles.value}>{formatValue(p.value)}</span>
        <span style={p.source === 'missing' ? styles.warn : styles.source}>{p.source}</span>
      </div>
    )
  }
  if (props.kind === 'path_input') {
    const p = props.item
    return (
      <div style={styles.row} title="A project PathInput: set its root folder in the sidebar">
        <span style={styles.name}>{p.name}</span>
        <span style={styles.value}>{p.template ?? '—'}</span>
        <span style={p.needs_root ? styles.warn : styles.source}>
          {p.needs_root ? 'needs a root folder' : p.root_folder}
        </span>
      </div>
    )
  }

  const sv = props.item
  const commit = () => {
    if (draft === null) return
    props.onSet(sv.key, parseStepValue(draft, sv.value))
    setDraft(null)
  }
  return (
    <div style={styles.row}>
      <span style={styles.name} title={sv.key}>{sv.key.split('.').slice(-2).join('.')}</span>
      {sv.parameter ? (
        <span style={styles.value} title={`Follows the project Parameter ${sv.parameter}`}>
          ⟨{sv.parameter}⟩ = {formatValue(sv.value)}
        </span>
      ) : (
        <input
          style={{ ...styles.input, ...(isChanged(sv) ? styles.changed : {}) }}
          value={draft ?? formatValue(sv.value)}
          onChange={e => setDraft(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') commit()
            if (e.key === 'Escape') setDraft(null)
          }}
          onBlur={commit}
          title={`Default: ${formatValue(sv.default)}`}
        />
      )}
      {!sv.parameter && (
        <button
          style={styles.btn}
          type="button"
          title="Make this a Parameter (a project-level value other steps can share)"
          onClick={() => {
            const name = window.prompt('New Parameter name', suggestParameterName(sv.key))
            if (name && name.trim()) props.onPromote(sv.key, name.trim())
          }}
        >
          P
        </button>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  row: { display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, padding: '2px 0' },
  name: { flex: '0 0 40%', fontFamily: 'monospace', color: '#ccc', overflow: 'hidden', textOverflow: 'ellipsis' },
  value: { flex: 1, fontFamily: 'monospace', color: '#ddd', overflow: 'hidden', textOverflow: 'ellipsis' },
  source: { color: '#888', fontSize: 10 },
  warn: { color: '#fbbf24', fontSize: 10 },
  input: {
    flex: 1, minWidth: 0, background: '#111127', border: '1px solid #3a3a5a', borderRadius: 3,
    color: '#ddd', fontSize: 11, padding: '2px 4px', fontFamily: 'monospace', outline: 'none',
  },
  changed: { borderColor: '#7b68ee' },
  btn: {
    background: 'transparent', border: '1px solid #3a3a5a', borderRadius: 3, color: '#aaa',
    cursor: 'pointer', fontSize: 10, padding: '1px 5px',
  },
}
