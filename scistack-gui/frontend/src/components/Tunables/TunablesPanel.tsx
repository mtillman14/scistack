/**
 * The tunable values of ONE placement of a library pipeline
 * (.claude/plan-library-fork-chain.md E1, E3, E5): its Parameters and
 * PathInputs (the project's values, read-only here), and its step values
 * (this placement's binding: edit, or make a Parameter).
 *
 * Reads `placement_tunables`, writes `set_step_value` / `promote_step_value`;
 * the backend owns every value (`services/library_service`). Rows are
 * `TunableRow`, shared with the project-wide view (`ProjectTunables`).
 */

import { useCallback, useEffect, useState } from 'react'
import { callBackend } from '../../api'
import TunableRow from './TunableRow'
import { tunableSummary, type Tunables } from './tunables'

interface Props {
  useId: string
  /** Already-fetched data (the project view passes its row). */
  initial?: Tunables
  onChanged?: () => void
}

export default function TunablesPanel({ useId, initial, onChanged }: Props) {
  const [data, setData] = useState<Tunables | null>(initial ?? null)
  const [error, setError] = useState('')

  const fetchData = useCallback(() => {
    callBackend('placement_tunables', { use_id: useId })
      .then(d => { setData(d as Tunables); setError('') })
      .catch(err => setError((err as Error).message))
  }, [useId])

  useEffect(() => { if (!initial) fetchData() }, [fetchData, initial])
  useEffect(() => { if (initial) setData(initial) }, [initial])

  const write = (method: string, params: Record<string, unknown>) => {
    callBackend(method, { use_id: useId, ...params })
      .then(d => { setData(d as Tunables); setError(''); onChanged?.() })
      .catch(err => setError((err as Error).message))
  }

  if (!data) return error ? <div style={styles.error}>{error}</div> : <div style={styles.muted}>…</div>
  return (
    <div style={styles.panel} onClick={e => e.stopPropagation()}>
      <div style={styles.summary}>{tunableSummary(data)}</div>
      {data.parameters.map(p => <TunableRow key={`p:${p.name}`} kind="parameter" item={p} />)}
      {data.path_inputs.map(p => <TunableRow key={`pi:${p.name}`} kind="path_input" item={p} />)}
      {data.step_values.map(sv => (
        <TunableRow
          key={`sv:${sv.key}`}
          kind="step_value"
          item={sv}
          onSet={(key, value) => write('set_step_value', { key, value })}
          onPromote={(key, parameter) => write('promote_step_value', { key, parameter })}
        />
      ))}
      {Object.keys(data.key_map).length > 0 && (
        <div style={styles.muted}>
          schema keys: {Object.entries(data.key_map).map(([k, v]) => `${k}→${v}`).join(', ')}
        </div>
      )}
      {error && <div style={styles.error}>{error}</div>}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  panel: {
    marginTop: 6, padding: 6, background: '#1a1a2e', border: '1px solid #3a3a5a', borderRadius: 4,
    color: '#ccc', cursor: 'default', minWidth: 240,
  },
  summary: { fontSize: 10, color: '#999', marginBottom: 4 },
  muted: { fontSize: 10, color: '#888' },
  error: { fontSize: 10, color: '#f87171', wordBreak: 'break-word' },
}
