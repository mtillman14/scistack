/**
 * MergeSettingsPanel — a selected Merge node's settings
 * (docs/claude/merge-node.md).
 *
 * The name is the Merge's identity (set when the node is placed). The join
 * method `how` is stored on the node (config `mergeHow`) and is what the
 * next run from the canvas uses. The choices come from the node data, which
 * the backend fills from scifor.merge.MERGE_HOWS (the one list).
 */

import { useState } from 'react'
import { callBackend } from '../../api'
import { useScope } from '../../context/ScopeContext'
import PanelHeader from './PanelHeader'

/** What each join does with a location present on one side only. */
const HOW_HELP: Record<string, string> = {
  exact: 'Every location on both sides, or an error (nothing dropped).',
  inner: 'Only locations on all sides; one-sided rows are dropped.',
  outer: 'Every location; missing values are NaN.',
  left: 'Every location of the first input; gaps NaN, extra rows dropped.',
  right: 'Every location of the last input; gaps NaN, extra rows dropped.',
}

interface Props {
  id: string
  label: string
  how: string
  hows: string[]
  constituents: string[]
  recordedHow?: string
  conflict: boolean
}

export default function MergeSettingsPanel({ id, label, how, hows, constituents, recordedHow, conflict }: Props) {
  const { bumpGraph } = useScope()
  const [value, setValue] = useState(how)
  const [error, setError] = useState('')
  const choices = hows.length ? hows : Object.keys(HOW_HELP)

  const save = (next: string) => {
    setValue(next)
    setError('')
    callBackend('put_node_config', { node_id: id, config: { mergeHow: next } })
      .then(() => bumpGraph())
      .catch(err => {
        setValue(how)
        setError((err as Error).message)
      })
  }

  return (
    <div>
      <PanelHeader kind="merge" name={label} />
      <div style={styles.section}>
        <label style={styles.fieldLabel} htmlFor={`merge-how-${id}`}>Join (how)</label>
        <select id={`merge-how-${id}`} style={styles.select} value={value} onChange={e => save(e.target.value)}>
          {choices.map(h => (
            <option key={h} value={h}>{h}</option>
          ))}
        </select>
        <div style={styles.help}>{HOW_HELP[value] ?? ''}</div>
        {value !== 'exact' && (
          <div style={styles.help}>Dropped and NaN-padded rows are logged as warnings in the run log.</div>
        )}
        {recordedHow && recordedHow !== value && (
          <div style={styles.warn}>History ran how={recordedHow}; the next run uses how={value} (a new variant).</div>
        )}
        {conflict && (
          <div style={styles.warn}>History recorded this name with two different definitions.</div>
        )}
        {error && <div style={styles.warn}>{error}</div>}
      </div>
      <div style={styles.section}>
        <div style={styles.fieldLabel}>Inputs (join order)</div>
        <div style={styles.help}>
          Wire variables into the node&apos;s ports, top to bottom. The joined table goes to the
          function argument its output is wired to.
        </div>
        {constituents.length > 0 && (
          <ol style={styles.list}>
            {constituents.map((c, i) => <li key={`${c}-${i}`}>{c}</li>)}
          </ol>
        )}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  section: { padding: '8px 12px', borderBottom: '1px solid #333' },
  fieldLabel: { display: 'block', fontSize: 12, fontWeight: 600, marginBottom: 4 },
  select: { width: '100%', fontSize: 12, padding: '3px 4px' },
  help: { fontSize: 11, color: '#9ca3af', marginTop: 4 },
  warn: { fontSize: 11, color: '#fbbf24', marginTop: 4 },
  list: { fontSize: 12, margin: '4px 0 0 18px', padding: 0 },
}
