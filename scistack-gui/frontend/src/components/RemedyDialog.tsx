/**
 * RemedyDialog — a popup for a failure that carries a remedy: what went
 * wrong, where, and the ways to fix it. Opened by NodeFailuresContext when a
 * run ends with such a failure, or from the FailuresPanel's "How to fix…".
 *
 * Every word comes from the exception (e.g. scidb.DuplicateLocationError
 * .remedy); this component only lays it out and runs the step buttons.
 * See docs/claude/duplicate-location-vs-stale.md and error-surfacing.md.
 */

import { useState } from 'react'
import { callBackend, isVSCodeMode } from '../api'
import { useNodeFailures } from '../context/NodeFailuresContext'
import type { RemedyStep } from './DAG/runFailures'
import { dialog, dialogTitle, overlay } from './modalStyles'

function StepCard({ step, fnName }: { step: RemedyStep; fnName: string }) {
  const [note, setNote] = useState<string | null>(null)
  const openFunction = async () => {
    // eslint-disable-next-line no-console
    console.debug(`[remedy] action=reveal_function fn=${fnName}`)
    try {
      const src = await callBackend('get_function_source', { name: fnName }) as {
        ok: boolean; file?: string; line?: number; error?: string
      }
      if (!src.ok) {
        setNote(`Could not locate the source of ${fnName}: ${src.error ?? 'unknown error'}`)
        return
      }
      if (!isVSCodeMode) {
        setNote(`${src.file}:${src.line}`)
        return
      }
      const r = await callBackend('reveal_in_editor', { file: src.file, line: src.line }) as { ok?: boolean; error?: string }
      if (r && r.ok === false) setNote(`${src.file}:${src.line} (${r.error ?? 'could not open'})`)
    } catch (err) {
      setNote(`Could not open ${fnName}: ${String(err)}`)
    }
  }
  return (
    <div style={styles.step}>
      <div style={styles.stepTitle}>{step.title}</div>
      <div style={styles.text}>{step.text}</div>
      {step.action === 'reveal_function' && fnName && (
        <button type="button" style={styles.button} onClick={openFunction}>
          Open {fnName}
        </button>
      )}
      {note && <div style={styles.note}>{note}</div>}
    </div>
  )
}

export default function RemedyDialog() {
  const { remedy: target, closeRemedy, openPanel } = useNodeFailures()
  const remedy = target?.group.remedy
  if (!target || !remedy) return null
  const showFailures = () => {
    closeRemedy()
    openPanel(target.nodeId)
  }
  const withColumns = remedy.items.some(i => i.differing_columns.length > 0)
  return (
    <div style={overlay} onClick={closeRemedy}>
      <div style={{ ...dialog, ...styles.panel }} onClick={e => e.stopPropagation()}>
        <div style={dialogTitle}>{remedy.title}</div>
        <div style={styles.text}>{remedy.explanation}</div>
        {remedy.items.length > 0 && (
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.th}>Location</th>
                <th style={styles.th}>Rows</th>
                {withColumns && <th style={styles.th}>Columns that differ</th>}
              </tr>
            </thead>
            <tbody>
              {remedy.items.map((item, i) => (
                <tr key={i}>
                  <td style={styles.tdMono}>{item.location}</td>
                  <td style={styles.td}>{item.count}</td>
                  {withColumns && <td style={styles.tdMono}>{item.differing_columns.join(', ')}</td>}
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {remedy.n_more > 0 && <div style={styles.note}>+{remedy.n_more} more location(s)</div>}
        <div style={styles.howTo}>How to fix it</div>
        {remedy.steps.map((step, i) => (
          <StepCard key={i} step={step} fnName={target.group.fn_name} />
        ))}
        <div style={styles.footer}>
          <button type="button" style={styles.button} onClick={showFailures}>Show failures</button>
          <button type="button" style={styles.button} onClick={closeRemedy}>Close</button>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  panel: { width: 'min(640px, 92vw)', borderTop: '3px solid #f59e0b' },
  text: { fontSize: 13, lineHeight: 1.45, margin: '4px 0 8px' },
  table: { borderCollapse: 'collapse', fontSize: 12, margin: '4px 0 8px', width: '100%' },
  th: { textAlign: 'left', color: '#aaa', fontWeight: 600, borderBottom: '1px solid #333', padding: '2px 8px 2px 0' },
  td: { padding: '2px 8px 2px 0' },
  tdMono: { padding: '2px 8px 2px 0', fontFamily: 'monospace', userSelect: 'text' },
  howTo: { fontSize: 13, fontWeight: 700, margin: '12px 0 6px' },
  step: {
    border: '1px solid #2f3b55',
    borderRadius: 4,
    padding: '8px 10px',
    marginBottom: 8,
    background: '#1e2440',
  },
  stepTitle: { fontWeight: 600, fontSize: 13, color: '#fde68a' },
  note: { fontSize: 11, color: '#aaa', fontFamily: 'monospace', userSelect: 'text', marginTop: 4 },
  footer: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 10 },
  button: {
    background: '#2a2a4a',
    color: '#eee',
    border: '1px solid #444',
    borderRadius: 4,
    padding: '4px 12px',
    cursor: 'pointer',
  },
}
