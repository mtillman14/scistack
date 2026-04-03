/**
 * FunctionSettingsPanel — shown in the sidebar when a function node is selected.
 *
 * Displays known variants from the DB and a form for running with custom constants.
 */

import { useState, useCallback } from 'react'
import { useRunLog } from '../../context/RunLogContext'
import { useWebSocket } from '../../hooks/useWebSocket'
import { useRef } from 'react'

interface FnVariant {
  constants: Record<string, unknown>
  input_types: Record<string, string>
  output_type: string
  record_count: number
}

interface Props {
  label: string
  variants: FnVariant[]
}

interface KVRow {
  key: string
  value: string
}

export default function FunctionSettingsPanel({ label, variants }: Props) {
  // Deduplicate variants by constants fingerprint (same fn may appear for multiple output types)
  const seen = new Set<string>()
  const uniqueVariants = variants.filter(v => {
    const fp = JSON.stringify(Object.entries(v.constants).sort())
    if (seen.has(fp)) return false
    seen.add(fp)
    return true
  })

  const [rows, setRows] = useState<KVRow[]>([{ key: '', value: '' }])
  const [running, setRunning] = useState(false)
  const { startRun, appendLine, finishRun } = useRunLog()
  const runIdRef = useRef<string | null>(null)

  useWebSocket(useCallback((msg) => {
    if (msg.run_id !== runIdRef.current) return
    if (msg.type === 'run_output') appendLine(msg.run_id as string, msg.text as string)
    else if (msg.type === 'run_done') {
      finishRun(msg.run_id as string)
      setRunning(false)
    }
  }, [appendLine, finishRun]))

  const updateRow = (i: number, field: 'key' | 'value', val: string) => {
    setRows(prev => prev.map((r, idx) => idx === i ? { ...r, [field]: val } : r))
  }

  const addRow = () => setRows(prev => [...prev, { key: '', value: '' }])

  const removeRow = (i: number) => {
    setRows(prev => prev.length === 1 ? [{ key: '', value: '' }] : prev.filter((_, idx) => idx !== i))
  }

  const handleRun = useCallback(async () => {
    const constants: Record<string, string> = {}
    for (const { key, value } of rows) {
      const k = key.trim()
      const v = value.trim()
      if (k) constants[k] = v
    }

    const newRunId = Math.random().toString(36).slice(2, 10)
    runIdRef.current = newRunId
    setRunning(true)
    startRun(newRunId, label)

    await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        function_name: label,
        variants: Object.keys(constants).length > 0 ? [constants] : [],
        run_id: newRunId,
      }),
    })
  }, [label, rows, startRun])

  // Pre-fill rows from a known variant
  const prefill = (v: FnVariant) => {
    const entries = Object.entries(v.constants)
    if (entries.length === 0) {
      setRows([{ key: '', value: '' }])
    } else {
      setRows(entries.map(([k, val]) => ({ key: k, value: String(val) })))
    }
  }

  return (
    <div style={styles.root}>
      <div style={styles.fnName}>{label}</div>

      {uniqueVariants.length > 0 && (
        <section style={styles.section}>
          <div style={styles.sectionTitle}>Known Variants</div>
          <table style={styles.table}>
            <tbody>
              {uniqueVariants.map((v, i) => {
                const desc = Object.entries(v.constants).length === 0
                  ? <em style={{ opacity: 0.5 }}>no constants</em>
                  : Object.entries(v.constants).map(([k, val]) => (
                      <span key={k} style={styles.pill}>{k}={String(val)}</span>
                    ))
                return (
                  <tr key={i} style={styles.variantRow}>
                    <td style={styles.variantDesc}>{desc}</td>
                    <td style={styles.variantCount}>{v.record_count} rec</td>
                    <td style={styles.variantAction}>
                      <button style={styles.prefillBtn} onClick={() => prefill(v)}>
                        Use
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </section>
      )}

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Run with Constants</div>
        <div style={styles.kvList}>
          {rows.map((row, i) => (
            <div key={i} style={styles.kvRow}>
              <input
                style={styles.input}
                placeholder="key"
                value={row.key}
                onChange={e => updateRow(i, 'key', e.target.value)}
              />
              <span style={styles.eq}>=</span>
              <input
                style={styles.input}
                placeholder="value"
                value={row.value}
                onChange={e => updateRow(i, 'value', e.target.value)}
              />
              <button style={styles.removeBtn} onClick={() => removeRow(i)}>×</button>
            </div>
          ))}
          <button style={styles.addBtn} onClick={addRow}>+ add row</button>
        </div>

        <button
          style={running ? styles.runBtnDisabled : styles.runBtn}
          onClick={handleRun}
          disabled={running}
        >
          {running ? '⏳ Running…' : '▶ Run'}
        </button>
      </section>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '12px',
    color: '#ccc',
    fontSize: 12,
  },
  fnName: {
    fontFamily: 'monospace',
    fontWeight: 700,
    fontSize: 13,
    color: '#a89cf0',
    marginBottom: 12,
    wordBreak: 'break-all',
  },
  section: {
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 10,
    fontWeight: 700,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    marginBottom: 6,
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
  },
  variantRow: {
    borderBottom: '1px solid #1e1e3a',
  },
  variantDesc: {
    padding: '4px 0',
    verticalAlign: 'middle',
  },
  variantCount: {
    padding: '4px 4px',
    color: '#666',
    whiteSpace: 'nowrap',
    verticalAlign: 'middle',
  },
  variantAction: {
    padding: '4px 0',
    textAlign: 'right',
    verticalAlign: 'middle',
  },
  pill: {
    display: 'inline-block',
    background: '#1e1e3a',
    borderRadius: 3,
    padding: '1px 5px',
    marginRight: 4,
    fontFamily: 'monospace',
    fontSize: 11,
  },
  prefillBtn: {
    background: 'transparent',
    border: '1px solid #444',
    borderRadius: 3,
    color: '#aaa',
    fontSize: 10,
    padding: '2px 6px',
    cursor: 'pointer',
  },
  kvList: {
    marginBottom: 8,
  },
  kvRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 4,
    marginBottom: 4,
  },
  input: {
    flex: 1,
    background: '#1a1a2e',
    border: '1px solid #333',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 11,
    padding: '3px 6px',
    minWidth: 0,
  },
  eq: {
    color: '#666',
    fontSize: 11,
  },
  removeBtn: {
    background: 'transparent',
    border: 'none',
    color: '#666',
    cursor: 'pointer',
    fontSize: 14,
    padding: '0 2px',
    lineHeight: 1,
  },
  addBtn: {
    background: 'transparent',
    border: 'none',
    color: '#7b68ee',
    fontSize: 11,
    cursor: 'pointer',
    padding: '2px 0',
  },
  runBtn: {
    width: '100%',
    padding: '6px 0',
    background: '#7b68ee',
    color: '#fff',
    border: 'none',
    borderRadius: 4,
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: 12,
    marginTop: 4,
  },
  runBtnDisabled: {
    width: '100%',
    padding: '6px 0',
    background: '#4a3fa0',
    color: '#aaa',
    border: 'none',
    borderRadius: 4,
    cursor: 'not-allowed',
    fontWeight: 600,
    fontSize: 12,
    marginTop: 4,
  },
}
