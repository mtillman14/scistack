/**
 * ⇪ on a submodule row: "Share as library…" (.claude/plan-portability.md
 * Stage 10b).
 *
 * The backend writes a new library package (services/library_share.py:
 * the submodule as a pipeline document, the Python and MATLAB code it runs,
 * its Variables, and its Parameters / PathInputs as defaults). Nothing is
 * installed: the report ends with the pip command to run, after which the
 * library can be listed (Libraries → +) in this or any project.
 */

import { useState } from 'react'
import { callBackend } from '../../api'

interface ShareReport {
  library: string
  dest: string
  pipeline: string
  files: string[]
  functions: Record<string, string>
  kept_labels: string[]
  variables: string[]
  parameters: string[]
  path_inputs: string[]
  dependencies: string[]
  rewritten_imports: string[]
  qualified_calls: string[]
  warnings: string[]
  install: string
}

export default function ShareLibraryButton({ pipelineId }: { pipelineId: string }) {
  const [open, setOpen] = useState<null | { top: number; left: number }>(null)
  const [name, setName] = useState('')
  const [dest, setDest] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [report, setReport] = useState<ShareReport | null>(null)

  const show = (e: React.MouseEvent<HTMLButtonElement>) => {
    e.stopPropagation()
    const r = e.currentTarget.getBoundingClientRect()
    setError('')
    setReport(null)
    callBackend('share_library_defaults', { pipeline_id: pipelineId })
      .then(d => {
        const s = d as { name: string; dest: string }
        setName(s.name)
        setDest(s.dest)
        setOpen({ top: r.bottom + 4, left: Math.max(8, r.left - 200) })
      })
      .catch(err => window.alert((err as Error).message))
  }

  const share = () => {
    setBusy(true)
    setError('')
    callBackend('share_as_library', { pipeline_id: pipelineId, name: name.trim(), dest: dest.trim() })
      .then(d => setReport(d as ShareReport))
      .catch(err => setError((err as Error).message))
      .finally(() => setBusy(false))
  }

  return (
    <>
      <button style={styles.rowBtn} title="Share as library…" onClick={show} type="button">
        ⇪
      </button>
      {open && (
        <div style={{ ...styles.popover, top: open.top, left: open.left }} onClick={e => e.stopPropagation()}>
          <div style={styles.title}>Share as library</div>
          {!report && (
            <>
              <label style={styles.label}>Library name (its import name)</label>
              <input style={styles.input} value={name} onChange={e => setName(e.target.value)} disabled={busy} />
              <label style={styles.label}>New folder (absolute path; must be empty or new)</label>
              <input style={styles.input} value={dest} onChange={e => setDest(e.target.value)} disabled={busy} />
              <div style={styles.note}>
                Writes the submodule, the code it runs (Python and MATLAB), its Variables, and its
                Parameters / PathInputs as defaults. Nothing is installed.
              </div>
            </>
          )}
          {report && (
            <div style={styles.report}>
              <div>Wrote <b>{report.library}</b> ({report.files.length} files) to {report.dest}</div>
              <div>Functions: {Object.values(report.functions).join(', ') || '—'}</div>
              {report.kept_labels.length > 0 && <div>From other libraries: {report.kept_labels.join(', ')}</div>}
              {(report.parameters.length + report.path_inputs.length) > 0 && (
                <div>Defaults for: {[...report.parameters, ...report.path_inputs].join(', ')}</div>
              )}
              {report.dependencies.length > 0 && <div>Depends on: {report.dependencies.join(', ')}</div>}
              {report.warnings.map((w, i) => <div key={i} style={styles.warn}>{w}</div>)}
              <div style={{ marginTop: 6 }}>Install it, then list it under Libraries:</div>
              <code style={styles.code}>{report.install}</code>
            </div>
          )}
          {error && <div style={styles.error}>{error}</div>}
          <div style={styles.actions}>
            <button style={styles.btn} onClick={() => setOpen(null)} type="button">Close</button>
            {!report && (
              <button style={styles.primary} onClick={share} disabled={busy || !name.trim() || !dest.trim()} type="button">
                {busy ? 'Sharing…' : 'Share'}
              </button>
            )}
          </div>
        </div>
      )}
    </>
  )
}

const styles: Record<string, React.CSSProperties> = {
  rowBtn: { background: 'transparent', border: 'none', color: '#888', cursor: 'pointer', fontSize: 12 },
  popover: {
    position: 'fixed', zIndex: 1000, width: 380, background: '#1a1a2e', border: '1px solid #3a3a5a',
    borderRadius: 6, padding: 12, boxShadow: '0 6px 20px rgba(0,0,0,0.5)', color: '#ccc', fontSize: 12,
    cursor: 'default',
  },
  title: { fontWeight: 700, fontSize: 13, color: '#fff', marginBottom: 6 },
  label: { display: 'block', color: '#999', fontSize: 11, marginTop: 6 },
  input: {
    width: '100%', boxSizing: 'border-box', background: '#111127', border: '1px solid #3a3a5a',
    borderRadius: 3, color: '#ddd', fontSize: 12, padding: '4px 6px', outline: 'none',
  },
  note: { color: '#888', fontSize: 11, marginTop: 8 },
  report: { display: 'flex', flexDirection: 'column', gap: 3, wordBreak: 'break-word' },
  warn: { color: '#fbbf24' },
  code: {
    display: 'block', background: '#111127', padding: 6, borderRadius: 3, fontSize: 11,
    wordBreak: 'break-all', userSelect: 'all',
  },
  error: { marginTop: 8, color: '#f87171', wordBreak: 'break-word' },
  actions: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 10 },
  btn: { background: 'transparent', border: '1px solid #3a3a5a', borderRadius: 3, color: '#ccc', padding: '4px 10px', cursor: 'pointer' },
  primary: { background: '#7b68ee', border: 'none', borderRadius: 3, color: '#fff', padding: '4px 12px', cursor: 'pointer' },
}
