/**
 * ⇪ on a submodule row: "Share as library…" (.claude/plan-portability.md
 * Stage 10b; .claude/plan-library-fork-chain.md R1/R2).
 *
 * No folder is asked: the backend (services/library_authoring.py) writes the
 * library to <project>/libs/<name>/ and switches the project to it — the
 * pipeline is now the library's (same id, still editable here), its
 * functions are named <name>.fn, and results whose code did not change are
 * carried over under the new names. On a pipeline of a library this project
 * AUTHORS the same button PUBLISHES: it writes the canvas (and any project
 * code it now uses) into that library. The terminal user is shown the
 * `pip install -e` command for their own scripts.
 */

import { useState } from 'react'
import { callBackend } from '../../api'
import { isLibraryName, reportLines, type AuthoringReport } from './libraryReport'

interface Defaults {
  name: string
  folder: string
  pipeline: string
  publish: boolean
}

export default function ShareLibraryButton({ pipelineId }: { pipelineId: string }) {
  const [open, setOpen] = useState<null | { top: number; left: number }>(null)
  const [defaults, setDefaults] = useState<Defaults | null>(null)
  const [name, setName] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [report, setReport] = useState<AuthoringReport | null>(null)

  const show = (e: React.MouseEvent<HTMLButtonElement>) => {
    e.stopPropagation()
    const r = e.currentTarget.getBoundingClientRect()
    setError('')
    setReport(null)
    callBackend('share_library_defaults', { pipeline_id: pipelineId })
      .then(d => {
        const s = d as Defaults
        setDefaults(s)
        setName(s.name)
        setOpen({ top: r.bottom + 4, left: Math.max(8, r.left - 200) })
      })
      .catch(err => window.alert((err as Error).message))
  }

  const share = () => {
    setBusy(true)
    setError('')
    callBackend('share_as_library', { pipeline_id: pipelineId, name: name.trim() })
      .then(d => setReport((d as { report: AuthoringReport }).report))
      .catch(err => setError((err as Error).message))
      .finally(() => setBusy(false))
  }

  const publish = defaults?.publish ?? false
  const folder = defaults ? defaults.folder.replace(/[^/\\]+$/, name.trim() || defaults.name) : ''

  return (
    <>
      <button style={styles.rowBtn} title={publish ? 'Publish to its library…' : 'Share as library…'} onClick={show} type="button">
        ⇪
      </button>
      {open && (
        <div style={{ ...styles.popover, top: open.top, left: open.left }} onClick={e => e.stopPropagation()}>
          <div style={styles.title}>{publish ? `Publish to ${defaults?.name}` : 'Share as library'}</div>
          {!report && !publish && (
            <>
              <label style={styles.label}>Library name (its import name)</label>
              <input style={styles.input} value={name} onChange={e => setName(e.target.value)} disabled={busy} />
              <div style={styles.note}>
                Written to <code>{folder}</code>. This project then uses it: the pipeline, the code it
                runs (Python and MATLAB), its Variables and its Parameters / PathInputs as defaults.
                Results whose code is unchanged carry over under the new names.
              </div>
            </>
          )}
          {!report && publish && (
            <div style={styles.note}>
              Writes this canvas into <code>{defaults?.folder}</code>, with any project code it now
              uses (renamed into the library). Code already in the library is never overwritten.
            </div>
          )}
          {report && (
            <div style={styles.report}>
              {reportLines(report).map((line, i) => <div key={i}>{line}</div>)}
              {report.warnings.map((w, i) => <div key={`w${i}`} style={styles.warn}>{w}</div>)}
              <div style={{ marginTop: 6 }}>To use it from your own scripts:</div>
              <code style={styles.code}>{report.install}</code>
            </div>
          )}
          {error && <div style={styles.error}>{error}</div>}
          <div style={styles.actions}>
            <button style={styles.btn} onClick={() => setOpen(null)} type="button">Close</button>
            {!report && (
              <button
                style={styles.primary}
                onClick={share}
                disabled={busy || (!publish && !isLibraryName(name))}
                type="button"
              >
                {busy ? (publish ? 'Publishing…' : 'Sharing…') : (publish ? 'Publish' : 'Share')}
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
