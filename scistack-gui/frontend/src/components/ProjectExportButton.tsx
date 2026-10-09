/**
 * "Export project…" — the whole project as a .scistack bundle
 * (.claude/plan-portability.md Stage 8).
 *
 * The checkboxes, their labels and their defaults all come from the backend
 * (`get_export_options` → `bundle_cli.export_choices` → `ExportOptions`), so
 * this dialog and `scistack export`'s flags can never disagree about what is
 * offered or what is on by default. The file is written by the backend
 * (`export_project_bundle`, the same composition `scistack export` uses) to
 * a path chosen in the host's save dialog (VS Code) or typed (browser).
 *
 * Importing a bundle makes a NEW project, so it is not here: it is the
 * extension's "SciStack: Import Project Bundle…" command.
 */

import { useState } from 'react'
import { callBackend, isVSCodeMode } from '../api'

interface ExportChoice {
  name: string
  flag: string
  label: string
  default: boolean
}

interface ExportOptionsResponse {
  choices: ExportChoice[]
  extension: string
  default_name: string
}

interface ExportResult {
  path: string
  bytes: number
  options: Record<string, boolean>
}

export default function ProjectExportButton() {
  const [open, setOpen] = useState<null | { top: number; left: number }>(null)
  const [spec, setSpec] = useState<ExportOptionsResponse | null>(null)
  const [values, setValues] = useState<Record<string, boolean>>({})
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')

  const show = (e: React.MouseEvent<HTMLButtonElement>) => {
    const r = e.currentTarget.getBoundingClientRect()
    setError('')
    setMessage('')
    callBackend('get_export_options')
      .then(d => {
        const s = d as ExportOptionsResponse
        setSpec(s)
        setValues(Object.fromEntries(s.choices.map(c => [c.name, c.default])))
        setOpen({ top: r.bottom + 4, left: Math.max(8, r.right - 340) })
      })
      .catch(err => setError((err as Error).message))
  }

  const doExport = async () => {
    if (!spec) return
    let path: string | null
    if (isVSCodeMode) {
      const picked = await callBackend('pick_save_path', {
        defaultName: spec.default_name,
        formats: [spec.extension.replace(/^\./, '')],
        filterName: 'SciStack project bundle',
      })
      path = (picked as { path: string | null }).path
    } else {
      path = window.prompt('Save the project bundle as (absolute path):', spec.default_name)
    }
    if (!path) return  // cancelled
    setBusy(true)
    setError('')
    setMessage('Exporting…')
    try {
      const r = await callBackend('export_project_bundle', { out_path: path, options: values }) as ExportResult
      const kb = Math.max(1, Math.round(r.bytes / 1024))
      setMessage(`Exported to ${r.path} (${kb.toLocaleString()} KB)`)
    } catch (err) {
      setMessage('')
      setError((err as Error).message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <>
      <button
        style={styles.trigger}
        onClick={e => (open ? setOpen(null) : show(e))}
        title="Export the whole project (code, config, canvas, history; data optional) as one .scistack file"
        type="button"
      >
        ⇩ export project
      </button>
      {error && !open && <span style={styles.errorText}>{error}</span>}
      {open && spec && (
        <div style={{ ...styles.popover, top: open.top, left: open.left }}>
          <div style={styles.title}>Export project</div>
          <div style={styles.note}>
            Always included: code, config, canvas, saved plots. Raw input files never are.
          </div>
          {spec.choices.map(c => (
            <label key={c.name} style={styles.row}>
              <input
                type="checkbox"
                checked={values[c.name] ?? c.default}
                disabled={busy}
                onChange={e => setValues(v => ({ ...v, [c.name]: e.target.checked }))}
              />
              <span>{c.label}</span>
            </label>
          ))}
          {message && <div style={styles.message}>{message}</div>}
          {error && <div style={styles.errorBlock}>{error}</div>}
          <div style={styles.actions}>
            <button style={styles.btn} onClick={() => setOpen(null)} type="button">
              Close
            </button>
            <button style={styles.primary} onClick={doExport} disabled={busy} type="button">
              {busy ? 'Exporting…' : 'Export…'}
            </button>
          </div>
        </div>
      )}
    </>
  )
}

const styles: Record<string, React.CSSProperties> = {
  trigger: {
    padding: '8px 10px',
    background: 'transparent',
    border: 'none',
    color: '#666',
    fontSize: 12,
    cursor: 'pointer',
    whiteSpace: 'nowrap',
  },
  popover: {
    position: 'fixed',
    zIndex: 1000,
    width: 340,
    background: '#1a1a2e',
    border: '1px solid #3a3a5a',
    borderRadius: 6,
    padding: 12,
    boxShadow: '0 6px 20px rgba(0,0,0,0.5)',
    color: '#ccc',
    fontSize: 12,
  },
  title: { fontWeight: 700, fontSize: 13, color: '#fff', marginBottom: 6 },
  note: { color: '#888', fontSize: 11, marginBottom: 8 },
  row: { display: 'flex', alignItems: 'flex-start', gap: 6, margin: '6px 0', cursor: 'pointer' },
  message: { marginTop: 8, color: '#9ae6b4', wordBreak: 'break-all' },
  errorBlock: { marginTop: 8, color: '#f87171', wordBreak: 'break-word' },
  errorText: { fontSize: 11, color: '#f87171', marginLeft: 8 },
  actions: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 10 },
  btn: {
    background: 'transparent',
    border: '1px solid #3a3a5a',
    borderRadius: 3,
    color: '#ccc',
    padding: '4px 10px',
    cursor: 'pointer',
  },
  primary: {
    background: '#7b68ee',
    border: 'none',
    borderRadius: 3,
    color: '#fff',
    padding: '4px 12px',
    cursor: 'pointer',
  },
}
