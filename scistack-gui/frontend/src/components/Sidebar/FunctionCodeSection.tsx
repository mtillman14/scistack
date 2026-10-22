/**
 * FunctionCodeSection — the body of a function created in the GUI, edited
 * here (backend: services/function_code_service.py).
 *
 * A function made with the functions tab's "+ New function" is one file in
 * the project's functions directory (`src/scistack_functions/`), found by
 * ordinary discovery. This is where the user writes it; the file is
 * persistence, as for a glue node (GlueSettingsPanel). Functions that live
 * anywhere else are the user's own modules and are edited in their editor,
 * so this section shows nothing for them.
 *
 * Saving rewrites the file and reloads it; a body that no longer imports is
 * rolled back and the error is shown. A new body is a new function hash, so
 * the step's next run recomputes.
 */

import { useCallback, useEffect, useState } from 'react'
import { callBackend, isVSCodeMode } from '../../api'
import GlueCodeEditor from './GlueCodeEditor'

interface FunctionSource {
  ok: boolean
  error?: string
  source?: string
  path?: string
  editable?: boolean
}

export default function FunctionCodeSection({ label }: { label: string }) {
  const [info, setInfo] = useState<FunctionSource | null>(null)
  const [draft, setDraft] = useState('')
  const [dirty, setDirty] = useState(false)
  const [saving, setSaving] = useState(false)
  const [status, setStatus] = useState('')

  useEffect(() => {
    setStatus('')
    setDirty(false)
    callBackend('get_function_code', { name: label })
      .then(d => {
        const res = d as FunctionSource
        setInfo(res)
        setDraft(res.source ?? '')
      })
      .catch(() => setInfo(null))
  }, [label])

  const save = useCallback(() => {
    if (!dirty || saving || !info?.editable) return
    setSaving(true)
    setStatus('Saving…')
    callBackend('save_function_code', { name: label, source: draft })
      .then(d => {
        const res = d as { ok: boolean; error?: string }
        if (res.ok) {
          setDirty(false)
          setStatus('Saved — this step recomputes on its next run.')
        } else {
          setStatus(res.error ?? 'Save failed.')
        }
      })
      .catch(err => setStatus((err as Error).message))
      .finally(() => setSaving(false))
  }, [dirty, saving, info, label, draft])

  if (!info?.ok || !info.editable) return null

  return (
    <div style={styles.root}>
      <div style={styles.title}>Code</div>
      <GlueCodeEditor
        value={draft}
        language="python"
        onChange={next => { setDraft(next); setDirty(true); setStatus('') }}
        onBlur={save}
      />
      <div style={styles.actions}>
        <button style={styles.saveBtn} onClick={save} disabled={!dirty || saving} type="button">
          {saving ? 'Saving…' : 'Save'}
        </button>
        {isVSCodeMode && info.path && (
          <button
            style={styles.linkBtn}
            onClick={() => callBackend('reveal_in_editor', { file: info.path, line: 1 }).catch(console.error)}
            type="button"
            title="Open the real file — available, never required"
          >
            Open in editor
          </button>
        )}
        {status && <span style={styles.status}>{status}</span>}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: { padding: 12, display: 'flex', flexDirection: 'column', gap: 8 },
  title: { fontWeight: 600, fontSize: 13 },
  actions: { display: 'flex', alignItems: 'center', gap: 8 },
  saveBtn: {
    fontSize: 12,
    padding: '3px 10px',
    borderRadius: 4,
    border: '1px solid #7b68ee',
    background: '#7b68ee',
    color: '#fff',
    cursor: 'pointer',
  },
  linkBtn: {
    fontSize: 11,
    background: 'transparent',
    border: 'none',
    color: '#7b68ee',
    cursor: 'pointer',
    padding: 0,
  },
  status: { fontSize: 11, color: '#6b7280' },
}
