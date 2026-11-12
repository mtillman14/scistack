/**
 * DeleteEntityDialog — the palette's Delete confirm for a Variable,
 * Parameter or PathInput (.claude/plan-palette-order-and-delete.md).
 *
 * Opens with a PREVIEW (`preview_delete_entity`: every backend check, no
 * write), so the user sees exactly what will be removed from the entities
 * file before anything happens — or why it can't be:
 *
 * - placed on a canvas → the pipelines are listed, each a link that opens it
 *   (hidden pipelines are named but not linked); there is no Delete button;
 * - declared outside the entities file / file changed on disk → the
 *   backend's message, verbatim.
 *
 * Confirming calls `delete_entity`, which is an undoable handler: it lands on
 * the page's undo stack like any edit, and Undo restores the file byte for
 * byte. Saved data is never touched. A refusal at confirm time is shown
 * here, never swallowed.
 */

import { useEffect, useState } from 'react'
import { callBackend } from '../../api'

export interface DeleteTarget {
  kind: 'variable' | 'parameter' | 'path_input'
  name: string
}

interface Placement {
  pipeline_id: string
  pipeline_name: string
  hidden_pipeline: boolean
  node_ids: string[]
}

export interface DeleteResult {
  ok: boolean
  error?: string
  reason?: string
  file?: string
  removed?: string
  placements?: Placement[]
  still_declared_in?: string | null
}

const KIND_LABEL: Record<DeleteTarget['kind'], string> = {
  variable: 'Variable',
  parameter: 'Parameter',
  path_input: 'Path Input',
}

export default function DeleteEntityDialog({
  target,
  onClose,
  onDeleted,
  onOpenPipeline,
}: {
  target: DeleteTarget
  onClose: () => void
  onDeleted: (result: DeleteResult) => void
  onOpenPipeline: (pipelineId: string, name: string) => void
}) {
  const [preview, setPreview] = useState<DeleteResult | null>(null)
  const [busy, setBusy] = useState(false)
  const [confirmError, setConfirmError] = useState('')

  useEffect(() => {
    let cancelled = false
    setPreview(null)
    setConfirmError('')
    callBackend('preview_delete_entity', { kind: target.kind, name: target.name })
      .then(d => {
        console.debug('[DeleteEntityDialog] preview', target, d)
        if (!cancelled) setPreview(d as DeleteResult)
      })
      .catch(err => { if (!cancelled) setPreview({ ok: false, error: (err as Error).message }) })
    return () => { cancelled = true }
  }, [target.kind, target.name])

  const confirm = () => {
    setBusy(true)
    setConfirmError('')
    callBackend('delete_entity', { kind: target.kind, name: target.name })
      .then(d => {
        const r = d as DeleteResult
        console.info('[DeleteEntityDialog] delete', target, r)
        if (r.ok) onDeleted(r)
        else setConfirmError(r.error ?? 'Delete failed.')
      })
      .catch(err => setConfirmError((err as Error).message))
      .finally(() => setBusy(false))
  }

  const fileName = preview?.file ? preview.file.split(/[\\/]/).pop() : 'the entities file'

  return (
    <>
      <div style={styles.backdrop} onClick={onClose} />
      <div style={styles.dialog} role="dialog" aria-label={`Delete ${target.name}`}>
        <div style={styles.title}>Delete {KIND_LABEL[target.kind]} “{target.name}”?</div>

        {!preview && <div style={styles.muted}>Checking…</div>}

        {preview?.ok && (
          <>
            <div style={styles.text}>
              Removes this from <span style={styles.mono} title={preview.file}>{fileName}</span>:
            </div>
            <pre style={styles.removed}>{preview.removed}</pre>
            <div style={styles.text}>
              Saved data is kept — declaring the name again brings it back.
              Undo with Ctrl/Cmd+Z.
            </div>
          </>
        )}

        {preview && !preview.ok && preview.reason === 'placed' && (
          <>
            <div style={styles.text}>
              It is still on the canvas of these pipelines. Remove it from there
              first, then delete:
            </div>
            <ul style={styles.list}>
              {(preview.placements ?? []).map(p => (
                <li key={p.pipeline_id}>
                  {p.hidden_pipeline ? (
                    <span title="This pipeline is hidden: restore it from the Submodules tab">
                      {p.pipeline_name} (hidden)
                    </span>
                  ) : (
                    <button
                      type="button"
                      style={styles.link}
                      onClick={() => { onOpenPipeline(p.pipeline_id, p.pipeline_name); onClose() }}
                      title="Open this pipeline"
                    >
                      {p.pipeline_name}
                    </button>
                  )}
                  {p.node_ids.length > 1 && <span style={styles.muted}> ×{p.node_ids.length}</span>}
                </li>
              ))}
            </ul>
          </>
        )}

        {preview && !preview.ok && preview.reason !== 'placed' && (
          <div style={styles.error}>{preview.error}</div>
        )}

        {confirmError && <div style={styles.error}>{confirmError}</div>}

        <div style={styles.actions}>
          <button type="button" style={styles.cancelBtn} onClick={onClose}>
            {preview?.ok ? 'Cancel' : 'Close'}
          </button>
          {preview?.ok && (
            <button type="button" style={styles.deleteBtn} onClick={confirm} disabled={busy} data-testid="delete-declaration-confirm">
              {busy ? 'Deleting…' : 'Delete declaration'}
            </button>
          )}
        </div>
      </div>
    </>
  )
}

const styles: Record<string, React.CSSProperties> = {
  backdrop: { position: 'fixed', inset: 0, zIndex: 1100, background: 'rgba(0,0,0,0.4)' },
  dialog: {
    position: 'fixed',
    zIndex: 1101,
    top: '20%',
    left: '50%',
    transform: 'translateX(-50%)',
    width: 'min(440px, calc(100vw - 32px))',
    background: '#1a1a2e',
    border: '1px solid #3a3a5a',
    borderRadius: 6,
    boxShadow: '0 8px 24px rgba(0,0,0,0.5)',
    padding: 14,
    color: '#ddd',
    fontSize: 12,
  },
  title: { fontWeight: 700, fontSize: 13, marginBottom: 8 },
  text: { marginBottom: 6, lineHeight: 1.4 },
  muted: { color: '#888' },
  mono: { fontFamily: 'monospace' },
  removed: {
    fontFamily: 'monospace',
    fontSize: 11,
    background: '#11111f',
    border: '1px solid #333',
    borderRadius: 3,
    padding: '6px 8px',
    margin: '0 0 8px',
    whiteSpace: 'pre-wrap',
    color: '#f87171',
    maxHeight: 160,
    overflow: 'auto',
  },
  list: { margin: '0 0 8px', paddingLeft: 18 },
  link: {
    background: 'none',
    border: 'none',
    padding: 0,
    color: '#60a5fa',
    textDecoration: 'underline',
    cursor: 'pointer',
    fontSize: 12,
  },
  error: { color: '#f87171', marginBottom: 6 },
  actions: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 8 },
  cancelBtn: {
    background: 'transparent',
    border: '1px solid #444',
    color: '#ccc',
    borderRadius: 3,
    padding: '3px 10px',
    cursor: 'pointer',
  },
  deleteBtn: {
    background: '#7f1d1d',
    border: '1px solid #b91c1c',
    color: '#fee2e2',
    borderRadius: 3,
    padding: '3px 10px',
    cursor: 'pointer',
  },
}
