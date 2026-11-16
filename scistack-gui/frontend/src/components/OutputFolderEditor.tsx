/**
 * OutputFolderEditor — the project's output folder (`[outputs] folder` in
 * scistack.toml), rendered inside PathsPopup.tsx.
 *
 * Every relative output path a node is given (a plot_ node's figure file,
 * e.g. `plots/step_length_{subject}.png`) is written under this folder, so
 * moving every figure is one edit here, not one per node. Unset, relative
 * output paths stay relative to the project root. The meaning is
 * scifor.project.outputs'; the backend reports `resolved` and any `problem`
 * (a machine root this machine doesn't map), and this only shows them.
 */

import { useState } from 'react'
import { callBackend } from '../api'

export interface OutputFolderInfo {
  /** `[outputs] folder` as written, or null when unset. */
  entry: string | null
  /** The folder on this machine (the project root when unset), or null. */
  resolved: string | null
  /** Why `entry` names nothing on this machine, or null. */
  problem: string | null
}

interface MutationResult {
  ok: boolean
  error?: string
}

export default function OutputFolderEditor({
  info,
  onChange,
}: {
  info: OutputFolderInfo | null
  onChange: () => void
}) {
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const save = async (path: string | null) => {
    setBusy(true)
    setError(null)
    try {
      const result = await callBackend('set_output_folder', { path }) as MutationResult
      if (!result.ok) {
        setError(result.error || 'Failed to set the output folder.')
        return
      }
      setEditing(false)
      onChange()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  const entry = info?.entry ?? null
  return (
    <div style={styles.wrap}>
      <div style={styles.label}>Output folder</div>
      <div style={styles.hint}>
        Where nodes write their output files (figures from plot_ nodes). Each node keeps only its own
        relative path, so changing this moves every future output at once. Relative to the project
        root, absolute, or a machine root like <span style={styles.mono}>{'{results}/aim2'}</span>.
        Files already written stay where they are until their node runs again.
      </div>
      {!editing ? (
        <div style={styles.row}>
          <div style={styles.value}>
            {entry ?? '(not set — the project root)'}
            {entry && info?.resolved && info.resolved !== entry && (
              <div style={styles.resolved}>{info.resolved}</div>
            )}
          </div>
          <button
            style={styles.smallBtn}
            onClick={() => { setDraft(entry ?? ''); setEditing(true); setError(null) }}
            disabled={busy}
            type="button"
          >
            {entry ? 'Change' : 'Set'}
          </button>
          {entry && (
            <button style={styles.smallBtn} onClick={() => save(null)} disabled={busy} type="button">
              Clear
            </button>
          )}
        </div>
      ) : (
        <div style={styles.row}>
          <input
            style={styles.input}
            type="text"
            placeholder="outputs"
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter') save(draft.trim() || null)
              if (e.key === 'Escape') setEditing(false)
            }}
            disabled={busy}
            autoFocus
          />
          <button style={styles.smallBtn} onClick={() => save(draft.trim() || null)} disabled={busy} type="button">
            {busy ? 'Saving…' : 'Save'}
          </button>
          <button style={styles.smallBtn} onClick={() => setEditing(false)} disabled={busy} type="button">
            Cancel
          </button>
        </div>
      )}
      {info?.problem && <div style={styles.errorBanner}>{info.problem}</div>}
      {error && <div style={styles.errorBanner}>{error}</div>}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  wrap: { marginTop: 12 },
  label: {
    fontSize: 10,
    fontWeight: 700,
    color: '#7b68ee',
    textTransform: 'uppercase',
    letterSpacing: 0.6,
    marginBottom: 3,
  },
  hint: { fontSize: 11, color: '#666', lineHeight: 1.5, marginBottom: 6 },
  mono: { fontFamily: 'monospace' },
  row: { display: 'flex', alignItems: 'center', gap: 6 },
  value: {
    flex: 1,
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#ccc',
    wordBreak: 'break-all',
    background: '#1a1a2e',
    border: '1px solid #2a2a4a',
    borderRadius: 4,
    padding: '5px 8px',
  },
  resolved: { color: '#777', marginTop: 2 },
  input: {
    flex: 1,
    background: '#0e0e20',
    color: '#ccc',
    border: '1px solid #3a3a5a',
    borderRadius: 4,
    padding: '4px 8px',
    fontSize: 12,
    fontFamily: 'monospace',
  },
  smallBtn: {
    background: '#2a2a5a',
    color: '#ccc',
    border: '1px solid #3a3a6a',
    borderRadius: 4,
    padding: '4px 10px',
    fontSize: 12,
    cursor: 'pointer',
    whiteSpace: 'nowrap',
  },
  errorBanner: {
    background: '#442222',
    color: '#ff8888',
    padding: '6px 10px',
    borderRadius: 4,
    marginTop: 6,
    fontSize: 12,
  },
}
