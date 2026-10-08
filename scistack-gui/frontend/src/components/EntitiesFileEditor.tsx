/**
 * EntitiesFileEditor — the TOML file new Variable/Parameter/PathInput
 * declarations created from the sidebar's "+" buttons are written to.
 * Rendered inside PathsPopup.tsx, for every project.
 *
 * Backed by set_entities_file/clear_entities_file, which write/rewrite
 * scistack.toml. Leaving the field blank and hitting "Set" auto-creates a
 * default src/scistack_entities.toml **in the project root** -- which is
 * not necessarily where the database lives (that is usually a datasets
 * folder; see config.resolve_project_root). Same default the
 * project-creation wizard pre-fills eagerly (ProjectBootstrapWizard.tsx)
 * and what happens transparently the first time an entity gets created
 * from the sidebar with nothing configured yet, so this editor is mainly
 * for visibility/override rather than a required setup step.
 */

import { useState } from 'react'
import { callBackend } from '../api'

interface MutationResult {
  ok: boolean
  error?: string
}

export default function EntitiesFileEditor({
  entitiesFile,
  onChange,
}: {
  entitiesFile: string | null
  onChange: () => void
}) {
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const startEdit = () => {
    setDraft(entitiesFile ?? '')
    setEditing(true)
    setError(null)
  }

  const cancelEdit = () => {
    setEditing(false)
    setError(null)
  }

  const submitSet = async () => {
    setBusy(true)
    setError(null)
    try {
      const path = draft.trim() || null
      const result = await callBackend('set_entities_file', { path }) as MutationResult
      if (!result.ok) {
        setError(result.error || 'Failed to set entities file.')
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

  const handleClear = async () => {
    if (!window.confirm('Stop targeting this file for new declarations?\n\nThis only edits scistack.toml — the file itself is left untouched.')) {
      return
    }
    setBusy(true)
    setError(null)
    try {
      const result = await callBackend('clear_entities_file') as MutationResult
      if (!result.ok) {
        setError(result.error || 'Failed to clear entities file.')
        return
      }
      onChange()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div style={styles.wrap}>
      <div style={styles.label}>Entities file</div>
      <div style={styles.hint}>
        New Variable/Parameter/PathInput declarations created from the sidebar are written here,
        as TOML. Left unset, one is auto-created in the project root the first time it's needed.
      </div>
      {!editing ? (
        <div style={styles.row}>
          <div style={styles.value}>{entitiesFile ?? '(not set — auto-creates on first use)'}</div>
          <button style={styles.smallBtn} onClick={startEdit} disabled={busy} type="button">
            {entitiesFile ? 'Change' : 'Set'}
          </button>
          {entitiesFile && (
            <button style={styles.smallBtn} onClick={handleClear} disabled={busy} type="button">
              Clear
            </button>
          )}
        </div>
      ) : (
        <div style={styles.addRow}>
          <input
            style={styles.input}
            type="text"
            placeholder="src/scistack_entities.toml (blank = auto default)"
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter') submitSet()
              if (e.key === 'Escape') cancelEdit()
            }}
            disabled={busy}
            autoFocus
          />
          <button style={styles.smallBtn} onClick={submitSet} disabled={busy} type="button">
            {busy ? 'Saving…' : 'Save'}
          </button>
          <button style={styles.smallBtn} onClick={cancelEdit} disabled={busy} type="button">
            Cancel
          </button>
        </div>
      )}
      {error && <div style={styles.errorBanner}>{error}</div>}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  wrap: {
    marginTop: 12,
  },
  label: {
    fontSize: 10,
    fontWeight: 700,
    color: '#7b68ee',
    textTransform: 'uppercase',
    letterSpacing: 0.6,
    marginBottom: 3,
  },
  hint: {
    fontSize: 11,
    color: '#666',
    lineHeight: 1.5,
    marginBottom: 6,
  },
  row: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
  },
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
  addRow: {
    display: 'flex',
    gap: 6,
  },
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
