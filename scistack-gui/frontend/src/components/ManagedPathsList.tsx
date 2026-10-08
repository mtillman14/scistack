/**
 * ManagedPathsList — the editable path list of the project's scistack.toml,
 * rendered inside PathsPopup.tsx (every project; 2026-10-08).
 *
 * Each entry is a directory the GUI recursively discovers Python (.py)
 * and MATLAB (.m) code under — typically an external, reusable
 * computational-code repository shared across projects, not necessarily
 * anything inside the project's own folder. Backed by
 * add_project_path/remove_project_path, which write/rewrite scistack.toml.
 */

import { useState } from 'react'
import { callBackend } from '../api'

interface PathMutationResult {
  ok: boolean
  error?: string
}

export default function ManagedPathsList({
  paths,
  onChange,
}: {
  paths: string[]
  onChange: () => void
}) {
  const [selected, setSelected] = useState<string | null>(null)
  const [adding, setAdding] = useState(false)
  const [newPath, setNewPath] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const startAdd = () => {
    setAdding(true)
    setNewPath('')
    setError(null)
  }

  const cancelAdd = () => {
    setAdding(false)
    setNewPath('')
    setError(null)
  }

  const submitAdd = async () => {
    const value = newPath.trim()
    if (!value) return
    setBusy(true)
    setError(null)
    try {
      const result = await callBackend('add_project_path', { path: value }) as PathMutationResult
      if (!result.ok) {
        setError(result.error || 'Failed to add path.')
        return
      }
      setAdding(false)
      setNewPath('')
      onChange()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  const handleRemove = async () => {
    if (!selected) return
    if (!window.confirm(`Stop discovering code under:\n${selected}\n\nThis only edits scistack.toml — no files are deleted.`)) {
      return
    }
    setBusy(true)
    setError(null)
    try {
      const result = await callBackend('remove_project_path', { path: selected }) as PathMutationResult
      if (!result.ok) {
        setError(result.error || 'Failed to remove path.')
        return
      }
      setSelected(null)
      onChange()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div>
      <div style={styles.toolbar}>
        <button
          style={styles.toolbarBtn}
          onClick={startAdd}
          disabled={busy || adding}
          title="Add a path"
          type="button"
        >
          +
        </button>
        <button
          style={styles.toolbarBtn}
          onClick={handleRemove}
          disabled={busy || !selected}
          title="Remove selected path"
          type="button"
        >
          −
        </button>
      </div>

      {adding && (
        <div style={styles.addRow}>
          <input
            style={styles.input}
            type="text"
            placeholder="/absolute/path/to/folder"
            value={newPath}
            onChange={e => setNewPath(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter') submitAdd()
              if (e.key === 'Escape') cancelAdd()
            }}
            disabled={busy}
            autoFocus
          />
          <button style={styles.smallBtn} onClick={submitAdd} disabled={busy || !newPath.trim()} type="button">
            {busy ? 'Adding…' : 'Add'}
          </button>
          <button style={styles.smallBtn} onClick={cancelAdd} disabled={busy} type="button">
            Cancel
          </button>
        </div>
      )}

      {error && <div style={styles.errorBanner}>{error}</div>}

      <div style={styles.list}>
        {paths.length === 0 ? (
          <div style={styles.emptyText}>No paths configured yet.</div>
        ) : (
          paths.map(p => (
            <div
              key={p}
              style={{
                ...styles.row,
                ...(p === selected ? styles.rowSelected : {}),
              }}
              onClick={() => setSelected(p === selected ? null : p)}
              role="button"
              tabIndex={0}
            >
              {p}
            </div>
          ))
        )}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  toolbar: {
    display: 'flex',
    gap: 6,
    marginBottom: 8,
  },
  toolbarBtn: {
    width: 26,
    height: 26,
    background: '#2a2a5a',
    color: '#ccc',
    border: '1px solid #3a3a6a',
    borderRadius: 4,
    fontSize: 15,
    lineHeight: 1,
    cursor: 'pointer',
  },
  addRow: {
    display: 'flex',
    gap: 6,
    marginBottom: 8,
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
  },
  errorBanner: {
    background: '#442222',
    color: '#ff8888',
    padding: '6px 10px',
    borderRadius: 4,
    marginBottom: 8,
    fontSize: 12,
  },
  list: {
    maxHeight: 160,
    overflowY: 'auto',
    background: '#0e0e20',
    border: '1px solid #2a2a4a',
    borderRadius: 4,
  },
  row: {
    padding: '5px 10px',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#ccc',
    cursor: 'pointer',
    wordBreak: 'break-all',
    borderBottom: '1px solid #1a1a2e',
  },
  rowSelected: {
    background: '#2a2a5a',
    color: '#fff',
  },
  emptyText: {
    color: '#666',
    fontSize: 12,
    fontStyle: 'italic',
    padding: '8px 10px',
  },
}
