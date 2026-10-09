/**
 * Libraries, inside the Submodules category (.claude/plan-portability.md
 * Stage 10a).
 *
 * A library is a package listed under `packages` in scistack.toml. Its
 * shared pipelines are seeded by the backend as LIBRARY-OWNED, read-only
 * pipelines (services/library_service.py); here each one is a row you drag
 * onto the canvas to place it (PipelineDAG's drop handler asks
 * `suggest_library_key_map` first when the payload says `library`), or
 * click to look inside. Editing inside one is refused by the backend
 * (library_lock), with the message shown wherever the edit was made.
 *
 * Adding a library only LISTS it: SciStack never installs anything. A name
 * that is not importable comes back with the backend's hint.
 */

import { useCallback, useEffect, useState } from 'react'
import { callBackend } from '../../api'
import { useBackendMessage } from '../../hooks/useBackendMessage'

export interface LibraryPipelineRow {
  name: string
  pipeline_id: string
  seeded: boolean
}

export interface LibraryRow {
  library: string
  schema_keys: string[] | null
  matlab: boolean
  pipelines: LibraryPipelineRow[]
  errors: string[]
}

interface Props {
  currentScope: string
  /** Every library-owned ROOT pipeline id, so the plain submodule list can
   *  leave them out (they are listed here instead). */
  onLibraryPipelineIds: (ids: Set<string>) => void
  onOpen: (pipelineId: string, name: string) => void
}

export default function LibrariesSection({ currentScope, onLibraryPipelineIds, onOpen }: Props) {
  const [libraries, setLibraries] = useState<LibraryRow[]>([])
  const [adding, setAdding] = useState(false)
  const [draft, setDraft] = useState('')
  const [error, setError] = useState('')
  const [hint, setHint] = useState('')

  const apply = useCallback((rows: LibraryRow[]) => {
    setLibraries(rows)
    onLibraryPipelineIds(new Set(rows.flatMap(l => l.pipelines.map(p => p.pipeline_id))))
  }, [onLibraryPipelineIds])

  const fetchLibraries = useCallback(() => {
    callBackend('list_libraries')
      .then(d => apply((d as { libraries: LibraryRow[] }).libraries))
      .catch(err => setError((err as Error).message))
  }, [apply])

  useEffect(() => { fetchLibraries() }, [fetchLibraries])
  useBackendMessage(useCallback((msg: Record<string, unknown>) => {
    if (msg.type === 'dag_updated' || msg.method === 'dag_updated') fetchLibraries()
  }, [fetchLibraries]))

  const commitAdd = () => {
    const name = draft.trim()
    setAdding(false)
    setDraft('')
    if (!name) return
    callBackend('add_library', { name })
      .then(d => {
        const r = d as { libraries: LibraryRow[]; hint?: string }
        apply(r.libraries)
        setError('')
        setHint(r.hint ?? '')
      })
      .catch(err => setError((err as Error).message))
  }

  const remove = (name: string) => {
    if (!window.confirm(`Stop using the library '${name}'? Its pipelines stay on your canvases.`)) return
    callBackend('remove_library', { name })
      .then(d => { apply((d as { libraries: LibraryRow[] }).libraries); setError(''); setHint('') })
      .catch(err => setError((err as Error).message))
  }

  const makeCopy = (name: string) => {
    if (!window.confirm(
      `Make your own copy of '${name}'? Its code is copied into this project, every `
      + `placement of it switches to the copy (names stay ${name}.fn, so history stays `
      + `current), its pipelines become editable, and it is no longer a library here.`
    )) return
    callBackend('make_library_copy', { name })
      .then(d => {
        const r = d as { libraries: LibraryRow[]; report: { files: string[]; warnings: string[]; released_pipelines: string[] } }
        apply(r.libraries)
        setError('')
        setHint(
          `Copied ${name}: ${r.report.files.length} file(s); editable now: `
          + `${r.report.released_pipelines.join(', ') || 'no pipelines'}.`
          + (r.report.warnings.length ? ` Warnings: ${r.report.warnings.join('; ')}` : '')
        )
      })
      .catch(err => setError((err as Error).message))
  }

  const onDragStart = (e: React.DragEvent, lib: LibraryRow, p: LibraryPipelineRow) => {
    e.dataTransfer.setData(
      'application/scistack-pipeline',
      JSON.stringify({ pipeline_id: p.pipeline_id, name: p.name, library: lib.library }),
    )
    e.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div style={styles.wrap}>
      <div style={styles.header}>
        <span>Libraries</span>
        <button style={styles.addBtn} onClick={() => setAdding(true)} title="Use a library (list an installed package)">
          +
        </button>
      </div>
      {adding && (
        <input
          autoFocus
          style={styles.input}
          value={draft}
          placeholder="package import name…"
          onChange={e => setDraft(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') commitAdd()
            if (e.key === 'Escape') { setAdding(false); setDraft('') }
          }}
          onBlur={commitAdd}
        />
      )}
      {libraries.length === 0 && !adding && (
        <div style={styles.empty}>No libraries. + lists an installed package.</div>
      )}
      {libraries.map(lib => (
        <div key={lib.library} style={styles.library}>
          <div style={styles.libRow}>
            <span style={{ flex: 1 }} title={lib.schema_keys ? `schema: ${lib.schema_keys.join(', ')}` : 'declares no schema keys'}>
              📦 {lib.library}{lib.matlab ? ' (MATLAB)' : ''}
            </span>
            <button style={styles.rowBtn} onClick={() => makeCopy(lib.library)} title="Make my own copy (edit it in this project)">
              ✎
            </button>
            <button style={styles.rowBtn} onClick={() => remove(lib.library)} title="Stop using this library">
              ×
            </button>
          </div>
          {lib.pipelines.length === 0 && <div style={styles.empty}>no shared pipelines</div>}
          {lib.pipelines.map(p => (
            <div
              key={p.pipeline_id}
              draggable={p.seeded}
              onDragStart={e => onDragStart(e, lib, p)}
              onClick={() => p.seeded && onOpen(p.pipeline_id, p.name)}
              style={{
                ...styles.item,
                ...(p.pipeline_id === currentScope ? styles.current : {}),
                opacity: p.seeded ? 1 : 0.5,
              }}
              title={p.seeded
                ? 'Read-only (from the library). Drag onto the canvas to place; click to look inside.'
                : 'Not loaded yet: Refresh'}
            >
              🔒 ⧉ {p.name}
            </div>
          ))}
          {lib.errors.map((e, i) => (
            <div key={i} style={styles.errorText}>{e}</div>
          ))}
        </div>
      ))}
      {hint && <div style={styles.hint}>{hint}</div>}
      {error && <div style={styles.errorText}>{error}</div>}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  wrap: { marginTop: 12, borderTop: '1px solid #2a2a4a', paddingTop: 8 },
  header: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    fontSize: 10, fontWeight: 700, color: '#888', textTransform: 'uppercase', letterSpacing: 0.6,
    marginBottom: 4,
  },
  addBtn: { background: 'transparent', border: 'none', color: '#888', cursor: 'pointer', fontSize: 14 },
  input: {
    width: '100%', boxSizing: 'border-box', background: '#1a1a2e', border: '1px solid #7b68ee',
    borderRadius: 3, color: '#ccc', fontSize: 12, padding: '4px 6px', outline: 'none', marginBottom: 4,
  },
  library: { marginBottom: 6 },
  libRow: { display: 'flex', alignItems: 'center', fontSize: 12, color: '#bbb', padding: '2px 0' },
  item: {
    fontSize: 12, color: '#ccc', padding: '4px 8px', margin: '2px 0 2px 10px', cursor: 'pointer',
    borderLeft: '3px solid #64748b', background: '#16162c', borderRadius: 3,
  },
  current: { background: '#2a2a4a' },
  rowBtn: { background: 'transparent', border: 'none', color: '#777', cursor: 'pointer', fontSize: 12 },
  empty: { fontSize: 11, color: '#666', padding: '2px 10px' },
  hint: { fontSize: 11, color: '#fbbf24', padding: '4px 0', wordBreak: 'break-word' },
  errorText: { fontSize: 11, color: '#f87171', padding: '2px 0', wordBreak: 'break-word' },
}
