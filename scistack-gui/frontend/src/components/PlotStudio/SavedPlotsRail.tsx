/**
 * The "Saved plots" rail: the right-hand column of Plot Studio
 * (.claude/plan-saved-plots.md, Stage 4).
 *
 * Lists the variable's saved plots, saves the current one under a name, and
 * opens, renames or removes one. Every question is asked INLINE, because a
 * VS Code webview blocks `window.prompt` / `window.confirm`: the name box, "a
 * different plot already has that name", "discard your changes?", and
 * "remove?".
 *
 * Presentation only. What a name may be, whether it clashes, and what
 * "remove" does are the backend's answers (`scistackplotdb.saved`). The rail
 * shows them; it does not repeat them.
 */

import { useEffect, useRef, useState } from 'react'

import { notesSummary, savedAtLabel, type RestoreNote, type SavedPlotInfo } from './savedPlots'

export interface SaveResult {
  ok: boolean
  /** A DIFFERENT plot already has the name; ask before adding a version to it. */
  exists?: SavedPlotInfo
}

interface Props {
  collapsed: boolean
  onToggle: () => void
  plots: SavedPlotInfo[]
  /** The saved plot the panel has open, or null for an unsaved plot. */
  loaded: SavedPlotInfo | null
  /** Whether the panel differs from what was last opened or saved. */
  modified: boolean
  /** What the last open could not restore. */
  notes: RestoreNote[]
  busy: boolean
  error: string
  onSave: (name: string, overwrite: boolean) => Promise<SaveResult>
  onOpen: (plotId: string) => void
  onRename: (plotId: string, name: string) => Promise<boolean>
  onRemove: (plotId: string) => Promise<void>
  onDismissNotes: () => void
}

type Pending =
  | { kind: 'naming'; text: string }
  | { kind: 'replace'; name: string; existing: SavedPlotInfo }
  | { kind: 'discard'; plot: SavedPlotInfo }
  | { kind: 'renaming'; plotId: string; text: string }
  | { kind: 'removing'; plot: SavedPlotInfo }
  | null

export default function SavedPlotsRail({
  collapsed,
  onToggle,
  plots,
  loaded,
  modified,
  notes,
  busy,
  error,
  onSave,
  onOpen,
  onRename,
  onRemove,
  onDismissNotes,
}: Props) {
  const [pending, setPending] = useState<Pending>(null)
  const [showNotes, setShowNotes] = useState(false)
  // A different plot loaded: questions about the previous one no longer apply.
  useEffect(() => { setPending(null) }, [loaded?.plot_id])
  useEffect(() => { setShowNotes(false) }, [notes])

  if (collapsed) {
    return (
      <div style={styles.collapsed}>
        <button type="button" style={styles.headerButton} onClick={onToggle} title="Show saved plots">
          ❮
        </button>
        <div style={styles.verticalLabel} onClick={onToggle}>
          Saved plots{plots.length ? ` (${plots.length})` : ''}
        </div>
        {loaded && modified && <span style={styles.modifiedDot} title="Unsaved changes">●</span>}
      </div>
    )
  }

  const startSaving = () =>
    setPending({ kind: 'naming', text: loaded?.name ?? '' })

  const save = async (name: string, overwrite: boolean) => {
    const result = await onSave(name, overwrite)
    if (result.ok) setPending(null)
    else if (result.exists) setPending({ kind: 'replace', name, existing: result.exists })
    // Anything else is an error the parent shows; keep the box open to fix it.
  }

  const open = (plot: SavedPlotInfo) => {
    if (plot.plot_id === loaded?.plot_id && !modified) return
    if (modified) setPending({ kind: 'discard', plot })
    else onOpen(plot.plot_id)
  }

  return (
    <div style={styles.rail}>
      <div style={styles.header}>
        <span style={styles.title}>Saved plots</span>
        <button type="button" style={styles.headerButton} onClick={onToggle} title="Hide saved plots">
          ❯
        </button>
      </div>

      <div style={styles.body}>
        <div style={styles.current}>
          <div style={styles.currentName} title={loaded ? `${loaded.name} (version ${loaded.version})` : ''}>
            {loaded ? loaded.name : <span style={styles.unsaved}>Unsaved plot</span>}
            {loaded && modified && <span style={styles.modifiedDot} title="Unsaved changes"> ●</span>}
          </div>
          {loaded && (
            <div style={styles.meta}>
              version {loaded.version}{modified ? ' · modified' : ''}
            </div>
          )}
          {pending?.kind === 'naming' ? (
            <NameBox
              initial={pending.text}
              actionLabel="Save"
              busy={busy}
              onSubmit={name => save(name, false)}
              onCancel={() => setPending(null)}
            />
          ) : (
            <button type="button" style={styles.primaryButton} onClick={startSaving} disabled={busy}>
              {loaded ? 'Save…' : 'Save plot…'}
            </button>
          )}
          {pending?.kind === 'replace' && (
            <Question
              text={`"${pending.existing.name}" is another saved plot. Save this as its next version? Its earlier versions are kept.`}
              confirmLabel="Save as its version"
              onConfirm={() => save(pending.name, true)}
              onCancel={() => setPending({ kind: 'naming', text: pending.name })}
            />
          )}
        </div>

        {notes.length > 0 && (
          <div style={styles.notes}>
            <div>
              Restored with changes: {notesSummary(notes)}.{' '}
              <button type="button" style={styles.linkButton} onClick={() => setShowNotes(v => !v)}>
                {showNotes ? 'Hide' : 'Details'}
              </button>
              {' · '}
              <button type="button" style={styles.linkButton} onClick={onDismissNotes}>
                Dismiss
              </button>
            </div>
            {showNotes && (
              <ul style={styles.noteList}>
                {notes.map((note, i) => (
                  <li key={i}>
                    <code>{note.path || 'plot'}</code>: {note.message}
                  </li>
                ))}
              </ul>
            )}
            <div style={styles.noteHint}>Save to keep these settings in the current format.</div>
          </div>
        )}

        {error && <div style={styles.error}>{error}</div>}

        {pending?.kind === 'discard' && (
          <Question
            text={`Open "${pending.plot.name}"? Your unsaved changes${loaded ? ` to "${loaded.name}"` : ''} will be lost.`}
            confirmLabel="Open anyway"
            onConfirm={() => { const id = pending.plot.plot_id; setPending(null); onOpen(id) }}
            onCancel={() => setPending(null)}
          />
        )}

        {plots.length === 0 ? (
          <div style={styles.hint}>
            No saved plots for this variable yet. Save one to reopen it later
            exactly as it looks now.
          </div>
        ) : (
          <div style={styles.list}>
            {plots.map(plot => {
              const isLoaded = plot.plot_id === loaded?.plot_id
              if (pending?.kind === 'renaming' && pending.plotId === plot.plot_id) {
                return (
                  <div key={plot.plot_id} style={styles.row}>
                    <NameBox
                      initial={pending.text}
                      actionLabel="Rename"
                      busy={busy}
                      onSubmit={async name => {
                        if (await onRename(plot.plot_id, name)) setPending(null)
                      }}
                      onCancel={() => setPending(null)}
                    />
                  </div>
                )
              }
              return (
                <div key={plot.plot_id}>
                  <div
                    style={{ ...styles.row, ...(isLoaded ? styles.rowLoaded : null) }}
                    title={`Open "${plot.name}" (version ${plot.version})`}
                  >
                    <button type="button" style={styles.rowOpen} onClick={() => open(plot)} disabled={busy}>
                      <span style={styles.rowName}>{plot.name}</span>
                      <span style={styles.meta}>
                        {savedAtLabel(plot.saved_at)} · v{plot.version}
                      </span>
                    </button>
                    <button
                      type="button"
                      style={styles.iconButton}
                      title="Rename"
                      onClick={() => setPending({ kind: 'renaming', plotId: plot.plot_id, text: plot.name })}
                    >
                      ✎
                    </button>
                    <button
                      type="button"
                      style={styles.iconButton}
                      title="Remove from this list"
                      onClick={() => setPending({ kind: 'removing', plot })}
                    >
                      ✕
                    </button>
                  </div>
                  {pending?.kind === 'removing' && pending.plot.plot_id === plot.plot_id && (
                    <Question
                      text={`Remove "${plot.name}" from this list? It is hidden, not deleted: every version is kept in the database.`}
                      confirmLabel="Remove"
                      onConfirm={async () => { setPending(null); await onRemove(plot.plot_id) }}
                      onCancel={() => setPending(null)}
                    />
                  )}
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}

interface NameBoxProps {
  initial: string
  actionLabel: string
  busy: boolean
  onSubmit: (name: string) => void | Promise<void>
  onCancel: () => void
}

function NameBox({ initial, actionLabel, busy, onSubmit, onCancel }: NameBoxProps) {
  const [text, setText] = useState(initial)
  const input = useRef<HTMLInputElement>(null)
  useEffect(() => {
    input.current?.focus()
    input.current?.select()
  }, [])
  const submit = () => { void onSubmit(text) }
  return (
    <div style={styles.nameBox}>
      <input
        ref={input}
        style={styles.input}
        value={text}
        placeholder="Plot name"
        onChange={e => setText(e.target.value)}
        onKeyDown={e => {
          if (e.key === 'Enter') submit()
          if (e.key === 'Escape') onCancel()
        }}
      />
      <div style={styles.buttonRow}>
        <button type="button" style={styles.primaryButton} onClick={submit} disabled={busy}>
          {actionLabel}
        </button>
        <button type="button" style={styles.headerButton} onClick={onCancel}>
          Cancel
        </button>
      </div>
    </div>
  )
}

interface QuestionProps {
  text: string
  confirmLabel: string
  onConfirm: () => void
  onCancel: () => void
}

function Question({ text, confirmLabel, onConfirm, onCancel }: QuestionProps) {
  return (
    <div style={styles.question}>
      <div>{text}</div>
      <div style={styles.buttonRow}>
        <button type="button" style={styles.primaryButton} onClick={onConfirm}>
          {confirmLabel}
        </button>
        <button type="button" style={styles.headerButton} onClick={onCancel}>
          Cancel
        </button>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  rail: {
    width: 230, flexShrink: 0, display: 'flex', flexDirection: 'column',
    minHeight: 0, borderLeft: '1px solid #2a2a4a',
  },
  collapsed: {
    flexShrink: 0, display: 'flex', flexDirection: 'column', alignItems: 'center',
    gap: 8, padding: '8px 4px', borderLeft: '1px solid #2a2a4a', background: '#1a1a2e',
  },
  verticalLabel: {
    writingMode: 'vertical-rl', color: '#999', fontSize: 11, cursor: 'pointer',
    letterSpacing: 0.6,
  },
  header: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    gap: 6, padding: '8px 12px', borderBottom: '1px solid #2a2a4a',
    background: '#1a1a2e', flexShrink: 0,
  },
  title: { color: '#eee', fontSize: 13, fontWeight: 600 },
  body: { padding: 12, flex: 1, minHeight: 0, overflowY: 'auto', overflowX: 'hidden' },
  current: { marginBottom: 12 },
  currentName: {
    color: '#eee', fontSize: 12, fontWeight: 600, marginBottom: 2,
    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
  },
  unsaved: { color: '#888', fontStyle: 'italic', fontWeight: 400 },
  meta: { fontSize: 10, color: '#888' },
  modifiedDot: { color: '#e0b050' },
  headerButton: {
    background: '#22223a', color: '#ccc', border: '1px solid #3a3a5a',
    borderRadius: 4, cursor: 'pointer', fontSize: 11, padding: '2px 8px',
  },
  primaryButton: {
    background: '#3b3280', color: '#eee', border: '1px solid #7b68ee',
    borderRadius: 4, cursor: 'pointer', fontSize: 11, padding: '3px 10px', marginTop: 6,
  },
  linkButton: {
    background: 'none', border: 'none', color: '#9d8cff', cursor: 'pointer',
    fontSize: 10, padding: 0, textDecoration: 'underline',
  },
  iconButton: {
    background: 'transparent', border: 'none', color: '#888', cursor: 'pointer',
    fontSize: 11, padding: '2px 4px', flexShrink: 0,
  },
  nameBox: { marginTop: 6 },
  input: {
    width: '100%', boxSizing: 'border-box', background: '#12121f', color: '#ddd',
    border: '1px solid #3a3a5a', borderRadius: 4, padding: '4px 6px', fontSize: 12,
  },
  buttonRow: { display: 'flex', gap: 6, alignItems: 'center' },
  question: {
    fontSize: 11, lineHeight: 1.45, color: '#ddd', margin: '6px 0',
    padding: '6px 8px', background: '#1d1d33', border: '1px solid #3a3a5a', borderRadius: 4,
  },
  notes: {
    fontSize: 10, lineHeight: 1.45, color: '#d9c48f', marginBottom: 10,
    padding: '5px 7px', background: '#221c0c', borderLeft: '2px solid #d9b45f',
  },
  noteList: { margin: '4px 0', paddingLeft: 14 },
  noteHint: { marginTop: 3, fontStyle: 'italic', color: '#b8a878' },
  error: {
    fontSize: 11, color: '#ff8a8a', marginBottom: 8, whiteSpace: 'pre-wrap',
  },
  hint: { fontSize: 10, color: '#777', fontStyle: 'italic' },
  list: { display: 'flex', flexDirection: 'column', gap: 2 },
  row: {
    display: 'flex', alignItems: 'center', gap: 2, borderRadius: 4,
    padding: '2px 2px 2px 0',
  },
  rowLoaded: { background: '#24244a' },
  rowOpen: {
    flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: 'flex-start',
    background: 'transparent', border: 'none', cursor: 'pointer', padding: '3px 6px',
    textAlign: 'left',
  },
  rowName: {
    color: '#ddd', fontSize: 12, maxWidth: '100%',
    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
  },
}
