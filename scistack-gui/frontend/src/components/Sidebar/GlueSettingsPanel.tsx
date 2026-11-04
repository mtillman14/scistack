/**
 * GlueSettingsPanel — the primary surface for a glue node (D1a).
 *
 * The file on disk is persistence, not a destination: the user writes and
 * edits the body here and never has to navigate to `src/scistack_glue/`.
 *
 * Beside the editor is the **live column list** for the wired input. That is
 * the genuinely non-obvious half of writing glue — the columns depend on how
 * the variable stores its data, and it is not visible from the canvas:
 *
 *   - a DataFrame-stored variable arrives under the user's own column names;
 *   - a scalar/array-stored variable arrives as schema keys plus ONE data
 *     column named after the class.
 *
 * It is read live on every open rather than scaffolded into the file as a
 * comment: a comment goes stale the moment the node is rewired.
 *
 * What this panel does NOT have: a Run button. A glue node is transient by
 * construction and runs only as part of a consuming function's run.
 */

import { useCallback, useEffect, useState } from 'react'
import { callBackend } from '../../api'
import GlueCodeEditor from './GlueCodeEditor'
import PanelHeader from './PanelHeader'

interface Props {
  label: string
  /** The variable type wired into this node, if any — drives the column list. */
  wiredType?: string
}

interface GlueSource {
  ok: boolean
  error?: string
  source?: string
  path?: string
  language?: string
  editable?: boolean
}

interface ColumnInfo {
  ok: boolean
  error?: string
  schema_keys?: string[]
  data_columns?: string[]
  note?: string
}

export default function GlueSettingsPanel({ label, wiredType }: Props) {
  const [info, setInfo] = useState<GlueSource | null>(null)
  const [draft, setDraft] = useState('')
  const [dirty, setDirty] = useState(false)
  const [saving, setSaving] = useState(false)
  const [status, setStatus] = useState('')
  const [columns, setColumns] = useState<ColumnInfo | null>(null)

  useEffect(() => {
    setStatus('')
    setDirty(false)
    callBackend('get_glue', { name: label })
      .then(d => {
        const res = d as GlueSource
        setInfo(res)
        setDraft(res.source ?? '')
      })
      .catch(err => setInfo({ ok: false, error: (err as Error).message }))
  }, [label])

  useEffect(() => {
    if (!wiredType) { setColumns(null); return }
    callBackend('get_glue_columns', { name: label, variable_type: wiredType })
      .then(d => setColumns(d as ColumnInfo))
      .catch(err => setColumns({ ok: false, error: (err as Error).message }))
  }, [label, wiredType])

  const save = useCallback(() => {
    if (!dirty || saving || !info?.editable) return
    setSaving(true)
    setStatus('Saving…')
    callBackend('save_glue', { name: label, source: draft })
      .then(d => {
        const res = d as { ok: boolean; error?: string }
        if (res.ok) {
          setDirty(false)
          // Say what saving actually MEANS here, because it is the thing
          // users get wrong: the body is hashed into the consuming
          // function's identity, so its next run recomputes.
          setStatus('Saved — the functions this feeds will recompute on their next run.')
        } else {
          setStatus(res.error ?? 'Save failed.')
        }
      })
      .catch(err => setStatus((err as Error).message))
      .finally(() => setSaving(false))
  }, [dirty, saving, info, label, draft])

  if (info && !info.ok) {
    return (
      <div style={styles.root}>
        <PanelHeader kind="glue" name={label} />
        <div style={styles.error}>{info.error}</div>
      </div>
    )
  }

  const readOnly = !info?.editable

  return (
    <div style={styles.root}>
      <PanelHeader
        kind="glue"
        name={label}
        source={info?.path ? { file: info.path, line: 1 } : null}
      />
      <div style={styles.subtitle}>
        Reshapes its input in memory. The result is never saved — it is fused
        into the run of whichever function consumes it.
      </div>

      {readOnly && info && (
        <div style={styles.readOnlyNote}>
          This glue lives outside the project's glue directory, so it is
          read-only here. Edit it in your editor: {info.path}
        </div>
      )}

      <GlueCodeEditor
        value={draft}
        language={info?.language}
        readOnly={readOnly}
        onChange={next => { setDraft(next); setDirty(true); setStatus('') }}
        onBlur={save}
      />

      <div style={styles.actions}>
        <button
          style={styles.saveBtn}
          onClick={save}
          disabled={!dirty || saving || readOnly}
          type="button"
        >
          {saving ? 'Saving…' : 'Save'}
        </button>
        {status && <span style={styles.status}>{status}</span>}
      </div>

      <div style={styles.sectionTitle}>Columns this glue receives</div>
      {!wiredType && (
        <div style={styles.hint}>
          Not wired to a variable yet. Connect one and this will list the exact
          columns your function will be handed.
        </div>
      )}
      {columns && !columns.ok && <div style={styles.hint}>{columns.error}</div>}
      {columns && columns.ok && (
        <div style={styles.columns}>
          <div style={styles.note}>{columns.note}</div>
          <div style={styles.colGroup}>
            <span style={styles.colGroupLabel}>schema keys</span>
            {(columns.schema_keys ?? []).map(c => (
              <code key={c} style={styles.protectedCol} title="Visible, but must come back unchanged">{c}</code>
            ))}
          </div>
          <div style={styles.colGroup}>
            <span style={styles.colGroupLabel}>data</span>
            {(columns.data_columns ?? []).map(c => (
              <code key={c} style={styles.col}>{c}</code>
            ))}
          </div>
          <div style={styles.contract}>
            You may add, drop, rename and retype columns. You may not change
            the row set, and schema-key columns must come back unchanged.
          </div>
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: { padding: 12, display: 'flex', flexDirection: 'column', gap: 8, overflowY: 'auto' },
  title: { fontWeight: 600, fontSize: 14, display: 'flex', alignItems: 'center', gap: 6 },
  roleBadge: {
    fontSize: 10,
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    color: '#6b7280',
    border: '1px solid #d1d5db',
    borderRadius: 3,
    padding: '0 4px',
  },
  subtitle: { fontSize: 11, color: '#6b7280', lineHeight: 1.4 },
  readOnlyNote: {
    fontSize: 11,
    color: '#92400e',
    background: '#fffbeb',
    border: '1px solid #fde68a',
    borderRadius: 4,
    padding: 6,
  },
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
  sectionTitle: { fontWeight: 600, fontSize: 12, marginTop: 4 },
  hint: { fontSize: 11, color: '#9ca3af', fontStyle: 'italic' },
  columns: { display: 'flex', flexDirection: 'column', gap: 6 },
  note: { fontSize: 11, color: '#6b7280' },
  colGroup: { display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 4 },
  colGroupLabel: { fontSize: 10, color: '#9ca3af', textTransform: 'uppercase', marginRight: 2 },
  col: {
    fontSize: 11,
    background: '#f3f4f6',
    borderRadius: 3,
    padding: '1px 5px',
  },
  protectedCol: {
    fontSize: 11,
    background: '#eef2ff',
    border: '1px dashed #c7d2fe',
    borderRadius: 3,
    padding: '1px 5px',
  },
  contract: { fontSize: 10, color: '#9ca3af', lineHeight: 1.4 },
  error: { fontSize: 12, color: '#b91c1c' },
}
