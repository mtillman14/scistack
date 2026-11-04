/**
 * PipelineSettingsPanel — sidebar Node tab for a selected pipeline node.
 *
 * The node is a USE of a child pipeline (node id = use_id, decision G1);
 * this panel edits the use's binding: key_map (child schema key → parent
 * key), params (constant overrides for the child subtree), and iterate
 * (iteration-value overrides). Saved via PUT /api/pipeline-uses/{use_id}/
 * binding — unknown keys 400 with a clear message, surfaced verbatim.
 */

import { useState, useEffect, useCallback } from 'react'
import { callBackend } from '../../api'
import { useScope, type BindingSpec } from '../../context/ScopeContext'
import type { PipelineNodeData } from '../DAG/PipelineNode'
import PanelHeader from './PanelHeader'

interface Row {
  key: string
  value: string
}

/** Non-string values render as JSON (numbers, lists); strings render raw. */
function valueToText(v: unknown): string {
  return typeof v === 'string' ? v : JSON.stringify(v)
}

/** Typed values round-trip through JSON; anything unparseable is a string. */
function textToValue(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

function rowsFromEntries(entries: Record<string, unknown> | undefined): Row[] {
  return Object.entries(entries ?? {}).map(([key, v]) => ({ key, value: valueToText(v) }))
}

interface Props {
  useId: string
  data: PipelineNodeData
}

export default function PipelineSettingsPanel({ useId, data }: Props) {
  const { descend, bumpGraph } = useScope()
  const [keyMapRows, setKeyMapRows] = useState<Row[]>([])
  const [paramRows, setParamRows] = useState<Row[]>([])
  const [iterateRows, setIterateRows] = useState<Row[]>([])
  const [status, setStatus] = useState<{ ok: boolean; text: string } | null>(null)

  // Re-seed the form whenever a different pipeline node is selected or the
  // graph refresh delivers an updated binding.
  useEffect(() => {
    const b = data.binding ?? {}
    setKeyMapRows(rowsFromEntries(b.key_map))
    setParamRows(rowsFromEntries(b.params))
    setIterateRows(rowsFromEntries(b.iterate))
    setStatus(null)
  }, [useId, data.binding])

  const handleSave = useCallback(() => {
    const binding: BindingSpec = {}
    const collect = (rows: Row[], parse: boolean): Record<string, unknown> => {
      const out: Record<string, unknown> = {}
      for (const r of rows) {
        const k = r.key.trim()
        if (!k) continue
        out[k] = parse ? textToValue(r.value) : r.value
      }
      return out
    }
    const keyMap = collect(keyMapRows, false) as Record<string, string>
    const params = collect(paramRows, true)
    const iterate = collect(iterateRows, true)
    if (Object.keys(keyMap).length > 0) binding.key_map = keyMap
    if (Object.keys(params).length > 0) binding.params = params
    if (Object.keys(iterate).length > 0) binding.iterate = iterate

    callBackend('update_use_binding', { use_id: useId, binding })
      .then(() => {
        setStatus({ ok: true, text: 'Binding saved.' })
        bumpGraph()
      })
      .catch(err => setStatus({ ok: false, text: (err as Error).message }))
  }, [useId, keyMapRows, paramRows, iterateRows, bumpGraph])

  const handleOpen = useCallback(() => {
    descend({
      use_id: useId,
      pipeline_id: data.child_pipeline_id,
      name: data.label,
      binding: data.binding,
    })
  }, [useId, data, descend])

  return (
    <div style={styles.root}>
      <PanelHeader kind="submodule" name={data.label} />
      <div style={styles.meta}>pipeline node · use {useId.slice(0, 8)}</div>
      <button style={styles.openBtn} onClick={handleOpen} type="button">
        Open pipeline
      </button>

      {(data.inputs.length > 0 || data.outputs.length > 0) && (
        <div style={styles.ports}>
          {data.inputs.length > 0 && (
            <div style={styles.portLine}>in: {data.inputs.join(', ')}</div>
          )}
          {data.outputs.length > 0 && (
            <div style={styles.portLine}>out: {data.outputs.join(', ')}</div>
          )}
        </div>
      )}

      <BindingSection
        title="key_map"
        hint="child key → parent key"
        rows={keyMapRows}
        setRows={setKeyMapRows}
      />
      <BindingSection
        title="params"
        hint="constant overrides (JSON values)"
        rows={paramRows}
        setRows={setParamRows}
      />
      <BindingSection
        title="iterate"
        hint="iteration overrides (JSON values, e.g. [1, 2])"
        rows={iterateRows}
        setRows={setIterateRows}
      />

      <button style={styles.saveBtn} onClick={handleSave} type="button">
        Save binding
      </button>
      {status && (
        <div style={status.ok ? styles.statusOk : styles.statusError}>
          {status.text}
        </div>
      )}
    </div>
  )
}

function BindingSection({
  title,
  hint,
  rows,
  setRows,
}: {
  title: string
  hint: string
  rows: Row[]
  setRows: React.Dispatch<React.SetStateAction<Row[]>>
}) {
  const update = (i: number, patch: Partial<Row>) =>
    setRows(prev => prev.map((r, j) => (j === i ? { ...r, ...patch } : r)))
  const remove = (i: number) =>
    setRows(prev => prev.filter((_, j) => j !== i))

  return (
    <div style={styles.section}>
      <div style={styles.sectionHeader}>
        <span style={styles.sectionTitle}>{title}</span>
        <button
          style={styles.addBtn}
          onClick={() => setRows(prev => [...prev, { key: '', value: '' }])}
          title={`Add ${title} entry`}
          type="button"
        >
          +
        </button>
      </div>
      <div style={styles.hint}>{hint}</div>
      {rows.map((row, i) => (
        <div key={i} style={styles.row}>
          <input
            style={styles.input}
            value={row.key}
            placeholder="key"
            onChange={e => update(i, { key: e.target.value })}
          />
          <input
            style={styles.input}
            value={row.value}
            placeholder="value"
            onChange={e => update(i, { value: e.target.value })}
          />
          <button
            style={styles.removeBtn}
            onClick={() => remove(i)}
            title="Remove entry"
            type="button"
          >
            ×
          </button>
        </div>
      ))}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '8px 12px',
  },
  title: {
    fontFamily: 'monospace',
    fontSize: 14,
    fontWeight: 700,
    color: '#d8b4fe',
  },
  meta: {
    fontSize: 10,
    color: '#666',
    fontFamily: 'monospace',
    marginBottom: 8,
  },
  openBtn: {
    width: '100%',
    padding: '4px 0',
    background: '#a21caf',
    color: '#fff',
    border: 'none',
    borderRadius: 4,
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: 12,
    marginBottom: 10,
  },
  ports: {
    background: '#1a1a2e',
    border: '1px solid #2a2a4a',
    borderRadius: 4,
    padding: '6px 8px',
    marginBottom: 10,
  },
  portLine: {
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#7a9ec2',
  },
  section: {
    marginBottom: 10,
  },
  sectionHeader: {
    display: 'flex',
    alignItems: 'center',
  },
  sectionTitle: {
    flex: 1,
    fontSize: 11,
    fontWeight: 700,
    color: '#888',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    fontFamily: 'monospace',
  },
  addBtn: {
    background: 'transparent',
    border: 'none',
    color: '#a21caf',
    fontSize: 16,
    lineHeight: 1,
    cursor: 'pointer',
    padding: '0 2px',
  },
  hint: {
    fontSize: 10,
    color: '#555',
    marginBottom: 4,
  },
  row: {
    display: 'flex',
    gap: 4,
    marginBottom: 4,
    alignItems: 'center',
  },
  input: {
    flex: 1,
    minWidth: 0,
    background: '#1a1a2e',
    border: '1px solid #2a2a4a',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 11,
    fontFamily: 'monospace',
    padding: '3px 6px',
    outline: 'none',
  },
  removeBtn: {
    flexShrink: 0,
    background: 'transparent',
    border: 'none',
    color: '#888',
    fontSize: 14,
    lineHeight: 1,
    cursor: 'pointer',
    padding: '0 2px',
  },
  saveBtn: {
    width: '100%',
    padding: '5px 0',
    background: '#7b68ee',
    color: '#fff',
    border: 'none',
    borderRadius: 4,
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: 12,
    marginTop: 4,
  },
  statusOk: {
    marginTop: 6,
    fontSize: 11,
    color: '#6be16b',
  },
  statusError: {
    marginTop: 6,
    fontSize: 11,
    color: '#f87171',
    whiteSpace: 'pre-wrap',
  },
}
