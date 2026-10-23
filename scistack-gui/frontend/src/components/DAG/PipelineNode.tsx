/**
 * PipelineNode — a nested pipeline placed on a parent canvas.
 *
 * The React Flow node id IS the use_id (decision G1: the same child placed
 * twice is two nodes with different use_ids; the binding lives on the use).
 *
 * data comes from GET /api/pipeline:
 *   { label, child_pipeline_id, binding, inputs, outputs }
 * inputs/outputs are variable-type name lists — the node's connection ports
 * (rendered like a function node's handles). A non-empty binding renders as
 * a compact badge. Double-click (handled at canvas level) descends into the
 * child scope; the ▶ Run button plan-previews mode=all on the CHILD
 * pipeline (per-step run_until of a child is not available descend-less).
 * ⚙ opens this placement's tunables (TunablesPanel; a library pipeline's
 * Parameters, PathInputs and step values — the step values are THIS
 * placement's binding, so two placements keep their own).
 */

import { useState, useCallback, useRef, useEffect } from 'react'
import { Handle, Position, useUpdateNodeInternals } from '@xyflow/react'
import { callBackend, isVSCodeMode } from '../../api'
import { usePlanRun } from '../../context/PlanRunContext'
import { useScope, bindingSummary, type BindingSpec } from '../../context/ScopeContext'
import TunablesPanel from '../Tunables/TunablesPanel'

export interface PipelineNodeData {
  label: string
  child_pipeline_id: string
  binding: BindingSpec | null
  inputs: string[]
  outputs: string[]
}

interface Props {
  id: string
  data: PipelineNodeData
}

export default function PipelineNode({ id, data }: Props) {
  const { requestPlan } = usePlanRun()
  const { bumpGraph } = useScope()
  const updateNodeInternals = useUpdateNodeInternals()
  const [duplicating, setDuplicating] = useState(false)
  // The placement's tunables (library pipelines; plan-library-fork-chain E1).
  const [tuning, setTuning] = useState(false)
  const [draftName, setDraftName] = useState('')
  const [error, setError] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (duplicating) inputRef.current?.focus()
  }, [duplicating])

  // Same stale-handle-bounds problem as FunctionNode: inputs/outputs arrive
  // from GET /api/pipeline and change on a dag_updated refetch, and every
  // handle's `top` depends on the total count. See FunctionNode for the
  // full note.
  const handleKey = `${data.inputs.join('|')}>${data.outputs.join('|')}`
  useEffect(() => {
    updateNodeInternals(id)
    // eslint-disable-next-line no-console
    console.debug(
      `[PipelineNode ${id}] handle set changed -> ${data.inputs.length} input(s), `
      + `${data.outputs.length} output(s): ${handleKey}`
    )
  }, [id, handleKey])  // eslint-disable-line react-hooks/exhaustive-deps

  const badge = bindingSummary(data.binding)

  // Overriding `transform` drops React Flow's own X centring for the side —
  // see the equivalent note in FunctionNode.
  const handleStyle = (
    index: number,
    total: number,
    side: 'left' | 'right',
  ): React.CSSProperties => ({
    top: `${((index + 1) / (total + 1)) * 100}%`,
    transform: `translate(${side === 'left' ? '-50%' : '50%'}, -50%)`,
  })

  const handleRun = (e: React.MouseEvent) => {
    e.stopPropagation()
    requestPlan({
      pipeline_id: data.child_pipeline_id,
      mode: 'all',
      label: data.label,
    })
  }

  const commitDuplicate = useCallback(() => {
    const name = draftName.trim()
    if (!name) {
      setDuplicating(false)
      return
    }
    callBackend('duplicate_pipeline', { pipeline_id: data.child_pipeline_id, name })
      .then(() => {
        setError('')
        setDuplicating(false)
        setDraftName('')
        bumpGraph()
      })
      .catch(err => setError((err as Error).message))
  }, [draftName, data.child_pipeline_id, bumpGraph])

  const handleExport = useCallback((e: React.MouseEvent) => {
    e.stopPropagation()
    callBackend('export_pipeline', { pipeline_id: data.child_pipeline_id })
      .then(res => {
        const r = res as { path: string; document: unknown }
        if (!isVSCodeMode) {
          const blob = new Blob([JSON.stringify(r.document, null, 2)], { type: 'application/json' })
          const url = URL.createObjectURL(blob)
          const a = window.document.createElement('a')
          a.href = url
          a.download = `${data.label.replace(/[^\w.-]+/g, '_')}.json`
          a.click()
          URL.revokeObjectURL(url)
        }
        window.alert(`Exported '${data.label}' to:\n${r.path}`)
      })
      .catch(err => window.alert(`Export failed: ${(err as Error).message}`))
  }, [data.child_pipeline_id, data.label])

  const handleExportCode = useCallback((e: React.MouseEvent) => {
    e.stopPropagation()
    callBackend('export_pipeline_code', { pipeline_id: data.child_pipeline_id })
      .then(res => {
        const r = res as { path: string; language: 'python' | 'matlab'; script: string; warnings: string[] }
        if (!isVSCodeMode) {
          const ext = r.language === 'matlab' ? 'm' : 'py'
          const blob = new Blob([r.script], { type: 'text/plain' })
          const url = URL.createObjectURL(blob)
          const a = window.document.createElement('a')
          a.href = url
          a.download = `${data.label.replace(/[^\w.-]+/g, '_')}.${ext}`
          a.click()
          URL.revokeObjectURL(url)
        }
        const warningText = r.warnings.length > 0
          ? `\n\n${r.warnings.length} step(s) skipped (see comments in the script):\n${r.warnings.join('\n')}`
          : ''
        window.alert(`Exported '${data.label}' (${r.language}) to:\n${r.path}${warningText}`)
      })
      .catch(err => window.alert(`Export failed: ${(err as Error).message}`))
  }, [data.child_pipeline_id, data.label])

  return (
    <div style={styles.container} title="Double-click to open this pipeline">
      {data.inputs.map((name, i) => (
        <Handle
          key={name}
          id={`in__${name}`}
          type="target"
          position={Position.Left}
          style={handleStyle(i, data.inputs.length, 'left')}
          title={name}
        />
      ))}

      <div style={styles.kind}>⧉ pipeline</div>
      <div style={styles.label}>{data.label}</div>
      {badge && (
        <div style={styles.badge} title={badge}>{badge}</div>
      )}

      <div style={styles.buttonRow}>
        <button style={styles.button} onClick={handleRun} type="button">
          ▶ Run
        </button>
        <button
          style={styles.button}
          onClick={e => { e.stopPropagation(); setDuplicating(true) }}
          type="button"
          title="Fork this submodule's own nodes into a new, independent pipeline"
        >
          ⎘ Duplicate
        </button>
        <button
          style={styles.button}
          onClick={handleExport}
          type="button"
          title="Export this submodule as a portable file to share with another SciStack user"
        >
          ⇩
        </button>
        <button
          style={styles.button}
          onClick={handleExportCode}
          type="button"
          title="Translate this submodule to a standalone Python/MATLAB script"
        >
          {'</>'}
        </button>
        <button
          style={styles.button}
          onClick={e => { e.stopPropagation(); setTuning(t => !t) }}
          type="button"
          title="Tunables of this placement: its Parameters, PathInputs and step values"
        >
          ⚙
        </button>
      </div>
      {tuning && <TunablesPanel useId={id} onChanged={bumpGraph} />}
      {duplicating && (
        <div style={styles.duplicateForm} onClick={e => e.stopPropagation()}>
          <input
            ref={inputRef}
            style={styles.duplicateInput}
            value={draftName}
            placeholder="new pipeline name…"
            onChange={e => { setDraftName(e.target.value); setError('') }}
            onKeyDown={e => {
              if (e.key === 'Enter') commitDuplicate()
              if (e.key === 'Escape') { setDuplicating(false); setDraftName(''); setError('') }
            }}
            onBlur={commitDuplicate}
          />
          {error && <div style={styles.duplicateError}>{error}</div>}
        </div>
      )}

      {data.outputs.map((name, i) => (
        <Handle
          key={name}
          id={`out__${name}`}
          type="source"
          position={Position.Right}
          style={handleStyle(i, data.outputs.length, 'right')}
          title={name}
        />
      ))}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    background: '#fdf4ff',
    border: '2px double #a21caf',
    borderRadius: 8,
    padding: '8px 12px',
    minWidth: 180,
    fontSize: 13,
    boxShadow: '0 2px 6px rgba(0,0,0,0.12)',
  },
  kind: {
    fontSize: 10,
    fontWeight: 700,
    color: '#a21caf',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    textAlign: 'center',
  },
  label: {
    fontWeight: 600,
    color: '#701a75',
    fontFamily: 'monospace',
    marginBottom: 4,
    textAlign: 'center',
  },
  badge: {
    display: 'block',
    margin: '0 auto 6px',
    padding: '1px 8px',
    maxWidth: 220,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    background: '#f5d0fe',
    color: '#701a75',
    border: '1px solid #d946ef',
    borderRadius: 10,
    fontSize: 10,
    fontFamily: 'monospace',
    textAlign: 'center',
    width: 'fit-content',
  },
  buttonRow: {
    display: 'flex',
    gap: 4,
  },
  duplicateForm: {
    marginTop: 4,
  },
  duplicateInput: {
    display: 'block',
    width: '100%',
    background: '#fff',
    border: '1px solid #a21caf',
    borderRadius: 3,
    color: '#701a75',
    fontSize: 11,
    fontFamily: 'monospace',
    padding: '3px 6px',
    outline: 'none',
    boxSizing: 'border-box',
  },
  duplicateError: {
    marginTop: 2,
    fontSize: 10,
    color: '#dc2626',
    whiteSpace: 'pre-wrap',
  },
  button: {
    flex: 1,
    padding: '4px 0',
    background: '#a21caf',
    color: '#fff',
    border: 'none',
    borderRadius: 4,
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: 12,
  },
}
