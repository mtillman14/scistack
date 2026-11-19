/**
 * MergeNode — a scidb.Merge on the canvas (docs/claude/merge-node.md).
 *
 * Joins the variables wired into its input ports into one table, which feeds
 * a function argument through its single output. Ports are slots (`in__m{n}`,
 * mergePorts.ts): one per wired variable, in join order, plus one empty port
 * for the next. The join method (`how`) is set in the sidebar panel.
 */

import { useEffect, useMemo } from 'react'
import { Handle, Position, useEdges, useUpdateNodeInternals } from '@xyflow/react'
import { MERGE_OUT_HANDLE, mergePorts, mergeSlotOf } from './mergePorts'

export interface MergeNodeData {
  label: string
  how: string
  /** History's constituent types, in join order (empty for a new node). */
  constituents: string[]
  /** One name recorded with two different definitions (history). */
  conflict: boolean
  recorded_how?: string
  hows: string[]
}

interface Props {
  id: string
  data: MergeNodeData
}

const ROW = 22

export default function MergeNode({ id, data }: Props) {
  const edges = useEdges()
  const sources = useMemo(() => {
    const bySlot = new Map<number, string>()
    for (const e of edges) {
      if (e.target !== id) continue
      const slot = mergeSlotOf(e.targetHandle)
      if (slot !== null) bySlot.set(slot, e.source)
    }
    return bySlot
  }, [edges, id])
  const ports = mergePorts(sources.keys())
  const portKey = ports.map(p => p.slot).join(',')

  // Handles are added/removed as edges change: React Flow must re-measure.
  const updateNodeInternals = useUpdateNodeInternals()
  useEffect(() => { updateNodeInternals(id) }, [id, portKey, updateNodeInternals])

  const howDrifted = data.recorded_how && data.recorded_how !== data.how

  return (
    <div style={{ ...styles.container, minHeight: 40 + ports.length * ROW }}>
      <div style={styles.label}>⋈ {data.label}</div>
      <div style={styles.how} title="Join method — change it in the sidebar">
        how={data.how}
        {howDrifted && (
          <span style={styles.warn} title={`History ran how=${data.recorded_how}; the next run from the canvas uses how=${data.how}`}> (was {data.recorded_how})</span>
        )}
      </div>
      {data.conflict && (
        <div style={styles.warn} title="History recorded this name with two different definitions; the first is shown">
          ⚠ two definitions in history
        </div>
      )}
      {ports.map((p, i) => (
        <div key={p.handle}>
          <Handle
            type="target"
            id={p.handle}
            position={Position.Left}
            style={{ top: 40 + i * ROW + ROW / 2, background: p.connected ? '#14b8a6' : '#334155' }}
          />
          <div style={{ ...styles.port, top: 40 + i * ROW }}>
            {p.connected ? `${i + 1}. ${shortName(sources.get(p.slot) ?? '')}` : `${i + 1}. (empty)`}
          </div>
        </div>
      ))}
      <Handle type="source" id={MERGE_OUT_HANDLE} position={Position.Right} />
    </div>
  )
}

/** `var__Gait`, `var__Gait__ab12cd` or `var__Gait::scope` -> `Gait`. */
function shortName(nodeId: string): string {
  const bare = nodeId.split('::')[0]
  const parts = bare.split('__')
  return parts.length >= 2 ? parts[1] : bare
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    position: 'relative',
    background: '#0f2a2a',
    border: '2px solid #0d9488',
    borderRadius: 6,
    padding: '6px 12px',
    minWidth: 170,
    fontSize: 13,
    boxShadow: '0 2px 6px rgba(0,0,0,0.10)',
  },
  label: {
    fontWeight: 600,
    color: '#5eead4',
    fontFamily: 'monospace',
    textAlign: 'center',
  },
  how: {
    fontSize: 11,
    color: '#99f6e4',
    fontFamily: 'monospace',
    textAlign: 'center',
  },
  warn: {
    fontSize: 10,
    color: '#fbbf24',
    fontFamily: 'monospace',
  },
  port: {
    position: 'absolute',
    left: 10,
    fontSize: 11,
    lineHeight: `${ROW}px`,
    color: '#ccfbf1',
    fontFamily: 'monospace',
  },
}
