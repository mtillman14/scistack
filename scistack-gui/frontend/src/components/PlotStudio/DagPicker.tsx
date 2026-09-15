/**
 * The shell both DAG pickers are built on: the pipeline canvas in a modal.
 *
 * Two pickers now draw the same graph to ask different questions — "which
 * variant of this variable?" (`VariantDagPopup`) and "which variable, and which
 * of its columns, groups this figure?" (`GroupingDagPopup`). What they share is
 * all mechanism: fetch the pipeline and its layout, run dagre, draw it in a
 * dialog you can leave with Escape. What differs is the node components and the
 * sidebar, which is exactly what stays in each caller.
 *
 * **Split as a hook plus a presentational dialog, not one component with
 * twenty props.** Both callers need the loaded `nodes` for their own logic —
 * the variant picker maps function labels to a backend call, the grouping
 * picker marks which variable nodes are clickable — so a shell that owned the
 * nodes privately would have to hand them back out through callbacks and take
 * decorated ones back in. A hook returns them; a dialog takes children.
 *
 * Why a modal at all: selection is a decision you come back from, and the state
 * being edited belongs to the panel underneath. Covering it is the point.
 */

import { useEffect, useState } from 'react'
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  type Edge,
  type Node,
  type NodeTypes,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import { applyDagreLayout } from '../../layout'
import { callBackend } from '../../api'
import {
  VariantSelectionProvider,
  type VariantSelectionValue,
} from '../../context/VariantSelectionContext'

/**
 * The project's pipeline graph, laid out and ready to draw.
 *
 * Independent of whatever the picker is asking about, so the canvas can render
 * while the question's own data is still loading — and so picking something
 * costs one RPC rather than re-fetching the pipeline.
 */
export function usePipelineCanvas() {
  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const pipeline = (await callBackend('get_pipeline', {
          pipeline_id: 'main',
        })) as { nodes: Node[]; edges: Edge[] }
        const layout = (await callBackend('get_layout', {
          pipeline_id: 'main',
        })) as Record<string, unknown>
        const saved = (layout.positions ?? layout) as Record<
          string,
          { x: number; y: number }
        >
        if (cancelled) return
        setNodes(applyDagreLayout(pipeline.nodes, pipeline.edges, saved))
        setEdges(pipeline.edges)
      } catch (err) {
        if (!cancelled) setError((err as Error).message)
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  return { nodes, edges, loading, error }
}

/** The modal chrome: header, canvas, optional sidebar, optional strip, footer. */
export function PickerDialog({
  title,
  subtitle,
  headerExtra,
  nodes,
  edges,
  nodeTypes,
  loading,
  error,
  sidebar,
  below,
  footer,
  selection,
  onCancel,
}: {
  title: string
  subtitle: string
  /** Inline with the title — a name field, the chosen variable. */
  headerExtra?: React.ReactNode
  nodes: Node[]
  edges: Edge[]
  nodeTypes: NodeTypes
  loading: boolean
  error: string
  /** Beside the canvas. The grouping picker's column checkboxes live here. */
  sidebar?: React.ReactNode
  /** Under the canvas, full width — the variant picker's unmapped axes. */
  below?: React.ReactNode
  footer: React.ReactNode
  /** What the node controls mean on this canvas. REQUIRED, and provided here
   *  rather than by each caller, because the DAG node components branch on the
   *  PRESENCE of this context: without it a function node mounts its pipeline
   *  body, which reaches for `useScope`/`useRunLog` and throws in the Plot
   *  Studio tab (`PlotRoot` mounts no providers). The grouping picker's first
   *  step did exactly that on 2026-09-15 and the whole tab went blank. A step
   *  with nothing to select passes `INERT_VARIANT_SELECTION`.
   *
   *  Around the CANVAS and not around the dialog, deliberately: only the nodes
   *  consume it, and a provider mounted higher would be one more thing the
   *  chrome depends on. */
  selection: VariantSelectionValue
  onCancel: () => void
}) {
  // Escape cancels — a modal that traps you is worse than one you can leave.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCancel()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onCancel])

  return (
    <div style={pickerStyles.backdrop} onClick={onCancel}>
      <div style={pickerStyles.dialog} onClick={e => e.stopPropagation()}>
        <div style={pickerStyles.header}>
          <div style={pickerStyles.headerLeft}>
            <span style={pickerStyles.title}>{title}</span>
            {headerExtra}
          </div>
          <span style={pickerStyles.subtitle}>{subtitle}</span>
        </div>

        <div style={pickerStyles.body}>
          <div style={pickerStyles.canvas}>
            {loading && <div style={pickerStyles.note}>Loading the pipeline…</div>}
            {error && (
              <div style={pickerStyles.error}>
                Could not load the pipeline: {error}
              </div>
            )}
            {!loading && !error && (
              <VariantSelectionProvider value={selection}>
              <ReactFlowProvider>
                <ReactFlow
                  nodes={nodes}
                  edges={edges}
                  nodeTypes={nodeTypes}
                  nodesDraggable={false}
                  nodesConnectable={false}
                  // MUST stay true. React Flow gives a node wrapper
                  // `pointer-events: none` unless it is selectable, draggable,
                  // or carries a mouse handler — so turning all three off made
                  // every control in these popups unclickable, checkboxes as
                  // well as the version dropdowns. Selection is inert here
                  // anyway (nothing reads node.selected); it exists to keep the
                  // nodes reachable by the mouse.
                  elementsSelectable
                  fitView
                  fitViewOptions={{ padding: 0.2 }}
                  proOptions={{ hideAttribution: true }}
                >
                  <Background />
                  <Controls showInteractive={false} />
                </ReactFlow>
              </ReactFlowProvider>
              </VariantSelectionProvider>
            )}
          </div>
          {sidebar && <div style={pickerStyles.sidebar}>{sidebar}</div>}
        </div>

        {below}

        <div style={pickerStyles.footer}>
          <button type="button" style={pickerStyles.button} onClick={onCancel}>
            Cancel
          </button>
          {footer}
        </div>
      </div>
    </div>
  )
}

export const pickerStyles: Record<string, React.CSSProperties> = {
  backdrop: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.72)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1100,
  },
  dialog: {
    width: '82vw', height: '80vh', background: '#16162a',
    border: '1px solid #7b68ee', borderRadius: 8,
    display: 'flex', flexDirection: 'column', overflow: 'hidden',
    boxShadow: '0 12px 40px rgba(0,0,0,0.5)',
  },
  header: {
    padding: '10px 14px', borderBottom: '1px solid #2a2a4a', background: '#1a1a2e',
    display: 'flex', flexDirection: 'column', gap: 4, flexShrink: 0,
  },
  headerLeft: { display: 'flex', alignItems: 'center', gap: 10 },
  title: { color: '#eee', fontSize: 14, fontWeight: 600 },
  subtitle: { color: '#8a8aa8', fontSize: 11, fontStyle: 'italic' },
  // Canvas and sidebar side by side; the canvas takes whatever is left.
  body: { flex: 1, minHeight: 0, display: 'flex', flexDirection: 'row' },
  canvas: { flex: 1, minWidth: 0, position: 'relative' },
  sidebar: {
    width: 280, flexShrink: 0, borderLeft: '1px solid #2a2a4a',
    background: '#1a1a2e', overflowY: 'auto', padding: '10px 12px',
  },
  footer: {
    display: 'flex', justifyContent: 'flex-end', gap: 8,
    padding: '10px 14px', borderTop: '1px solid #2a2a4a', flexShrink: 0,
  },
  button: {
    padding: '5px 14px', background: '#22223a', color: '#ccc',
    border: '1px solid #3a3a5a', borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
  primaryButton: {
    padding: '5px 14px', background: '#7b68ee', color: '#fff',
    border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  note: { fontSize: 12, color: '#777', fontStyle: 'italic', padding: 12 },
  error: { fontSize: 12, color: '#f87171', padding: 12 },
  // The chosen variable, beside the title in step two.
  chosenVariable: {
    fontFamily: 'monospace', fontSize: 12, color: '#9d92f5',
    background: '#22223a', border: '1px solid #4c3a8a', borderRadius: 4,
    padding: '2px 8px',
  },
  inertNode: {
    background: '#2a2438', border: '2px dashed #6b5a9a', borderRadius: 6,
    padding: '8px 12px', minWidth: 150, opacity: 0.55,
  },
  inertNodeLabel: {
    fontWeight: 600, color: '#c4b5fd', fontFamily: 'monospace',
    textAlign: 'center', fontSize: 13,
  },
  inertNodeHint: {
    fontSize: 10, color: '#8a8aa8', fontStyle: 'italic', textAlign: 'center',
  },
  pickNode: {
    borderRadius: 6, padding: '8px 12px', minWidth: 150, textAlign: 'center',
  },
  pickNodeReady: {
    background: '#1e2a44', border: '2px solid #4f7fd0', cursor: 'pointer',
  },
  // Drawn, not hidden, and not merely dim: the reason is the content. A node
  // the user expected to click has to say why it cannot be, in place.
  pickNodeRefused: {
    background: '#22223a', border: '2px dashed #3a3a5a', opacity: 0.6,
    cursor: 'not-allowed',
  },
  pickNodeLabel: {
    fontWeight: 600, color: '#ddd', fontFamily: 'monospace', fontSize: 13,
  },
  pickNodeHint: {
    fontSize: 9, color: '#8a8aa8', fontStyle: 'italic', marginTop: 2,
    maxWidth: 200, whiteSpace: 'normal', lineHeight: 1.3,
  },
}
