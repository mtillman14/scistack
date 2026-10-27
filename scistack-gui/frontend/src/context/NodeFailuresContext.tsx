/**
 * NodeFailuresContext — the canvas's one store of run failures per node.
 *
 * Written by the two run-message consumers (FunctionNode for its own runs,
 * PipelineRunController for pipeline runs) through the pure rules in
 * DAG/runFailures.ts; read by the node badge and the FailuresPanel. The
 * extension's "Show failures" notification button opens the panel through
 * a `show_failures` message. A failure that carries a remedy (a structured
 * explanation, e.g. a save refused for duplicate locations) also opens
 * RemedyDialog when its run ends; this context is the one place that decides.
 * See docs/claude/error-surfacing.md.
 */

import { createContext, useCallback, useContext, useState } from 'react'
import { useBackendMessage } from '../hooks/useBackendMessage'
import {
  applyRunDone,
  clearNode,
  firstRemedy,
  noteSkip,
  readRunFailures,
} from '../components/DAG/runFailures'
import type { FailureGroup, FailureState, RemedyTarget, SkipFailure } from '../components/DAG/runFailures'

interface NodeFailuresValue {
  failures: FailureState
  runStarted: (nodeId: string) => void
  comboFailed: (nodeId: string, failure: SkipFailure | null | undefined) => void
  runDone: (params: Record<string, unknown>) => void
  /** The node whose failures panel is open; '' = node-less, null = closed. */
  openNodeId: string | null
  openPanel: (nodeId: string) => void
  closePanel: () => void
  /** The remedy popup on screen, or null. */
  remedy: RemedyTarget | null
  openRemedy: (nodeId: string, group: FailureGroup) => void
  closeRemedy: () => void
}

const NodeFailuresContext = createContext<NodeFailuresValue | null>(null)

export function NodeFailuresProvider({ children }: { children: React.ReactNode }) {
  const [failures, setFailures] = useState<FailureState>({})
  const [openNodeId, setOpenNodeId] = useState<string | null>(null)
  const [remedy, setRemedy] = useState<RemedyTarget | null>(null)

  const runStarted = useCallback((nodeId: string) => {
    setFailures(prev => clearNode(prev, nodeId))
  }, [])

  const comboFailed = useCallback((nodeId: string, failure: SkipFailure | null | undefined) => {
    setFailures(prev => noteSkip(prev, nodeId, failure))
  }, [])

  const runDone = useCallback((params: Record<string, unknown>) => {
    const wire = readRunFailures(params.failures)
    // eslint-disable-next-line no-console
    console.debug(
      `[NodeFailures] run_done ${String(params.run_id)}: covered ${Object.keys(wire.by_node).length} node(s), `
      + `${Object.values(wire.by_node).flat().length + wire.other.length} group(s)`,
    )
    setFailures(prev => applyRunDone(prev, wire, Boolean(params.stopped)))
    const target = firstRemedy(wire)
    if (target) {
      // eslint-disable-next-line no-console
      console.debug(`[remedy] open kind=${target.group.remedy?.kind} node=${target.nodeId || '(none)'} run=${String(params.run_id)}`)
      setRemedy(target)
    }
  }, [])

  // The extension's notification button ("Show failures").
  useBackendMessage(useCallback((msg) => {
    const msgType = (msg.type ?? msg.method) as string
    if (msgType !== 'show_failures') return
    const params = (msg.params ?? msg) as Record<string, unknown>
    setOpenNodeId(((params.node_id as string | null | undefined) ?? '') || '')
  }, []))

  const openPanel = useCallback((nodeId: string) => setOpenNodeId(nodeId), [])
  const closePanel = useCallback(() => setOpenNodeId(null), [])
  const openRemedy = useCallback((nodeId: string, group: FailureGroup) => setRemedy({ nodeId, group }), [])
  const closeRemedy = useCallback(() => setRemedy(null), [])

  return (
    <NodeFailuresContext.Provider
      value={{
        failures, runStarted, comboFailed, runDone, openNodeId, openPanel, closePanel,
        remedy, openRemedy, closeRemedy,
      }}
    >
      {children}
    </NodeFailuresContext.Provider>
  )
}

export function useNodeFailures() {
  const ctx = useContext(NodeFailuresContext)
  if (!ctx) throw new Error('useNodeFailures must be used within NodeFailuresProvider')
  return ctx
}
