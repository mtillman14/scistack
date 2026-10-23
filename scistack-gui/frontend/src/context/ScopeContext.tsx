/**
 * ScopeContext — which pipeline scope the canvas is showing.
 *
 * Composition is a DAG (one pipeline can be used by many parents), so the
 * breadcrumb is the navigation PATH taken — a list of crumbs — not a unique
 * address (decision G3). The root scope ('main') is always the first crumb.
 *
 * PipelineDAG re-fetches whenever currentScope or graphVersion changes;
 * scope mutations (create/delete pipeline, place/remove a use, binding
 * edits) call bumpGraph() to trigger a refetch.
 *
 * markNodeDirty/clearNodeDirty/getDirtyPatch guard against that refetch:
 * ANY node's put_layout broadcasts a 'dag_updated' websocket message to
 * every client, which triggers the exact same refetch path as bumpGraph()
 * -- including the client that's mid-edit. Sidebar panels that preview
 * edits live (e.g. FunctionSettingsPanel's where-filter fields, updated on
 * every keystroke but only persisted on blur/Enter) call markNodeDirty()
 * alongside their live canvas update; PipelineDAG's fetch-merge re-applies
 * the pending patch on top of the freshly fetched node so an unrelated
 * refetch -- like the one triggered by dragging a brand new node onto the
 * canvas -- can't silently revert an unsaved draft back to the last-saved
 * value. Cleared once the panel's save round-trip actually lands.
 */

import { createContext, useContext, useState, useCallback, useRef } from 'react'

export interface BindingSpec {
  key_map?: Record<string, string>
  params?: Record<string, unknown>
  iterate?: Record<string, unknown>
}

export interface Crumb {
  use_id: string | null      // the use edge entered through (null for root/direct jumps)
  pipeline_id: string
  name: string
  binding: BindingSpec | null
}

export const ROOT_CRUMB: Crumb = {
  use_id: null,
  pipeline_id: 'main',
  name: 'main',
  binding: null,
}

/** Compact one-line binding summary: `session→subject, low_hz=30`. */
export function bindingSummary(binding: BindingSpec | null | undefined): string {
  if (!binding) return ''
  const parts: string[] = []
  for (const [k, v] of Object.entries(binding.key_map ?? {})) parts.push(`${k}→${v}`)
  for (const [k, v] of Object.entries(binding.params ?? {})) {
    // A step value that follows a project Parameter (E5) names it.
    const ref = v && typeof v === 'object' && 'parameter' in (v as object)
      ? (v as { parameter: string }).parameter : null
    parts.push(ref ? `${k}=⟨${ref}⟩` : `${k}=${JSON.stringify(v)}`)
  }
  for (const [k, v] of Object.entries(binding.iterate ?? {})) parts.push(`${k}=${JSON.stringify(v)}`)
  return parts.join(', ')
}

interface ScopeContextValue {
  currentScope: string
  breadcrumb: Crumb[]
  descend: (crumb: Crumb) => void
  ascendTo: (index: number) => void
  jumpTo: (pipeline_id: string, name: string) => void
  jumpToRoot: (pipeline_id: string, name: string) => void
  renameInPath: (pipeline_id: string, name: string) => void
  graphVersion: number
  bumpGraph: () => void
  markNodeDirty: (nodeId: string, patch: Record<string, unknown>) => void
  clearNodeDirty: (nodeId: string) => void
  getDirtyPatch: (nodeId: string) => Record<string, unknown> | undefined
}

const ScopeContext = createContext<ScopeContextValue | null>(null)

export function ScopeProvider({ children }: { children: React.ReactNode }) {
  const [breadcrumb, setBreadcrumb] = useState<Crumb[]>([ROOT_CRUMB])
  const [graphVersion, setGraphVersion] = useState(0)

  const descend = useCallback((crumb: Crumb) => {
    setBreadcrumb(prev => [...prev, crumb])
  }, [])

  const ascendTo = useCallback((index: number) => {
    setBreadcrumb(prev => prev.slice(0, index + 1))
  }, [])

  // Direct sidebar jump: the path taken is "from the root, straight here".
  const jumpTo = useCallback((pipeline_id: string, name: string) => {
    setBreadcrumb(pipeline_id === ROOT_CRUMB.pipeline_id
      ? [ROOT_CRUMB]
      : [ROOT_CRUMB, { use_id: null, pipeline_id, name, binding: null }])
  }, [])

  // Hypothesis navigation: hypotheses are true top-level siblings (see
  // pipeline_store.py's module docstring — 'main' is just the default one),
  // not scopes nested under 'main'. Unlike jumpTo, this never prepends the
  // root crumb — the target IS its own root.
  const jumpToRoot = useCallback((pipeline_id: string, name: string) => {
    setBreadcrumb([{ use_id: null, pipeline_id, name, binding: null }])
  }, [])

  // Keep crumb labels fresh after a pipeline rename.
  const renameInPath = useCallback((pipeline_id: string, name: string) => {
    setBreadcrumb(prev => prev.map(c =>
      c.pipeline_id === pipeline_id ? { ...c, name } : c
    ))
  }, [])

  const bumpGraph = useCallback(() => setGraphVersion(v => v + 1), [])

  // A ref, not state: patches land here on every keystroke and must not
  // trigger a re-render of the whole scope tree.
  const dirtyNodeDataRef = useRef<Map<string, Record<string, unknown>>>(new Map())

  const markNodeDirty = useCallback((nodeId: string, patch: Record<string, unknown>) => {
    const existing = dirtyNodeDataRef.current.get(nodeId) ?? {}
    dirtyNodeDataRef.current.set(nodeId, { ...existing, ...patch })
  }, [])

  const clearNodeDirty = useCallback((nodeId: string) => {
    dirtyNodeDataRef.current.delete(nodeId)
  }, [])

  const getDirtyPatch = useCallback(
    (nodeId: string) => dirtyNodeDataRef.current.get(nodeId),
    []
  )

  const currentScope = breadcrumb[breadcrumb.length - 1].pipeline_id

  return (
    <ScopeContext.Provider value={{
      currentScope, breadcrumb, descend, ascendTo, jumpTo, jumpToRoot, renameInPath,
      graphVersion, bumpGraph, markNodeDirty, clearNodeDirty, getDirtyPatch,
    }}>
      {children}
    </ScopeContext.Provider>
  )
}

export function useScope() {
  const ctx = useContext(ScopeContext)
  if (!ctx) throw new Error('useScope must be used within ScopeProvider')
  return ctx
}
