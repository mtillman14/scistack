/**
 * The Activity Bar "SciStack" tree's rules (GitHub #28), with no vscode
 * import so `npm test` runs them.
 *
 * Data: the step verdicts already built for the canvas. The extension host
 * relays every `get_pipeline` reply it forwards to the webview
 * (dagPanel.ts), so no endpoint is added (grug: keep the endpoint count down).
 * Every word is scidb's (`node.data.verdict`, attached by
 * services/step_status_service); this module orders steps and shapes rows.
 */

export interface WireVerdict {
  word: string
  glyph: string
  label: string
  status_line: string
  chip: string
  new_chip: string
  reasons: { kind: string; count: number | null; text: string; locations?: Record<string, string>[] }[]
  needs_running: boolean
}

export interface WireNode {
  id: string
  type: string
  data: {
    label?: string
    output_types?: string[]
    verdict?: WireVerdict
    outdated_variants?: number
    total_records?: number
  }
}

export interface WireEdge { source: string; target: string }

export interface WireExclusion {
  keys: Record<string, string>
  reason: string
  changed_at: string | null
  variable: string | null
}

export interface GraphPayload {
  nodes?: WireNode[]
  edges?: WireEdge[]
  pipeline_id?: string
  exclusions?: WireExclusion[]
}

export interface StepRow {
  nodeId: string
  label: string
  description: string
  tooltip: string
  glyph: string
  needsRunning: boolean
  reasons: { kind: string; text: string }[]
  outputs: { variable: string; description: string }[]
}

/** Function nodes in pipeline order: a topological sort over the
 *  function → variable → function edges, ties by label. */
export function stepsInOrder(graph: GraphPayload): WireNode[] {
  const nodes = graph.nodes ?? []
  const fns = nodes.filter(n => n.type === 'functionNode')
  const byId = new Map(nodes.map(n => [n.id, n]))
  // fn -> fn edges through a variable.
  const producers = new Map<string, Set<string>>() // variable id -> fn ids
  const consumers = new Map<string, Set<string>>()
  for (const e of graph.edges ?? []) {
    const s = byId.get(e.source), t = byId.get(e.target)
    if (!s || !t) continue
    if (s.type === 'functionNode') {
      if (!producers.has(t.id)) producers.set(t.id, new Set())
      producers.get(t.id)!.add(s.id)
    }
    if (t.type === 'functionNode') {
      if (!consumers.has(s.id)) consumers.set(s.id, new Set())
      consumers.get(s.id)!.add(t.id)
    }
  }
  const upstream = new Map<string, Set<string>>(fns.map(f => [f.id, new Set<string>()]))
  for (const [varId, cons] of consumers) {
    for (const c of cons) {
      for (const p of producers.get(varId) ?? []) if (p !== c) upstream.get(c)?.add(p)
    }
  }
  const out: WireNode[] = []
  const done = new Set<string>()
  const remaining = [...fns].sort((a, b) => (a.data.label ?? '').localeCompare(b.data.label ?? ''))
  while (remaining.length) {
    const i = remaining.findIndex(f => [...(upstream.get(f.id) ?? [])].every(u => done.has(u)))
    const next = remaining.splice(i >= 0 ? i : 0, 1)[0] // a cycle: take the first
    done.add(next.id)
    out.push(next)
  }
  return out
}

export function stepRows(graph: GraphPayload): StepRow[] {
  const vars = new Map((graph.nodes ?? []).filter(n => n.type === 'variableNode').map(n => [n.data.label ?? '', n]))
  return stepsInOrder(graph).map(n => {
    const v = n.data.verdict
    const chips = [v?.chip, v?.new_chip].filter(Boolean).join(' · ')
    return {
      nodeId: n.id,
      label: n.data.label ?? n.id,
      glyph: v?.glyph ?? '○',
      description: v ? `${v.glyph} ${v.label}${chips ? ` — ${chips}` : ''}` : 'no verdict yet',
      tooltip: v?.status_line ?? 'Not judged yet (open or refresh the canvas).',
      needsRunning: v?.needs_running ?? true,
      reasons: (v?.reasons ?? []).map(r => ({ kind: r.kind, text: r.text })),
      outputs: (n.data.output_types ?? []).map(variable => {
        const vn = vars.get(variable)
        const outdated = vn?.data.outdated_variants ?? 0
        const records = vn?.data.total_records
        return {
          variable,
          description: [
            records !== undefined ? `${records} record(s)` : '',
            outdated ? `${outdated} outdated variant(s)` : '',
          ].filter(Boolean).join(' · '),
        }
      }),
    }
  })
}

export function exclusionRows(graph: GraphPayload): { label: string; description: string }[] {
  return (graph.exclusions ?? []).map(e => ({
    label: Object.entries(e.keys).map(([k, v]) => `${k}=${v}`).join(', ') + (e.variable ? ` (${e.variable})` : ''),
    description: `${e.reason}${e.changed_at ? ` · ${e.changed_at.slice(0, 16).replace('T', ' ')}` : ''}`,
  }))
}
