/**
 * The input ports a function node draws, in the order it draws them.
 *
 * The backend owns the order (graph_builder.input_port_order: signature order,
 * whatever feeds each argument) and sends it as `input_ports`. This draws that
 * list as is and never re-orders it. The canvas node and the variant-graph
 * node both call this, so the two cannot drift.
 *
 * Until 2026-10-21 both spread `input_params` then `constant_params`, so a
 * Parameter-fed argument was drawn after every variable-fed one whatever its
 * place in the signature (docs/claude/function-node-input-params.md §4).
 */

export interface InputPortData {
  input_ports?: string[]
  input_params?: Record<string, string>
  constant_params?: string[]
}

export interface InputPort {
  /** The argument name; the handle id is `in__{name}`. */
  name: string
  /** Wired variable type, '' for a Parameter-fed / unwired / PathInput port. */
  type: string
}

export function inputPorts(data: InputPortData): InputPort[] {
  const params = data.input_params ?? {}
  // A node built before the backend's final pass (none in a fetched graph)
  // has no input_ports; fall back to the names it does carry, deduped so two
  // handles never share an id (React Flow would attach edges to only one).
  const names = data.input_ports ?? [
    ...Object.keys(params),
    ...(data.constant_params ?? []).filter(c => !(c in params)),
  ]
  return names.map(name => ({ name, type: params[name] ?? '' }))
}

/**
 * An argument the node does not draw yet (backend graph_builder.port_visibility:
 * defaulted, nothing feeds it), offered behind "+ argument".
 * `default` is a short repr ('' when there is none, `**kwargs` for a catch-all).
 */
export interface OptionalPort {
  name: string
  default: string
}

export function optionalPorts(data: { optional_ports?: OptionalPort[] }): OptionalPort[] {
  return data.optional_ports ?? []
}

/**
 * The picker's list for what the user typed: case-insensitive substring on
 * the name, names that START with it first, otherwise signature order (the
 * order the backend sent). An empty query lists everything.
 */
export function filterOptionalPorts(ports: OptionalPort[], query: string): OptionalPort[] {
  const q = query.trim().toLowerCase()
  if (!q) return ports
  const hits = ports.filter(p => p.name.toLowerCase().includes(q))
  return [
    ...hits.filter(p => p.name.toLowerCase().startsWith(q)),
    ...hits.filter(p => !p.name.toLowerCase().startsWith(q)),
  ]
}

/** One picker row: `sep = ','`, or the bare name when there is no default. */
export function optionalPortLabel(p: OptionalPort): string {
  return p.default ? `${p.name} = ${p.default}` : p.name
}

/** The DOM attribute marking a node's "+ argument" control as a drop target. */
export const OPTIONAL_ARGS_ATTR = 'data-optional-args-node'
