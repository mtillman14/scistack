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
