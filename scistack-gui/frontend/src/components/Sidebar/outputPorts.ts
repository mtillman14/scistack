/**
 * A Python function node's declared output ports, as the Outputs section
 * edits them (.claude/plan-python-output-ports.md). Pure helpers; whether a
 * name is valid is the backend's answer (`set_output_ports`), and which
 * variable a port saves to is its edge's.
 *
 * Port i is return value i, so the list order IS the outputs= order.
 */

/** The ports to show: the declared list, else the node's current output
 *  handles, else the single default port. */
export function currentPorts(declared: string[] | undefined, handles: string[] | undefined): string[] {
  if (declared && declared.length > 0) return [...declared]
  if (handles && handles.length > 0) return [...handles]
  return ['output']
}

/** A name not yet used: output, output_2, output_3, ... */
export function newPortName(ports: string[]): string {
  if (!ports.includes('output')) return 'output'
  let i = 2
  while (ports.includes(`output_${i}`)) i += 1
  return `output_${i}`
}

export function movePort(ports: string[], i: number, delta: number): string[] {
  const j = i + delta
  if (i < 0 || i >= ports.length || j < 0 || j >= ports.length) return ports
  const next = [...ports]
  ;[next[i], next[j]] = [next[j], next[i]]
  return next
}

/** The list and the `{old: new}` rename the backend applies to the port's
 *  edges, so a renamed port keeps its connection. */
export function renamePort(ports: string[], i: number, name: string): { ports: string[]; renames: Record<string, string> } {
  const old = ports[i]
  const trimmed = name.trim()
  if (old === undefined || !trimmed || trimmed === old) return { ports, renames: {} }
  return { ports: ports.map((p, k) => (k === i ? trimmed : p)), renames: { [old]: trimmed } }
}
