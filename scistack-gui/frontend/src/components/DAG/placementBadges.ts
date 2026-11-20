/**
 * The badges a submodule placement draws (GitHub #27). The backend decides
 * every fact (scope_service.build_pipeline_nodes); this only turns them into
 * badge text. Rule: docs/claude/submodule-ports.md, "A placement's badges".
 *
 * - 'port': a port of this placement has no line on this canvas (error).
 * - 'inside': steps inside are disconnected. A warning while something inside
 *   still runs from a start to an output port (ports.runnable_routes), an
 *   error when nothing does.
 */

export interface PlacementBadgeData {
  open_ports?: { inputs: string[]; outputs: string[] }
  disconnected_members?: string[]
  disconnected_severity?: 'warning' | 'error'
}

export interface PlacementBadge {
  kind: 'port' | 'inside'
  severity: 'warning' | 'error'
  text: string
  title: string
}

export function placementBadges(data: PlacementBadgeData): PlacementBadge[] {
  const badges: PlacementBadge[] = []
  const inputs = data.open_ports?.inputs ?? []
  const outputs = data.open_ports?.outputs ?? []
  if (inputs.length || outputs.length) {
    const lines = [
      ...(inputs.length ? [`input(s): ${inputs.join(', ')}`] : []),
      ...(outputs.length ? [`output(s): ${outputs.join(', ')}`] : []),
    ]
    badges.push({
      kind: 'port',
      severity: 'error',
      text: `🔌 ${inputs.length + outputs.length} port(s) not connected`,
      title: `Nothing is connected to these ports on this canvas:\n${lines.join('\n')}`,
    })
  }
  const members = data.disconnected_members ?? []
  if (members.length) {
    const severity = data.disconnected_severity ?? 'error'
    badges.push({
      kind: 'inside',
      severity,
      text: `${severity === 'warning' ? '⚠' : '🔌'} ${members.length} step(s) disconnected inside`,
      title:
        `Disconnected inside: ${members.join(', ')}\n`
        + (severity === 'warning'
          ? 'Other steps still run from an input to an output port.'
          : 'Nothing inside runs from an input to an output port.'),
    })
  }
  return badges
}
