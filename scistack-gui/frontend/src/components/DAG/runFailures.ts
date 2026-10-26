/**
 * Per-node failure state for the canvas: the red badge on a function node and
 * the failures panel it opens.
 *
 * The wire shape is scifor's failure record (scifor/failures.py →
 * FailureGroup.to_dict), grouped by the backend
 * (scistack_gui/services/run_failures.py):
 * `run_done.failures = {by_node: {nodeId: [group]}, other: [group]}`.
 * `by_node` names EVERY node the run covered — an empty list means the node
 * ran clean — so `applyRunDone` only ASSIGNS; it never decides which nodes a
 * rerun replaces. See docs/claude/error-surfacing.md.
 */

export interface FailureFrame {
  file: string
  line: number
  function: string
  code: string
  origin: 'user' | 'framework' | 'library'
}

export interface FailureGroup {
  reason: string
  fn_name: string
  exc_type: string
  message: string
  no_data: boolean
  count: number
  combos: string[]
  user_frame: FailureFrame | null
  node_id?: string | null
  target?: string
  example?: { stage?: string; frames?: FailureFrame[]; metadata?: Record<string, string> }
}

export interface RunFailuresWire {
  by_node: Record<string, FailureGroup[]>
  other: FailureGroup[]
}

/** The small per-combo form a `combo_skip` progress event carries. */
export interface SkipFailure {
  reason: string
  no_data: boolean
  user_frame: FailureFrame | null
}

export interface NodeFailures {
  /** Final groups from run_done; empty while a run is still live. */
  groups: FailureGroup[]
  /** Failed combos counted live from combo_skip, until run_done replaces it. */
  liveCount: number
  stopped: boolean
}

export type FailureState = Record<string, NodeFailures>

/** Key for groups the run could not attach to a node. */
export const OTHER_KEY = ''

export function readRunFailures(raw: unknown): RunFailuresWire {
  const value = (raw ?? {}) as Partial<RunFailuresWire>
  return {
    by_node: value.by_node && typeof value.by_node === 'object' ? value.by_node : {},
    other: Array.isArray(value.other) ? value.other : [],
  }
}

/** A run of this node started: its old badge goes. */
export function clearNode(state: FailureState, nodeId: string): FailureState {
  if (!(nodeId in state)) return state
  const next = { ...state }
  delete next[nodeId]
  return next
}

/** A combo failed mid-run: bump the live count (no-data is not a failure). */
export function noteSkip(state: FailureState, nodeId: string, failure: SkipFailure | null | undefined): FailureState {
  if (!failure || failure.no_data) return state
  const prev = state[nodeId] ?? { groups: [], liveCount: 0, stopped: false }
  return { ...state, [nodeId]: { ...prev, liveCount: prev.liveCount + 1 } }
}

/** run_done: every covered node is assigned its final groups. */
export function applyRunDone(state: FailureState, failures: RunFailuresWire, stopped: boolean): FailureState {
  const next = { ...state }
  const assign = (key: string, groups: FailureGroup[]) => {
    const failed = groups.filter(g => !g.no_data)
    if (failed.length) next[key] = { groups: failed, liveCount: 0, stopped }
    else delete next[key]
  }
  for (const [nodeId, groups] of Object.entries(failures.by_node)) assign(nodeId, groups)
  assign(OTHER_KEY, failures.other)
  return next
}

/** What the badge shows: final count once known, else the live count. */
export function failedCount(entry: NodeFailures | undefined): number {
  if (!entry) return 0
  if (entry.groups.length) return entry.groups.reduce((n, g) => n + (g.count || 0), 0)
  return entry.liveCount
}

export type FrameSegment =
  | { kind: 'frame'; frame: FailureFrame }
  | { kind: 'collapsed'; frames: FailureFrame[] }

/**
 * User frames are shown; each run of consecutive framework/library frames
 * collapses into one expandable segment.
 */
export function segmentFrames(frames: FailureFrame[]): FrameSegment[] {
  const out: FrameSegment[] = []
  for (const frame of frames) {
    if (frame.origin === 'user') {
      out.push({ kind: 'frame', frame })
      continue
    }
    const last = out[out.length - 1]
    if (last && last.kind === 'collapsed') last.frames.push(frame)
    else out.push({ kind: 'collapsed', frames: [frame] })
  }
  return out
}
