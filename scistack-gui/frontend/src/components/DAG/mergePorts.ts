/**
 * The input ports a Merge node draws (docs/claude/merge-node.md).
 *
 * Ports are SLOTS, `in__m{n}`. A slot id is never reused, so disconnecting a
 * middle port renames no stored edge: the remaining slots simply close up on
 * screen (they are drawn in ascending order). There is always exactly one
 * empty trailing slot, and a fresh node shows two.
 *
 * The backend reads join order the same way: ascending slot number
 * (edge_resolver.merge_constituent_edges).
 */

export const MERGE_SLOT_PREFIX = 'in__m'
export const MERGE_OUT_HANDLE = 'out__merge'

export function mergeSlotHandle(n: number): string {
  return `${MERGE_SLOT_PREFIX}${n}`
}

/** The slot number of a handle, or null if it is not a Merge slot. */
export function mergeSlotOf(handle: string | null | undefined): number | null {
  if (!handle || !handle.startsWith(MERGE_SLOT_PREFIX)) return null
  const tail = handle.slice(MERGE_SLOT_PREFIX.length)
  return /^\d+$/.test(tail) ? Number(tail) : null
}

export interface MergePort {
  slot: number
  handle: string
  connected: boolean
}

/** Connected slots in order, then empty ones until there are at least two
 * ports and exactly one empty port at the end. */
export function mergePorts(connectedSlots: Iterable<number>): MergePort[] {
  const connected = [...new Set(connectedSlots)].sort((a, b) => a - b)
  const ports: MergePort[] = connected.map(slot => ({ slot, handle: mergeSlotHandle(slot), connected: true }))
  let next = connected.length ? connected[connected.length - 1] + 1 : 0
  do {
    ports.push({ slot: next, handle: mergeSlotHandle(next), connected: false })
    next += 1
  } while (ports.length < 2)
  return ports
}
