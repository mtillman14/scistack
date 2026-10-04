/**
 * The undo stack as pure data — no React, no DOM, no transport — so
 * `npm test` covers every rule. `undo.ts` is the runtime around it.
 *
 * Two kinds of entry share one stack (docs/claude/undo-redo.md):
 *
 * - **server**: a change the backend recorded (`scistack_gui.history`). The
 *   entry is only its id and label; what it changed lives on the backend.
 * - **local**: a change to frontend state (the Plot Studio spec). The entry
 *   holds both values and the function that puts one back.
 *
 * The frontend owns the ORDER (one stack per surface); the backend owns what
 * each server entry changed.
 */

export interface ServerEntry {
  kind: 'server'
  changeId: string
  label: string
}

export interface LocalEntry<T = unknown> {
  kind: 'local'
  label: string
  before: T
  after: T
  /** Puts a value back into the state it came from. */
  apply: (value: T) => void
  /** Entries with the same key arriving within MERGE_WINDOW_MS merge. */
  mergeKey?: string
  /** When the entry last changed (ms), for merging. */
  at: number
}

export type Entry = ServerEntry | LocalEntry

export interface Stack {
  past: Entry[]
  future: Entry[]
}

/** Typing in a field is one step, not one per keystroke. */
export const MERGE_WINDOW_MS = 600
/** Oldest entries fall off beyond this; the backend keeps 500 records. */
export const MAX_ENTRIES = 200

export const EMPTY: Stack = { past: [], future: [] }

function capped(past: Entry[]): Entry[] {
  return past.length > MAX_ENTRIES ? past.slice(past.length - MAX_ENTRIES) : past
}

/**
 * A server change succeeded. The same change id at the top is not pushed
 * twice: several requests of one gesture share an id and are ONE step.
 * Any new change clears the redo side.
 */
export function pushServer(stack: Stack, changeId: string, label: string): Stack {
  const top = stack.past[stack.past.length - 1]
  if (top?.kind === 'server' && top.changeId === changeId) return stack
  // The id may be further down (its gesture interleaved with another): it is
  // still one step, so do not record it again.
  if (stack.past.some(e => e.kind === 'server' && e.changeId === changeId)) return stack
  return { past: capped([...stack.past, { kind: 'server', changeId, label }]), future: [] }
}

/**
 * A local change. Merges into the top entry when it is local, has the same
 * merge key and changed less than MERGE_WINDOW_MS ago: the merged entry keeps
 * the FIRST before and takes the latest after. A change that ends where it
 * began (after === before) leaves no entry at all.
 */
export function pushLocal<T>(stack: Stack, entry: LocalEntry<T>): Stack {
  const top = stack.past[stack.past.length - 1]
  if (
    top?.kind === 'local' &&
    entry.mergeKey !== undefined &&
    top.mergeKey === entry.mergeKey &&
    entry.at - top.at < MERGE_WINDOW_MS
  ) {
    const rest = stack.past.slice(0, -1)
    if (Object.is(top.before, entry.after)) return { past: rest, future: [] }
    const merged: LocalEntry = { ...top, after: entry.after, at: entry.at }
    return { past: [...rest, merged], future: [] }
  }
  if (Object.is(entry.before, entry.after)) return stack
  return { past: capped([...stack.past, entry as LocalEntry]), future: [] }
}

export function peekUndo(stack: Stack): Entry | undefined {
  return stack.past[stack.past.length - 1]
}

export function peekRedo(stack: Stack): Entry | undefined {
  return stack.future[stack.future.length - 1]
}

/*
 * The moves below take the ENTRY, not "the top": a server undo is awaited,
 * and an edit made meanwhile pushes a new top. Moving by identity moves the
 * entry that was actually undone (and an edit clearing `future` meanwhile is
 * simply not undone into).
 */

/** *entry* (just undone) moved from the undo side to the redo side. */
export function movedToFuture(stack: Stack, entry: Entry): Stack {
  if (!stack.past.includes(entry)) return stack
  return { past: stack.past.filter(e => e !== entry), future: [...stack.future, entry] }
}

/** *entry* (just redone) moved from the redo side back to the undo side. */
export function movedToPast(stack: Stack, entry: Entry): Stack {
  if (!stack.future.includes(entry)) return stack
  return { past: capped([...stack.past, entry]), future: stack.future.filter(e => e !== entry) }
}

/** *entry* removed from either side — it could not be applied. */
export function dropped(stack: Stack, entry: Entry): Stack {
  return { past: stack.past.filter(e => e !== entry), future: stack.future.filter(e => e !== entry) }
}

/** The backend's answer to history_undo / history_redo. */
export type ServerStatus = 'ok' | 'noop' | 'empty' | 'unknown' | 'conflict'

/**
 * What the runner does with each answer:
 * - `move`: the entry is now on the other side; the user's key press is done.
 * - `skip`: it moves too, but changed nothing visible — keep going, so one key
 *   press always does something when anything is left.
 * - `drop`: it cannot be applied (the backend forgot it, or the state moved
 *   on since); remove it and tell the user why.
 */
export function outcome(status: ServerStatus): 'move' | 'skip' | 'drop' {
  if (status === 'ok') return 'move'
  if (status === 'noop' || status === 'empty') return 'skip'
  return 'drop'
}

/** Text for a dropped entry. */
export function dropNotice(
  direction: 'undo' | 'redo',
  label: string,
  status: ServerStatus,
  conflicts: string[] = [],
): string {
  const verb = direction === 'undo' ? 'Undo' : 'Redo'
  if (status === 'unknown') {
    return `${verb} of "${label}" is no longer available (the backend restarted or forgot it).`
  }
  const what = conflicts.length ? `: ${conflicts.slice(0, 3).join('; ')}${conflicts.length > 3 ? '…' : ''}` : ''
  return `Cannot ${verb.toLowerCase()} "${label}" — it was changed since${what}`
}
