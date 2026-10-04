/**
 * The undo runtime: one `UndoStack` per surface, and how requests are grouped
 * into undo steps. React-free (the hooks are in `useUndo.ts`) so `npm test`
 * can drive it with a fake transport. See docs/claude/undo-redo.md.
 *
 * - A **surface** is whatever the user perceives as one editor: the pipeline
 *   page (`pageUndo` in api.ts) or one Plot Studio. Each has its own stack,
 *   like each VS Code editor does.
 * - A **change** is one undo step's id. Every undoable request started in
 *   the same event-loop task shares one (`taskChange`): a single user gesture
 *   that sends several requests (deleting a node and its edges) is one step.
 *   A gesture that spans tasks (drop, then re-centre once measured) passes
 *   its change explicitly (`newChange`).
 */

import {
  EMPTY,
  dropNotice,
  dropped,
  movedToFuture,
  movedToPast,
  outcome,
  peekRedo,
  peekUndo,
  pushLocal as pushLocalEntry,
  pushServer as pushServerEntry,
  type LocalEntry,
  type ServerEntry,
  type ServerStatus,
  type Stack,
} from './history.js'

export interface Change {
  id: string
  /** Set by the first undoable request that uses the change. */
  label?: string
}

export type Send = (method: string, params: Record<string, unknown>) => Promise<unknown>

export type Direction = 'undo' | 'redo'

export interface UndoSnapshot {
  canUndo: boolean
  canRedo: boolean
  undoLabel: string | null
  redoLabel: string | null
  busy: boolean
  notice: string
}

function randomId(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto
  if (c?.randomUUID) return c.randomUUID()
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`
}

/** A fresh undo step, for a gesture whose requests span several tasks. */
export function newChange(label?: string): Change {
  return { id: randomId(), label }
}

let taskGroup: Change | null = null

/**
 * The change shared by every request started in the current task. Cleared
 * by a timer, i.e. after this task and its microtasks — so it must be read
 * SYNCHRONOUSLY when a request starts (callBackend does, before any await).
 */
export function taskChange(): Change {
  if (!taskGroup) {
    taskGroup = newChange()
    setTimeout(() => { taskGroup = null }, 0)
  }
  return taskGroup
}

/** How long a notice (a refused undo) stays up. */
export const NOTICE_MS = 8000

export class UndoStack {
  private stack: Stack = EMPTY
  private busy = false
  private notice = ''
  private noticeTimer: ReturnType<typeof setTimeout> | null = null
  private listeners = new Set<() => void>()
  private cached: UndoSnapshot | null = null
  /** Called after a server entry was undone/redone, for views that refetch
   * something no broadcast reaches (Plot Studio re-resolves its figure). */
  onServerApplied: ((entry: ServerEntry, direction: Direction) => void) | null = null

  constructor(private readonly send: Send, readonly name: string) {}

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener)
    return () => { this.listeners.delete(listener) }
  }

  /** Stable between changes, as `useSyncExternalStore` requires. */
  getSnapshot = (): UndoSnapshot => {
    if (!this.cached) {
      const u = peekUndo(this.stack)
      const r = peekRedo(this.stack)
      this.cached = {
        canUndo: !!u,
        canRedo: !!r,
        undoLabel: u?.label ?? null,
        redoLabel: r?.label ?? null,
        busy: this.busy,
        notice: this.notice,
      }
    }
    return this.cached
  }

  /** For tests and diagnostics. */
  get entries(): Stack {
    return this.stack
  }

  private emit(): void {
    this.cached = null
    this.listeners.forEach(l => l())
  }

  private setNotice(text: string): void {
    this.notice = text
    if (this.noticeTimer) clearTimeout(this.noticeTimer)
    this.noticeTimer = text
      ? setTimeout(() => { this.notice = ''; this.noticeTimer = null; this.emit() }, NOTICE_MS)
      : null
    // Under node (tests) a pending notice must not keep the process alive.
    ;(this.noticeTimer as { unref?: () => void } | null)?.unref?.()
    this.emit()
  }

  /** Forget every step — the surface now shows a different document. */
  clear(): void {
    if (this.stack === EMPTY) return
    this.stack = EMPTY
    this.emit()
  }

  pushServer(changeId: string, label: string): void {
    const next = pushServerEntry(this.stack, changeId, label)
    if (next === this.stack) return
    this.stack = next
    console.info(`[undo:${this.name}] recorded`, { changeId, label, depth: next.past.length })
    this.emit()
  }

  pushLocal<T>(entry: LocalEntry<T>): void {
    const next = pushLocalEntry(this.stack, entry)
    if (next === this.stack) return
    this.stack = next
    this.emit()
  }

  undo(): Promise<void> {
    return this.step('undo')
  }

  redo(): Promise<void> {
    return this.step('redo')
  }

  private async step(direction: Direction): Promise<void> {
    // One at a time: a key held down must not send overlapping requests for
    // the same entry. (The backend is idempotent anyway — this keeps the
    // STACK right, which only moves once the backend has answered.)
    if (this.busy) return
    this.busy = true
    this.emit()
    try {
      for (;;) {
        const entry = direction === 'undo' ? peekUndo(this.stack) : peekRedo(this.stack)
        if (!entry) return
        const move = (s: Stack) => (direction === 'undo' ? movedToFuture(s, entry) : movedToPast(s, entry))
        if (entry.kind === 'local') {
          entry.apply(direction === 'undo' ? entry.before : entry.after)
          this.stack = move(this.stack)
          console.info(`[undo:${this.name}] ${direction} local`, { label: entry.label })
          return
        }
        let reply: { status?: ServerStatus; conflicts?: string[] } | null
        try {
          reply = (await this.send(`history_${direction}`, { change_id: entry.changeId })) as typeof reply
        } catch (err) {
          // Transport failure: nothing is known to have happened. Keep the
          // entry so the user can try again.
          this.setNotice(`${direction === 'undo' ? 'Undo' : 'Redo'} failed: ${(err as Error).message}`)
          console.warn(`[undo:${this.name}] ${direction} failed`, { changeId: entry.changeId, err })
          return
        }
        const status: ServerStatus = reply?.status ?? 'unknown'
        console.info(`[undo:${this.name}] ${direction} server`, {
          changeId: entry.changeId, label: entry.label, status,
        })
        const what = outcome(status)
        if (what === 'drop') {
          this.stack = dropped(this.stack, entry)
          this.setNotice(dropNotice(direction, entry.label, status, reply?.conflicts ?? []))
          return
        }
        this.stack = move(this.stack)
        if (what === 'move') {
          if (this.notice) this.setNotice('')
          this.onServerApplied?.(entry, direction)
          return
        }
        // 'skip': nothing visible changed — carry on to the next entry.
      }
    } finally {
      this.busy = false
      this.emit()
    }
  }
}
