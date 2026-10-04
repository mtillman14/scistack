/**
 * React glue for the undo runtime (`undo.ts`). docs/claude/undo-redo.md.
 *
 * - `useUndoSnapshot(stack)`: what the Undo/Redo buttons show.
 * - `useUndoShortcuts(stack)`: Cmd/Ctrl+Z, Cmd/Ctrl+Shift+Z, Ctrl+Y. Only the
 *   most recently mounted surface answers, so a Plot Studio modal shadows the
 *   canvas under it. Never inside a text field: those keep native text undo.
 * - `useHistoryState(stack, initial, label)`: `useState` whose user edits are
 *   undo steps; `replace` sets a value WITHOUT one (loading, not editing).
 */

import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react'
import type { UndoSnapshot, UndoStack } from '../undo'

export function useUndoSnapshot(stack: UndoStack): UndoSnapshot {
  return useSyncExternalStore(stack.subscribe, stack.getSnapshot)
}

// The surfaces that want the shortcuts, oldest first; the last one answers.
const owners: UndoStack[] = []
let listening = false

function isTextField(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null
  if (!el) return false
  const tag = el.tagName
  return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || el.isContentEditable
}

function onKeyDown(e: KeyboardEvent): void {
  const stack = owners[owners.length - 1]
  if (!stack || !(e.metaKey || e.ctrlKey) || e.altKey) return
  if (isTextField(e.target) || isTextField(document.activeElement)) return
  const key = e.key.toLowerCase()
  let direction: 'undo' | 'redo' | null = null
  if (key === 'z') direction = e.shiftKey ? 'redo' : 'undo'
  else if (key === 'y' && e.ctrlKey && !e.metaKey) direction = 'redo'
  if (!direction) return
  e.preventDefault()
  void (direction === 'undo' ? stack.undo() : stack.redo())
}

export function useUndoShortcuts(stack: UndoStack, enabled = true): void {
  useEffect(() => {
    if (!enabled) return
    owners.push(stack)
    if (!listening) {
      window.addEventListener('keydown', onKeyDown)
      listening = true
    }
    return () => {
      const i = owners.lastIndexOf(stack)
      if (i >= 0) owners.splice(i, 1)
    }
  }, [stack, enabled])
}

type Updater<T> = T | ((prev: T) => T)

/**
 * `useState`, plus every change through the returned setter becomes a local
 * undo step on *stack* (consecutive edits within the merge window are one
 * step — typing). Undo/redo put the value back through the same state, so
 * every effect that follows the value sees an ordinary change.
 */
export function useHistoryState<T>(
  stack: UndoStack,
  initial: T,
  label: string,
): [T, (update: Updater<T>) => void, (value: T) => void] {
  const [value, setValue] = useState<T>(initial)
  // The value the NEXT updater sees: updaters run here, synchronously, so
  // two calls in one tick chain like React's own queue does.
  const current = useRef<T>(initial)

  const apply = useCallback((v: T) => {
    current.current = v
    setValue(v)
  }, [])

  const set = useCallback((update: Updater<T>) => {
    const before = current.current
    const after = typeof update === 'function' ? (update as (prev: T) => T)(before) : update
    if (Object.is(after, before)) return
    apply(after)
    stack.pushLocal({ kind: 'local', label, before, after, apply, mergeKey: label, at: Date.now() })
  }, [stack, label, apply])

  return [value, set, apply]
}
