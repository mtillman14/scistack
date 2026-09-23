/**
 * useSourceEdit — shared submit/error handling for panels that rewrite a
 * declaration in source.
 *
 * Every entity edit goes to the real source file (see
 * docs/claude/entity-editability-model.md), so every edit has failure modes
 * a layout.json write never had: the declaration may live outside the
 * entities file (read-only), or the file may have changed on disk since the
 * GUI read it (stale). Both come back as `{ok: false, reason, ...}` with
 * HTTP 200 — they are expected outcomes carrying structured data, not
 * transport errors.
 *
 * **The failure must be shown, never silently reverted.** Before the
 * update_* endpoints were restored, these panels wired their Save buttons to
 * RPCs that no longer existed; every save quietly no-opped and the field
 * appeared to snap back to its old value, which read as a GUI bug rather
 * than a refused write. That is the specific regression this hook exists to
 * prevent — see SweepSettingsPanel's old docstring.
 */

import { useCallback, useEffect, useState } from 'react'
import { callBackend } from '../../api'
import type { EntityEditability } from './sourceLocation'

export interface SourceEditResult {
  ok?: boolean
  error?: string
  reason?: string
  file?: string
  line?: number | null
  unchanged?: boolean
}

export interface SourceEditState {
  /** Fire an edit. Resolves true only when source was actually written. */
  submit: (method: string, params: Record<string, unknown>) => Promise<boolean>
  /** Human-readable failure, or '' — render this, don't swallow it. */
  error: string
  /** Where a read-only declaration lives, for a "declared in foo.py:42" hint. */
  readOnlyAt: { file: string; line: number | null } | null
  saving: boolean
  clearError: () => void
}

export function useSourceEdit(): SourceEditState {
  const [error, setError] = useState('')
  const [readOnlyAt, setReadOnlyAt] = useState<{ file: string; line: number | null } | null>(null)
  const [saving, setSaving] = useState(false)

  const clearError = useCallback(() => {
    setError('')
    setReadOnlyAt(null)
  }, [])

  const submit = useCallback(async (method: string, params: Record<string, unknown>) => {
    setSaving(true)
    setError('')
    setReadOnlyAt(null)
    try {
      const res = (await callBackend(method, params)) as SourceEditResult
      if (res?.ok === false) {
        setError(res.error || 'The edit was refused.')
        if (res.reason === 'read_only' && res.file) {
          setReadOnlyAt({ file: res.file, line: res.line ?? null })
        }
        return false
      }
      return true
    } catch (err) {
      // A genuine transport/server failure, as opposed to a refused write.
      setError((err as Error).message || 'Request failed')
      return false
    } finally {
      setSaving(false)
    }
  }, [])

  return { submit, error, readOnlyAt, saving, clearError }
}

export type EditableEntityKind = 'parameter' | 'path_input'

/**
 * Ask the backend UP FRONT whether this entity's declaration is writable, so
 * a panel can grey its controls out instead of accepting input that the
 * write then refuses (a source-declared PathInput's root folder used to take
 * typing and only say "declared in foo.m:32" after blur).
 *
 * The answer comes from `target_file_service.entity_editability` — the same
 * function the write path refuses with — so the two can't disagree. `null`
 * until it arrives; callers treat that as editable so a slow backend never
 * locks an editable entity (see sourceLocation.isLockedForEditing). The
 * write-time refusal still stands behind this as the backstop.
 */
export function useEntityEditability(
  kind: EditableEntityKind,
  name: string,
): EntityEditability | null {
  const [state, setState] = useState<EntityEditability | null>(null)

  useEffect(() => {
    let cancelled = false
    setState(null)
    callBackend('get_entity_editability', { kind, name })
      .then(res => { if (!cancelled) setState(res as EntityEditability) })
      .catch(err => {
        // Fail open: the write path still refuses a read-only declaration.
        console.warn(`[useEntityEditability] ${kind} '${name}':`, err)
      })
    return () => { cancelled = true }
  }, [kind, name])

  return state
}
