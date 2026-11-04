/**
 * The default of every function-node run option, from the backend
 * (`get_run_option_defaults` — read from scidb.for_each's signature in
 * scistack_gui/domain/run_options.py, the one owner). Fetched once per page
 * and shared; `null` until it arrives.
 *
 * The frontend never restates a default: before this, Sidebar.tsx carried
 * `{ dry_run: false, save: true, ... }` as a second copy that could drift
 * from the backend's, and the non-default badges need one answer.
 */

import { useEffect, useState } from 'react'
import { callBackend } from '../api'
import type { RunOptionDefaults } from '../components/Sidebar/sidebarBadges'

let cached: RunOptionDefaults | null = null
let inflight: Promise<RunOptionDefaults> | null = null

function load(): Promise<RunOptionDefaults> {
  if (!inflight) {
    inflight = callBackend('get_run_option_defaults')
      .then(d => {
        cached = d as RunOptionDefaults
        console.debug('[runOptionDefaults] loaded', cached)
        return cached
      })
      .catch(err => {
        // Let a later mount retry rather than caching the failure.
        inflight = null
        throw err
      })
  }
  return inflight
}

export function useRunOptionDefaults(): RunOptionDefaults | null {
  const [defaults, setDefaults] = useState<RunOptionDefaults | null>(cached)
  useEffect(() => {
    if (cached) return
    let cancelled = false
    load()
      .then(d => { if (!cancelled) setDefaults(d) })
      .catch(err => console.error('[runOptionDefaults] failed to load', err))
    return () => { cancelled = true }
  }, [])
  return defaults
}
