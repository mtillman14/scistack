/**
 * Expected-locations badges on Variable nodes.
 *
 * PipelineDAG fetches `expectation_badges` for the variables on the canvas
 * (cached server-side against a database fingerprint, so a refetch of an
 * unchanged database is cheap) and refetches on `dag_updated` and
 * `run_done`. VariableNode reads its badge here and opens the popup through
 * `open`. Counts are scidb's (Expectation.tone_counts, own marks).
 */

import { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react'
import { callBackend } from '../api'
import { useBackendMessage } from '../hooks/useBackendMessage'
import type { ExpectedFilter } from '../components/Expectation/ExpectedLocationsPopup'

export interface ExpectationBadge {
  /** Locations whose fact does not match the expectation. */
  problems: number
  /** The variable's own Expected / Not expected marks. */
  marks: number
  error?: string
}

interface Ctx {
  badges: Record<string, ExpectationBadge>
  open: (variable: string, filter: ExpectedFilter) => void
}

const ExpectationBadgesContext = createContext<Ctx>({ badges: {}, open: () => {} })

export const ExpectationBadgesProvider = ExpectationBadgesContext.Provider

export function useExpectationBadge(variable: string) {
  const ctx = useContext(ExpectationBadgesContext)
  return { badge: ctx.badges[variable] as ExpectationBadge | undefined, open: ctx.open }
}

/** The badges for *variables*, kept fresh. */
export function useExpectationBadgeFetch(variables: string[]): Record<string, ExpectationBadge> {
  const [badges, setBadges] = useState<Record<string, ExpectationBadge>>({})
  const key = [...variables].sort().join('\u0000')
  const queryId = useRef(0)

  const fetchBadges = useCallback(() => {
    const names = key ? key.split('\u0000') : []
    if (names.length === 0) { setBadges({}); return }
    const id = ++queryId.current
    callBackend('expectation_badges', { variables: names })
      .then(d => {
        if (id !== queryId.current) return
        setBadges(d as Record<string, ExpectationBadge>)
        console.debug(`[ExpectationBadges] ${names.length} variable(s)`)
      })
      .catch(err => console.debug(`[ExpectationBadges] unavailable: ${(err as Error).message}`))
  }, [key])

  useEffect(() => { fetchBadges() }, [fetchBadges])

  useBackendMessage(useCallback((msg) => {
    const kind = (msg.type ?? msg.method) as string | undefined
    if (kind === 'dag_updated' || kind === 'run_done') fetchBadges()
  }, [fetchBadges]))

  return badges
}
