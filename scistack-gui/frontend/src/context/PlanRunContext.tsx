/**
 * PlanRunContext — pending plan-preview request for pipeline runs (R2/G2).
 *
 * Every pipeline run control (function-node "Run until here", pipeline-node
 * "Run") funnels through requestPlan(); the
 * PipelineRunController renders the plan-preview dialog for the pending
 * request and starts the run on confirmation.
 */

import { createContext, useContext, useState } from 'react'

export interface PlanRequest {
  pipeline_id: string
  mode: 'all' | 'until'
  target?: string
  /** Display name for the dialog title and the Runs-tab card. */
  label: string
}

interface PlanRunContextValue {
  planRequest: PlanRequest | null
  requestPlan: (req: PlanRequest) => void
  clearPlan: () => void
}

const PlanRunContext = createContext<PlanRunContextValue | null>(null)

export function PlanRunProvider({ children }: { children: React.ReactNode }) {
  const [planRequest, setPlanRequest] = useState<PlanRequest | null>(null)
  return (
    <PlanRunContext.Provider value={{
      planRequest,
      requestPlan: setPlanRequest,
      clearPlan: () => setPlanRequest(null),
    }}>
      {children}
    </PlanRunContext.Provider>
  )
}

export function usePlanRun() {
  const ctx = useContext(PlanRunContext)
  if (!ctx) throw new Error('usePlanRun must be used within PlanRunProvider')
  return ctx
}
