/**
 * What StatStudio must ask the user, and what it recommends, read from a
 * compute result. Pure, so the rules are tested (statsActions.test.ts).
 *
 * The backend owns the wording and the choices (scistackstats
 * catalog.needed_action / catalog.recommend); this only collects them.
 */

export interface StratumAction {
  factor: string
  message: string
  choices: string[]
}

export interface Recommended {
  omnibus: string | null
  pairwise: string | null
  evidence: string[]
}

export interface StratumSummary {
  selection?: { why?: string; recommended?: Recommended | null }
  action?: StratumAction | null
}

export interface AnalysisResult {
  id: string
  strata?: StratumSummary[]
}

export interface PendingAction extends StratumAction {
  analysisId: string
}

/** One entry per (analysis, factor) needing a decision, in result order. */
export function pendingActions(results: readonly AnalysisResult[] | undefined): PendingAction[] {
  const out: PendingAction[] = []
  const seen = new Set<string>()
  for (const r of results ?? []) {
    for (const s of r.strata ?? []) {
      if (!s.action) continue
      const key = `${r.id}\u0000${s.action.factor}`
      if (seen.has(key)) continue
      seen.add(key)
      out.push({ analysisId: r.id, ...s.action })
    }
  }
  return out
}

/** The first stratum's recommendation for one analysis, or null. */
export function recommendedFor(results: readonly AnalysisResult[] | undefined, analysisId: string): Recommended | null {
  const r = (results ?? []).find(x => x.id === analysisId)
  for (const s of r?.strata ?? []) {
    if (s.selection?.recommended) return s.selection.recommended
  }
  return null
}
