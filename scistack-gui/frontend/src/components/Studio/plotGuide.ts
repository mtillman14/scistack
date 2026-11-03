/**
 * Plot Studio's step-by-step view (docs/claude/studio-unification.md, Stage 5).
 *
 * The screens a figure shares with a StatStudio table come from the backend
 * (`stats_plot_guide`: scistackstats.guide.plot_guide, the same builders the
 * table's wizard uses). The figure reuses StatStudio's `GuidedStudio` and its
 * navigation rules (`guideSteps.ts`), which work on a StatsSpec. This module
 * is the adapter: the figure's spec dressed as one (`asGuideSpec`), and a
 * wizard edit turned back into the figure's own role edits (`roleChanges`),
 * so Plot Studio's grouping-order rule (`groups.placeGroupLayer`) still
 * places each new layer.
 *
 * Pure (no React, no DOM): tested under node --test.
 */

import type { GuideState, StatsSpec } from '../StatStudio/statsSpec.js'

export type StudioView = 'guided' | 'advanced'

/** Per viewer: the view the last fresh figure was left in. */
export const PLOT_VIEW_STORAGE_KEY = 'scistack.plotStudio.view'

export const NEW_GUIDE: GuideState = { mode: 'guided', done: [] }

/**
 * The view a figure opens in. A saved plot, and a `plot_` node's own figure,
 * open on every setting (as a saved StatStudio table reopens in its view);
 * a fresh figure opens the way this viewer left the last one, step by step
 * the first time.
 */
export function initialView(stored: string | null, opts: { plotNode?: boolean }): StudioView {
  if (opts.plotNode) return 'advanced'
  return stored === 'advanced' ? 'advanced' : 'guided'
}

/** The figure's spec as the wizard's StatsSpec (only `design` and `guide` are read). */
export function asGuideSpec(plotSpec: Record<string, unknown>, guide: GuideState): StatsSpec {
  return {
    format: 1,
    design: plotSpec as unknown as StatsSpec['design'],
    dvs: [],
    descriptives: { cell: 'mean_sd', statistics: [] },
    comparisons: [],
    variant_mode: 'pinned',
    guide,
  }
}

/** The roles a wizard edit changed, in factor order, for the figure to apply. */
export function roleChanges(
  before: Record<string, string> | undefined,
  after: Record<string, string> | undefined,
): { factor: string; role: string }[] {
  const out: { factor: string; role: string }[] = []
  for (const [factor, role] of Object.entries(after ?? {})) {
    if ((before ?? {})[factor] !== role) out.push({ factor, role })
  }
  return out
}
