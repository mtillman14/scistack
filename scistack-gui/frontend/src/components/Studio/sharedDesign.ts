/**
 * The `design_changed` broadcast between a linked figure and table
 * (docs/claude/studio-unification.md, Stage 6). The design itself lives in
 * ONE row set (`scistackplotdb.designs`); this message only says "the
 * shared design of plot X changed, re-read it" (`design_apply`).
 *
 * Every studio tab gets every broadcast of its session, including its own,
 * so each tab tags its messages with an origin and ignores its own. The
 * echo the other tab's apply causes stops in the backend: `design_update`
 * appends nothing when the design is unchanged, and only a change is
 * broadcast.
 *
 * Pure (no React, no DOM): tested under node --test.
 */

export const DESIGN_CHANGED = 'design_changed'

export interface DesignChange {
  plot_id: string
  version: number | null
  origin: string
}

let counter = 0

/** A tab's id in broadcasts: its kind plus something unique to this page. */
export function newDesignOrigin(kind: 'plot' | 'stat'): string {
  counter += 1
  return `${kind}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}-${counter}`
}

/** The change in *msg* when it is about *plotId* and came from another tab. */
export function designChangeFor(
  msg: { method?: string; params?: unknown },
  plotId: string | null | undefined,
  ownOrigin: string,
): DesignChange | null {
  if (msg.method !== DESIGN_CHANGED || !plotId) return null
  const params = (msg.params ?? {}) as Partial<DesignChange>
  if (params.plot_id !== plotId) return null
  if (params.origin === ownOrigin) return null
  return { plot_id: plotId, version: params.version ?? null, origin: params.origin ?? '' }
}
