/**
 * The breadcrumb after stepping into a pipeline.
 *
 * Nesting can't repeat: placing a pipeline refuses a cycle. So a pipeline already
 * on the path means the click came from something out of date (a canvas that
 * hadn't caught up), or from a path `jumpTo` built without one parent holding the
 * next. Going back to the existing crumb gets the user where they asked in every
 * case, and never leaves the click doing nothing.
 *
 * Pure: no React, no RPC. ScopeProvider owns the state and the logging.
 */

export function descendInto<C extends { pipeline_id: string }>(path: C[], crumb: C): C[] {
  const existing = path.findIndex(c => c.pipeline_id === crumb.pipeline_id)
  if (existing < 0) return [...path, crumb]
  if (existing === path.length - 1) return path
  return path.slice(0, existing + 1)
}
