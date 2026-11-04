/**
 * Which part of the breadcrumb still exists.
 *
 * Undo can remove the pipeline you are looking at: extract to a submodule,
 * open it, then undo. The path is cut at the first pipeline that is gone, so
 * you land on the nearest parent still there. A pipeline below a missing one
 * goes too, since the path that reached it no longer exists.
 *
 * Pure: no React, no RPC. ScopeProvider supplies the known pipelines.
 */

export function survivingBreadcrumb<C extends { pipeline_id: string }>(
  breadcrumb: C[],
  known: ReadonlySet<string>,
  root: C,
): C[] {
  const missing = breadcrumb.findIndex(c => !known.has(c.pipeline_id))
  if (missing < 0) return breadcrumb
  return missing === 0 ? [root] : breadcrumb.slice(0, missing)
}
