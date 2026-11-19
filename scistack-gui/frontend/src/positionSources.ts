/**
 * Where each node's position comes from in a canvas refresh — React-free, so
 * `npm test` runs it (tsconfig.test.json).
 *
 * PipelineDAG.fetchPipeline places a node from the screen (already drawn on
 * this canvas), else its saved position, else a fresh dagre layout (no saved
 * key matches its id; layout.applyDagreLayout). The result rides in the
 * canvas-refresh report so the log says which nodes were laid out fresh and
 * which saved keys match no node (2026-11-19: main's nodes shifted after
 * leaving a submodule, and nothing said why).
 */

export interface PositionSources {
  placed_on_screen: number
  placed_saved: number
  placed_dagre: string[]
  saved_unmatched: string[]
}

export function positionSources(
  nodeIds: string[],
  savedPositions: Record<string, { x: number; y: number }>,
  onScreenIds: string[],
): PositionSources {
  const onScreen = new Set(onScreenIds)
  const ids = new Set(nodeIds)
  let placedOnScreen = 0
  let placedSaved = 0
  const placedDagre: string[] = []
  for (const id of nodeIds) {
    if (onScreen.has(id)) placedOnScreen += 1
    else if (savedPositions[id]) placedSaved += 1
    else placedDagre.push(id)
  }
  return {
    placed_on_screen: placedOnScreen,
    placed_saved: placedSaved,
    placed_dagre: placedDagre,
    saved_unmatched: Object.keys(savedPositions).filter(k => !ids.has(k)).sort(),
  }
}
