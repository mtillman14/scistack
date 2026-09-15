/**
 * Where a factor sits on a nested x axis.
 *
 * The mirror of `PlotSpec.ordered_x_layers` (scistackplot/spec.py), and the
 * reason it is a module of its own rather than three lines inside a
 * `useCallback`: the panel writes `spec.x_layers` on every role change, so the
 * placement it chooses is the placement the figure gets — Python's rule only
 * ever sees an already-full list and never fires. A rule that decides what the
 * user sees needs a test, and `npm test` can only run React-free modules.
 *
 * **The rule is depth, and depth comes from Python.** `FactorInfo.depth` is
 * "how many schema keys pin one value of this factor" (`table.py`), published
 * through `describe()`; nothing here invents it. So what is duplicated is a
 * sort, not a policy — and `test_new_layers_are_placed_by_depth` in
 * `scistackplot/tests/test_x_nesting.py` pins the Python half against the same
 * cases this file's test pins.
 *
 * Before this, a factor newly given X was appended, i.e. placed INNERMOST. So
 * ticking a subject-level `InterventionGroup` beside `session` produced one bar
 * per group inside each session — the transpose of what people ask for, and
 * reachable only by finding the ↑ button.
 */

/** Sort key for a depth, with "no depth" last — `spec._depth_rank`'s twin.
 *
 * A variant axis or a derived bucket has no depth: it is not a place in the
 * hierarchy at all, so it cannot be claimed to sit outside a subject. */
function depthRank(depth: number | null | undefined): number {
  return depth === null || depth === undefined ? Number.POSITIVE_INFINITY : depth
}

/**
 * Insert `factor` into `layers` by depth, without moving anything already there.
 *
 * It lands before the first layer nested strictly DEEPER than it, so a
 * subject-level grouping clusters the sessions inside it. Nothing else moves:
 * a user who reordered the list with the arrows has made a decision, and a new
 * layer arriving is not a reason to re-sort around it.
 */
export function placeXLayer(
  layers: string[],
  factor: string,
  depths: Record<string, number | null | undefined>
): string[] {
  if (layers.includes(factor)) return layers
  const own = depthRank(depths[factor])
  const at = layers.findIndex(name => depthRank(depths[name]) > own)
  if (at < 0) return [...layers, factor]
  return [...layers.slice(0, at), factor, ...layers.slice(at)]
}

/**
 * The layers as the FIGURE will nest them — `ordered_x_layers`, in TypeScript.
 *
 * Membership is who holds `Role.X`; `declared` is only the order. Names that no
 * longer hold X drop out, and holders the order never mentioned (a spec from
 * `default_spec`, or a saved endpoint) are placed by depth rather than appended
 * — the same second half Python does, so the control cannot show an order the
 * renderer disagrees with.
 */
export function orderXLayers(
  declared: string[],
  holders: string[],
  depths: Record<string, number | null | undefined>
): string[] {
  const ordered = declared.filter(name => holders.includes(name))
  const rest = holders.filter(name => !ordered.includes(name))
  // Stable, like Python's: equal depths keep declaration order rather than
  // swapping whenever the roles object is rebuilt.
  //
  // Compared, not subtracted: two depthless factors are both Infinity and
  // `Infinity - Infinity` is NaN, which leaves a sort's order unspecified.
  rest.sort((a, b) => {
    const left = depthRank(depths[a])
    const right = depthRank(depths[b])
    return left === right ? 0 : left < right ? -1 : 1
  })
  return [...ordered, ...rest]
}
