/**
 * Where a factor sits in the grouping list — INNERMOST FIRST.
 *
 * The mirror of `PlotSpec.ordered_groups` (scistackplot/spec.py), and the
 * reason it is a module of its own rather than three lines inside a
 * `useCallback`: the panel writes `spec.groups` on every role change, so the
 * placement it chooses is the placement the figure gets — Python's rule only
 * ever sees an already-full list and never fires. A rule that decides what the
 * user sees needs a test, and `npm test` can only run React-free modules.
 *
 * **The rule is depth, and depth comes from Python.** `FactorInfo.depth` is
 * "how many schema keys pin one value of this factor" (`table.py`), published
 * through `describe()`; nothing here invents it. So what is duplicated is a
 * sort, not a policy — and `test_grouping_layers.py` pins the Python half
 * against the same cases this file's test pins.
 *
 * The list runs innermost first (2026-09-17): the first entry is the mark's
 * own identity — the bars inside a tick, the spaghetti lines — and each later
 * entry wraps around it. So a DEEPER key goes earlier, and a factor with no
 * depth at all (a variant axis, a derived bucket — not a place in the
 * hierarchy) goes first: "v1 against v2 inside each subject".
 */

/** How deep a factor nests, with "no depth" DEEPEST — `spec.depth_rank`'s
 * twin, read the same way round: bigger sorts earlier in an innermost-first
 * list. */
function deepness(depth: number | null | undefined): number {
  return depth === null || depth === undefined ? Number.POSITIVE_INFINITY : depth
}

/**
 * Insert `factor` into `layers` by depth, without moving anything already there.
 *
 * It lands before the first layer nested strictly SHALLOWER than it, so a
 * subject-level grouping wraps the sessions rather than sitting inside them.
 * Nothing else moves: a user who reordered the list with the arrows has made
 * a decision, and a new layer arriving is not a reason to re-sort around it.
 */
export function placeGroupLayer(
  layers: string[],
  factor: string,
  depths: Record<string, number | null | undefined>
): string[] {
  if (layers.includes(factor)) return layers
  const own = deepness(depths[factor])
  const at = layers.findIndex(name => deepness(depths[name]) < own)
  if (at < 0) return [...layers, factor]
  return [...layers.slice(0, at), factor, ...layers.slice(at)]
}

/**
 * The layers as the FIGURE will nest them — `ordered_groups`, in TypeScript.
 *
 * Membership is who holds `group`; `declared` is only the order. Names that no
 * longer group drop out, and holders the order never mentioned (a spec from
 * `default_spec`, or a saved endpoint) are placed by depth rather than appended
 * — the same second half Python does, so the control cannot show an order the
 * renderer disagrees with.
 */
export function orderGroups(
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
    const left = deepness(depths[a])
    const right = deepness(depths[b])
    return left === right ? 0 : left > right ? -1 : 1
  })
  return [...ordered, ...rest]
}
