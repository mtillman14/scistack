/**
 * The Inputs section's ColName rules — pure, so they are unit-tested.
 *
 * A parameter with no variable feeding it (an `n/a` row) may instead receive
 * the NAME of the column a "Run once per column" input is on, as `ColName()`
 * does in code. Stored as the node's `colNameInputs` (the backend's `colname`
 * intent aspect, owned by `scidb.intent.normalize_colname_inputs`).
 */

/** The shape of one column pick, as far as these rules need it. */
export interface IterateFlag {
  iterate: boolean
}

/** Does any input run once per column? Without one a ColName parameter has
 *  nothing to resolve against (scifor refuses the run), so the dropdown is
 *  offered only then. */
export function hasIterateInput(selections: Record<string, IterateFlag | undefined>): boolean {
  return Object.values(selections).some(sel => sel?.iterate)
}

/** `current` with `param` set to ColName (`on`) or back to n/a — sorted and
 *  without repeats, the same spelling the backend normalizes to, so a
 *  refetch never reorders the list under the user. */
export function withColName(current: readonly string[], param: string, on: boolean): string[] {
  const others = current.filter(p => p !== param)
  return on ? [...others, param].sort() : others
}

/** The ColName list to save alongside a column-selection change: emptied
 *  when the change leaves no input iterating, so no statement survives that
 *  the next run would only ignore. `null` means "unchanged". */
export function colNamesAfterSelectionChange(
  current: readonly string[],
  nextSelections: Record<string, IterateFlag | undefined>,
): string[] | null {
  if (current.length > 0 && !hasIterateInput(nextSelections)) return []
  return null
}
