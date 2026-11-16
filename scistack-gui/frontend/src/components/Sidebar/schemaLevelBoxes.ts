/**
 * Which schema-level checkboxes the function settings panel shows, and which
 * are disabled. React-free so `npm test` can run it.
 *
 * The panel holds NO rule of its own. The backend owns the rule
 * (`scifor.schema.SchemaTree.offered`, via `get_schema_level`) and sends
 * `offered` (the keys to show) and `disabled` (`{key: reason}`); this module
 * only lays them out. The plan and the cases are in
 * docs/claude/schema-branch-cases.json, which the test reads.
 *
 * An old payload with neither field (a backend that predates branched keys)
 * shows every key, enabled.
 */

export interface LevelBox {
  key: string
  /** The tick would make the selection incoherent; `reason` says why. */
  disabled: boolean
  /** The tooltip for a disabled box. */
  reason?: string
}

export interface OfferedPayload {
  offered?: string[]
  disabled?: Record<string, string>
}

/** The boxes to draw, in dataset order: offered keys only. A ticked key is
 *  never disabled (the user must always be able to untick it). */
export function levelBoxes(
  allKeys: string[],
  payload: OfferedPayload | null | undefined,
  ticked: string[] = [],
): LevelBox[] {
  const offered = payload?.offered
  const disabled = payload?.disabled ?? {}
  return allKeys
    .filter(key => offered === undefined || offered.includes(key))
    .map(key => {
      const reason = disabled[key]
      const isDisabled = reason !== undefined && !ticked.includes(key)
      return isDisabled ? { key, disabled: true, reason } : { key, disabled: false }
    })
}
