/**
 * THE colours of a node's run state. One owner: function, variable and
 * submodule (pipeline) nodes all draw their state with these, so green means
 * the same thing on every node. The state itself comes from the backend
 * (`data.run_state`; a submodule's is rolled up from its steps by
 * `domain/run_state.placement_state`).
 */

export type RunState = 'green' | 'pending' | 'red'

export const STATE_STYLES: Record<RunState, { border: string; background: string }> = {
  green:   { border: '#16a34a', background: '#f0fdf4' },
  // Yellow (not orange — orange belongs to path-input nodes): a change
  // staged in the GUI that isn't in the database yet.
  pending: { border: '#eab308', background: '#fefce8' },
  red:     { border: '#dc2626', background: '#fef2f2' },
}
