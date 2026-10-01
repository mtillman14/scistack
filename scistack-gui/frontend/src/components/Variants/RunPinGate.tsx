/**
 * The before-run question for pinned variables (plan decision D6).
 *
 * A pin is sticky: a run never moves it. So before a GUI run that would write
 * to a pinned variable, the user chooses:
 *
 * - **Keep pin**: run; new output that differs from the pinned variant is
 *   not the default;
 * - **Move pin to the new output**: run, then pin each such variable's newest
 *   variant (`pin_newest_variant`, which refuses a tie rather than guessing);
 * - **Cancel**: don't run.
 *
 * Which variables a run writes to is the backend's answer
 * (`run_pin_conflicts`, from the same target derivation the run uses). Both
 * run paths share this hook: a node's Run button passes its node id, and a
 * pipeline run passes its plan's step names.
 *
 * A failed check does not block the run. Pins never change what a run
 * computes, only what is the default afterwards, so the cost of missing the
 * question is "the pin did not move", which the popup shows.
 */

import { useCallback, useState } from 'react'
import { createPortal } from 'react-dom'

import { callBackend } from '../../api'
import * as modalStyles from '../modalStyles'
import { conflictVariables, pinConflictMessage } from './variantCards'
import type { PinConflict } from './variantCards'

export interface PinDecision {
  proceed: boolean
  /** Variables whose pin moves to the newest variant once the run succeeds. */
  moveVariables: string[]
}

const NO_CONFLICT: PinDecision = { proceed: true, moveVariables: [] }

export function usePinGate() {
  const [pending, setPending] = useState<{
    conflicts: PinConflict[]
    resolve: (d: PinDecision) => void
  } | null>(null)

  const gate = useCallback(
    async (sources: { node_ids?: string[]; function_names?: string[] }): Promise<PinDecision> => {
      let conflicts: PinConflict[] = []
      try {
        const reply = (await callBackend('run_pin_conflicts', {
          node_ids: sources.node_ids ?? [],
          function_names: sources.function_names ?? [],
        })) as { conflicts: PinConflict[] }
        conflicts = reply.conflicts ?? []
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn('run_pin_conflicts failed; running without the pin question:', err)
        return NO_CONFLICT
      }
      if (conflicts.length === 0) return NO_CONFLICT
      return new Promise<PinDecision>(resolve => setPending({ conflicts, resolve }))
    },
    [],
  )

  const finish = (decision: PinDecision) => {
    pending?.resolve(decision)
    setPending(null)
  }

  const dialog = pending
    ? createPortal(
        <div style={modalStyles.overlay} role="dialog" aria-modal="true">
          <div style={{ ...modalStyles.dialog, width: 520 }}>
            <div style={modalStyles.dialogTitle}>Pinned variable{conflictVariables(pending.conflicts).length === 1 ? '' : 's'}</div>
            <div style={styles.text}>{pinConflictMessage(pending.conflicts)}</div>
            {conflictVariables(pending.conflicts).map(v => {
              const pin = pending.conflicts.find(c => c.variable === v)?.pin
              return (
                <div key={v} style={styles.dim}>
                  {v}: pinned {pin?.pinned_at}
                  {pin?.reason ? ` (“${pin.reason}”)` : ''}
                </div>
              )
            })}
            <div style={styles.footer}>
              <button type="button" style={styles.button} onClick={() => finish({ proceed: false, moveVariables: [] })}>
                Cancel
              </button>
              <button
                type="button"
                style={styles.button}
                onClick={() => finish({ proceed: true, moveVariables: conflictVariables(pending.conflicts) })}
                title="After the run succeeds, the newest variant of each pinned variable becomes current"
              >
                Move pin to the new output
              </button>
              <button type="button" style={styles.primary} onClick={() => finish({ proceed: true, moveVariables: [] })}>
                Keep pin
              </button>
            </div>
          </div>
        </div>,
        document.body,
      )
    : null

  return { gate, dialog }
}

/** Apply a "Move pin to the new output" decision once the run has succeeded. */
export async function movePinsAfterRun(variables: string[], runId: string): Promise<void> {
  for (const variable of variables) {
    try {
      await callBackend('pin_newest_variant', {
        variable,
        reason: `moved to the new output of run ${runId}`,
      })
    } catch (err) {
      // eslint-disable-next-line no-console
      console.warn(`pin_newest_variant(${variable}) failed; the pin stays where it was:`, err)
    }
  }
}

const styles: Record<string, React.CSSProperties> = {
  text: { fontSize: 12, color: '#ccc', lineHeight: 1.5, marginBottom: 8 },
  dim: { fontSize: 11, color: '#9a9ab0' },
  footer: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 14 },
  button: {
    padding: '5px 12px', background: '#2a2a4a', color: '#ccc', border: '1px solid #3a3a5a',
    borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
  primary: {
    padding: '5px 12px', background: '#164e63', color: '#a5f3fc', border: '1px solid #0891b2',
    borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
}
