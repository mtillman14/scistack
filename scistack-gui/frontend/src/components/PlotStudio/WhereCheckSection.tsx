/**
 * The where= check's controls (issue #13): a banner saying what is being
 * checked, one row per filter (tick, comparison, value) and Apply / Discard.
 *
 * Shown only when the figure carries `spec.where_check`; PlotStudioRail's Data
 * group mounts it and nothing else about the rail knows a check exists. The
 * figure redraws on every committed edit because the edit is an ordinary spec
 * change (`editWhereCheck`); the verdicts are the backend's.
 *
 * Apply writes the TICKED rows to the node (`set_where_filters`) and closes
 * the tab; Discard just closes it. Nothing is stored until Apply.
 */

import { useEffect, useState } from 'react'
import { callBackend, isVSCodeMode } from '../../api'
import type { Spec } from './plotStudioTypes'
import type { PlotSpecAction } from './plotSpecReducer'
import { rowsToApply } from './whereCheck'
import { styles } from './plotStudioStyles'

const OPS = ['==', '!=', '<', '<=', '>', '>=']

function ValueInput({ value, onCommit }: { value: string; onCommit: (value: string) => void }) {
  // Committed on blur / Enter, not per keystroke: each commit is a resolve.
  const [draft, setDraft] = useState(value)
  useEffect(() => setDraft(value), [value])
  const commit = () => { if (draft !== value && draft.trim() !== '') onCommit(draft) }
  return (
    <input
      style={{ ...styles.select, width: 80 }}
      value={draft}
      onChange={e => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={e => { if (e.key === 'Enter') commit() }}
    />
  )
}

export default function WhereCheckSection({
  spec,
  dispatchSpec,
  onClose,
  onNotice,
}: {
  spec: Spec | null
  dispatchSpec: (action: PlotSpecAction) => void
  onClose: () => void
  onNotice: (text: string) => void
}) {
  const check = spec?.where_check
  const [applying, setApplying] = useState(false)
  if (!check || !(check.filters ?? []).length) return null
  const rows = check.filters ?? []
  const ticked = rows.filter(r => r.enabled !== false).length

  const apply = async () => {
    if (!check.node_id) return
    setApplying(true)
    const filters = rowsToApply(check)
    console.info('[where_check] apply', { node: check.node_id, filters })
    try {
      await callBackend('set_where_filters', { node_id: check.node_id, filters })
      onClose()
    } catch (err) {
      console.error('[where_check] apply failed', err)
      onNotice(`Could not apply the filters: ${(err as Error).message}`)
    } finally {
      setApplying(false)
    }
  }

  return (
    <div style={styles.section} data-testid="where-check-section">
      <div style={styles.sectionTitle}>
        Checking where= of {check.function_name ?? check.node_id ?? 'a function'}
      </div>
      <div style={styles.hint}>
        Tick a filter to count it toward “Matching”. Edit a comparison to see
        its effect; nothing is saved until Apply.
      </div>
      {rows.map((row, index) => (
        <div key={index} style={styles.factorRow}>
          <label style={styles.checkRow} title="Counts toward Matching">
            <input
              type="checkbox"
              checked={row.enabled !== false}
              onChange={e => dispatchSpec({ type: 'editWhereCheck', index, patch: { enabled: e.target.checked } })}
            />
            <span style={styles.factorName}>
              {rows.length > 1 ? `Filter ${index + 1}: ` : ''}
              {row.column ? `${row.variable}['${row.column}']` : row.variable}
            </span>
          </label>
          <select
            style={styles.select}
            value={row.op ?? '=='}
            onChange={e => dispatchSpec({ type: 'editWhereCheck', index, patch: { op: e.target.value } })}
          >
            {OPS.map(op => <option key={op} value={op}>{op}</option>)}
          </select>
          <ValueInput
            value={row.value ?? ''}
            onCommit={value => dispatchSpec({ type: 'editWhereCheck', index, patch: { value } })}
          />
        </div>
      ))}
      <div style={{ display: 'flex', gap: 6, marginTop: 6 }}>
        <button
          type="button"
          style={styles.inlineButton}
          disabled={applying || !check.node_id || ticked === 0}
          onClick={apply}
          title={
            ticked === 0
              ? 'Tick at least one filter'
              : 'Write the ticked filters to the node and close this tab'
          }
        >
          Apply to node
        </button>
        <button type="button" style={styles.inlineButton} onClick={onClose} title="Close without changing the node">
          Discard
        </button>
      </div>
    </div>
  )
}

interface CheckTarget {
  node_id: string
  function_name: string | null
  filters: { variable: string; column?: string | null; op: string; value: string }[]
}

/**
 * "Check a where= filter…" in an ORDINARY figure: the function nodes that take
 * this variable as an input and state a filter, so Apply always has a node to
 * write to. Choosing one opens a check in a sibling tab: the plotted variable
 * against the node's first filter. Nothing is listed when no node qualifies.
 * VS Code only (only the host can open a tab).
 */
export function WhereCheckEntry({
  spec,
  variable,
  csvPath,
}: {
  spec: Spec | null
  variable: string | undefined
  csvPath?: string
}) {
  const [targets, setTargets] = useState<CheckTarget[]>([])
  useEffect(() => {
    if (!variable || csvPath || !isVSCodeMode) { setTargets([]); return }
    let live = true
    callBackend('where_check_nodes', { variable })
      .then(raw => { if (live) setTargets(raw as CheckTarget[]) })
      .catch(err => console.warn('[where_check] could not list nodes', err))
    return () => { live = false }
  }, [variable, csvPath])
  if (spec?.where_check || targets.length === 0 || !variable) return null

  const open = (target: CheckTarget) => {
    const where_check = {
      node_id: target.node_id,
      function_name: target.function_name,
      filters: target.filters.map(f => ({ ...f, enabled: true })),
      filter_index: 0,
      against: true,
    }
    console.info('[where_check] open from figure', { node: target.node_id, variable })
    callBackend('open_plot_panel', { variable, whereCheck: where_check, from: 'where-chip' })
      .catch(err => console.error('[where_check] open failed', err))
  }

  return (
    <select
      style={styles.select}
      value=""
      onChange={e => {
        const target = targets.find(t => t.node_id === e.target.value)
        if (target) open(target)
      }}
      title="Open this variable against a filter of a function that takes it as input"
    >
      <option value="">Check a where= filter…</option>
      {targets.map(t => (
        <option key={t.node_id} value={t.node_id}>{t.function_name ?? t.node_id}</option>
      ))}
    </select>
  )
}
