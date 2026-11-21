/**
 * DeleteEntityDialog — the sidebar's Delete confirm, for every palette kind
 * (GitHub #26, .claude/plan-delete-entries-with-data.md;
 * docs/claude/deleting-entries-with-data.md).
 *
 * Opens with the backend's DRY RUN (`delete_entity_plan`: writes nothing) and
 * shows, before anything happens:
 *
 * 1. **Data** — every variable that loses records, downstream included;
 * 2. **Declaration / code** — what is removed from source, or why it is kept;
 * 3. **Canvases** — every pipeline whose nodes are removed (never a blocker);
 * 4. **Undo** — what Ctrl/Cmd+Z brings back (the data too, from scidb's trash).
 *
 * A Parameter shows one checkbox per value (declared and history). All ticked
 * deletes the whole Parameter; a subset deletes only those values, and the
 * plan is re-asked on every change so the counts always match the ticks.
 *
 * Confirming calls `delete_entity` with the plan's fingerprint: if the data
 * changed meanwhile it is refused, and the new plan is shown. The call is one
 * undoable handler, so it is one Ctrl/Cmd+Z step.
 */

import { useEffect, useMemo, useRef, useState } from 'react'
import { callBackend } from '../../api'
import {
  KIND_LABEL,
  dataLines,
  formatValue,
  undoLine,
  valuesForRequest,
  type DeleteKind,
  type DeletePlan,
} from './deletePlan'

export interface DeleteTarget {
  kind: DeleteKind
  /** The entity's name; a pipeline id for a submodule. */
  name: string
  /** Display name when it differs from ``name`` (a submodule's). */
  label?: string
}

export interface DeleteResult {
  ok: boolean
  error?: string
  reason?: string
  warnings?: string[]
  tombstone_id?: string
  undoable?: boolean
  deleted?: Record<string, number>
  nodes_removed?: number
  plan?: DeletePlan
}

export default function DeleteEntityDialog({
  target,
  onClose,
  onDeleted,
  onOpenPipeline,
}: {
  target: DeleteTarget
  onClose: () => void
  onDeleted: (result: DeleteResult) => void
  onOpenPipeline: (pipelineId: string, name: string) => void
}) {
  const [plan, setPlan] = useState<DeletePlan | null>(null)
  const [options, setOptions] = useState<DeletePlan['value_options']>(undefined)
  const [ticked, setTicked] = useState<Set<number>>(new Set())
  const [reason, setReason] = useState('')
  const [busy, setBusy] = useState(false)
  const [confirmError, setConfirmError] = useState('')
  // Which target the value checkboxes were seeded for, and whether the user
  // typed a reason: refs, because the plan callback must see the CURRENT
  // answer, not the one its effect closed over.
  const seededFor = useRef<string | null>(null)
  const reasonEdited = useRef(false)
  const targetKey = `${target.kind}:${target.name}`

  const values = useMemo(
    () => (options ? valuesForRequest(options, ticked) : null),
    [options, ticked],
  )
  const nothingTicked = Array.isArray(values) && values.length === 0
  const valuesKey = JSON.stringify(values)

  // The first plan is for the whole entity; it also brings the value options.
  useEffect(() => {
    seededFor.current = null
    reasonEdited.current = false
    setOptions(undefined)
    setTicked(new Set())
    setReason('')
  }, [targetKey])

  useEffect(() => {
    if (nothingTicked) return
    let cancelled = false
    setPlan(null)
    setConfirmError('')
    callBackend('delete_entity_plan', { kind: target.kind, name: target.name, values })
      .then(d => {
        const p = d as DeletePlan
        console.debug('[DeleteEntityDialog] plan', target, values, p)
        if (cancelled) return
        setPlan(p)
        if (p.ok && seededFor.current !== targetKey) {
          seededFor.current = targetKey
          if (p.value_options) {
            setOptions(p.value_options)
            setTicked(new Set(p.value_options.map((_, i) => i)))
          }
        }
        if (!reasonEdited.current) setReason(p.reason ?? '')
      })
      .catch(err => { if (!cancelled) setPlan({ ok: false, error: (err as Error).message }) })
    return () => { cancelled = true }
    // Re-planning is keyed on what is asked for; `values` is that key's value.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [targetKey, valuesKey])

  const toggle = (i: number) => setTicked(prev => {
    const next = new Set(prev)
    if (next.has(i)) next.delete(i)
    else next.add(i)
    return next
  })

  const confirm = () => {
    if (!plan?.ok) return
    setBusy(true)
    setConfirmError('')
    callBackend('delete_entity', {
      kind: target.kind,
      name: target.name,
      values,
      fingerprint: plan.data?.fingerprint ?? null,
      reason,
    })
      .then(d => {
        const r = d as DeleteResult
        console.info('[DeleteEntityDialog] delete', target, values, r)
        if (r.ok) onDeleted(r)
        else {
          setConfirmError(r.error ?? 'Delete failed.')
          if (r.plan) setPlan(r.plan)
        }
      })
      .catch(err => setConfirmError((err as Error).message))
      .finally(() => setBusy(false))
  }

  const label = target.label ?? plan?.label ?? target.name
  const data = plan?.data
  const blocked = plan?.blocked ?? []
  const canDelete = !!plan?.ok && blocked.length === 0 && !nothingTicked && !busy

  return (
    <>
      <div style={styles.backdrop} onClick={onClose} />
      <div style={styles.dialog} role="dialog" aria-label={`Delete ${label}`}>
        <div style={styles.title}>Delete {KIND_LABEL[target.kind]} “{label}”?</div>

        {options && options.length > 0 && (
          <div style={styles.block}>
            <div style={styles.heading}>Values</div>
            {options.map((o, i) => (
              <label key={i} style={styles.valueRow}>
                <input type="checkbox" checked={ticked.has(i)} onChange={() => toggle(i)} />
                <span style={styles.mono}>{formatValue(o.value)}</span>
                {!o.declared && <span style={styles.tag}>history</span>}
                {o.has_data && <span style={styles.tag}>has data</span>}
              </label>
            ))}
            {nothingTicked && <div style={styles.muted}>Tick at least one value.</div>}
          </div>
        )}

        {!plan && !nothingTicked && <div style={styles.muted}>Working out what this deletes…</div>}

        {plan && !plan.ok && <div style={styles.error}>{plan.error}</div>}

        {plan?.ok && data && (
          <>
            <div style={styles.block}>
              <div style={styles.heading}>Data</div>
              {data.total_records > 0 ? (
                <>
                  <div style={styles.danger}>
                    Deletes {data.total_records} record{data.total_records === 1 ? '' : 's'} from the
                    database, including everything computed from them:
                  </div>
                  <ul style={styles.list}>
                    {dataLines(data).map(line => <li key={line}>{line}</li>)}
                  </ul>
                </>
              ) : (
                <div style={styles.muted}>No saved data.</div>
              )}
              {(data.warnings ?? []).map(w => <div key={w} style={styles.muted}>{w}</div>)}
            </div>

            <div style={styles.block}>
              <div style={styles.heading}>{target.kind === 'function' ? 'Code' : target.kind === 'submodule' ? 'Pipeline' : 'Declaration'}</div>
              <div style={styles.text}>{plan.declaration?.message}</div>
              {plan.declaration?.removed && <pre style={styles.removed}>{plan.declaration.removed}</pre>}
            </div>

            {(plan.placements ?? []).length > 0 && (
              <div style={styles.block}>
                <div style={styles.heading}>Removed from these canvases</div>
                <ul style={styles.list}>
                  {(plan.placements ?? []).map(p => (
                    <li key={p.pipeline_id}>
                      {p.hidden_pipeline ? (
                        <span title="This pipeline is hidden">{p.pipeline_name} (hidden)</span>
                      ) : (
                        <button
                          type="button"
                          style={styles.link}
                          onClick={() => { onOpenPipeline(p.pipeline_id, p.pipeline_name); onClose() }}
                          title="Open this pipeline"
                        >
                          {p.pipeline_name}
                        </button>
                      )}
                      {p.node_ids.length > 1 && <span style={styles.muted}> ×{p.node_ids.length}</span>}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {blocked.length > 0 && (
              <div style={styles.block}>
                <div style={styles.heading}>Can’t delete</div>
                {blocked.map(b => <div key={b} style={styles.error}>{b}</div>)}
              </div>
            )}

            <div style={{ ...styles.text, ...(data.undoable ? {} : styles.danger) }}>{undoLine(data)}</div>

            {data.total_records > 0 && (
              <label style={styles.reasonRow}>
                <span style={styles.muted}>Reason (kept with the record of this delete)</span>
                <input style={styles.reasonInput} value={reason} onChange={e => { reasonEdited.current = true; setReason(e.target.value) }} />
              </label>
            )}
          </>
        )}

        {confirmError && <div style={styles.error}>{confirmError}</div>}

        <div style={styles.actions}>
          <button type="button" style={styles.cancelBtn} onClick={onClose}>Cancel</button>
          {plan?.ok && blocked.length === 0 && (
            <button
              type="button"
              style={canDelete ? styles.deleteBtn : { ...styles.deleteBtn, opacity: 0.5, cursor: 'default' }}
              onClick={confirm}
              disabled={!canDelete}
              data-testid="delete-confirm"
            >
              {busy ? 'Deleting…' : data && data.total_records > 0 ? `Delete, with ${data.total_records} record${data.total_records === 1 ? '' : 's'}` : 'Delete'}
            </button>
          )}
        </div>
      </div>
    </>
  )
}

const styles: Record<string, React.CSSProperties> = {
  backdrop: { position: 'fixed', inset: 0, zIndex: 1100, background: 'rgba(0,0,0,0.4)' },
  dialog: {
    position: 'fixed',
    zIndex: 1101,
    top: '12%',
    left: '50%',
    transform: 'translateX(-50%)',
    width: 'min(480px, calc(100vw - 32px))',
    maxHeight: '76vh',
    overflow: 'auto',
    background: '#1a1a2e',
    border: '1px solid #3a3a5a',
    borderRadius: 6,
    boxShadow: '0 8px 24px rgba(0,0,0,0.5)',
    padding: 14,
    color: '#ddd',
    fontSize: 12,
  },
  title: { fontWeight: 700, fontSize: 13, marginBottom: 10 },
  block: { marginBottom: 10 },
  heading: { fontWeight: 600, color: '#aaa', textTransform: 'uppercase', fontSize: 10, letterSpacing: 0.5, marginBottom: 4 },
  text: { marginBottom: 6, lineHeight: 1.4 },
  danger: { color: '#fca5a5', marginBottom: 4, lineHeight: 1.4 },
  muted: { color: '#888' },
  mono: { fontFamily: 'monospace' },
  tag: { fontSize: 10, color: '#aaa', border: '1px solid #444', borderRadius: 3, padding: '0 4px' },
  valueRow: { display: 'flex', alignItems: 'center', gap: 6, padding: '1px 0', cursor: 'pointer' },
  removed: {
    fontFamily: 'monospace',
    fontSize: 11,
    background: '#11111f',
    border: '1px solid #333',
    borderRadius: 3,
    padding: '6px 8px',
    margin: '0 0 4px',
    whiteSpace: 'pre-wrap',
    color: '#f87171',
    maxHeight: 120,
    overflow: 'auto',
  },
  list: { margin: '0 0 4px', paddingLeft: 18 },
  link: {
    background: 'none',
    border: 'none',
    padding: 0,
    color: '#60a5fa',
    textDecoration: 'underline',
    cursor: 'pointer',
    fontSize: 12,
  },
  reasonRow: { display: 'flex', flexDirection: 'column', gap: 3, marginTop: 4 },
  reasonInput: {
    background: '#11111f',
    border: '1px solid #444',
    color: '#ddd',
    borderRadius: 3,
    padding: '3px 6px',
    fontSize: 12,
  },
  error: { color: '#f87171', marginBottom: 6 },
  actions: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 10 },
  cancelBtn: {
    background: 'transparent',
    border: '1px solid #444',
    color: '#ccc',
    borderRadius: 3,
    padding: '3px 10px',
    cursor: 'pointer',
  },
  deleteBtn: {
    background: '#7f1d1d',
    border: '1px solid #b91c1c',
    color: '#fee2e2',
    borderRadius: 3,
    padding: '3px 10px',
    cursor: 'pointer',
  },
}
