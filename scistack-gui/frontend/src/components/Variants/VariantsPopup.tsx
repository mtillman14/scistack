/**
 * VariantsPopup: every variant of one variable, as collapsible cards.
 *
 * `.claude/plan-variants-popup.md` Stage 5; rules in
 * `docs/claude/variant-pins-and-deletion.md`. Opened from a Variable node's
 * right-click → **Variants**. It replaces the old Provenance and Variants
 * panels: one card per full-chain variant, and on each card
 *
 * - **Make current**: pin it as the variable's DEFAULT. Unnamed loads
 *   (downstream steps, Plot Studio's opening choice, stats) get it; nothing
 *   else is hidden, so other variants stay reachable by name.
 * - **Delete**: REALLY delete it and everything computed from it, after a
 *   dry-run plan, with a reason, leaving a tombstone. This is the one real
 *   delete in the project.
 *
 * Expanding a card shows what defines it: the whole upstream pipeline as a
 * read-only mini DAG with every setting, every location, and its runs.
 *
 * **Nothing about variants is decided here** (CLAUDE.md NOTE 3/4). The cards
 * are `Inspector.variant_cards`; a pin sends back the card's own `selection`;
 * a delete sends back its `card_id` plus the fingerprint of the plan the user
 * saw. Wording and shapes live in `variantCards.ts` (tested by `npm test`).
 *
 * The mini DAG uses its own two read-only node types, not the canvas's
 * `FunctionNode`. It draws RECORDED history (what produced these records),
 * which can differ from the canvas as it is now, and the canvas components
 * expect live canvas state.
 */

import { useCallback, useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import {
  Background,
  Handle,
  Position,
  ReactFlow,
  ReactFlowProvider,
  type Edge,
  type Node,
  type NodeProps,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import { callBackend } from '../../api'
import { applyDagreLayout } from '../../layout'
import * as modalStyles from '../modalStyles'
import {
  canConfirmDelete,
  cardHeading,
  deleteLines,
  locationTree,
  popupSummary,
  selectionLines,
  statusBadge,
  upstreamGraph,
} from './variantCards'
import type {
  DeletePlanReply,
  LocationBranch,
  ParameterOffer,
  VariantCard,
  VariantCardsReply,
} from './variantCards'

const short = (id: string | null | undefined, n = 8) => (id ? id.slice(0, n) : '—')

export default function VariantsPopup({
  variable,
  onClose,
}: {
  variable: string
  onClose: () => void
}) {
  const [reply, setReply] = useState<VariantCardsReply | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [pinFor, setPinFor] = useState<{ card: VariantCard; release: boolean } | null>(null)
  // A whole card, or (records) only the record(s) at one location of it.
  const [deleteFor, setDeleteFor] = useState<{
    card: VariantCard
    records?: { ids: string[]; label: string }
  } | null>(null)
  const [showHistory, setShowHistory] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      setReply((await callBackend('variable_variants', { variable })) as VariantCardsReply)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }, [variable])

  useEffect(() => {
    void load()
  }, [load])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !pinFor && !deleteFor) onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose, pinFor, deleteFor])

  const toggle = (cardId: string) =>
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(cardId)) next.delete(cardId)
      else next.add(cardId)
      return next
    })

  const hasDefault = !!reply?.default_selection

  return createPortal(
    <div style={modalStyles.overlay} onClick={onClose}>
      <div
        style={{ ...modalStyles.dialog, width: '86vw', maxWidth: 1180, maxHeight: '88vh' }}
        onClick={e => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label={`Variants of ${variable}`}
      >
        <div style={styles.titleRow}>
          <div style={modalStyles.dialogTitle}>Variants of {variable}</div>
          <button type="button" style={styles.closeX} onClick={onClose} aria-label="Close">
            ×
          </button>
        </div>
        {reply && <div style={styles.summary}>{popupSummary(reply)}</div>}
        {reply && (
          // Every screen has a terminal equivalent: what makes "the CLI
          // powers the GUI" checkable rather than a claim.
          <code style={styles.command}>{reply.command}</code>
        )}

        {loading && <div style={styles.note}>Reading the variants…</div>}
        {error && <div style={styles.error}>{error}</div>}

        {reply && !error && (
          <div style={styles.body}>
            {reply.cards.length === 0 && (
              <div style={styles.note}>No records of {variable}.</div>
            )}
            {reply.cards.map((card, index) => (
              <CardView
                key={card.card_id}
                index={index + 1}
                card={card}
                variable={variable}
                hasDefault={hasDefault}
                expanded={expanded.has(card.card_id)}
                onToggle={() => toggle(card.card_id)}
                onPin={() => setPinFor({ card, release: false })}
                onRelease={() => setPinFor({ card, release: true })}
                onDelete={() => setDeleteFor({ card })}
                onDeleteRecords={(ids, label) => setDeleteFor({ card, records: { ids, label } })}
              />
            ))}

            {(reply.pin_history.length > 0 || reply.tombstones.length > 0) && (
              <div style={styles.history}>
                <button type="button" style={styles.linkButton} onClick={() => setShowHistory(v => !v)}>
                  {showHistory ? '▾' : '▸'} History ({reply.pin_history.length} pin
                  {reply.pin_history.length === 1 ? '' : 's'}, {reply.tombstones.length} deletion
                  {reply.tombstones.length === 1 ? '' : 's'})
                </button>
                {showHistory && (
                  <div style={styles.historyBody}>
                    {reply.pin_history.map(p => (
                      <div key={p.pin_id} style={styles.dim}>
                        pinned {p.pinned_at}: {selectionLines(p.selection).join(', ')}
                        {' — '}“{p.reason}”
                        {p.released_at ? ` · released ${p.released_at} (“${p.release_reason}”)` : ' · active'}
                      </div>
                    ))}
                    {reply.tombstones.map(t => (
                      <div key={t.tombstone_id} style={styles.dim}>
                        deleted {t.deleted_at}:{' '}
                        {Object.entries(t.by_variable).map(([v, n]) => `${v} ${n}`).join(', ')}
                        {' — '}“{t.reason}”
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {pinFor && reply && (
        <ReasonDialog
          title={pinFor.release ? `Release the pin on ${variable}?` : `Make this the current ${variable}?`}
          explanation={
            pinFor.release
              ? 'Unnamed loads go back to every current variant. The pin is kept in the history.'
              : 'Downstream steps, Plot Studio and stats will use this variant by default. Other variants are not hidden: anything that names one still gets it.'
          }
          defaultReason={pinFor.release ? 'released in the Variants popup' : 'made current in the Variants popup'}
          confirmLabel={pinFor.release ? 'Release pin' : 'Make current'}
          onCancel={() => setPinFor(null)}
          onConfirm={async reason => {
            if (pinFor.release) {
              await callBackend('release_pin', { variable, reason })
            } else {
              await callBackend('pin_variant', { variable, selection: pinFor.card.selection, reason })
            }
            setPinFor(null)
            await load()
          }}
        />
      )}

      {deleteFor && (
        <DeleteDialog
          variable={variable}
          card={deleteFor.card}
          records={deleteFor.records}
          onCancel={() => setDeleteFor(null)}
          onDone={async () => {
            setDeleteFor(null)
            await load()
          }}
        />
      )}
    </div>,
    document.body,
  )
}

// ---------------------------------------------------------------------------
// One card
// ---------------------------------------------------------------------------

function CardView({
  index,
  card,
  variable,
  hasDefault,
  expanded,
  onToggle,
  onPin,
  onRelease,
  onDelete,
  onDeleteRecords,
}: {
  index: number
  card: VariantCard
  variable: string
  hasDefault: boolean
  expanded: boolean
  onToggle: () => void
  onPin: () => void
  onRelease: () => void
  onDelete: () => void
  onDeleteRecords: (ids: string[], label: string) => void
}) {
  const badge = statusBadge(card, hasDefault)
  const badgeStyle = { ...styles.badge, ...BADGE_COLORS[badge.code] }
  return (
    <div style={{ ...styles.card, ...(card.is_pinned ? styles.cardPinned : null) }}>
      <div style={styles.cardHead}>
        <button type="button" style={styles.expander} onClick={onToggle} aria-expanded={expanded}>
          {expanded ? '▾' : '▸'}
        </button>
        <span style={styles.ordinal}>[{index}]</span>
        <span style={badgeStyle} title={card.verdict_label}>
          {badge.label}
        </span>
        <span style={styles.heading} onClick={onToggle}>
          {cardHeading(card)}
        </span>
        <span style={styles.dim}>
          {card.record_count} record{card.record_count === 1 ? '' : 's'} · {card.locations.length} location
          {card.locations.length === 1 ? '' : 's'}
          {card.last_saved ? ` · last ${card.last_saved}` : ''}
        </span>
        <span style={styles.spacer} />
        {card.is_pinned ? (
          <button type="button" style={styles.button} onClick={onRelease}>
            Release pin
          </button>
        ) : (
          <button
            type="button"
            style={card.selection_exact ? styles.primary : styles.buttonDisabled}
            onClick={onPin}
            disabled={!card.selection_exact}
            title={
              card.selection_exact
                ? 'Make this the default for unnamed loads (nothing is hidden)'
                : `This variant cannot be named on its own (its selection also matches ${card.overlaps_with.join(', ')})`
            }
          >
            Make current
          </button>
        )}
        <button type="button" style={styles.danger} onClick={onDelete} title="Really delete this variant and everything computed from it">
          Delete…
        </button>
      </div>
      {card.pin_conflict && <div style={styles.warn}>note: {card.pin_conflict}</div>}
      {!card.selection_exact && (
        <div style={styles.warn}>its selection also matches card(s) {card.overlaps_with.join(', ')}</div>
      )}

      {expanded && (
        <div style={styles.cardBody}>
          <div style={styles.sectionTitle}>What defines it</div>
          <div style={styles.dim}>{card.verdict_label}</div>
          <div style={styles.selection}>
            {selectionLines(card.selection).map(line => (
              <code key={line} style={styles.chip}>
                {line}
              </code>
            ))}
            {Object.keys(card.selection).length === 0 && <span style={styles.dim}>no settings: a direct save</span>}
          </div>

          <div style={styles.sectionTitle}>Upstream pipeline</div>
          {card.upstream.steps.length === 0 ? (
            <div style={styles.dim}>Saved directly; no pipeline step produced it.</div>
          ) : (
            <MiniDag card={card} variable={variable} />
          )}

          <div style={styles.sectionTitle}>Locations ({card.locations.length})</div>
          <LocationTreeView branches={locationTree(card)} onDelete={onDeleteRecords} />

          <div style={styles.sectionTitle}>Runs ({card.runs.length})</div>
          {card.runs.length === 0 && (
            <div style={styles.dim}>
              No tracked run: produced outside a for_each execution (a terminal MATLAB run, or a direct save).
            </div>
          )}
          {card.runs.map(run => (
            <div key={`${run.run_id}-${run.invocation_id}`} style={styles.dim}>
              <code>{short(run.run_id)}</code> {run.timestamp}
              {run.run_options ? ` · ${run.run_options}` : ''}
              {run.where_clause ? ` · where ${run.where_clause}` : ''}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// The read-only mini DAG
// ---------------------------------------------------------------------------

type MiniData = { label: string; lines: string[]; isRoot: boolean }

function VariableBox({ data }: NodeProps<Node<MiniData>>) {
  return (
    <div style={{ ...styles.miniVar, ...(data.isRoot ? styles.miniRoot : null) }}>
      <Handle type="target" position={Position.Left} style={styles.handle} />
      {data.label}
      <Handle type="source" position={Position.Right} style={styles.handle} />
    </div>
  )
}

function StepBox({ data }: NodeProps<Node<MiniData>>) {
  return (
    <div style={styles.miniStep}>
      <Handle type="target" position={Position.Left} style={styles.handle} />
      <div style={styles.miniStepName}>{data.label}</div>
      {data.lines.map(line => (
        <div key={line} style={styles.miniLine}>
          {line}
        </div>
      ))}
      <Handle type="source" position={Position.Right} style={styles.handle} />
    </div>
  )
}

const MINI_NODE_TYPES = { variable: VariableBox, step: StepBox }

function MiniDag({ card, variable }: { card: VariantCard; variable: string }) {
  const { nodes, edges } = useMemo(() => {
    const graph = upstreamGraph(card, variable)
    const rfNodes: Node[] = graph.nodes.map(n => ({
      id: n.id,
      type: n.kind,
      position: { x: 0, y: 0 },
      data: { label: n.label, lines: n.lines, isRoot: n.isRoot },
      draggable: false,
      selectable: false,
    }))
    const rfEdges: Edge[] = graph.edges.map(e => ({
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.label || undefined,
      style: { stroke: '#5a5a8a' },
      labelStyle: { fill: '#9a9ab0', fontSize: 10 },
      labelBgStyle: { fill: '#16162c' },
    }))
    return { nodes: applyDagreLayout(rfNodes, rfEdges, {}), edges: rfEdges }
  }, [card, variable])

  return (
    <div style={styles.miniWrap}>
      <ReactFlowProvider>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={MINI_NODE_TYPES}
          fitView
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
          proOptions={{ hideAttribution: true }}
          minZoom={0.2}
        >
          <Background color="#2a2a4a" gap={16} />
        </ReactFlow>
      </ReactFlowProvider>
    </div>
  )
}

function LocationTreeView({
  branches,
  depth = 0,
  path = [],
  onDelete,
}: {
  branches: LocationBranch[]
  depth?: number
  /** The labels of the branches above, so a leaf can name its whole location. */
  path?: string[]
  /** Per-location delete: the record(s) at a leaf, never a whole branch. */
  onDelete?: (ids: string[], label: string) => void
}) {
  const [open, setOpen] = useState<Set<string>>(new Set())
  if (branches.length === 0) return <div style={styles.dim}>none</div>
  return (
    <div style={{ marginLeft: depth === 0 ? 0 : 14 }}>
      {branches.map(b => {
        const leaf = b.children.length === 0
        const isOpen = open.has(b.label)
        const where = [...path, b.label].join(' · ')
        return (
          <div key={b.label}>
            <span
              style={leaf ? styles.leaf : styles.branch}
              onClick={() =>
                !leaf &&
                setOpen(prev => {
                  const next = new Set(prev)
                  if (next.has(b.label)) next.delete(b.label)
                  else next.add(b.label)
                  return next
                })
              }
            >
              {leaf ? '·' : isOpen ? '▾' : '▸'} {b.label}
              {!leaf && <span style={styles.dim}> ({b.count})</span>}
              {leaf && b.recordIds.length > 1 && (
                <span style={styles.dim}> ({b.recordIds.length} records)</span>
              )}
            </span>
            {leaf && onDelete && b.recordIds.length > 0 && (
              <button
                type="button"
                style={styles.leafDelete}
                onClick={() => onDelete(b.recordIds, where)}
                title={`Delete only the record${b.recordIds.length === 1 ? '' : 's'} of this variant at ${where} (and what was computed from it)`}
              >
                🗑
              </button>
            )}
            {!leaf && isOpen && (
              <LocationTreeView
                branches={b.children}
                depth={depth + 1}
                path={[...path, b.label]}
                onDelete={onDelete}
              />
            )}
          </div>
        )
      })}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Dialogs
// ---------------------------------------------------------------------------

function ReasonDialog({
  title,
  explanation,
  defaultReason,
  confirmLabel,
  onCancel,
  onConfirm,
}: {
  title: string
  explanation: string
  defaultReason: string
  confirmLabel: string
  onCancel: () => void
  onConfirm: (reason: string) => Promise<void>
}) {
  const [reason, setReason] = useState(defaultReason)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  return (
    <div style={styles.subOverlay} onClick={e => { e.stopPropagation(); onCancel() }}>
      <div style={styles.subDialog} onClick={e => e.stopPropagation()} role="dialog" aria-modal="true">
        <div style={modalStyles.dialogTitle}>{title}</div>
        <div style={styles.dim}>{explanation}</div>
        <label style={styles.label}>
          Reason (kept in the history)
          <input style={styles.input} value={reason} onChange={e => setReason(e.target.value)} autoFocus />
        </label>
        {error && <div style={styles.error}>{error}</div>}
        <div style={styles.footer}>
          <button type="button" style={styles.button} onClick={onCancel}>
            Cancel
          </button>
          <button
            type="button"
            style={reason.trim() && !busy ? styles.primary : styles.buttonDisabled}
            disabled={!reason.trim() || busy}
            onClick={async () => {
              setBusy(true)
              setError('')
              try {
                await onConfirm(reason.trim())
              } catch (err) {
                setError((err as Error).message)
                setBusy(false)
              }
            }}
          >
            {busy ? 'Working…' : confirmLabel}
          </button>
        </div>
      </div>
    </div>
  )
}

function DeleteDialog({
  variable,
  card,
  records,
  onCancel,
  onDone,
}: {
  variable: string
  card: VariantCard
  /** Set: delete only these record(s) of the card (one location), not the card. */
  records?: { ids: string[]; label: string }
  onCancel: () => void
  onDone: () => Promise<void>
}) {
  const recordIds = records?.ids ?? []
  const [remove, setRemove] = useState<ParameterOffer[]>([])
  const [plan, setPlan] = useState<DeletePlanReply | null>(null)
  const [planning, setPlanning] = useState(true)
  const [reason, setReason] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [edits, setEdits] = useState<{ parameter: string; ok: boolean; reason?: string; message?: string }[] | null>(null)

  const removeValues = useMemo(
    () => remove.map(o => ({ parameter: o.parameter, value: o.value })),
    [remove],
  )

  // Re-plan whenever the Parameter choices change: ticking one can widen the
  // delete to other variables, and the user must see that before confirming.
  useEffect(() => {
    let live = true
    setPlanning(true)
    setPlan(null)
    setError('')
    callBackend('delete_variant_plan', {
      variable,
      card_id: card.card_id,
      record_ids: recordIds,
      remove_parameter_values: removeValues,
    })
      .then(p => live && setPlan(p as DeletePlanReply))
      .catch(err => live && setError((err as Error).message))
      .finally(() => live && setPlanning(false))
    return () => {
      live = false
    }
    // recordIds is derived from `records`, which is fixed for the dialog's life.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [variable, card.card_id, removeValues, records])

  // Removing a value from a Parameter retires a SETTING, which is a whole-card
  // decision; a per-location delete never offers it.
  const offers = records ? [] : card.parameter_offers.filter(o => o.declared)

  return (
    <div style={styles.subOverlay} onClick={e => { e.stopPropagation(); if (!busy) onCancel() }}>
      <div style={{ ...styles.subDialog, width: 620 }} onClick={e => e.stopPropagation()} role="dialog" aria-modal="true">
        <div style={{ ...modalStyles.dialogTitle, color: '#ff9a9a' }}>
          {records
            ? `Delete ${cardHeading(card)} at ${records.label}?`
            : `Delete ${cardHeading(card)}?`}
        </div>
        <div style={styles.dim}>
          This really deletes the records, and everything computed from them. It cannot be undone; a tombstone records
          what was deleted, by whom and why.
        </div>

        {offers.length > 0 && !edits && (
          <div style={styles.offers}>
            {offers.map(o => {
              const checked = remove.some(r => r.parameter === o.parameter && r.label === o.label)
              return (
                <label key={o.label} style={o.editable ? styles.checkbox : styles.checkboxDisabled} title={o.message}>
                  <input
                    type="checkbox"
                    checked={checked}
                    disabled={!o.editable || busy}
                    onChange={e =>
                      setRemove(prev =>
                        e.target.checked ? [...prev, o] : prev.filter(r => r.label !== o.label),
                      )
                    }
                  />
                  Also remove {String(o.value)} from the “{o.parameter}” Parameter, deleting every variant of every
                  variable built with it
                  {!o.editable && o.message ? ` (${o.message})` : ''}
                </label>
              )
            })}
          </div>
        )}

        {planning && <div style={styles.note}>Working out what would be deleted…</div>}
        {plan && !edits && (
          <div style={styles.planBox}>
            <div style={styles.planHead}>
              Would delete {plan.total_records} record{plan.total_records === 1 ? '' : 's'}
            </div>
            {deleteLines(plan).map(line => (
              <div key={line} style={styles.planLine}>
                {line}
              </div>
            ))}
            <div style={styles.dim}>
              {plan.invocation_count} function call(s) and {plan.run_count} run(s) left with nothing are deleted too.
            </div>
            {plan.pins_to_release.length > 0 && (
              <div style={styles.warn}>Pins released: {plan.pins_to_release.join(', ')}</div>
            )}
            {plan.warnings.map(w => (
              <div key={w} style={styles.warn}>
                {w}
              </div>
            ))}
          </div>
        )}

        {edits && (
          <div style={styles.planBox}>
            <div style={styles.planHead}>Deleted.</div>
            {edits.map(e => (
              <div key={e.parameter} style={e.ok ? styles.dim : styles.error}>
                {e.parameter}: {e.ok ? (e.reason === 'already_absent' ? 'value was already gone' : 'value removed from source') : `not edited (${e.message || e.reason})`}
              </div>
            ))}
          </div>
        )}

        {!edits && (
          <label style={styles.label}>
            Reason (required; stored on the tombstone)
            <input
              style={styles.input}
              value={reason}
              onChange={e => setReason(e.target.value)}
              placeholder="e.g. wrong calibration file"
              autoFocus
            />
          </label>
        )}
        {error && <div style={styles.error}>{error}</div>}

        <div style={styles.footer}>
          {edits ? (
            <button type="button" style={styles.primary} onClick={() => void onDone()}>
              Close
            </button>
          ) : (
            <>
              <button type="button" style={styles.button} onClick={onCancel} disabled={busy}>
                Cancel
              </button>
              <button
                type="button"
                style={canConfirmDelete(plan, reason, busy || planning) ? styles.dangerSolid : styles.buttonDisabled}
                disabled={!canConfirmDelete(plan, reason, busy || planning)}
                onClick={async () => {
                  if (!plan) return
                  setBusy(true)
                  setError('')
                  try {
                    const out = (await callBackend('delete_variant', {
                      variable,
                      card_id: card.card_id,
                      record_ids: recordIds,
                      reason: reason.trim(),
                      fingerprint: plan.fingerprint,
                      remove_parameter_values: removeValues,
                    })) as { parameter_edits?: { parameter: string; ok: boolean; reason?: string; message?: string }[] }
                    const parameterEdits = out.parameter_edits ?? []
                    if (parameterEdits.length > 0) setEdits(parameterEdits)
                    else await onDone()
                  } catch (err) {
                    setError((err as Error).message)
                  } finally {
                    setBusy(false)
                  }
                }}
              >
                {busy ? 'Deleting…' : plan ? `Delete ${plan.total_records} record${plan.total_records === 1 ? '' : 's'}` : 'Delete'}
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const BADGE_COLORS: Record<string, React.CSSProperties> = {
  pinned: { color: '#1a1a2e', background: '#e0b050' },
  default: { color: '#e0b050', background: '#2a2410', border: '1px solid #e0b050' },
  current: { color: '#8fd48f', background: '#132a13' },
  partial: { color: '#e0b050', background: '#2a2410' },
  superseded: { color: '#9a9ab0', background: '#22223a' },
}

const styles: Record<string, React.CSSProperties> = {
  titleRow: { display: 'flex', alignItems: 'center', justifyContent: 'space-between' },
  closeX: { background: 'none', border: 'none', color: '#aaa', fontSize: 20, cursor: 'pointer' },
  summary: { fontSize: 12, color: '#ccc', marginBottom: 6 },
  command: {
    display: 'block', padding: '5px 10px', background: '#0f0f1e', border: '1px solid #2a2a4a',
    borderRadius: 4, fontFamily: 'monospace', fontSize: 11, color: '#9ad', overflowX: 'auto',
  },
  body: { marginTop: 12 },
  card: {
    border: '1px solid #2a2a4a', borderRadius: 5, padding: '6px 10px', marginBottom: 8, background: '#16162c',
  },
  cardPinned: { borderColor: '#e0b050' },
  cardHead: { display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' },
  expander: { background: 'none', border: 'none', color: '#aaa', cursor: 'pointer', fontSize: 13, padding: 0, width: 16 },
  ordinal: { fontFamily: 'monospace', fontSize: 11, color: '#666' },
  badge: { fontSize: 10, borderRadius: 3, padding: '1px 6px', whiteSpace: 'nowrap' },
  heading: { fontFamily: 'monospace', fontSize: 12, color: '#ddd', cursor: 'pointer' },
  spacer: { flex: 1 },
  cardBody: { marginTop: 8, paddingLeft: 24 },
  sectionTitle: {
    fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.6, color: '#888', margin: '10px 0 4px',
  },
  selection: { display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 4 },
  chip: {
    fontSize: 11, color: '#cfd', background: '#0f0f1e', border: '1px solid #2a2a4a', borderRadius: 3, padding: '1px 6px',
  },
  miniWrap: { height: 260, border: '1px solid #2a2a4a', borderRadius: 4, background: '#101022' },
  miniVar: {
    padding: '6px 10px', borderRadius: 14, border: '1px solid #0891b2', background: '#164e63',
    color: '#a5f3fc', fontSize: 11, fontFamily: 'monospace', minWidth: 120, textAlign: 'center',
  },
  miniRoot: { border: '2px solid #e0b050' },
  miniStep: {
    padding: '6px 10px', borderRadius: 4, border: '1px solid #5a5a8a', background: '#1f1f3a',
    color: '#ddd', fontSize: 10, minWidth: 160, maxWidth: 220,
  },
  miniStepName: { fontWeight: 600, fontSize: 11, marginBottom: 3, fontFamily: 'monospace' },
  miniLine: { fontFamily: 'monospace', color: '#b8b8d0', wordBreak: 'break-word' },
  handle: { opacity: 0, pointerEvents: 'none' },
  branch: { fontSize: 11, color: '#ccc', cursor: 'pointer', fontFamily: 'monospace' },
  leaf: { fontSize: 11, color: '#aaa', fontFamily: 'monospace' },
  leafDelete: {
    background: 'none', border: 'none', cursor: 'pointer', fontSize: 11,
    padding: '0 4px', opacity: 0.7,
  },
  history: { marginTop: 10 },
  historyBody: { marginTop: 6, display: 'flex', flexDirection: 'column', gap: 3 },
  dim: { fontSize: 11, color: '#9a9ab0', wordBreak: 'break-word' },
  note: { fontSize: 12, color: '#888', padding: '8px 0' },
  error: { fontSize: 12, color: '#ff8a8a', padding: '6px 0' },
  warn: { fontSize: 11, color: '#e0b050', marginTop: 3 },
  linkButton: { background: 'none', border: 'none', color: '#9ad', cursor: 'pointer', fontSize: 12, padding: 0 },
  subOverlay: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex',
    alignItems: 'center', justifyContent: 'center', zIndex: 10001,
  },
  subDialog: {
    ...modalStyles.dialog, width: 480, display: 'flex', flexDirection: 'column', gap: 8,
  },
  label: { display: 'flex', flexDirection: 'column', gap: 4, fontSize: 12, color: '#ccc' },
  input: {
    background: '#0f0f1e', color: '#ddd', border: '1px solid #3a3a5a', borderRadius: 4, padding: '5px 8px', fontSize: 12,
  },
  offers: { display: 'flex', flexDirection: 'column', gap: 4 },
  checkbox: { fontSize: 12, color: '#ddd', display: 'flex', gap: 6, alignItems: 'flex-start' },
  checkboxDisabled: { fontSize: 12, color: '#777', display: 'flex', gap: 6, alignItems: 'flex-start' },
  planBox: { border: '1px solid #4a2a2a', borderRadius: 4, padding: '8px 10px', background: '#1e1420' },
  planHead: { fontSize: 13, fontWeight: 600, color: '#ffb0b0', marginBottom: 4 },
  planLine: { fontSize: 12, color: '#ddd', fontFamily: 'monospace' },
  footer: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 6 },
  button: {
    padding: '4px 12px', background: '#2a2a4a', color: '#ccc', border: '1px solid #3a3a5a',
    borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
  buttonDisabled: {
    padding: '4px 12px', background: '#202036', color: '#666', border: '1px solid #2a2a40',
    borderRadius: 4, cursor: 'not-allowed', fontSize: 12,
  },
  primary: {
    padding: '4px 12px', background: '#164e63', color: '#a5f3fc', border: '1px solid #0891b2',
    borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
  danger: {
    padding: '4px 12px', background: 'transparent', color: '#ff9a9a', border: '1px solid #7a3a3a',
    borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
  dangerSolid: {
    padding: '4px 12px', background: '#7a2020', color: '#fff', border: '1px solid #a33',
    borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
}
