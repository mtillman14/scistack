/**
 * DataViewPopup — "What data do I have, and where did it go?" (GitHub #28).
 *
 * One row per location of the schema hierarchy, an "any data" column, then
 * one narrow column per variable in pipeline order, each cell a glyph
 * (✓ ⚠ ↻ ✕ ↓ ＋ ○ ⊘, shapes so they read without colour). The footer holds
 * each column's count and the drop-off from step to step. Clicking a cell
 * opens the right pane: its value, what made it, why it is in that state,
 * and what you can do about it.
 *
 * Every state, count and sentence is scidb's (`data_view` handler →
 * `scidb.provenance.data_view` / `status`); the rules for which cell a row
 * shows are `dataView.ts`. Replaces Inventory, Coverage, View Schema
 * Locations and Expected locations as GUI entry points (those stay hidden).
 */

import { useCallback, useEffect, useMemo, useState } from 'react'
import { callBackend } from '../../api'
import { askConfirm, askText, showMessage } from '../dialogs'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import { overlay } from '../modalStyles'
import {
  allRowIds,
  buildRows,
  cellFor,
  dropOffText,
  filterRows,
  legendEntries,
  type Cell,
  type CellState,
  type Column,
  type DataViewReply,
  type Row,
  type Step,
} from './dataView'
import type { DataViewRequest } from './openDataView'
import DataPreviewPopup from '../DataPreview/DataPreviewPopup'

const STATE_COLOR: Record<CellState, string> = {
  present: '#16a34a',
  warning: '#d97706',
  outdated: '#d97706',
  failed: '#dc2626',
  missing_input: '#dc2626',
  new: '#2563eb',
  not_run: '#6b7280',
  excluded: '#9ca3af',
}


interface Selected {
  column: Column
  row: Row
  cell: Cell | null
}

interface CellRecord {
  record_id: string
  made_by: string
  saved_at?: string | null
  findings: { kind: string; label: string; text: string }[]
  checked: boolean
}

export default function DataViewPopup({ request, onClose }: { request: DataViewRequest; onClose: () => void }) {
  const [reply, setReply] = useState<DataViewReply | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [states, setStates] = useState<string[]>(request.states ?? [])
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [selected, setSelected] = useState<Selected | null>(null)
  const [cleanupOpen, setCleanupOpen] = useState(Boolean(request.cleanup))
  const variablesKey = JSON.stringify(request.variables ?? null)

  const load = useCallback(() => {
    const t0 = performance.now()
    callBackend('data_view', { part: 'view', variables: request.variables ?? null })
      .then(raw => {
        const r = raw as DataViewReply
        setReply(r)
        setError(null)
        console.info(`[DataView] ${r.columns.length} column(s), ${r.any_data.length} location(s) in ${Math.round(performance.now() - t0)} ms`)
      })
      .catch(err => setError((err as Error).message))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [variablesKey])

  useEffect(() => { load() }, [load])
  useBackendMessage(useCallback((msg) => {
    const kind = (msg.type ?? msg.method) as string | undefined
    if (kind === 'dag_updated' || kind === 'run_done') load()
  }, [load]))
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const columns = reply?.columns ?? []
  const rows = useMemo(() => (reply ? buildRows(reply.any_data, reply.schema_keys) : []), [reply])
  const shown = useMemo(() => filterRows(rows, columns, states), [rows, columns, states])
  // A filter opens every row it kept, so the bad locations are in view.
  useEffect(() => { if (states.length) setExpanded(new Set(allRowIds(shown))) }, [states, shown])

  const toggleFilter = (f: CellState[]) => {
    setStates(prev => {
      const on = f.every(s => prev.includes(s))
      return on ? prev.filter(s => !(f as string[]).includes(s)) : [...new Set([...prev, ...f])]
    })
  }

  const renderRow = (row: Row): JSX.Element[] => {
    const open = expanded.has(row.id)
    const out = [
      <tr key={row.id}>
        <td style={{ ...styles.locCell, paddingLeft: 6 + row.depth * 14 }}>
          {row.children.length > 0 ? (
            <button type="button" style={styles.twisty} onClick={() => setExpanded(prev => {
              const next = new Set(prev)
              if (next.has(row.id)) next.delete(row.id)
              else next.add(row.id)
              return next
            })}>{open ? '▾' : '▸'}</button>
          ) : <span style={styles.twistySpace} />}
          <span style={styles.keyName}>{row.key}</span> {row.value}
        </td>
        <td style={styles.anyCell} title="Something is saved at this location">●</td>
        {columns.map(col => {
          const cell = cellFor(col, row.path)
          const isSel = selected?.column.variable === col.variable && selected.row.id === row.id
          return (
            <td
              key={col.variable}
              style={{ ...styles.glyphCell, ...(isSel ? styles.selectedCell : {}) }}
              title={cell ? `${col.variable}: ${cell.detail || cell.state}` : `${col.variable}: not at this level`}
              onClick={() => setSelected({ column: col, row, cell })}
            >
              {cell ? <span style={{ color: STATE_COLOR[cell.state] }}>{cell.glyph}</span> : <span style={styles.none}>·</span>}
            </td>
          )
        })}
      </tr>,
    ]
    if (open) for (const c of row.children) out.push(...renderRow(c))
    return out
  }

  const title = request.functionName ? `Data — ${request.functionName}` : 'Data'
  return (
    <div style={overlay} onClick={onClose}>
      <div style={styles.dialog} onClick={e => e.stopPropagation()} data-testid="data-view">
        <div style={styles.header}>
          <span style={styles.title}>▦ {title}</span>
          <div style={styles.filters}>
            {/* The legend is the filter row: every state, its meaning on hover. */}
            {reply && legendEntries(reply).map(e => (
              <button
                key={e.state}
                type="button"
                style={{ ...styles.filter, ...(states.includes(e.state) ? styles.filterOn : {}) }}
                onClick={() => toggleFilter([e.state])}
                title={`${e.meaning} Click to show only these.`}
              ><span style={{ color: STATE_COLOR[e.state] }}>{e.glyph}</span> {e.label}</button>
            ))}
          </div>
          <button type="button" style={styles.action} onClick={() => setExpanded(new Set(allRowIds(shown)))}>Expand all</button>
          <button type="button" style={styles.action} onClick={() => setCleanupOpen(true)} title="Delete variants made by code that is no longer the function's source">Clean up outdated…</button>
          <button type="button" style={styles.close} onClick={onClose} title="Close">×</button>
        </div>
        {error && <div style={styles.error}>{error}</div>}
        {!reply && !error && <div style={styles.muted}>Loading…</div>}
        {reply && (
          <div style={styles.body}>
            <div style={styles.tableWrap}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    <th style={styles.locHead}>Location</th>
                    <th style={styles.colHead} title="Is anything saved here?">any data</th>
                    {columns.map(col => (
                      <th key={col.variable} style={styles.colHead} title={[col.variable, ...(col.notes ?? []), col.error ?? ''].filter(Boolean).join('\n')}>
                        <div style={styles.vertical}>{col.variable}</div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>{shown.flatMap(renderRow)}</tbody>
                <tfoot>
                  <tr>
                    <td style={styles.footHead}>present / planned</td>
                    <td style={styles.footCell}>{reply.any_data.length}</td>
                    {columns.map(col => (
                      <td key={col.variable} style={styles.footCell} title={col.unchecked ? `${col.unchecked} location(s) not checked (saved outside a run)` : ''}>
                        {col.error ? '!' : col.footer}
                      </td>
                    ))}
                  </tr>
                </tfoot>
              </table>
              {shown.length === 0 && <div style={styles.muted}>No location matches the filter.</div>}
            </div>
            {selected && (
              <CellPane
                selected={selected}
                onChanged={load}
                onClose={() => setSelected(null)}
              />
            )}
          </div>
        )}
        {reply && reply.dropoff.length > 0 && (
          <div style={styles.dropoff}>
            <b>Where did my {reply.dropoff[0].key}s go?</b> {dropOffText(reply.dropoff)}
            {reply.dropoff.filter(d => d.lost_count > 0).map(d => (
              <div key={d.variable} style={styles.loss}>
                {d.variable}: −{d.lost_count} ({d.lost.slice(0, 8).map(l => `${l.value} ${reply.glyphs[l.state] ?? ''}`).join(', ')}{d.lost_count > 8 ? ', …' : ''})
              </div>
            ))}
          </div>
        )}
        {cleanupOpen && <CleanupOutdated variables={request.variables ?? null} onClose={() => { setCleanupOpen(false); load() }} />}
      </div>
    </div>
  )
}

function CellPane({ selected, onChanged, onClose }: {
  selected: Selected
  onChanged: () => void
  onClose: () => void
}) {
  const { column, row, cell } = selected
  const [records, setRecords] = useState<CellRecord[] | null>(null)
  const [previewOpen, setPreviewOpen] = useState(false)
  const keys = column.keys.filter(k => row.path.some(([rk]) => rk === k))
  const loc: Step[] = row.path.filter(([k]) => keys.includes(k))
  const locText = loc.map(([k, v]) => `${k}=${v}`).join(', ') || '(whole dataset)'
  const ids = cell?.record_ids ?? []

  useEffect(() => {
    setRecords(null)
    setPreviewOpen(false)
    if (ids.length === 0) return
    callBackend('data_view', { part: 'cell', variable: column.variable, record_ids: ids })
      .then(r => setRecords((r as { records: CellRecord[] }).records))
      .catch(err => console.debug('[DataView] cell detail unavailable', err))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [column.variable, row.id])

  const change = async (method: 'exclude_schema_combo' | 'include_schema_combo', variable: string | null) => {
    if (loc.length === 0) { await showMessage('Pick a cell at a location (not the whole dataset).'); return }
    const scope = variable ? `, ${variable} only` : ', every variable'
    const suggestion = method === 'exclude_schema_combo' && variable ? 'missing on purpose' : ''
    const reason = await askText(`Why? (${locText}${scope})`, suggestion)
    if (!reason) return
    try {
      await callBackend(method, {
        columns: loc.map(([k]) => k), row: loc.map(([, v]) => v), reason, variable,
      })
      console.info(`[DataView] ${method} ${locText} variable=${variable ?? '(everything)'} reason=${reason}`)
      onChanged()
    } catch (err) {
      await showMessage((err as Error).message)
    }
  }

  const deleteHere = async () => {
    if (ids.length === 0) return
    try {
      const plan = await callBackend('delete_variant_plan', { variable: column.variable, record_ids: ids }) as {
        total_records: number; downstream_by_variable: Record<string, number>; fingerprint: string
      }
      const down = Object.entries(plan.downstream_by_variable ?? {}).map(([v, n]) => `${n} ${v}`).join(', ')
      const ok = await askConfirm(
        `Delete ${column.variable} at ${locText}: ${plan.total_records} record(s)` +
        (down ? `, including downstream ${down}` : '') + '. Ctrl Z undoes it.', 'Delete')
      if (!ok) return
      const reason = await askText('Why is this data being deleted?', 'erroneous run')
      if (!reason) return
      await callBackend('delete_variant', {
        variable: column.variable, record_ids: ids, reason, fingerprint: plan.fingerprint,
      })
      onChanged()
    } catch (err) {
      await showMessage((err as Error).message)
    }
  }

  return (
    <div style={styles.pane}>
      <div style={styles.paneHead}>
        <b>{column.variable}</b> at {locText}
        <button type="button" style={styles.close} onClick={onClose} title="Close">×</button>
      </div>
      {cell ? (
        <div style={{ color: STATE_COLOR[cell.state], margin: '4px 0' }}>{cell.glyph} {cell.detail || cell.state}</div>
      ) : (
        <div style={styles.muted}>{column.variable} is not at this level.</div>
      )}
      {records?.map(r => (
        <div key={r.record_id} style={styles.record}>
          <div>{r.made_by}</div>
          {r.saved_at && <div style={styles.muted}>saved {r.saved_at}</div>}
          {!r.checked && <div style={styles.muted}>not checked (saved outside a run)</div>}
          {r.findings.map(f => <div key={f.kind} style={{ color: STATE_COLOR.warning }}>⚠ {f.text}</div>)}
        </div>
      ))}
      <div style={styles.paneActions}>
        {ids.length > 0 && (
          <button type="button" style={styles.action} onClick={() => setPreviewOpen(true)}
            title="The values here, column by column, as a function receives them">Preview…</button>
        )}
        {/* One word, exclude / include: "missing on purpose" is an exclusion
            scoped to this variable (scidb.store.exclusions). */}
        {cell?.state === 'excluded' ? (
          <>
            <button type="button" style={styles.action} onClick={() => change('include_schema_combo', null)}
              title="Remove an exclusion that covers every variable here">Include again…</button>
            <button type="button" style={styles.action} onClick={() => change('include_schema_combo', column.variable)}
              title={`Remove an exclusion of ${column.variable} only`}>Include {column.variable} again…</button>
          </>
        ) : (
          <>
            <button type="button" style={styles.action} onClick={() => change('exclude_schema_combo', null)}
              title="Skip this location in every step (e.g. a subject who withdrew)">Exclude here…</button>
            <button type="button" style={styles.action} onClick={() => change('exclude_schema_combo', column.variable)}
              title={`Only ${column.variable} is bad or missing on purpose here; steps reading it skip this location, other steps still run`}>Exclude {column.variable} only…</button>
          </>
        )}
        {ids.length > 0 && (
          <button type="button" style={{ ...styles.action, ...styles.danger }} onClick={deleteHere}>Delete this variant here…</button>
        )}
      </div>
      {previewOpen && (
        <DataPreviewPopup
          target={{ variable: column.variable, location: Object.fromEntries(loc) }}
          onClose={() => setPreviewOpen(false)}
        />
      )}
    </div>
  )
}

interface Candidate {
  variable: string
  ordinal: number
  text: string
  records: number
  downstream_total: number
  downstream: Record<string, number>
  warn: string[]
  ticked: boolean
  target: Record<string, unknown>
}

function CleanupOutdated({ variables, onClose }: { variables: string[] | null; onClose: () => void }) {
  const [candidates, setCandidates] = useState<Candidate[] | null>(null)
  const [ticked, setTicked] = useState<Set<number>>(new Set())
  const [unknown, setUnknown] = useState(0)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    callBackend('data_view', { part: 'cleanup_plan', variables })
      .then(raw => {
        const r = raw as { candidates: Candidate[]; unknown: number }
        setCandidates(r.candidates)
        setUnknown(r.unknown)
        setTicked(new Set(r.candidates.map((c, i) => (c.ticked ? i : -1)).filter(i => i >= 0)))
      })
      .catch(async err => { await showMessage((err as Error).message); onClose() })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const run = async () => {
    if (!candidates) return
    const targets = candidates.filter((_c, i) => ticked.has(i)).map(c => c.target)
    if (targets.length === 0) return
    setBusy(true)
    try {
      await callBackend('cleanup_outdated', { targets, reason: 'clean up outdated variants' })
      onClose()
    } catch (err) {
      await showMessage((err as Error).message)
    } finally {
      setBusy(false)
    }
  }

  const total = candidates?.filter((_c, i) => ticked.has(i)).reduce((n, c) => n + c.records + c.downstream_total, 0) ?? 0
  return (
    <div style={styles.cleanup}>
      <div style={styles.paneHead}><b>Clean up outdated…</b>
        <button type="button" style={styles.close} onClick={onClose}>×</button>
      </div>
      {!candidates && <div style={styles.muted}>Looking for variants made by old code…</div>}
      {candidates && candidates.length === 0 && <div style={styles.muted}>Nothing is outdated.</div>}
      {candidates?.map((c, i) => (
        <label key={`${c.variable}-${c.ordinal}`} style={styles.candidate}>
          <input type="checkbox" checked={ticked.has(i)} onChange={() => setTicked(prev => {
            const next = new Set(prev)
            if (next.has(i)) next.delete(i)
            else next.add(i)
            return next
          })} />
          <span><b>{c.variable} V{c.ordinal}</b>: {c.text}; {c.records} record(s)
            {c.downstream_total > 0 && `, ${c.downstream_total} downstream (${Object.entries(c.downstream).map(([v, n]) => `${n} ${v}`).join(', ')})`}
            {c.warn.map(w => <span key={w} style={{ color: STATE_COLOR.failed }}> ⚠ {w}</span>)}
          </span>
        </label>
      ))}
      {unknown > 0 && <div style={styles.muted}>{unknown} variant(s) could not be checked (their current code cannot be read or trusted).</div>}
      <div style={styles.paneActions}>
        <button type="button" style={{ ...styles.action, ...styles.danger }} disabled={busy || ticked.size === 0} onClick={run}>
          Delete {ticked.size} variant(s), {total} record(s) — one Ctrl Z undoes it
        </button>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  dialog: {
    position: 'relative', background: '#fff', color: '#1f2937', borderRadius: 8, width: '92vw', maxWidth: 1400,
    height: '86vh', display: 'flex', flexDirection: 'column', overflow: 'hidden',
    boxShadow: '0 10px 40px rgba(0,0,0,0.4)', fontSize: 12,
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 8, padding: '8px 12px',
    background: '#ecfeff', borderBottom: '1px solid #a5f3fc', flexWrap: 'wrap',
  },
  title: { fontWeight: 700, fontSize: 15, color: '#0e7490', marginRight: 8 },
  filters: { display: 'flex', gap: 4, flexWrap: 'wrap', flex: 1 },
  filter: { border: '1px solid #cbd5e1', borderRadius: 10, background: '#fff', fontSize: 11, padding: '1px 8px', cursor: 'pointer' },
  filterOn: { background: '#0e7490', color: '#fff', borderColor: '#0e7490' },
  action: { border: '1px solid #cbd5e1', borderRadius: 4, background: '#f8fafc', fontSize: 11, padding: '2px 8px', cursor: 'pointer' },
  danger: { borderColor: '#dc2626', color: '#b91c1c' },
  close: { border: 'none', background: 'none', fontSize: 18, cursor: 'pointer', marginLeft: 'auto' },
  error: { color: '#b91c1c', padding: 12 },
  muted: { color: '#6b7280', padding: '4px 0' },
  body: { display: 'flex', flex: 1, minHeight: 0 },
  tableWrap: { flex: 1, overflow: 'auto', padding: '0 8px' },
  table: { borderCollapse: 'collapse' },
  locHead: { textAlign: 'left', position: 'sticky', top: 0, background: '#fff', padding: 4, minWidth: 180 },
  colHead: { position: 'sticky', top: 0, background: '#fff', padding: '4px 2px', verticalAlign: 'bottom', width: 26 },
  vertical: { writingMode: 'vertical-rl', transform: 'rotate(180deg)', whiteSpace: 'nowrap', maxHeight: 140, overflow: 'hidden', textOverflow: 'ellipsis', fontWeight: 600 },
  locCell: { whiteSpace: 'nowrap', padding: '1px 4px', borderBottom: '1px solid #f1f5f9' },
  keyName: { color: '#94a3b8', fontSize: 10 },
  twisty: { border: 'none', background: 'none', cursor: 'pointer', width: 14, padding: 0 },
  twistySpace: { display: 'inline-block', width: 14 },
  anyCell: { textAlign: 'center', color: '#94a3b8', borderBottom: '1px solid #f1f5f9' },
  glyphCell: { textAlign: 'center', cursor: 'pointer', borderBottom: '1px solid #f1f5f9', borderLeft: '1px solid #f8fafc', fontSize: 13 },
  selectedCell: { outline: '2px solid #0e7490' },
  none: { color: '#e2e8f0' },
  footHead: { fontWeight: 600, padding: 4, position: 'sticky', bottom: 0, background: '#f8fafc' },
  footCell: { textAlign: 'center', fontSize: 10, padding: '4px 2px', position: 'sticky', bottom: 0, background: '#f8fafc', whiteSpace: 'nowrap' },
  pane: { width: 340, borderLeft: '1px solid #e2e8f0', padding: 10, overflow: 'auto' },
  paneHead: { display: 'flex', alignItems: 'center', gap: 6 },
  paneActions: { display: 'flex', flexWrap: 'wrap', gap: 4, marginTop: 10 },
  record: { borderTop: '1px solid #f1f5f9', padding: '6px 0' },
  dropoff: { borderTop: '1px solid #e2e8f0', padding: '6px 12px', background: '#f8fafc' },
  loss: { color: '#6b7280', fontSize: 11 },
  cleanup: {
    position: 'absolute', right: 24, top: 60, width: 520, maxHeight: '70vh', overflow: 'auto',
    background: '#fff', border: '1px solid #cbd5e1', borderRadius: 6, padding: 12,
    boxShadow: '0 6px 24px rgba(0,0,0,0.25)',
  },
  candidate: { display: 'flex', gap: 6, alignItems: 'flex-start', padding: '4px 0' },
}

