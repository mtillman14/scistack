/**
 * SchemaLocationPicker — every schema location for one Variable Variant, with
 * its integrity status, and the two things you can do with one.
 *
 * Backed by `plot_location_tree` (scidb.locations.location_states). Four states,
 * defined once in Python and only *drawn* here — see
 * docs/claude/schema-location-status.md:
 *
 *   green  a record exists for this variant, and everything it was computed
 *          from is still the newest at its own location
 *   amber  a record exists, but something upstream has been RE-SAVED since
 *   red    expected here, and no record for this variant
 *   grey   deliberately excluded, and so counted in neither the numerator nor
 *          the denominator
 *
 * Two interactions, deliberately distinct (plan D3 + D5):
 *
 *   click a row      "only this one" — the single select. Clears every other
 *                    selection, so the figure shows that location alone.
 *   tick a checkbox  add or remove that location from what is drawn, leaving
 *                    the rest of the selection alone.
 *
 * The checkbox is why `LocationFilter` exists: a tree of them means something
 * RAGGED (all of subject 01, plus trials 1-3 of subject 02), and per-column
 * `Filter` include-lists can only express a Cartesian product. See
 * scistackplot.spec.LocationFilter.
 *
 * Selection is stored as the MINIMAL COVERING SET: a fully-ticked subject
 * collapses to its own one-element prefix, so a trial added to that subject
 * later is inside the selection rather than silently outside it. Empty means
 * "everything", which is also what an untouched picker means.
 */

import { useEffect, useMemo, useState } from 'react'
import { callBackend } from '../../api'
import { overlay, dialog, dialogTitle } from '../modalStyles'
import {
  type LocationNode,
  type LocationTree,
  type PathStep,
  type Prefix,
  coverageOf,
  hasProblem,
  matchesQuery,
  without,
  withPath,
} from './locationSelection'

// Re-exported so callers that hold a picked path (PlotStudio, PipelineDAG,
// PlotRoot) import the type from the component they got it from, rather than
// each reaching into the rules module for a two-element tuple.
export type { PathStep, Prefix, LocationNode, LocationTree }


interface Props {
  variable: string
  /** The plotting layer's column-keyed variant selection; omit on the canvas. */
  selection?: Record<string, unknown> | null
  /** Current `spec.location_filter.include`; `[]` or undefined means "all". */
  value?: Prefix[]
  /**
   * Checkbox edits. Omitted (the canvas entry, where no spec is open) hides
   * the checkboxes entirely: a control that cannot change anything should not
   * be drawn as though it could.
   */
  onChange?: (include: Prefix[]) => void
  /** A row was clicked: show this location alone. */
  onPick?: (path: Prefix) => void
  onClose: () => void
  csvPath?: string
}


// --- view -------------------------------------------------------------------

const MARK: Record<string, string> = { green: '●', amber: '◐', red: '○', grey: '·' }
const COLOR: Record<string, string> = {
  green: '#16a34a',
  amber: '#eab308',
  red: '#dc2626',
  grey: '#6b7280',
}

export default function SchemaLocationPicker({
  variable,
  selection = null,
  value,
  onChange,
  onPick,
  onClose,
  csvPath,
}: Props) {
  const [tree, setTree] = useState<LocationTree | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [query, setQuery] = useState('')
  const [problemsOnly, setProblemsOnly] = useState(false)
  const [expanded, setExpanded] = useState<Set<string>>(new Set())

  const include = useMemo<Prefix[]>(() => value ?? [], [value])

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const result = (await callBackend('plot_location_tree', {
          variable,
          selection,
          csv_path: csvPath ?? null,
        })) as LocationTree
        if (!cancelled) setTree(result)
      } catch (err) {
        if (!cancelled) setError((err as Error).message)
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [variable, selection, csvPath])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const toggleExpanded = (id: string) => {
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const normalizedQuery = query.trim().toLowerCase()

  const renderNode = (node: LocationNode, depth: number) => {
    const id = JSON.stringify(node.path)
    if (problemsOnly && !hasProblem(node)) return null
    if (!matchesQuery(node, normalizedQuery)) return null

    // A search auto-expands: hiding the hit inside a collapsed parent is the
    // one thing a search must never do.
    const open = expanded.has(id) || normalizedQuery.length > 0
    const cov = onChange ? coverageOf(node, include) : 'none'
    const countable = node.counts.green + node.counts.amber + node.counts.red

    return (
      <div key={id}>
        <div style={{ ...styles.row, paddingLeft: 8 + depth * 16 }}>
          <span
            style={{ ...styles.twisty, visibility: node.children.length ? 'visible' : 'hidden' }}
            onClick={() => toggleExpanded(id)}
            role="button"
          >
            {open ? '▾' : '▸'}
          </span>

          {onChange && (
            <input
              type="checkbox"
              checked={cov === 'full'}
              ref={el => { if (el) el.indeterminate = cov === 'partial' }}
              onChange={() =>
                onChange(
                  cov === 'full'
                    ? without(tree!.roots, include, node.path)
                    : withPath(tree!.roots, include, node.path)
                )
              }
              title="Include this location in the figure"
              style={styles.checkbox}
            />
          )}

          <span style={{ ...styles.mark, color: COLOR[node.state] }}>{MARK[node.state]}</span>

          <span
            style={styles.label}
            onClick={() => onPick?.(node.path)}
            title="Plot this location on its own"
          >
            {node.key}={node.value}
          </span>

          {/* Counts on parents only: at a leaf they are always 1/1 or 0/1,
              which the dot has already said. */}
          {node.children.length > 0 && (
            <span style={styles.counts}>{node.counts.green}/{countable}</span>
          )}
          {node.state === 'grey' && node.children.length === 0 && (
            <span style={styles.excluded}>excluded</span>
          )}
          {node.code_version && (
            <span style={styles.version} title="the code version that produced this record">
              {node.code_version}
            </span>
          )}
        </div>
        {open && node.children.map(child => renderNode(child, depth + 1))}
      </div>
    )
  }

  const variantLabel = tree
    ? Object.entries(tree.selection ?? {})
        .map(([k, v]) => `${k}=${Array.isArray(v) ? v.join('/') : String(v)}`)
        .join(', ')
    : ''

  return (
    <div style={overlay} onClick={onClose}>
      <div style={{ ...dialog, width: 620, display: 'flex', flexDirection: 'column' }}
           onClick={e => e.stopPropagation()}>
        <div style={dialogTitle}>Schema locations — {variable}</div>

        {loading && <div style={styles.muted}>Checking every location…</div>}
        {error && <div style={styles.error}>{error}</div>}

        {tree && (
          <>
            <div style={styles.header}>
              <span style={{ ...styles.verdictMark, color: COLOR[tree.verdict] }}>
                {MARK[tree.verdict]}
              </span>
              <span style={styles.verdictText}>
                {tree.green}/{tree.total} locations green
              </span>
              {tree.counts.amber > 0 && (
                <span style={{ color: COLOR.amber }}>{tree.counts.amber} stale</span>
              )}
              {tree.counts.red > 0 && (
                <span style={{ color: COLOR.red }}>{tree.counts.red} missing</span>
              )}
              {tree.counts.grey > 0 && (
                <span style={{ color: COLOR.grey }}>{tree.counts.grey} excluded</span>
              )}
            </div>

            {variantLabel && <div style={styles.muted}>variant: {variantLabel}</div>}

            {/* Notes are caveats about what the count is WORTH — a denominator
                nothing can derive, a loader that can only be checked against
                the filesystem. They belong beside the number, not buried. */}
            {tree.notes.map(note => (
              <div key={note} style={styles.note}>{note}</div>
            ))}

            <div style={styles.controls}>
              <input
                type="text"
                placeholder="Search locations…"
                value={query}
                onChange={e => setQuery(e.target.value)}
                style={styles.search}
              />
              {/* The query a scientist actually runs. A name search is the
                  weaker half: a healthy study is mostly green, and the
                  exceptions are why the pane was opened. */}
              <label style={styles.toggle}>
                <input
                  type="checkbox"
                  checked={problemsOnly}
                  onChange={e => setProblemsOnly(e.target.checked)}
                />
                Problems only
              </label>
            </div>

            <div style={styles.scroll}>
              {tree.roots.length === 0 && (
                <div style={styles.muted}>(no schema locations)</div>
              )}
              {tree.roots.map(root => renderNode(root, 0))}
            </div>

            {onChange && (
              <div style={styles.footer}>
                <span style={styles.muted}>
                  {include.length === 0
                    ? 'Every location included'
                    : `${include.length} selection${include.length === 1 ? '' : 's'}`}
                </span>
                <button style={styles.button} onClick={() => onChange([])} type="button">
                  Select all
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  header: { display: 'flex', alignItems: 'center', gap: 10, fontSize: 13, marginBottom: 6 },
  verdictMark: { fontSize: 16 },
  verdictText: { fontWeight: 600 },
  muted: { fontSize: 11, color: '#9aa0b4', marginBottom: 6 },
  note: {
    fontSize: 11,
    color: '#eab308',
    background: '#2a2a1a',
    border: '1px solid #4a4a2a',
    borderRadius: 4,
    padding: '4px 8px',
    marginBottom: 6,
  },
  error: { fontSize: 12, color: '#f87171', marginBottom: 8 },
  controls: { display: 'flex', gap: 10, alignItems: 'center', marginBottom: 8 },
  search: {
    flex: 1,
    background: '#12121f',
    border: '1px solid #2a2a4a',
    color: '#eee',
    borderRadius: 4,
    padding: '4px 8px',
    fontSize: 12,
  },
  toggle: { fontSize: 11, color: '#9aa0b4', display: 'flex', alignItems: 'center', gap: 4 },
  scroll: { overflowY: 'auto', maxHeight: '45vh', border: '1px solid #2a2a4a', borderRadius: 4 },
  row: { display: 'flex', alignItems: 'center', gap: 6, padding: '2px 6px', fontSize: 12 },
  twisty: { cursor: 'pointer', width: 12, color: '#9aa0b4', userSelect: 'none' },
  checkbox: { margin: 0 },
  mark: { fontSize: 13, width: 12, textAlign: 'center' },
  label: { cursor: 'pointer', flex: 1 },
  counts: { fontSize: 11, color: '#9aa0b4', fontVariantNumeric: 'tabular-nums' },
  excluded: { fontSize: 10, color: '#6b7280', fontStyle: 'italic' },
  version: {
    fontSize: 10,
    color: '#9aa0b4',
    border: '1px solid #2a2a4a',
    borderRadius: 3,
    padding: '0 4px',
  },
  footer: { display: 'flex', alignItems: 'center', gap: 10, marginTop: 8 },
  button: {
    background: '#2a2a4a',
    border: '1px solid #3a3a5a',
    color: '#eee',
    borderRadius: 4,
    padding: '3px 10px',
    fontSize: 11,
    cursor: 'pointer',
  },
}
