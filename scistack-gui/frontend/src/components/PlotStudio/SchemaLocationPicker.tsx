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
 * TWO PANES, ONE SELECTION. They are two lenses on the same stored pair, and
 * neither holds its own checkbox state, so they cannot disagree:
 *
 *   left, "By key"     one entry per schema key, with its levels nested. A
 *                      tick here is a standing RULE — "this session is out,
 *                      everywhere, including in data collected next month".
 *   right, "Locations" the hierarchy. A tick here is a PLACE, and the set of
 *                      them may be RAGGED: all of subject 01, plus trials 1-3
 *                      of subject 02.
 *
 * Neither can express the other. A tree has one subtree per subject and so no
 * single box for "that session, everywhere"; a per-key list cannot untick one
 * trial of one subject without unticking that trial for all of them. A
 * selection is therefore a PAIR, and the full rule — including why an omitted
 * level is stored as a rule rather than exploded into prefixes — is
 * docs/claude/location-filter-semantics.md.
 *
 * Three interactions, deliberately distinct:
 *
 *   click a row      "only this one" — the single select. Clears every other
 *                    selection, so the figure shows that location alone.
 *   tick a checkbox  add or remove that location from what is drawn, leaving
 *                    the rest of the selection alone.
 *   tick on the left omit (or restore) a level across every other key. A level
 *                    the tree has partly unticked shows AMBER here; clicking
 *                    it resolves to fully on, so one click is always
 *                    recoverable by a second.
 *
 * `include` is stored as the MINIMAL COVERING SET: a fully-ticked subject
 * collapses to its own one-element prefix, so a trial added to that subject
 * later is inside the selection rather than silently outside it. An empty pair
 * means "everything", which is also what an untouched picker means.
 */

import { useEffect, useMemo, useState } from 'react'
import { callBackend } from '../../api'
import SchemaKeyLevels from './SchemaKeyLevels'
import { overlay, dialog, dialogTitle } from '../modalStyles'
import {
  type LocationNode,
  type LocationSelection,
  type LocationTree,
  type PathStep,
  type Prefix,
  EMPTY_SELECTION,
  asSelection,
  coverageOf,
  describeSelection,
  hasProblem,
  keyCoverage,
  levelCoverage,
  levelsByKey,
  matchesQuery,
  toggleKey,
  toggleLevel,
  without,
  visibleSelection,
  withPath,
} from './locationSelection'

// Re-exported so callers that hold a picked path (PlotStudio, PipelineDAG,
// PlotRoot) import the type from the component they got it from, rather than
// each reaching into the rules module for a two-element tuple.
export type { PathStep, Prefix, LocationNode, LocationTree }


interface Props {
  variable: string
  /**
   * A function node, instead of a variable. The tree is then the INNER JOIN of
   * that node's input variables' locations (`node_location_tree` →
   * `scidb.locations.intersect_location_states`): the locations it can
   * actually run, which is the processing tab's question.
   */
  nodeId?: string
  /** The plotting layer's column-keyed variant selection; omit on the canvas. */
  selection?: Record<string, unknown> | null
  /** Current `spec.location_filter`; an empty pair means "all". */
  value?: LocationSelection | null
  /**
   * Checkbox edits. Omitted (the canvas entry, where no spec is open) hides
   * the checkboxes entirely: a control that cannot change anything should not
   * be drawn as though it could.
   */
  onChange?: (selection: LocationSelection) => void
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
  nodeId,
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

  // What this TREE can show: a spec's filter is shared by every variant row,
  // so it may carry steps for a key this variable was never saved at.
  //
  // Named `locations` rather than `selection` because the PROP called
  // `selection` is a different thing entirely — the column-keyed VARIANT
  // selection the tree is computed for.
  const locations = useMemo(
    () => visibleSelection(tree?.roots ?? [], asSelection(value)),
    [tree, value]
  )

  // Levels come from the TREE, so the by-key pane lists only what this
  // variable and variant actually have: a level with no data here cannot be
  // omitted from a figure that was never going to draw it, and offering it
  // would invite exactly that click.
  const levels = useMemo(() => levelsByKey(tree?.roots ?? []), [tree])
  // Schema order for the keys — `levelsByKey` returns them in tree order,
  // which is the same thing until a variable skips a level of the hierarchy.
  const keyOrder = useMemo(() => {
    const found = Object.keys(levels)
    const declared = (tree?.schema_keys ?? []).filter(key => key in levels)
    return [...declared, ...found.filter(key => !declared.includes(key))]
  }, [levels, tree])

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        // One component, two questions: a VARIABLE under a variant (the
        // plotting tab), or a NODE's inputs intersected (the processing
        // tab). Same payload shape on purpose — a second shape here would
        // be a second renderer.
        const result = (await callBackend(
          nodeId ? 'node_location_tree' : 'plot_location_tree',
          nodeId
            ? { node_id: nodeId }
            : { variable, selection, csv_path: csvPath ?? null }
        )) as LocationTree
        if (!cancelled) setTree(result)
      } catch (err) {
        if (!cancelled) setError((err as Error).message)
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [variable, nodeId, selection, csvPath])

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
    const cov = onChange ? coverageOf(node, locations) : 'none'
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
                    ? without(tree!.roots, locations, node.path)
                    : withPath(tree!.roots, locations, node.path)
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
      <div style={{ ...dialog, width: 860, maxWidth: '95vw', display: 'flex', flexDirection: 'column' }}
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

            {/* Two panes, one selection. The left says "which LEVELS" (a
                standing rule, applying across every other key); the right says
                "which PLACES" (ragged, per location). Both are drawn from the
                same pair and neither stores its own checkbox state, so ticking
                a subject on the left ticks its whole subtree on the right, and
                unticking one of its trials on the right turns the left entry
                amber. See docs/claude/location-filter-semantics.md. */}
            <div style={styles.panes}>
              <div style={{ ...styles.scroll, ...styles.keyPane }}>
                <div style={styles.paneTitle}>By key</div>
                <SchemaKeyLevels
                  keys={keyOrder}
                  levels={levels}
                  coverageOfKey={key => keyCoverage(tree.roots, locations, key)}
                  coverageOfLevel={(key, value) =>
                    levelCoverage(tree.roots, locations, key, value)
                  }
                  onToggleKey={
                    onChange
                      ? key => onChange(toggleKey(tree.roots, locations, key))
                      : undefined
                  }
                  onToggleLevel={
                    onChange
                      ? (key, value) =>
                          onChange(toggleLevel(tree.roots, locations, key, value))
                      : undefined
                  }
                />
              </div>

              <div style={{ ...styles.scroll, ...styles.treePane }}>
                <div style={styles.paneTitle}>Locations</div>
                {tree.roots.length === 0 && (
                  <div style={styles.muted}>(no schema locations)</div>
                )}
                {tree.roots.map(root => renderNode(root, 0))}
              </div>
            </div>

            {onChange && (
              <div style={styles.footer}>
                <span style={styles.muted}>
                  {describeSelection(locations)}
                </span>
                <button style={styles.button} onClick={() => onChange(EMPTY_SELECTION)} type="button">
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
  panes: { display: 'flex', gap: 8, alignItems: 'stretch' },
  scroll: { overflowY: 'auto', maxHeight: '45vh', border: '1px solid #2a2a4a', borderRadius: 4 },
  // The by-key pane is deliberately THIN: it lists short level names, and the
  // tree beside it carries the long ones plus their status.
  keyPane: { flex: '0 0 220px', minWidth: 160 },
  treePane: { flex: 1, minWidth: 0 },
  paneTitle: {
    fontSize: 10,
    letterSpacing: 0.6,
    textTransform: 'uppercase',
    color: '#6b7280',
    padding: '4px 6px 2px',
    position: 'sticky',
    top: 0,
    background: '#161626',
  },
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
