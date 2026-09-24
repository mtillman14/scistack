/**
 * SchemaKeyLevels — schema keys, and the levels of each, as one tri-state list.
 *
 * Two levels of nesting and no more: the key, and its levels. That shape is the
 * feature. A hierarchical tree answers "which PLACES" — all of subject 01, plus
 * trials 1-3 of subject 02 — and cannot say "this session is out, everywhere",
 * because there is one subtree per subject and no single box to untick. This
 * list is the other half: one entry per level, applying across every other key.
 *
 * Deliberately knows nothing about plotting, specs, variables or RPCs. It is
 * handed the keys, their levels and two coverage functions, and reports clicks
 * back — so the Plot Studio picker and the processing tab's function settings
 * can use the same control over completely different state.
 *
 * The tri-state is COMPUTED BY THE CALLER (`coverageOfKey` / `coverageOfLevel`),
 * never stored here. That is what keeps this list and the location tree beside
 * it from disagreeing: both are derived from one selection
 * (docs/claude/location-filter-semantics.md), so a level unticked in the tree
 * shows up here as mixed rather than as a stale checkbox.
 */

import { useState } from 'react'

export type Coverage = 'full' | 'partial' | 'none'

interface Props {
  /** Keys in display order — the schema's own order, not the levels' order. */
  keys: string[]
  /** `{key: [levels]}`. A key with no levels is not drawn. */
  levels: Record<string, string[]>
  coverageOfKey: (key: string) => Coverage
  coverageOfLevel: (key: string, value: string) => Coverage
  /**
   * Clicks. Omitted (a read-only entry point) draws the boxes disabled rather
   * than hiding them: what is included is still worth seeing when it cannot be
   * changed here.
   */
  onToggleKey?: (key: string) => void
  onToggleLevel?: (key: string, value: string) => void
  /** Shown when there is nothing to list. */
  emptyNote?: string
}

const COLOR: Record<Coverage, string> = {
  full: 'var(--ps-status-full)',
  partial: 'var(--ps-amber)',
  none: 'var(--ps-text-faint-cool)',
}

export default function SchemaKeyLevels({
  keys,
  levels,
  coverageOfKey,
  coverageOfLevel,
  onToggleKey,
  onToggleLevel,
  emptyNote = '(no schema keys)',
}: Props) {
  // Collapsed by default: the list exists to be scanned key by key, and five
  // keys with twenty levels each would bury the thing being looked for.
  const [open, setOpen] = useState<Set<string>>(new Set())
  const editable = Boolean(onToggleKey || onToggleLevel)

  const toggleOpen = (key: string) =>
    setOpen(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })

  const present = keys.filter(key => (levels[key] ?? []).length > 0)
  if (present.length === 0) return <div style={styles.muted}>{emptyNote}</div>

  return (
    <div style={styles.list}>
      {present.map(key => {
        const keyCoverage = coverageOfKey(key)
        const expanded = open.has(key)
        return (
          <div key={key}>
            <div style={styles.keyRow}>
              <span
                style={styles.twisty}
                onClick={() => toggleOpen(key)}
                role="button"
                title={expanded ? 'Hide levels' : 'Show levels'}
              >
                {expanded ? '▾' : '▸'}
              </span>
              <input
                type="checkbox"
                checked={keyCoverage === 'full'}
                ref={el => { if (el) el.indeterminate = keyCoverage === 'partial' }}
                disabled={!editable}
                onChange={() => {
                  // Auto-expand on a toggle: a click that changes twenty
                  // levels should show what it changed.
                  setOpen(prev => new Set(prev).add(key))
                  onToggleKey?.(key)
                }}
                style={styles.checkbox}
                title="Include every level of this key"
              />
              <span
                style={{ ...styles.keyName, color: COLOR[keyCoverage] }}
                onClick={() => toggleOpen(key)}
              >
                {key}
              </span>
              <span style={styles.count}>{(levels[key] ?? []).length}</span>
            </div>

            {expanded &&
              (levels[key] ?? []).map(value => {
                const cov = coverageOfLevel(key, value)
                return (
                  <div key={value} style={styles.levelRow}>
                    <input
                      type="checkbox"
                      checked={cov === 'full'}
                      ref={el => { if (el) el.indeterminate = cov === 'partial' }}
                      disabled={!editable}
                      onChange={() => onToggleLevel?.(key, value)}
                      style={styles.checkbox}
                      title={`Include ${key}=${value} everywhere`}
                    />
                    <span style={{ ...styles.levelName, color: COLOR[cov] }}>
                      {value}
                    </span>
                  </div>
                )
              })}
          </div>
        )
      })}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  list: { fontSize: 12 },
  muted: { fontSize: 11, color: 'var(--ps-text-muted-slate)', padding: '4px 6px' },
  keyRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '3px 6px',
    borderTop: '1px solid var(--ps-border-faint)',
  },
  levelRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '2px 6px 2px 26px',
  },
  twisty: { cursor: 'pointer', width: 12, color: 'var(--ps-text-muted-slate)', userSelect: 'none' },
  checkbox: { margin: 0 },
  keyName: { cursor: 'pointer', fontWeight: 600, flex: 1 },
  levelName: { flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
  count: { fontSize: 10, color: 'var(--ps-text-faint-cool)', fontVariantNumeric: 'tabular-nums' },
}
