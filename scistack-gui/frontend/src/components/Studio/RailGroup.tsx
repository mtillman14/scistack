/**
 * The rail's building blocks, shared by Plot Studio and StatStudio
 * (docs/claude/studio-unification.md): one look and one behaviour for every
 * studio's controls.
 *
 * - `Group`: a collapsible group of Sections. Collapsed, the header carries
 *   the group's one-line summary. The body is hidden, not unmounted, so an
 *   input mid-edit keeps its text.
 * - `Section`: a titled block, with its fixed explanation behind ⓘ. A note
 *   about the CURRENT figure or table is a child, always shown.
 *
 * Colours only through `var(--ps-*)` tokens (plotTheme.ts).
 */

import { useState } from 'react'

export function Section({ title, hint, children }: { title: string; hint?: string; children: React.ReactNode }) {
  const [explain, setExplain] = useState(false)
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitleRow}>
        <div style={styles.sectionTitle}>{title}</div>
        {hint && (
          <button
            type="button"
            style={{ ...styles.hintToggle, ...(explain ? styles.hintToggleOn : null) }}
            onClick={() => setExplain(v => !v)}
            aria-pressed={explain}
            title={explain ? 'Hide the explanation' : 'What is this?'}
          >
            ⓘ
          </button>
        )}
      </div>
      {hint && explain && <div style={styles.hint}>{hint}</div>}
      {children}
    </div>
  )
}

export function Group({
  title,
  summary,
  open,
  onToggle,
  children,
}: {
  title: string
  summary: string
  open: boolean
  onToggle: () => void
  children: React.ReactNode
}) {
  return (
    <div style={styles.group} data-testid={`rail-group-${title}`}>
      <button
        type="button"
        style={styles.groupHeader}
        onClick={onToggle}
        aria-expanded={open}
        title={summary}
      >
        <span style={styles.groupChevron}>{open ? '▾' : '▸'}</span>
        <span style={styles.groupTitle}>{title}</span>
        {!open && <span style={styles.groupSummary}>{summary}</span>}
      </button>
      <div style={{ ...styles.groupBody, display: open ? 'block' : 'none' }}>{children}</div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  group: { borderBottom: '1px solid var(--ps-border)', marginBottom: 4 },
  groupHeader: {
    display: 'flex', alignItems: 'baseline', gap: 6, width: '100%', minWidth: 0,
    padding: '6px 0', background: 'none', border: 'none', cursor: 'pointer',
    textAlign: 'left', color: 'var(--ps-text-control)',
  },
  groupChevron: { fontSize: 10, color: 'var(--ps-text-muted)', flexShrink: 0, width: 10 },
  groupTitle: {
    fontSize: 11, textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 700,
    color: 'var(--ps-text-control)', flexShrink: 0,
  },
  groupSummary: {
    fontSize: 10, color: 'var(--ps-text-muted)', minWidth: 0,
    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  groupBody: { padding: '4px 0 4px 16px' },
  section: { marginBottom: 14 },
  sectionTitleRow: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 6,
    marginBottom: 6,
  },
  sectionTitle: {
    fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.6,
    color: 'var(--ps-accent)', fontWeight: 700,
  },
  hintToggle: {
    background: 'none', border: 'none', padding: 0, cursor: 'pointer', fontSize: 11,
    lineHeight: 1, color: 'var(--ps-text-muted)',
  },
  hintToggleOn: { color: 'var(--ps-accent)' },
  hint: { fontSize: 10, color: 'var(--ps-text-faint)', marginBottom: 6, fontStyle: 'italic' },
}
