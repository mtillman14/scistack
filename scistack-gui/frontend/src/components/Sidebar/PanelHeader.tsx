/**
 * PanelHeader — the one header every node settings panel in the sidebar uses
 * (.claude/plan-palette-order-and-delete.md, step `panel_header`).
 *
 * Before this each panel built its own title (FunctionSettingsPanel's
 * `fnName`, PathInput's rename field, Variable/Parameter/Glue/Pipeline each
 * styled differently), and only GUI-made functions and glue could open their
 * source. One component now owns:
 *
 * - the node KIND (icon + label) and NAME (or a `nameSlot`, so PathInput's
 *   editable rename field stays where it is);
 * - "declared in `file:line`" (formatLocation, the one owner of that text)
 *   and an **Open source** button whenever the location is known;
 * - the read-only banner (ReadOnlyDeclarationBanner, unchanged) for an entity
 *   declared outside the entities file;
 * - **← Palette**, which clears the selection exactly as clicking the empty
 *   canvas does;
 * - the non-default badge summary (sidebarBadges.ts); clicking a chip
 *   scrolls to its section.
 */

import { useState } from 'react'
import { useStoreApi } from '@xyflow/react'
import { callBackend, isVSCodeMode } from '../../api'
import { useSelectedNode } from '../../context/SelectedNodeContext'
import { useSidebarSelection } from '../../context/SidebarSelectionContext'
import { SourceLocationDialog } from '../SourceLocationDialog'
import type { SourceLocation } from '../SourceLocationDialog'
import ReadOnlyDeclarationBanner from './ReadOnlyDeclarationBanner'
import { formatLocation } from './sourceLocation'
import type { Badge, BadgeSection } from './sidebarBadges'

export type PanelKind =
  | 'function' | 'plot' | 'stat' | 'glue'
  | 'variable' | 'parameter' | 'pathInput' | 'submodule'

const KIND_DISPLAY: Record<PanelKind, { icon: string; label: string; color: string }> = {
  function: { icon: 'f(x)', label: 'Function', color: '#7b68ee' },
  plot: { icon: '📈', label: 'Plot', color: '#7b68ee' },
  stat: { icon: 'Σ', label: 'Stat', color: '#7b68ee' },
  glue: { icon: '⋯', label: 'Glue', color: '#9ca3af' },
  variable: { icon: 'x', label: 'Variable', color: '#2a9d8f' },
  parameter: { icon: 'P', label: 'Parameter', color: '#2a9d8f' },
  pathInput: { icon: '📁', label: 'Path Input', color: '#d97706' },
  submodule: { icon: '⧉', label: 'Submodule', color: '#a21caf' },
}

/** DOM attribute a panel puts on each section so a badge chip can find it. */
export const SECTION_ATTR = 'data-panel-section'

export interface PanelHeaderProps {
  kind: PanelKind
  name: string
  /** Replaces the plain name (PathInput's rename field). */
  nameSlot?: React.ReactNode
  /** Where it is declared; `null` when unknown or not applicable. */
  source?: { file: string; line: number | null } | null
  /** Set when declared outside the entities file: shows the read-only banner. */
  readOnly?: { file: string; line: number | null } | null
  badges?: readonly Badge[]
}

export default function PanelHeader({ kind, name, nameSlot, source, readOnly, badges = [] }: PanelHeaderProps) {
  const { setSelectedNode } = useSelectedNode()
  const { setSelectedItem } = useSidebarSelection()
  const flowStore = useStoreApi()
  const [sourceLoc, setSourceLoc] = useState<SourceLocation | null>(null)
  const display = KIND_DISPLAY[kind]

  const backToPalette = () => {
    // Same as PipelineDAG's onPaneClick, plus clearing React Flow's own
    // selected flags so the node doesn't stay highlighted.
    console.debug('[PanelHeader] back to palette from', kind, name)
    setSelectedNode(null)
    setSelectedItem(null)
    flowStore.getState().unselectNodesAndEdges?.()
  }

  const openSource = () => {
    if (!source) return
    console.debug('[PanelHeader] open source', source)
    if (isVSCodeMode) {
      callBackend('reveal_in_editor', { file: source.file, line: source.line ?? 1 })
        .catch(err => window.alert(`Failed to open source: ${(err as Error).message}`))
    } else {
      setSourceLoc({ name, file: source.file, line: source.line ?? 0 })
    }
  }

  const scrollTo = (section: BadgeSection) => {
    const el = document.querySelector(`[${SECTION_ATTR}="${section}"]`)
    el?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }

  return (
    <div style={styles.root}>
      <div style={styles.topRow}>
        <button type="button" style={styles.backBtn} onClick={backToPalette} title="Back to the palette (deselect)">
          ← Palette
        </button>
        <span style={{ ...styles.kind, borderColor: display.color, color: display.color }} title={display.label}>
          <span style={styles.kindIcon}>{display.icon}</span>
          {display.label}
        </span>
        <span style={{ flex: 1 }} />
        {source && (
          <button type="button" style={styles.actionBtn} onClick={openSource} title={`Open ${source.file}`}>
            Open source
          </button>
        )}
      </div>
      <div style={styles.nameRow}>
        {nameSlot ?? <span style={styles.name} title={name}>{name}</span>}
      </div>
      {source && (
        <div style={styles.declared} title={source.line ? `${source.file}:${source.line}` : source.file}>
          declared in <span style={styles.mono}>{formatLocation(source)}</span>
        </div>
      )}
      {badges.length > 0 && (
        <div style={styles.badgeRow}>
          {badges.map(b => (
            <BadgeChip key={`${b.section}:${b.label}`} badge={b} onClick={() => scrollTo(b.section)} />
          ))}
        </div>
      )}
      {readOnly && <ReadOnlyDeclarationBanner file={readOnly.file} line={readOnly.line} />}
      {sourceLoc && <SourceLocationDialog location={sourceLoc} onClose={() => setSourceLoc(null)} />}
    </div>
  )
}

/** One badge. Also used beside section titles, so both places look alike. */
export function BadgeChip({ badge, onClick }: { badge: Badge; onClick?: () => void }) {
  const tone = badge.tone === 'warn' ? styles.chipWarn : styles.chipInfo
  return onClick ? (
    <button type="button" style={{ ...styles.chip, ...tone, cursor: 'pointer' }} onClick={onClick} title={badge.title}>
      {badge.label}
    </button>
  ) : (
    <span style={{ ...styles.chip, ...tone }} title={badge.title}>{badge.label}</span>
  )
}

/** A section's chips, for beside its title. Renders nothing when empty. */
export function SectionBadges({ badges }: { badges: readonly Badge[] }) {
  if (badges.length === 0) return null
  return (
    <span style={styles.sectionBadges}>
      {badges.map(b => <BadgeChip key={b.label} badge={b} />)}
    </span>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '4px 12px 8px',
    borderBottom: '1px solid #333',
    marginBottom: 8,
  },
  topRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
  },
  backBtn: {
    background: 'transparent',
    border: 'none',
    color: '#9ca3af',
    fontSize: 11,
    padding: '2px 0',
    cursor: 'pointer',
  },
  kind: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 4,
    fontSize: 10,
    fontWeight: 600,
    textTransform: 'uppercase',
    letterSpacing: 0.4,
    border: '1px solid',
    borderRadius: 3,
    padding: '0 5px',
  },
  kindIcon: {
    fontFamily: 'monospace',
    textTransform: 'none',
  },
  actionBtn: {
    background: 'transparent',
    border: '1px solid #444',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 11,
    padding: '1px 6px',
    cursor: 'pointer',
  },
  nameRow: {
    marginTop: 4,
  },
  name: {
    display: 'block',
    fontWeight: 700,
    fontSize: 14,
    color: '#eee',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  declared: {
    fontSize: 11,
    color: '#888',
    marginTop: 2,
  },
  mono: {
    fontFamily: 'monospace',
  },
  badgeRow: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 4,
    marginTop: 6,
  },
  sectionBadges: {
    display: 'inline-flex',
    flexWrap: 'wrap',
    gap: 4,
    marginLeft: 6,
    verticalAlign: 'middle',
  },
  chip: {
    fontSize: 10,
    lineHeight: '14px',
    borderRadius: 7,
    padding: '0 6px',
    border: '1px solid',
    background: 'transparent',
    fontWeight: 500,
    textTransform: 'none',
    letterSpacing: 0,
  },
  chipWarn: {
    color: '#f59e0b',
    borderColor: '#b45309',
    background: 'rgba(245, 158, 11, 0.10)',
  },
  chipInfo: {
    color: '#9ca3af',
    borderColor: '#4b5563',
  },
}
