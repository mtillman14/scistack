/**
 * VariableNode — represents a named variable type in the pipeline.
 *
 * Shows the variable type name, and the record count of the variant the
 * Variants popup marks current with its V-number (variableNodeText.ts).
 * Variant selection (which constants produced this variable) now lives
 * on the upstream ConstantNode.
 */

import { Handle, Position } from '@xyflow/react'
import { variableNodeText, type NodeVariants } from './variableNodeText'
import { useExpectationBadge } from '../../context/ExpectationBadgesContext'

export interface Variant {
  constants: Record<string, unknown>
  checked?: boolean
}

export interface VariableNodeData {
  label: string
  /** Every record of the type, all variants pooled (the tooltip's "in all"). */
  total_records: number
  /** The Variants popup's current card(s) — what the node shows (variableNodeText.ts). */
  variant?: NodeVariants
  // 'pending' is GUI-only: a staged (unrun) constant value — not in the DB.
  run_state?: 'green' | 'pending' | 'red'
}

interface Props {
  data: VariableNodeData
}

const STATE_STYLES: Record<string, { border: string; background: string }> = {
  green:   { border: '#16a34a', background: '#f0fdf4' },
  // Yellow (not orange — orange belongs to path-input nodes): a change
  // staged in the GUI that isn't in the database yet.
  pending: { border: '#eab308', background: '#fefce8' },
  red:     { border: '#dc2626', background: '#fef2f2' },
}

export default function VariableNode({ data }: Props) {
  const stateStyle = data.run_state ? STATE_STYLES[data.run_state] : null
  const text = variableNodeText(data.total_records, data.variant)
  const { badge, open } = useExpectationBadge(data.label)

  return (
    <div style={{
      ...styles.container,
      ...(stateStyle ? { border: `2px solid ${stateStyle.border}`, background: stateStyle.background } : {}),
    }}>
      <Handle type="target" position={Position.Left} />

      <div style={styles.label}>{data.label}</div>

      <div title={text.tooltip}>
        <div style={styles.count}>
          {text.countLine ?? <span style={styles.empty}>empty</span>}
          {text.more && <span style={styles.more}> {text.more}</span>}
        </div>
        {text.heading && <div style={styles.heading}>{text.heading}</div>}
      </div>

      {badge && (badge.problems > 0 || badge.marks > 0) && (
        <div style={styles.badges}>
          {badge.problems > 0 && (
            <button
              type="button"
              className="nodrag"
              style={{ ...styles.badge, ...styles.badgeProblem }}
              title={`${badge.problems} location(s) do not match what is expected (missing or present unexpectedly). Click to see them.`}
              onClick={e => { e.stopPropagation(); open(data.label, 'problems') }}
            >
              ✕ {badge.problems}
            </button>
          )}
          {badge.marks > 0 && (
            <button
              type="button"
              className="nodrag"
              style={styles.badge}
              title={`${badge.marks} Expected / Not expected mark(s) on this variable. Click to see them.`}
              onClick={e => { e.stopPropagation(); open(data.label, 'marks') }}
            >
              🏷 {badge.marks}
            </button>
          )}
        </div>
      )}

      <Handle type="source" position={Position.Right} />
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  badges: { display: 'flex', gap: 4, marginTop: 4 },
  badge: {
    border: '1px solid #9ca3af',
    borderRadius: 8,
    background: '#f3f4f6',
    color: '#374151',
    fontSize: 10,
    padding: '0 6px',
    cursor: 'pointer',
    lineHeight: '16px',
  },
  badgeProblem: { border: '1px solid #dc2626', background: '#fef2f2', color: '#b91c1c' },
  container: {
    background: '#fff',
    border: '2px solid #4a90d9',
    borderRadius: 8,
    padding: '8px 12px',
    minWidth: 160,
    fontSize: 13,
    boxShadow: '0 2px 6px rgba(0,0,0,0.12)',
  },
  label: {
    fontWeight: 600,
    color: '#1a1a2e',
    marginBottom: 4,
  },
  count: {
    fontSize: 11,
    color: '#666',
  },
  empty: {
    fontStyle: 'italic',
    color: '#bbb',
  },
  more: {
    color: '#999',
  },
  // The current variant's settings; full text in the tooltip.
  heading: {
    fontSize: 10,
    color: '#888',
    maxWidth: 220,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
}
