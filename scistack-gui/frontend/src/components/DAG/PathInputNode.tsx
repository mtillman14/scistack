/**
 * PathInputNode — represents a scifor.PathInput in the pipeline.
 *
 * Displays the parameter name, path template, root folder (if set),
 * and the schema keys referenced by the template (e.g. {subject}, {trial}).
 *
 * Always a source node (feeds into functions); no target handle.
 */

import { Handle, Position } from '@xyflow/react'
import { aliasCount, type PathInputAliases } from '../Sidebar/pathInputAliases'
import type { PathInputKeyRegex } from '../Sidebar/pathInputKeyRegex'

export interface PathInputAlternate {
  template: string
  root_folder: string | null
  aliases?: PathInputAliases
  key_regex?: PathInputKeyRegex
}

export interface PathInputNodeData {
  label: string
  template: string
  root_folder: string | null
  aliases?: PathInputAliases
  key_regex?: PathInputKeyRegex
  alternate_templates: PathInputAlternate[]
}

interface Props {
  data: PathInputNodeData
}

/** Extract {placeholder} names from a template string. */
function parseTemplateKeys(template: string): string[] {
  const matches = template.match(/\{(\w+)\}/g)
  if (!matches) return []
  return [...new Set(matches.map(m => m.slice(1, -1)))]
}

export default function PathInputNode({ data }: Props) {
  const keys = parseTemplateKeys(data.template)
  const nAliases = aliasCount(data.aliases)

  return (
    <div style={styles.container}>
      <div style={styles.label}>{data.label}</div>

      <div style={styles.template}>{data.template}</div>

      {data.root_folder && (
        <div style={styles.rootFolder}>root: {data.root_folder}</div>
      )}

      {data.alternate_templates?.length > 0 && (
        <div style={styles.altBadge} title="Multiple templates — runs as EachOf(...)">
          +{data.alternate_templates.length} alt{data.alternate_templates.length > 1 ? 's' : ''}
        </div>
      )}

      {nAliases > 0 && (
        <div style={styles.altBadge} title="Several on-disk spellings of a schema-key value (aliases)">
          ≈{nAliases} alias{nAliases > 1 ? 'es' : ''}
        </div>
      )}

      {keys.length > 0 && (
        <div style={styles.keysRow}>
          {keys.map(k => (
            <span key={k} style={styles.keyPill}>{k}</span>
          ))}
        </div>
      )}

      <Handle type="source" position={Position.Right} />
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    background: '#2d2416',
    border: '2px solid #d97706',
    borderRadius: 6,
    padding: '6px 12px',
    minWidth: 160,
    fontSize: 13,
    boxShadow: '0 2px 6px rgba(0,0,0,0.10)',
  },
  label: {
    fontWeight: 600,
    color: '#fbbf24',
    fontFamily: 'monospace',
    textAlign: 'center',
    marginBottom: 4,
  },
  template: {
    fontSize: 11,
    color: '#e5c8a0',
    fontFamily: 'monospace',
    wordBreak: 'break-all',
    marginBottom: 3,
  },
  rootFolder: {
    fontSize: 10,
    color: '#8a7a60',
    fontFamily: 'monospace',
    wordBreak: 'break-all',
    marginBottom: 3,
  },
  keysRow: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 4,
    marginTop: 3,
  },
  keyPill: {
    fontSize: 10,
    fontFamily: 'monospace',
    background: '#3d2e1a',
    border: '1px solid #92702a',
    borderRadius: 3,
    padding: '1px 5px',
    color: '#fbbf24',
  },
  altBadge: {
    marginTop: 3,
    fontSize: 10,
    fontFamily: 'monospace',
    color: '#fbbf24',
    fontWeight: 600,
  },
}
