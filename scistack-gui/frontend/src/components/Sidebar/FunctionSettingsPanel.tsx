/**
 * FunctionSettingsPanel — shown in the sidebar when a function node is selected.
 *
 * Sections:
 *   1. Variants — read-only Cartesian product of constant node values.
 *   2. Schema Filter — checkboxes per schema key to restrict which combos run.
 *   3. Run Options — dry_run, save, distribute toggles.
 *
 * Schema filter and run options are stored on the function node's data so they
 * persist across selection changes and are available to handleRun.
 */

import { useEffect, useState, useCallback } from 'react'
import { useReactFlow } from '@xyflow/react'
import { callBackend } from '../../api'

interface VariantRow {
  [constantName: string]: string
}

export interface SchemaFilter {
  [key: string]: unknown[]  // schema key → selected values
}

export interface RunOptions {
  dry_run: boolean
  save: boolean
  distribute: boolean
}

interface Props {
  id: string
  label: string
  variants: VariantRow[]
  constantNames: string[]
  schemaFilter: SchemaFilter | null
  schemaLevel: string[] | null    // which schema keys to iterate over; null = all
  runOptions: RunOptions
}

interface SchemaInfo {
  keys: string[]
  values: Record<string, unknown[]>
}

export default function FunctionSettingsPanel({ id, label, variants, constantNames, schemaFilter, schemaLevel, runOptions }: Props) {
  const { setNodes } = useReactFlow()
  const [schema, setSchema] = useState<SchemaInfo | null>(null)

  useEffect(() => {
    callBackend('get_schema')
      .then(d => setSchema(d as SchemaInfo))
      .catch(console.error)
  }, [])

  // Helper to update function node data and persist config to backend.
  const updateNodeData = useCallback((patch: Record<string, unknown>) => {
    setNodes(nds => {
      const updated = nds.map(node =>
        node.id === id
          ? { ...node, data: { ...node.data, ...patch } }
          : node
      )
      // Persist config to backend.
      const node = updated.find(n => n.id === id)
      if (node) {
        const config: Record<string, unknown> = {}
        const d = node.data as Record<string, unknown>
        if (d.schemaFilter) config.schemaFilter = d.schemaFilter
        if (d.schemaLevel) config.schemaLevel = d.schemaLevel
        if (d.runOptions) config.runOptions = d.runOptions
        callBackend('put_node_config', { node_id: id, config })
      }
      return updated
    })
  }, [id, setNodes])

  // Toggle a single value in the schema filter.
  const toggleSchemaValue = useCallback((key: string, value: unknown) => {
    if (!schema) return
    const allValues = schema.values[key] ?? []
    const current = schemaFilter?.[key] ?? allValues
    const valStr = String(value)
    const isSelected = current.some(v => String(v) === valStr)

    let updated: unknown[]
    if (isSelected) {
      updated = current.filter(v => String(v) !== valStr)
    } else {
      updated = [...current, value]
    }

    const newFilter: SchemaFilter = { ...(schemaFilter ?? {}), [key]: updated }

    // If all values selected for this key, remove the key from filter (means "all").
    if (updated.length === allValues.length) {
      delete newFilter[key]
    }

    // If filter is empty (all keys have all values), store null.
    const hasFilter = Object.keys(newFilter).length > 0
    updateNodeData({ schemaFilter: hasFilter ? newFilter : null })
  }, [schema, schemaFilter, updateNodeData])

  // Select all / none helpers.
  const selectAll = useCallback((key: string) => {
    const newFilter = { ...(schemaFilter ?? {}) }
    delete newFilter[key]
    const hasFilter = Object.keys(newFilter).length > 0
    updateNodeData({ schemaFilter: hasFilter ? newFilter : null })
  }, [schemaFilter, updateNodeData])

  const selectNone = useCallback((key: string) => {
    const newFilter: SchemaFilter = { ...(schemaFilter ?? {}), [key]: [] }
    updateNodeData({ schemaFilter: newFilter })
  }, [schemaFilter, updateNodeData])

  // Toggle a schema key in the iteration level.
  const toggleSchemaLevel = useCallback((key: string) => {
    if (!schema) return
    const allKeys = schema.keys
    const current = schemaLevel ?? allKeys
    const isSelected = current.includes(key)

    let updated: string[]
    if (isSelected) {
      updated = current.filter(k => k !== key)
    } else {
      // Maintain the original order from schema.keys.
      updated = allKeys.filter(k => current.includes(k) || k === key)
    }

    // If all keys selected, store null (means "all").
    const isAll = updated.length === allKeys.length
    updateNodeData({ schemaLevel: isAll ? null : updated })
  }, [schema, schemaLevel, updateNodeData])

  // Toggle a run option.
  const toggleRunOption = useCallback((opt: keyof RunOptions) => {
    const updated = { ...runOptions, [opt]: !runOptions[opt] }
    updateNodeData({ runOptions: updated })
  }, [runOptions, updateNodeData])

  return (
    <div style={styles.root}>
      <div style={styles.fnName}>{label}</div>

      {/* ---- Variants ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Variants</div>

        {variants.length === 0 && (
          <div style={styles.empty}>
            {constantNames.length === 0
              ? 'No constant nodes on canvas.'
              : 'No values defined on constant nodes.'}
          </div>
        )}

        {variants.length > 0 && (
          <table style={styles.table}>
            <thead>
              <tr>
                {constantNames.map(name => (
                  <th key={name} style={styles.th}>{name}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {variants.map((row, i) => (
                <tr key={i} style={styles.variantRow}>
                  {constantNames.map(name => (
                    <td key={name} style={styles.td}>
                      <span style={styles.pill}>{row[name] ?? '\u2014'}</span>
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {/* ---- Schema Filter ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Schema Filter</div>

        {!schema && <div style={styles.empty}>Loading schema...</div>}

        {schema && schema.keys.length === 0 && (
          <div style={styles.empty}>No schema keys configured.</div>
        )}

        {schema && schema.keys.map(key => {
          const allValues = schema.values[key] ?? []
          const selected = schemaFilter?.[key] ?? allValues

          return (
            <div key={key} style={styles.schemaKey}>
              <div style={styles.schemaKeyHeader}>
                <span style={styles.schemaKeyName}>{key}</span>
                <span style={styles.schemaKeyActions}>
                  <button
                    style={styles.linkBtn}
                    onClick={() => selectAll(key)}
                    title="Select all"
                  >all</button>
                  <span style={styles.separator}>/</span>
                  <button
                    style={styles.linkBtn}
                    onClick={() => selectNone(key)}
                    title="Select none"
                  >none</button>
                </span>
              </div>
              <div style={styles.checkboxGrid}>
                {allValues.map(value => {
                  const valStr = String(value)
                  const checked = selected.some(v => String(v) === valStr)
                  return (
                    <label key={valStr} style={styles.checkboxLabel}>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggleSchemaValue(key, value)}
                        style={styles.checkbox}
                      />
                      <span style={styles.checkboxText}>{valStr}</span>
                    </label>
                  )
                })}
              </div>
            </div>
          )
        })}
      </section>

      {/* ---- Schema Level ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Schema Level</div>

        {!schema && <div style={styles.empty}>Loading schema...</div>}

        {schema && schema.keys.length === 0 && (
          <div style={styles.empty}>No schema keys configured.</div>
        )}

        {schema && schema.keys.length > 0 && (
          <>
            <div style={styles.schemaLevelHint}>
              Which schema keys to iterate over
            </div>
            <div style={styles.checkboxGrid}>
              {schema.keys.map(key => {
                const checked = schemaLevel === null || schemaLevel.includes(key)
                return (
                  <label key={key} style={styles.checkboxLabel}>
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleSchemaLevel(key)}
                      style={styles.checkbox}
                    />
                    <span style={styles.checkboxText}>{key}</span>
                  </label>
                )
              })}
            </div>
          </>
        )}
      </section>

      {/* ---- Run Options ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Run Options</div>

        <label style={styles.optionLabel}>
          <input
            type="checkbox"
            checked={runOptions.dry_run}
            onChange={() => toggleRunOption('dry_run')}
            style={styles.checkbox}
          />
          <span style={styles.optionText}>Dry run</span>
          <span style={styles.optionHint}>Preview without executing</span>
        </label>

        <label style={styles.optionLabel}>
          <input
            type="checkbox"
            checked={runOptions.save}
            onChange={() => toggleRunOption('save')}
            style={styles.checkbox}
          />
          <span style={styles.optionText}>Save results</span>
          <span style={styles.optionHint}>Persist outputs to DB</span>
        </label>

        <label style={styles.optionLabel}>
          <input
            type="checkbox"
            checked={runOptions.distribute}
            onChange={() => toggleRunOption('distribute')}
            style={styles.checkbox}
          />
          <span style={styles.optionText}>Distribute</span>
          <span style={styles.optionHint}>Save at lower schema level</span>
        </label>
      </section>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '12px',
    color: '#ccc',
    fontSize: 12,
  },
  fnName: {
    fontFamily: 'monospace',
    fontWeight: 700,
    fontSize: 13,
    color: '#a89cf0',
    marginBottom: 12,
    wordBreak: 'break-all',
  },
  section: {
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 10,
    fontWeight: 700,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    marginBottom: 6,
  },
  empty: {
    color: '#555',
    fontStyle: 'italic',
    fontSize: 11,
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
  },
  th: {
    textAlign: 'left',
    fontSize: 10,
    color: '#888',
    fontWeight: 600,
    padding: '2px 4px 4px 0',
    borderBottom: '1px solid #2a2a4a',
    fontFamily: 'monospace',
  },
  variantRow: {
    borderBottom: '1px solid #1e1e3a',
  },
  td: {
    padding: '4px 4px 4px 0',
    verticalAlign: 'middle',
  },
  pill: {
    display: 'inline-block',
    background: '#1e1e3a',
    borderRadius: 3,
    padding: '1px 5px',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b2ded9',
  },
  // Schema filter styles
  schemaKey: {
    marginBottom: 10,
  },
  schemaKeyHeader: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 4,
  },
  schemaKeyName: {
    fontFamily: 'monospace',
    fontSize: 11,
    fontWeight: 600,
    color: '#a89cf0',
  },
  schemaKeyActions: {
    fontSize: 10,
    color: '#555',
  },
  linkBtn: {
    background: 'none',
    border: 'none',
    color: '#7b68ee',
    fontSize: 10,
    cursor: 'pointer',
    padding: 0,
    textDecoration: 'underline',
  },
  separator: {
    margin: '0 2px',
    color: '#444',
  },
  checkboxGrid: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 4,
  },
  checkboxLabel: {
    display: 'flex',
    alignItems: 'center',
    gap: 3,
    background: '#1e1e3a',
    borderRadius: 3,
    padding: '2px 6px',
    cursor: 'pointer',
  },
  checkbox: {
    margin: 0,
    accentColor: '#7b68ee',
  },
  checkboxText: {
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b2ded9',
  },
  schemaLevelHint: {
    fontSize: 10,
    color: '#555',
    fontStyle: 'italic',
    marginBottom: 4,
  },
  // Run options styles
  optionLabel: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    marginBottom: 6,
    cursor: 'pointer',
  },
  optionText: {
    fontSize: 11,
    color: '#ccc',
    fontWeight: 600,
  },
  optionHint: {
    fontSize: 10,
    color: '#555',
    fontStyle: 'italic',
  },
}
