/**
 * Structure > Compare: re-express the measure relative to one level of a
 * grouping layer — a difference, or a % change — so the reference level sits
 * at 0. Plan: .claude/plan-compare-to-reference.md.
 *
 * Everything shown comes from Python: the layers and levels, the default, the
 * inert reason (`capabilities.comparison`), and whether the drawn figure was
 * paired and what it dropped (`layout.meta.comparison`). Edits go through
 * compare.ts; the transform itself is `scistackplot.compare`.
 */

import {
  MODE_LABELS,
  droppedLines,
  layerChoices,
  levelsOf,
  modeChoice,
  withLayer,
  withLevel,
  withMode,
  type CompareCapability,
  type CompareMeta,
  type Comparison,
  type ModeChoice,
} from './compare'

interface Props {
  comparison: Comparison | null | undefined
  capability: CompareCapability | undefined
  meta: CompareMeta | null
  onChange: (next: Comparison | null | undefined) => void
}

export default function CompareSection({ comparison, capability, meta, onChange }: Props) {
  if (capability && !capability.available && !comparison) {
    return <div style={styles.note}>{capability.reason}</div>
  }
  const choice = modeChoice(comparison)
  const on = choice !== 'off' && comparison
  const levels = comparison ? levelsOf(capability, comparison.layer) : []
  const state = capability?.state
  return (
    <>
      <div style={styles.row}>
        <span style={styles.label}>Mode</span>
        <select
          value={choice}
          onChange={e => onChange(withMode(comparison, e.target.value as ModeChoice, capability))}
          style={styles.input}
        >
          {(['off', 'difference', 'percent'] as ModeChoice[]).map(option => (
            <option key={option} value={option}>
              {MODE_LABELS[option]}
            </option>
          ))}
        </select>
      </div>
      {on && (
        <>
          <div style={styles.row}>
            <span style={styles.label}>Layer</span>
            <select
              value={comparison.layer}
              onChange={e => onChange(withLayer(comparison, e.target.value, capability))}
              style={styles.input}
            >
              {layerChoices(comparison, capability).map(name => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div style={styles.row}>
            <span style={styles.label}>Reference</span>
            <select
              value={comparison.level}
              onChange={e => onChange(withLevel(comparison, e.target.value))}
              style={styles.input}
            >
              {/* The current level stays listed when filtered away, so the
                  control never silently shows a different one. */}
              {(levels.includes(comparison.level) ? levels : [comparison.level, ...levels]).map(
                level => (
                  <option key={level} value={level}>
                    {level}
                  </option>
                ),
              )}
            </select>
          </div>
          {state?.inert ? (
            <div style={styles.warn}>Not applied: {state.inert}</div>
          ) : (
            <div style={styles.note}>
              {state?.paired === false
                ? `Per summary — each value from the ${comparison.layer} ${comparison.level} centre. `
                : state?.paired
                  ? 'Paired — each unit against its own reference. '
                  : ''}
              {state?.reason ?? ''}
            </div>
          )}
          {droppedLines(meta).map(line => (
            <div key={line} style={styles.warn}>
              {line}
            </div>
          ))}
        </>
      )}
    </>
  )
}

const styles: Record<string, React.CSSProperties> = {
  row: { display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 },
  label: { fontSize: 11, color: 'var(--ps-text-secondary)', flex: '0 0 64px' },
  input: {
    flex: 1, minWidth: 0, background: 'var(--ps-control)', color: 'var(--ps-text-body)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, fontSize: 11, padding: '3px 5px',
  },
  note: { fontSize: 10, color: 'var(--ps-text-faint)', marginBottom: 6, lineHeight: 1.4 },
  warn: { fontSize: 10, color: 'var(--ps-accent-text)', marginBottom: 4, lineHeight: 1.4 },
}
