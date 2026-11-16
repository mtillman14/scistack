/**
 * Statistics > Test results: the statistical results the figure draws
 * (plan-stats-on-figure.md). The figure owns the request (`spec.stat_marks`);
 * `scistackstats.marks` computes the facts and `scistackstats.format` writes
 * every word. This section edits the request and shows what ran, and — never
 * silence — why nothing is drawn.
 */

import { barText } from './differenceBars'
import type { StatMarksState } from './useStatMarks'
import type { MarksResult, StatMarks } from './statMarks'

interface Props {
  marks: StatMarks | undefined
  /** `capabilities.difference_bars`: the vocabulary and whether bars fit. */
  capability: { available: boolean; reason: string | null; stat_pairs?: string[]; stat_labels?: string[] } | undefined
  state: StatMarksState
  onChange: (patch: Partial<StatMarks>) => void
  onToggleHidden: (comparisonId: string) => void
}

const LABEL_TEXT: Record<string, string> = {
  stars: 'stars', p: 'adjusted p', effect: 'effect size', difference: 'mean difference',
}

export default function TestResultsSection({ marks, capability, state, onChange, onToggleHidden }: Props) {
  const bracketsFit = capability?.available !== false
  const result: MarksResult | null = state.result
  const hidden = new Set(marks?.hidden ?? [])
  return (
    <>
      <label style={styles.check} title={bracketsFit ? undefined : capability?.reason ?? undefined}>
        <input type="checkbox" checked={!!marks?.brackets} disabled={!bracketsFit}
               onChange={e => onChange({ brackets: e.target.checked })} /> Brackets
      </label>
      {marks?.brackets && (
        <>
          <div style={styles.row}>
            <span style={styles.label}>Pairs</span>
            <select value={marks.pairs ?? 'significant'} style={styles.input}
                    onChange={e => onChange({ pairs: e.target.value as StatMarks['pairs'] })}>
              {(capability?.stat_pairs ?? []).map(p => <option key={p} value={p}>{p}</option>)}
            </select>
          </div>
          <div style={styles.row}>
            <span style={styles.label}>Label</span>
            <select value={marks.label ?? 'stars'} style={styles.input}
                    onChange={e => onChange({ label: e.target.value as StatMarks['label'] })}>
              {(capability?.stat_labels ?? []).map(l => <option key={l} value={l}>{LABEL_TEXT[l] ?? l}</option>)}
            </select>
          </div>
        </>
      )}
      <label style={styles.check}>
        <input type="checkbox" checked={!!marks?.omnibus} onChange={e => onChange({ omnibus: e.target.checked })} />
        {' '}Omnibus result in each panel
      </label>
      <label style={styles.check}>
        <input type="checkbox" checked={!!marks?.caption} onChange={e => onChange({ caption: e.target.checked })} />
        {' '}Caption under the figure
      </label>
      {state.loading && <div style={styles.note}>Running the test…</div>}
      {state.error && <div style={styles.warn}>{state.error}</div>}
      {result?.test && <div style={styles.note}>{result.test} (recommended from the data).</div>}
      {result && result.reason && <div style={styles.warn}>{result.reason}</div>}
      {result?.notes.map(note => <div key={note} style={styles.warn}>{note}</div>)}
      {marks?.brackets && result && result.ids.length > 0 && (
        <div style={styles.list}>
          {result.ids.map((id, index) => (
            <label key={id} style={styles.item}>
              <input type="checkbox" checked={!hidden.has(id)} onChange={() => onToggleHidden(id)} />
              {' '}<span style={styles.tag}>test</span> {barText(result.bars![index] as Parameters<typeof barText>[0])}
            </label>
          ))}
        </div>
      )}
      {marks?.brackets && result && (marks.hidden ?? []).length > 0 && (
        <div style={styles.note}>{(marks.hidden ?? []).length} bracket(s) hidden; untick a bracket above to hide it, tick to show it again.</div>
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
  check: { display: 'block', fontSize: 11, color: 'var(--ps-text-body)', marginBottom: 4 },
  note: { fontSize: 10, color: 'var(--ps-text-faint)', marginBottom: 6, lineHeight: 1.4 },
  warn: { fontSize: 10, color: 'var(--ps-accent-text)', marginBottom: 4, lineHeight: 1.4 },
  list: { margin: '4px 0 6px' },
  item: { display: 'block', fontSize: 11, color: 'var(--ps-text-body)', marginBottom: 2 },
  tag: { fontSize: 9, border: '1px solid var(--ps-border-strong)', borderRadius: 3, padding: '0 3px', color: 'var(--ps-text-secondary)' },
}
