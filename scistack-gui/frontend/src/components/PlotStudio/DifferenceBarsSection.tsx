/**
 * Statistics > Difference bars: the bars joining two marks of a panel, and
 * "+ Add difference bar", which picks the two ends by clicking marks in the
 * preview. Plan: .claude/plan-difference-bars.md.
 *
 * Everything shown comes from Python (`layout.meta.difference_bars`,
 * `scistackplot.diffbars.difference_meta`): the panels, each one's ticks, the
 * bars drawn, the ones that cannot be drawn and why, and the ones that do not
 * fit under a typed Max. Edits go to `spec.difference_bars` through
 * `differenceBars.ts`; the picking itself (clicks on the preview) lives in
 * PlotStudio, which owns the plot.
 */

import {
  barText,
  barsForPanel,
  dictId,
  pickingPrompt,
  removeBar,
  samePair,
  setLabel,
  type DifferenceBar,
  type DifferenceMeta,
  type Picking,
} from './differenceBars'

interface Props {
  meta: DifferenceMeta | null
  bars: DifferenceBar[] | undefined
  /** `capabilities.difference_bars`: whether this plot type can carry bars. */
  capability: { available: boolean; reason: string | null } | undefined
  picking: Picking
  /** The panel the list shows (an index into `meta.panels`). */
  panelIndex: number
  onPanel: (index: number) => void
  onStartPicking: () => void
  onCancelPicking: () => void
  onBars: (next: DifferenceBar[]) => void
}

export default function DifferenceBarsSection({
  meta,
  bars,
  capability,
  picking,
  panelIndex,
  onPanel,
  onStartPicking,
  onCancelPicking,
  onBars,
}: Props) {
  if (capability && !capability.available) {
    return <div style={styles.note}>{capability.reason}</div>
  }
  if (!meta) return <div style={styles.note}>Draw the figure to add difference bars.</div>

  const panel = meta.panels.find(p => p.index === panelIndex) ?? meta.panels[0]
  if (!panel) return null
  const listed = barsForPanel(bars, panel.match)
  const unfit = (bar: DifferenceBar) => panel.unfit.some(entry => samePair(entry, bar))
  const unresolved = (bar: DifferenceBar) =>
    panel.unresolved.find(entry => samePair(entry.bar, bar))?.reason
  const canPick = panel.targets.length >= 2 || meta.panels.some(p => p.targets.length >= 2)

  return (
    <div>
      {meta.panels.length > 1 && (
        <label style={styles.row}>
          <span style={styles.label}>Panel</span>
          <select
            value={panel.index}
            onChange={e => onPanel(Number(e.target.value))}
            style={styles.input}
          >
            {meta.panels.map(p => (
              <option key={p.index} value={p.index}>
                {p.display_title || `panel ${p.index + 1}`}
                {barsForPanel(bars, p.match).length ? ` (${barsForPanel(bars, p.match).length})` : ''}
              </option>
            ))}
          </select>
        </label>
      )}

      {listed.length === 0 && <div style={styles.note}>No bars on this panel.</div>}
      {listed.map(bar => {
        const reason = unresolved(bar)
        return (
          <div key={dictId(bar.a) + dictId(bar.b)} style={styles.bar}>
            <div style={styles.row}>
              <span style={styles.pair} title={barText(bar, panel)}>
                {barText(bar, panel)}
              </span>
              <input
                type="text"
                value={bar.label ?? '*'}
                onChange={e => onBars(setLabel(bars, bar, e.target.value))}
                style={styles.labelInput}
                title="The text over the bar"
              />
              <button
                type="button"
                style={styles.remove}
                onClick={() => onBars(removeBar(bars, bar))}
                title="Remove this bar (the data is untouched)"
              >
                ✕
              </button>
            </div>
            {unfit(bar) && (
              <div style={styles.warn}>Does not fit under the Max you set, so it is not drawn.</div>
            )}
            {reason && <div style={styles.warn}>Not drawn: {reason}.</div>}
          </div>
        )
      })}

      <div style={styles.row}>
        {picking.phase === 'idle' ? (
          <button
            type="button"
            style={styles.button}
            disabled={!canPick}
            onClick={onStartPicking}
            title="Then click two marks in the preview (a bar, a box or any of its sample points)"
          >
            + Add difference bar
          </button>
        ) : (
          <button type="button" style={styles.button} onClick={onCancelPicking}>
            Cancel
          </button>
        )}
      </div>
      {picking.phase !== 'idle' && <div style={styles.prompt}>{pickingPrompt(picking)}</div>}
      {!canPick && <div style={styles.note}>This figure has fewer than two ticks to join.</div>}

      {meta.not_in_figure.length > 0 && (
        <details style={styles.elsewhere}>
          <summary style={styles.label}>In other figures ({meta.not_in_figure.length})</summary>
          {meta.not_in_figure.map(bar => (
            <div key={dictId(bar.match) + dictId(bar.a) + dictId(bar.b)} style={styles.row}>
              <span style={styles.pair}>
                {Object.values(bar.match).join(' · ')}: {barText(bar)}
              </span>
              <button
                type="button"
                style={styles.remove}
                onClick={() => onBars(removeBar(bars, bar))}
                title="Remove this bar (the data is untouched)"
              >
                ✕
              </button>
            </div>
          ))}
        </details>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  row: { display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 },
  label: { fontSize: 11, color: 'var(--ps-text-secondary)', flex: '0 0 52px' },
  input: {
    flex: 1, minWidth: 0, background: 'var(--ps-control)', color: 'var(--ps-text-body)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, fontSize: 11, padding: '3px 5px',
  },
  bar: { marginBottom: 2 },
  pair: {
    flex: 1, minWidth: 0, fontSize: 11, color: 'var(--ps-text-body)',
    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  labelInput: {
    flex: '0 0 56px', minWidth: 0, background: 'var(--ps-control)', color: 'var(--ps-text-body)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, fontSize: 11, padding: '2px 4px',
  },
  remove: {
    padding: '0 6px', background: 'transparent', color: 'var(--ps-text-secondary)',
    border: 'none', cursor: 'pointer', fontSize: 11,
  },
  button: {
    padding: '2px 8px', background: 'var(--ps-control)', color: 'var(--ps-text-secondary-alt)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, cursor: 'pointer', fontSize: 10,
  },
  prompt: { fontSize: 10, color: 'var(--ps-accent-text)', marginBottom: 6, lineHeight: 1.4 },
  note: { fontSize: 10, color: 'var(--ps-text-faint)', marginBottom: 6, lineHeight: 1.4 },
  warn: { fontSize: 10, color: 'var(--ps-text-faint)', marginLeft: 4, marginBottom: 4 },
  elsewhere: { marginTop: 6 },
}
