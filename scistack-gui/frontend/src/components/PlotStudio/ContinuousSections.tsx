/**
 * Data > X variable, Chart > Histogram and Chart > Regression.
 *
 * The settings are `PlotSpec.x_measure`, `PlotSpec.histogram` and
 * `PlotSpec.regression`; whether they are valid and what was drawn come from
 * Python (`capabilities.histogram` / `.regression`, `layout.meta.bins` /
 * `.fits`). Pure logic: continuousKinds.ts.
 */

import { useEffect, useState } from 'react'

import {
  binText,
  binsSummary,
  fitsSummary,
  parseBinCount,
  parseBinValue,
  type BinsMeta,
  type HistogramCapability,
  type HistogramOptions,
  type PanelFitsMeta,
  type RegressionCapability,
  type RegressionOptions,
} from './continuousKinds'

/** A text box that commits on Enter or blur, not per keystroke (each commit
 *  re-resolves the figure). */
function CommitInput({
  value,
  placeholder,
  onCommit,
  title,
}: {
  value: string
  placeholder?: string
  onCommit: (text: string) => void
  title?: string
}) {
  const [text, setText] = useState(value)
  useEffect(() => setText(value), [value])
  const commit = () => {
    if (text !== value) onCommit(text)
  }
  return (
    <input
      type="text"
      value={text}
      placeholder={placeholder}
      title={title}
      onChange={e => setText(e.target.value)}
      onKeyDown={e => {
        if (e.key === 'Enter') commit()
        if (e.key === 'Escape') setText(value)
      }}
      onBlur={commit}
      style={styles.input}
    />
  )
}

export function XMeasureSection({
  current,
  options,
  temporal,
  onChange,
}: {
  current: string | null | undefined
  options: string[]
  temporal: boolean
  onChange: (name: string | null) => void
}) {
  const choices = current && !options.includes(current) ? [current, ...options] : options
  return (
    <>
      <div style={styles.row}>
        <span style={styles.label}>X variable</span>
        <select
          value={current ?? ''}
          onChange={e => onChange(e.target.value || null)}
          style={styles.input}
        >
          <option value="">None — x from the grouping</option>
          {choices.map(name => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>
      </div>
      <div style={styles.note}>
        {current
          ? `Each point sits at its ${current} value${temporal ? ' (a date axis)' : ''}. Kinds: scatter, line, regression.`
          : options.length > 0
            ? 'Pick a variable to plot this one against it (scatter, line or regression).'
            : 'No other variable can be lined up with this one record for record.'}
      </div>
    </>
  )
}

export function HistogramSection({
  options,
  capability,
  meta,
  onChange,
}: {
  options: HistogramOptions | undefined
  capability: HistogramCapability | undefined
  meta: BinsMeta | null
  onChange: (patch: HistogramOptions) => void
}) {
  const settings = { ...(capability?.settings ?? {}), ...(options ?? {}) }
  const temporal = Boolean(capability?.temporal)
  const mode = settings.bins ?? 'auto'
  return (
    <>
      <div style={styles.row}>
        <span style={styles.label}>Bins</span>
        <select
          value={mode}
          onChange={e => onChange({ bins: e.target.value as HistogramOptions['bins'] })}
          style={styles.input}
        >
          <option value="auto">Automatic</option>
          <option value="count">Number of bins</option>
          <option value="width">Bin width</option>
        </select>
      </div>
      {mode === 'count' && (
        <div style={styles.row}>
          <span style={styles.label}>Count</span>
          <CommitInput
            value={binText(settings.bin_count)}
            placeholder="10"
            title={`1 to ${capability?.max_bins ?? 1000}`}
            onCommit={text => onChange({ bin_count: parseBinCount(text) })}
          />
        </div>
      )}
      {mode === 'width' && (
        <div style={styles.row}>
          <span style={styles.label}>Width</span>
          <CommitInput
            value={binText(settings.bin_width)}
            placeholder={temporal ? '1h, 1D, 7D, 30min' : 'e.g. 0.5'}
            title={
              temporal
                ? 'A fixed duration: s, min, h, D or W. The first bin starts at a multiple of it (1D: midnight).'
                : 'The first bin starts at a multiple of the width.'
            }
            onCommit={text => onChange({ bin_width: parseBinValue(text, temporal) })}
          />
        </div>
      )}
      <div style={styles.row}>
        <span style={styles.label}>From</span>
        <CommitInput
          value={binText(settings.range_min)}
          placeholder={temporal ? 'data start (2026-07-01 12:00)' : 'data minimum'}
          onCommit={text => onChange({ range_min: parseBinValue(text, temporal) })}
        />
      </div>
      <div style={styles.row}>
        <span style={styles.label}>To</span>
        <CommitInput
          value={binText(settings.range_max)}
          placeholder={temporal ? 'data end' : 'data maximum'}
          onCommit={text => onChange({ range_max: parseBinValue(text, temporal) })}
        />
      </div>
      <div style={styles.row}>
        <span style={styles.label}>Height</span>
        <select
          value={settings.stat ?? 'count'}
          onChange={e => onChange({ stat: e.target.value as HistogramOptions['stat'] })}
          style={styles.input}
        >
          <option value="count">Count</option>
          <option value="percent">Percent (per colour)</option>
          <option value="density">Density (per colour)</option>
        </select>
      </div>
      <div style={styles.row}>
        <span style={styles.label}>Colours</span>
        <select
          value={settings.multiple ?? 'layer'}
          onChange={e => onChange({ multiple: e.target.value as HistogramOptions['multiple'] })}
          style={styles.input}
        >
          <option value="layer">Overlaid</option>
          <option value="stack">Stacked</option>
          <option value="dodge">Side by side</option>
        </select>
      </div>
      {meta && <div style={styles.note}>{binsSummary(meta, temporal)}</div>}
      {capability?.problem && <div style={styles.warn}>{capability.problem}</div>}
      {(meta?.notes ?? []).map(line => (
        <div key={line} style={styles.warn}>
          {line}
        </div>
      ))}
    </>
  )
}

export function RegressionSection({
  options,
  capability,
  fits,
  onChange,
}: {
  options: RegressionOptions | undefined
  capability: RegressionCapability | undefined
  fits: PanelFitsMeta[] | null
  onChange: (patch: RegressionOptions) => void
}) {
  const settings = { ...(capability?.settings ?? {}), ...(options ?? {}) }
  const check = (key: 'fit_line' | 'show_stats' | 'ci_band', label: string, fallback: boolean) => (
    <label style={styles.row}>
      <input
        type="checkbox"
        checked={settings[key] ?? fallback}
        onChange={e => onChange({ [key]: e.target.checked })}
      />
      <span style={{ fontSize: 11 }}>{label}</span>
    </label>
  )
  return (
    <>
      {check('fit_line', 'Line of best fit', true)}
      {check('show_stats', 'Show R² and p on the plot', true)}
      {check('ci_band', '95% confidence band', false)}
      <div style={styles.row}>
        <span style={styles.label}>Fit</span>
        <select
          value={settings.fit_by ?? 'color'}
          onChange={e => onChange({ fit_by: e.target.value as RegressionOptions['fit_by'] })}
          style={styles.input}
        >
          <option value="color">One line per colour</option>
          <option value="pooled">One line per panel</option>
        </select>
      </div>
      <div style={styles.row}>
        <span style={styles.label}>Text at</span>
        <select
          value={settings.stats_corner ?? 'upper_left'}
          onChange={e =>
            onChange({ stats_corner: e.target.value as RegressionOptions['stats_corner'] })
          }
          style={styles.input}
        >
          <option value="upper_left">Top left</option>
          <option value="upper_right">Top right</option>
          <option value="lower_left">Bottom left</option>
          <option value="lower_right">Bottom right</option>
        </select>
      </div>
      {capability?.problem && <div style={styles.warn}>{capability.problem}</div>}
      {fitsSummary(fits).map(line => (
        <div key={line} style={styles.note}>
          {line}
        </div>
      ))}
      {capability?.temporal_x && (
        <div style={styles.note}>
          The x axis is a date: R² and p do not depend on its units; the slope is not shown.
        </div>
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
