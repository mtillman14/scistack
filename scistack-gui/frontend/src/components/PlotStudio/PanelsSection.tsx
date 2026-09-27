/**
 * Appearance > Panels: one faceted panel's own y limits and y title, and the
 * grid-wide "Y titles: every panel / first column only" toggle.
 * docs/claude/per-panel-overrides.md.
 *
 * Everything shown comes from Python (`layout.meta.panel_overrides`): the
 * panels, each one's `match` (already text), the title and range DRAWN, and
 * overrides that match no panel here. Edits go to `spec.panel_overrides`
 * through `panelOverrides.ts`; nothing here decides which override applies.
 */

import { useState, type ReactNode } from 'react'
import {
  clearOverride,
  hiddenFor,
  matchId,
  matchLabel,
  overrideFor,
  showState,
  upsertOverride,
  type PanelOverride,
  type PanelOverrideMeta,
  type ShowState,
  type YTitles,
} from './panelOverrides'

interface Props {
  meta: PanelOverrideMeta
  overrides: PanelOverride[] | undefined
  yTitles: YTitles
  onOverrides: (next: PanelOverride[]) => void
  onYTitles: (value: YTitles) => void
  /** The panel's own Min/Max box (PlotStudio's LimitInput), so a typed limit
   *  behaves exactly like the figure's. */
  renderLimit: (
    label: string,
    value: number | null,
    placeholder: string,
    onChange: (value: number | null) => void,
  ) => ReactNode
}

export default function PanelsSection({
  meta,
  overrides,
  yTitles,
  onOverrides,
  onYTitles,
  renderLimit,
}: Props) {
  const [selected, setSelected] = useState<string | null>(null)

  const choices = [
    ...meta.panels.map(panel => ({ id: matchId(panel.match), match: panel.match, panel })),
    ...meta.unmatched.map(entry => ({ id: matchId(entry.match), match: entry.match, panel: null })),
  ]
  const choice = choices.find(c => c.id === selected) ?? choices[0]
  if (!choice) return null

  const override = overrideFor(overrides, choice.match)
  const drawn = choice.panel
  const patch = (fields: Partial<Omit<PanelOverride, 'match'>>) =>
    onOverrides(upsertOverride(overrides, choice.match, fields))
  const limitPlaceholder = (end: 0 | 1) =>
    drawn?.y_limits ? drawn.y_limits[end].toPrecision(3) : 'auto'
  const anyLimit = (overrides ?? []).some(
    entry => entry.y_minimum != null || entry.y_maximum != null,
  )

  return (
    <div>
      <label style={styles.row} title="Which panels draw their y title. On a faceted figure the y title is the panel's name.">
        <span style={styles.label}>Y titles</span>
        <select
          value={yTitles}
          onChange={e => onYTitles(e.target.value as YTitles)}
          style={styles.input}
        >
          <option value="every_panel">every panel</option>
          <option value="first_column">first column only</option>
        </select>
      </label>
      {yTitles === 'first_column' && (
        <div style={styles.note}>
          The other columns' panels are no longer named. Keep them readable with a
          title on the first column that names the row (e.g. "Hamstrings (L | R)").
        </div>
      )}

      <label style={{ ...styles.row, marginTop: 6 }}>
        <span style={styles.label}>Panel</span>
        <select
          value={choice.id}
          onChange={e => setSelected(e.target.value)}
          style={styles.input}
        >
          {meta.panels.map(panel => (
            <option key={matchId(panel.match)} value={matchId(panel.match)}>
              {panel.display_title}
              {panel.override ? ' •' : ''}
            </option>
          ))}
          {meta.unmatched.length > 0 && (
            <optgroup label={`Not in this figure (${meta.unmatched.length})`}>
              {meta.unmatched.map(entry => (
                <option key={matchId(entry.match)} value={matchId(entry.match)}>
                  {matchLabel(entry.match)}
                </option>
              ))}
            </optgroup>
          )}
        </select>
      </label>
      {!drawn && (
        <div style={styles.note}>
          No panel of this figure has these values. The setting is kept and applies
          wherever the panel is drawn; Clear removes it.
        </div>
      )}

      {/* Keyed by panel so a half-typed box never carries over to another panel. */}
      <div key={choice.id}>
        <div style={styles.limits}>
          {renderLimit('Min', override?.y_minimum ?? null, limitPlaceholder(0), v =>
            patch({ y_minimum: v }),
          )}
          {renderLimit('Max', override?.y_maximum ?? null, limitPlaceholder(1), v =>
            patch({ y_maximum: v }),
          )}
        </div>
        <label style={styles.row}>
          <span style={styles.label}>Y title</span>
          <input
            type="text"
            value={override?.y_label ?? ''}
            placeholder={drawn?.display_title ?? matchLabel(choice.match)}
            onChange={e => patch({ y_label: e.target.value === '' ? null : e.target.value })}
            style={styles.input}
          />
        </label>
        <label style={styles.row} title="Follow the Y titles setting above, or show/hide this panel's title regardless. Typed text is kept while hidden.">
          <span style={styles.label}>Show</span>
          <select
            value={showState(override)}
            onChange={e => patch({ y_label_hidden: hiddenFor(e.target.value as ShowState) })}
            style={styles.input}
          >
            <option value="follow">follow Y titles</option>
            <option value="show">show</option>
            <option value="hide">hide</option>
          </select>
        </label>
        <div style={styles.row}>
          <button
            type="button"
            style={styles.button}
            disabled={!override}
            onClick={() => onOverrides(clearOverride(overrides, choice.match))}
          >
            Clear this panel
          </button>
        </div>
      </div>

      {anyLimit && !meta.shares_y && (
        <div style={styles.note}>
          Panels now have different scales, so every panel shows its tick numbers.
        </div>
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
  limits: { display: 'flex', gap: 8, marginBottom: 4, flexWrap: 'wrap' },
  note: { fontSize: 10, color: 'var(--ps-text-faint)', marginBottom: 6, lineHeight: 1.4 },
  button: {
    padding: '2px 8px', background: 'var(--ps-control)', color: 'var(--ps-text-secondary-alt)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, cursor: 'pointer', fontSize: 10,
  },
}
