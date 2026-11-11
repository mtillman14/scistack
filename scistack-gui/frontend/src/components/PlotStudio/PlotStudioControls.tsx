// PlotStudio: the leaf input components and rail widgets.
// Moved verbatim out of PlotStudio.tsx (grug stage plotstudio_split,
// docs/claude/grugbrain-principles.md); PlotStudio.tsx is the consumer.
import { usePlotTheme, setPlotTheme } from './usePlotTheme'
import ResizeHandle, { useResizablePanel } from '../ResizeHandle'
import { plotThemeVars, otherPlotTheme } from './plotTheme'
import { useState, useEffect } from 'react'
import { parseLimitText } from './timeZone'
import { roundTo, stepValue } from './stepper'
import { type SizeUnit, unitInfo, fromInches, toInches } from './figureUnits'
import { type Matcher, type MatchOp } from './plotSpecReducer'
import { type LevelGroup, readBuckets, unusedLevels, writeBuckets, assignLevels, levelRange, combineSummary, renameBucket } from './combine'
import { SAVE_DPI, FactorInfo, Filter, SpanInfo, describeSpan, VariantSummary, Capabilities, MATCH_OPS, Spec } from './plotStudioTypes'
import { styles } from './plotStudioStyles'

export interface ShellProps {
  variable: string
  /** The measure's shape (`scalar`, `series_1d`, …), badged beside the title. */
  shape?: string
  /**
   * Which database this figure is drawn from, badged after the shape.
   * Undefined for a CSV tab, whose filename is already the title.
   */
  source?: string
  onClose: () => void
  embedded?: boolean
  /** The controls rail. It sits under the header, in the same narrow column. */
  sidebar?: React.ReactNode
  /** The figure area — it owns the full height of the panel. */
  children: React.ReactNode
  panelRef?: React.RefObject<HTMLDivElement>
  controlsHidden?: boolean
  onToggleControls?: () => void
  /** The "Saved plots" rail, right of the figure; collapses itself. */
  rightRail?: React.ReactNode
}

export function Shell({
  variable,
  shape,
  source,
  onClose,
  embedded,
  sidebar,
  children,
  panelRef,
  controlsHidden,
  onToggleControls,
  rightRail,
}: ShellProps) {
  const theme = usePlotTheme()
  // The controls rail drags from its right edge. The reserve keeps room for
  // the figure and the Saved-plots rail beside it.
  const railResize = useResizablePanel({
    panel: 'plotStudio.controls',
    edge: 'right',
    min: 240,
    reserve: 520,
  })
  return (
    // No overlay click-to-close: a stray click on the backdrop while dragging a
    // plotly selection would throw the panel away mid-exploration.
    <div
      style={{
        ...(embedded ? styles.embeddedRoot : styles.overlay),
        // The theme's owner: every `var(--ps-…)` below resolves here.
        ...plotThemeVars(theme),
        colorScheme: theme,
      }}
    >
      {/* A row, not a column: the title bar caps the controls rail rather than
          spanning the panel, so the figure keeps the tab's full height. */}
      <div ref={panelRef} style={embedded ? styles.embeddedPanel : styles.panel}>
        <div
          ref={railResize.panelRef}
          style={{
            ...styles.rail,
            // Collapsed is as wide as its toggle, whatever it was dragged to.
            ...(controlsHidden ? styles.railCollapsed : railResize.widthStyle),
          }}
        >
          <div style={styles.header}>
            {/* The toggle sits over the rail it collapses, so the control is
                where the thing it controls is. */}
            <div style={styles.headerLeft}>
              {onToggleControls && (
                <button
                  type="button"
                  style={styles.headerButton}
                  onClick={onToggleControls}
                  title={controlsHidden ? 'Show the controls' : "Give the figure the controls' width"}
                >
                  {controlsHidden ? '❯' : '❮'}
                </button>
              )}
              {/* A collapsed rail is only as wide as its buttons, so the title
                  would be the one thing keeping it wide. */}
              {!controlsHidden && (
                <span
                  style={styles.title}
                  title={source ? `Plot — ${variable} · ${source}` : `Plot — ${variable}`}
                >
                  Plot — {variable}
                  {shape && <span style={styles.shapeTag}>{shape}</span>}
                  {/* Which database this figure is drawn from. With plot tabs
                      open across two databases, the variable name alone does
                      not say which one you are looking at — and a figure you
                      cannot attribute is a figure you cannot use. */}
                  {source && <span style={styles.sourceTag}>{source}</span>}
                </span>
              )}
            </div>
            <div style={styles.headerActions}>
              <button
                type="button"
                style={styles.headerButton}
                onClick={() => setPlotTheme(otherPlotTheme(theme))}
                title={
                  theme === 'dark'
                    ? 'Light mode: preview the figure exactly as it will be saved (every plot tab)'
                    : 'Dark mode: figure recoloured for the screen, not as saved (every plot tab)'
                }
              >
                {theme === 'dark' ? '☀' : '☾'}
              </button>
              {!embedded && (
                <button type="button" style={styles.close} onClick={onClose}>✕</button>
              )}
            </div>
          </div>
          {sidebar}
        </div>
        {!controlsHidden && <ResizeHandle {...railResize.handleProps} />}
        {children}
        {rightRail}
      </div>
    </div>
  )
}

export interface GridSizeInputProps {
  label: string
  /** What the user pinned, or null when this dimension is computed. */
  value: number | null
  /** What the backend laid out — shown as a placeholder while unpinned. */
  effective: number
  /** One panel: the grid is 1 x 1 whatever is pinned, so show 1, read-only. */
  locked?: boolean
  onChange: (value: number | null) => void
}

/**
 * One dimension of the facet grid.
 *
 * An empty box is not "1", it is "you decide": the backend computes it from the
 * other dimension and the subplot count, and the result shows through as the
 * placeholder. That is what makes "I set 2 columns" answer "so, 3 rows".
 */
export function GridSizeInput({ label, value, effective, locked = false, onChange }: GridSizeInputProps) {
  if (locked) {
    return (
      <label style={styles.gridSizeField}>
        <span style={styles.gridSizeLabel}>{label}</span>
        <input
          type="number"
          value={1}
          disabled
          readOnly
          title="One panel, so the grid is 1 x 1. A number pinned here applies again once there is more than one panel."
          style={{ ...styles.select, width: 56, ...styles.gridSizeAuto }}
        />
      </label>
    )
  }
  return (
    <label style={styles.gridSizeField}>
      <span style={styles.gridSizeLabel}>{label}</span>
      <input
        type="number"
        min={1}
        value={value ?? ''}
        placeholder={String(effective)}
        onChange={e => onChange(e.target.value ? Number(e.target.value) : null)}
        title={value === null ? `Computed: ${effective}. Type a number to pin it.` : 'Clear to compute automatically'}
        style={{
          ...styles.select,
          width: 56,
          ...(value === null ? styles.gridSizeAuto : null),
        }}
      />
    </label>
  )
}

export interface LimitInputProps {
  label: string
  /** A number, or the ISO date text a datetime axis was given. */
  value: number | string | null
  onChange: (value: number | null) => void
  /** A datetime axis (`layout.meta.temporal`): the box takes date text
   *  (`timeZone.parseLimitText`), committed through `onDate`. */
  temporal?: boolean
  onDate?: (value: string) => void
  /** What an empty box shows; the value in effect when known (default "auto"). */
  placeholder?: string
}

/**
 * One end of a manual y range. Blank means "compute it".
 *
 * Deliberately NOT `Number(e.target.value)` unguarded: an empty box gives
 * `Number('') === 0`, which would turn clearing the field into pinning the axis
 * at zero — the opposite of what clearing means. A half-typed "-" or "1e" is
 * held as-is too, rather than snapping the axis around while it is being typed.
 */
export function LimitInput({
  label,
  value,
  onChange,
  temporal = false,
  onDate,
  placeholder = 'auto',
}: LimitInputProps) {
  const [text, setText] = useState<string | null>(null)
  const shown = text ?? (value === null || value === undefined ? '' : String(value))
  return (
    <label style={styles.gridSizeField}>
      <span style={styles.gridSizeLabel}>{label}</span>
      <input
        type="text"
        inputMode={temporal ? 'text' : 'decimal'}
        value={shown}
        placeholder={placeholder}
        onChange={e => {
          const next = e.target.value
          setText(next)
          const parsed = parseLimitText(next, temporal)
          if (parsed === undefined) return // still typing
          if (typeof parsed === 'string') onDate?.(parsed)
          else onChange(parsed)
        }}
        onBlur={() => setText(null)}
        title={
          temporal
            ? 'A date and time (2026-07-01 12:00) on the time axis; blank computes it'
            : 'Blank computes the limit from the data'
        }
        style={{
          ...styles.select,
          width: temporal ? 128 : 72,
          ...(value === null || value === undefined ? styles.gridSizeAuto : null),
        }}
      />
    </label>
  )
}

export interface StepperInputProps {
  /** The committed value, or null for an empty box. */
  value: number | null
  /** What an empty box steps from: the value it is drawn at. */
  fallback: number | null
  /** One ▲▼ click (stepper.stepValue: to the next multiple of it). */
  step: number
  min: number
  onChange: (value: number | null) => void
  /** Blank commits null ("auto"); otherwise blank falls back to `value`. */
  allowBlank: boolean
  /** Decimals the box shows a committed value to. */
  decimals?: number
  placeholder?: string
  title?: string
  width?: number
  /** Show the value as derived rather than typed. */
  auto?: boolean
}

/**
 * A numeric box with ▲▼ arrows beside it, for the values that are tuned a
 * step at a time (font sizes, mark weights, the figure size). ArrowUp and
 * ArrowDown in the box step too.
 *
 * Not `type="number"`: the native spinner steps an empty box from 0, and an
 * empty text size means "auto · 11.7", so its first click has to go to 12.
 *
 * Typing discipline as before (`LimitInput`): the text is held while it is
 * typed, and only a positive finite number commits — `Number('')` is 0, and a
 * 0 pt font or a 0-inch figure is a matplotlib error, not a size.
 */
export function StepperInput({
  value,
  fallback,
  step,
  min,
  onChange,
  allowBlank,
  decimals,
  placeholder,
  title,
  width = 48,
  auto = false,
}: StepperInputProps) {
  const [text, setText] = useState<string | null>(null)
  const shown =
    text ?? (value === null ? '' : String(decimals === undefined ? value : roundTo(value, decimals)))
  const stepBy = (direction: 1 | -1) => {
    setText(null)
    const next = stepValue(value, fallback, direction, step, min)
    if (next !== null) onChange(next)
  }
  return (
    <span style={styles.stepper} title={title}>
      <input
        type="text"
        inputMode="decimal"
        value={shown}
        placeholder={placeholder}
        onChange={e => {
          const next = e.target.value
          setText(next)
          const parsed = Number(next)
          if (next.trim() === '') {
            if (allowBlank) onChange(null)
          } else if (Number.isFinite(parsed) && parsed > 0) {
            onChange(parsed)
          }
        }}
        onKeyDown={e => {
          if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
            e.preventDefault()
            stepBy(e.key === 'ArrowUp' ? 1 : -1)
          }
        }}
        onBlur={() => setText(null)}
        style={{ ...styles.stepperInput, width, ...(auto ? styles.gridSizeAuto : null) }}
      />
      <span style={styles.stepperArrows}>
        {/* Out of the tab order: the box itself takes ArrowUp/ArrowDown. */}
        <button
          type="button"
          tabIndex={-1}
          aria-label="Increase"
          style={styles.stepperArrow}
          onClick={() => stepBy(1)}
        >
          ▲
        </button>
        <button
          type="button"
          tabIndex={-1}
          aria-label="Decrease"
          style={styles.stepperArrow}
          onClick={() => stepBy(-1)}
        >
          ▼
        </button>
      </span>
    </span>
  )
}

export interface PositiveNumberInputProps {
  label: string
  value: number
  onChange: (value: number) => void
  title?: string
}

/** The base font size (points). Blank means nothing: the box falls back to
 *  the value it had. ▲▼ step 1 pt. */
export function PositiveNumberInput({ label, value, onChange, title }: PositiveNumberInputProps) {
  return (
    <label style={styles.gridSizeField}>
      <span style={styles.gridSizeLabel}>{label}</span>
      <StepperInput
        value={value}
        fallback={value}
        step={1}
        min={1}
        allowBlank={false}
        onChange={next => {
          if (next !== null) onChange(next)
        }}
        title={title}
      />
    </label>
  )
}

export interface FigureSizeInputProps {
  label: string
  /** The size in inches (what the spec stores). */
  inches: number
  unit: SizeUnit
  onChange: (inches: number) => void
  title?: string
}

/**
 * One dimension of the written figure, shown in `unit` (figureUnits.ts) and
 * stored in inches. ▲▼ step 0.1 in / 1 mm / 10 px.
 */
export function FigureSizeInput({ label, inches, unit, onChange, title }: FigureSizeInputProps) {
  const info = unitInfo(unit)
  const shown = fromInches(inches, unit, SAVE_DPI)
  return (
    <label style={styles.gridSizeField}>
      <span style={styles.gridSizeLabel}>{label}</span>
      <StepperInput
        // Re-keyed per unit so a half-typed value in one unit is not carried
        // into the other.
        key={unit}
        value={shown}
        fallback={shown}
        step={info.step}
        min={info.step}
        decimals={info.decimals}
        allowBlank={false}
        onChange={next => {
          if (next !== null) onChange(toInches(next, unit, SAVE_DPI))
        }}
        title={title}
        width={unit === 'px' ? 44 : 40}
      />
    </label>
  )
}

export interface SizeInputProps {
  label: string
  value: number | null
  placeholder: string
  /** The size it is drawn at while empty (Python's), for the first ▲▼. */
  resolved: number | null
  onChange: (value: number | null) => void
  title?: string
}

/**
 * One text element's size in points. Blank = derived from Font (the
 * placeholder says what that comes to); a positive number fixes it.
 * ▲▼ step 1 pt, from the resolved size when the box is empty.
 */
export function SizeInput({ label, value, placeholder, resolved, onChange, title }: SizeInputProps) {
  return (
    <label style={styles.gridSizeField} title={title}>
      <span style={styles.textSizeLabel}>{label}</span>
      <StepperInput
        value={value ?? null}
        fallback={resolved}
        step={1}
        min={1}
        allowBlank
        placeholder={placeholder}
        onChange={onChange}
      />
    </label>
  )
}

export interface WeightInputProps {
  label: string
  /** The stored multiplier, or null for the default (1). */
  value: number | null
  title: string
  onChange: (value: number | null) => void
}

/**
 * A mark-weight multiplier box on a factor row. Blank = 1×; ▲▼ step 0.1
 * (stepping onto exactly 1 clears it again, markWeights.withWeight).
 */
export function WeightInput({ label, value, title, onChange }: WeightInputProps) {
  return (
    <label style={styles.factorRow} title={title}>
      <span style={styles.factorName}>{label}</span>
      <StepperInput
        value={value}
        fallback={1}
        step={0.1}
        min={0.1}
        allowBlank
        placeholder={'1×'}
        onChange={onChange}
      />
    </label>
  )
}

export interface RuleSlotsProps {
  title: string
  /** Number of slots = the grid dimension. Comes from the rendered layout. */
  count: number
  rules: Matcher[]
  onEdit: (index: number, patch: Partial<Matcher>) => void
}

/**
 * One axis of the facet grid, one editor per slot.
 *
 * Fixed length, not a list with +/- buttons: the grid already has a known
 * number of rows and columns, so a rule is a property OF row 2, not an extra
 * row. Each slot claims the panels whose name it matches; a blank slot takes
 * whatever is left over, in order, and anything matching no slot at all lands
 * in a trailing "other" row/column rather than disappearing.
 */
export function RuleSlots({ title, count, rules, onEdit }: RuleSlotsProps) {
  const singular = title.replace(/s$/, '')
  return (
    <div style={styles.ruleList}>
      <div style={styles.ruleTitle}>{title}</div>
      {Array.from({ length: count }, (_, index) => {
        const rule = rules[index] ?? { op: 'contains' as MatchOp, value: '' }
        return (
          <div key={index} style={styles.ruleRow}>
            <span style={styles.slotIndex}>{singular} {index + 1}</span>
            <select
              value={rule.op}
              onChange={e => onEdit(index, { op: e.target.value as MatchOp })}
              style={{ ...styles.select, flex: '0 0 88px' }}
            >
              {MATCH_OPS.map(op => (
                <option key={op.value} value={op.value}>{op.label}</option>
              ))}
            </select>
            <input
              value={rule.value}
              onChange={e => onEdit(index, { value: e.target.value })}
              placeholder="anything"
              style={{ ...styles.select, flex: 1, minWidth: 0 }}
            />
          </div>
        )
      })}
    </div>
  )
}

export interface VariantRow {
  /** The user's name, or null while the label follows the selection. */
  explicitName: string | null
  /** What the selection says it is, used as the placeholder. */
  autoLabel: string
  /** False until the row selects something. Such a row changes nothing. */
  defined: boolean
  /** Rows it contributes, once the backend has measured it. */
  rowCount?: number
  /** Which variable this row draws from; null means the plot's primary measure. */
  variable: string | null
  /** Code axes it leaves open and disagrees on, with which locations hold
   *  which version. See `SpanInfo`. */
  spans: Record<string, SpanInfo>
  /** A reopened saved plot's code versions (`{"Code:fn": ["v1"]}`), empty when none. */
  frozen: Record<string, string[]>
  /** The row always draws current code (answered "switch to latest"). */
  followLatest: boolean
}

export interface VariantRowsProps {
  rows: VariantRow[]
  /** The plot's primary measure — what a row with no variable draws from. */
  primary: string
  /** Other variables a row may draw from (same shape, same schema level). */
  stackable: string[]
  onRename: (index: number, name: string | null) => void
  onSetVariable: (index: number, variable: string | null) => void
  onEdit: (index: number) => void
  onRemove: (index: number) => void
  onAdd: () => void
}

/**
 * The Variants section: a name and a "select on the DAG" button, per row.
 *
 * Shaped like Factors — two columns, many rows — because it is the same kind of
 * list: a thing, and what to do with it. The name is editable and lands in the
 * figure's legend, which is the whole reason a variant is *named* rather than
 * numbered: "baseline" and "20 Hz filter" survive being read a week later,
 * "variant 2" does not.
 *
 * The selection itself is not editable here. It is a point in a space whose
 * coordinates are pipeline nodes, and the pipeline canvas is already the
 * picture of that space — see VariantDagPopup.
 */
export function VariantRows({
  rows,
  primary,
  stackable,
  onRename,
  onSetVariable,
  onEdit,
  onRemove,
  onAdd,
}: VariantRowsProps) {
  return (
    <div style={styles.variantPicker}>
      {rows.map((row, index) => (
        <div key={index} style={styles.variantSetRow}>
          <input
            value={row.explicitName ?? ''}
            placeholder={row.autoLabel}
            onChange={e => onRename(index, e.target.value || null)}
            style={styles.variantNameInput}
            title={
              row.explicitName === null
                ? `Named from its selection: ${row.autoLabel}. Type to override.`
                : 'The name this variant carries in the figure'
            }
          />
          {/* Which variable this row draws from — ALWAYS shown, as a dropdown
              when there is something to switch to and as a plain label when
              there is not.
              It used to appear only when another variable could be stacked, so
              the common single-variable case rendered a row that never said what
              it was plotting. With rows now able to name different variables,
              a row that does not state its own is unreadable: two rows called
              "current" and "v1" say nothing about which is EMG and which is
              force. The name is the subject of the row; the selection is only
              what narrows it. */}
          {stackable.length > 0 ? (
            <select
              value={row.variable ?? ''}
              onChange={e => onSetVariable(index, e.target.value || null)}
              style={styles.variantVariableSelect}
              title="The variable this variant plots"
            >
              <option value="">{primary}</option>
              {stackable.map(name => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          ) : (
            <span
              style={styles.variantVariableLabel}
              title="The variable this variant plots — nothing else here can be plotted alongside it"
            >
              {row.variable ?? primary}
            </span>
          )}
          <button
            type="button"
            style={styles.variantSelectButton}
            onClick={() => onEdit(index)}
            title="Choose this variant on the pipeline graph"
          >
            ⋔ Select
          </button>
          {/* One variant is the pin; there is nothing to remove down to zero. */}
          {rows.length > 1 && (
            <button
              type="button"
              style={styles.variantRemove}
              onClick={() => onRemove(index)}
              title="Remove this variant"
            >
              ✕
            </button>
          )}
          {/* An unfilled row is inert, not broken: it must not wear the same
              warning as a selection that genuinely matched nothing. */}
          {!row.defined && (
            <span style={styles.variantUnsetTag} title="Nothing chosen yet — this row does not affect the figure. Pick a variable, or Select a variant on the graph.">
              not set
            </span>
          )}
          {/* A reopened saved plot drawing the code version it was saved with.
              Grey: a deliberate state, not a problem. */}
          {Object.keys(row.frozen).length > 0 && (
            <span
              style={styles.variantUnsetTag}
              title={
                'This saved plot shows the code version it was saved with: ' +
                Object.entries(row.frozen)
                  .map(([column, levels]) => `${column.replace(/^Code:/, '')} ${levels.join('+')}`)
                  .join(', ') +
                '. Select a variant to change it.'
              }
            >
              saved version
            </span>
          )}
          {row.followLatest && (
            <span
              style={styles.variantUnsetTag}
              title="This saved plot always shows the latest code version"
            >
              follows latest
            </span>
          )}
          {row.defined && row.rowCount === 0 && (
            <span style={styles.variantEmptyTag} title="This selection matched no records — check it against what has actually run">
              no data
            </span>
          )}
          {Object.keys(row.spans).length > 0 && (
            <span
              style={styles.variantEmptyTag}
              title={
                `This variant's rows were built by more than one version of the ` +
                `code, or more than one set of run options (distribute/as_table): ` +
                `${Object.values(row.spans)
                  .map(describeSpan)
                  .join(' | ')}. Pin a version on it, or split it into one ` +
                `variant per version.`
              }
            >
              pools {Object.keys(Object.values(row.spans)[0].versions).length} versions
            </span>
          )}
        </div>
      ))}
      <button
        type="button"
        style={styles.variantAdd}
        onClick={onAdd}
        title="Plot another variable, or another variant of this one, alongside"
      >
        + Add series
      </button>
    </div>
  )
}

/**
 * How much of the variant space is on screen.
 *
 * The readout matters as much as the rows. A canvas or a factor list shows
 * coordinates; neither shows the PRODUCT, and an exploding panel count is the
 * thing that actually catches people out.
 */
export function VariantReadout({ summary }: { summary?: VariantSummary }) {
  if (!summary || summary.factors.length === 0) return null
  const { total_combinations, selected_combinations } = summary
  const none = selected_combinations === 0
  return (
    <div style={none ? styles.variantCountEmpty : styles.variantCount}>
      {none
        ? 'Nothing selected — the figure would be empty.'
        : `${selected_combinations} of ${total_combinations} variant combination${
            total_combinations === 1 ? '' : 's'
          }`}
    </div>
  )
}

/**
 * Step through an ITERATE fan-out one figure at a time.
 *
 * The order is the backend's (`scistackplot.roles.fanout_keys`): schema keys
 * outermost-first, each in its declared level order. So with a `[subject,
 * trial]` schema this walks subject 1's trials, then rolls over to subject 2's
 * first trial — the arrows are a plain cursor, and none of that ordering is
 * decided here.
 */
export function FigureNavigator({
  index,
  count,
  label,
  onStep,
}: {
  index: number
  count: number
  label: string
  onStep: (delta: number) => void
}) {
  return (
    <div style={styles.navigator}>
      <button
        type="button"
        style={{ ...styles.navButton, ...(index === 0 ? styles.navButtonOff : null) }}
        onClick={() => onStep(-1)}
        disabled={index === 0}
        title="Previous figure (←)"
      >
        ◀
      </button>
      <span style={styles.navLabel}>{label || '(unlabelled)'}</span>
      <span style={styles.navCount}>
        {index + 1} of {count}
      </span>
      <button
        type="button"
        style={{
          ...styles.navButton,
          ...(index >= count - 1 ? styles.navButtonOff : null),
        }}
        onClick={() => onStep(1)}
        disabled={index >= count - 1}
        title="Next figure (→)"
      >
        ▶
      </button>
    </div>
  )
}

/**
 * One factor's levels, with a checkbox each and a "3 of 12" readout.
 *
 * Collapsed by default: a schema key can have thirty levels, and the common
 * case is not filtering at all. The count is the backend's — `selected` is
 * measured through the same `apply_filters` the figure uses, so this can never
 * claim a selection the figure disagrees with.
 */
export function LevelPicker({
  factor,
  onChange,
}: {
  factor: FactorInfo
  onChange: (levels: (string | number)[] | null) => void
}) {
  const [open, setOpen] = useState(false)
  const levels = factor.levels ?? []
  const selected = factor.selected ?? levels
  const chosen = new Set(selected.map(String))
  const all = chosen.size === levels.length

  const toggle = (level: string | number) => {
    const next = new Set(chosen)
    if (next.has(String(level))) next.delete(String(level))
    else next.add(String(level))
    // Back to everything selected means "no filter", not a list of all levels.
    if (next.size === levels.length) onChange(null)
    else onChange(levels.filter(l => next.has(String(l))))
  }

  return (
    <div style={styles.levelPicker}>
      <button
        type="button"
        style={styles.levelPickerHead}
        onClick={() => setOpen(v => !v)}
        title={`Choose which ${factor.display} values to plot`}
      >
        <span style={styles.factorName}>{factor.display}</span>
        <span style={all ? styles.levelCount : styles.levelCountFiltered}>
          {all ? `all ${levels.length}` : `${chosen.size} of ${levels.length}`}
        </span>
        <span style={styles.levelChevron}>{open ? '▾' : '▸'}</span>
      </button>
      {open && (
        <div style={styles.levelList}>
          {levels.map(level => (
            <label key={String(level)} style={styles.levelRow}>
              <input
                type="checkbox"
                checked={chosen.has(String(level))}
                onChange={() => toggle(level)}
                style={{ marginRight: 6 }}
              />
              {String(level)}
            </label>
          ))}
          {/* Both bulk actions, so a 51-column ColName list can be cleared
              and then one or two columns ticked, instead of unticking 49.
              Deselect passes [] (an explicit empty filter), never null --
              null means "no filter" and would re-select everything. */}
          <div style={styles.levelBulkRow}>
            {!all && (
              <button type="button" style={styles.levelReset} onClick={() => onChange(null)}>
                Select all
              </button>
            )}
            {chosen.size > 0 && (
              <button type="button" style={styles.levelReset} onClick={() => onChange([])}>
                Deselect all
              </button>
            )}
          </div>
          {chosen.size === 0 && (
            <div style={styles.levelEmpty}>
              Nothing selected — the figure will be empty.
            </div>
          )}
        </div>
      )}
    </div>
  )
}

/**
 * One combine in Structure > Combine.
 *
 * Collapsed (the default): one line — `▸ Stim ← condition · 4 → 2`. Expanded:
 * the combine's name, its buckets, and ONE grid of the source's levels.
 * Bucket-first: pick a bucket (the target), then click levels to put them in
 * it or take them out; shift-click takes a range. A level belongs to one
 * bucket, so clicking it for the target moves it out of any other. Scales to
 * 40 subjects, where a text box per level (the old editor) did not.
 *
 * Every edit goes through the bucket model in `combine.ts` and is written back
 * as `mapping`; nothing here decides what a combine means.
 */
export function CombineEditor({
  group,
  levels,
  open,
  onToggle,
  onEdit,
  onRename,
  onUse,
  onRemove,
}: {
  group: LevelGroup
  levels: (string | number)[]
  open: boolean
  onToggle: () => void
  onEdit: (patch: Partial<LevelGroup>) => void
  onRename: (name: string) => void
  onUse: () => void
  onRemove: () => void
}) {
  const active = group.active !== false
  const buckets = readBuckets(group, levels)
  const unused = unusedLevels(buckets, levels)

  // Buckets named but still empty: a mapping only holds levels, so these live
  // here until the first level lands in one.
  const [pending, setPending] = useState<string[]>([])
  const labels = [...buckets.map(b => b.label), ...pending.filter(p => !buckets.some(b => b.label === p))]
  const [target, setTarget] = useState<string | null>(null)
  const current = target !== null && labels.includes(target) ? target : labels[0] ?? null
  const [anchor, setAnchor] = useState<string | null>(null)
  const [nameDraft, setNameDraft] = useState(group.name)
  useEffect(() => setNameDraft(group.name), [group.name])

  // Diagnostic for the "keeps springing open" report: a row that remounts
  // loses local state. Open/closed now lives in the parent; this confirms it.
  useEffect(() => {
    console.debug('[combine] editor mount', group.source, group.name)
    return () => console.debug('[combine] editor unmount', group.source, group.name)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const write = (next: ReturnType<typeof readBuckets>) => onEdit({ mapping: writeBuckets(next) })
  const bucketOf = (level: string) => buckets.find(b => b.levels.includes(level))?.label ?? null

  const clickLevel = (level: string, shift: boolean) => {
    if (current === null) return
    if (shift) {
      write(assignLevels(buckets, levelRange(levels, anchor, level), current, levels))
    } else {
      const inTarget = bucketOf(level) === current
      write(assignLevels(buckets, [level], inTarget ? null : current, levels))
    }
    setAnchor(level)
  }

  const addBucket = () => {
    let n = labels.length + 1
    while (labels.includes(`group ${n}`)) n++
    const label = `group ${n}`
    setPending(p => [...p, label])
    setTarget(label)
  }

  const commitName = () => {
    const name = nameDraft.trim()
    if (name && name !== group.name) onRename(name)
    else setNameDraft(group.name)
  }

  return (
    <div style={styles.levelPicker}>
      <div style={styles.variantSetRow}>
        <button
          type="button"
          style={styles.combineHead}
          onClick={onToggle}
          aria-expanded={open}
          title={open ? 'Collapse' : 'Edit this combine'}
        >
          <span style={styles.levelChevron}>{open ? '▾' : '▸'}</span>
          <span style={styles.factorName}>{group.name}</span>
          <span style={styles.combineFrom}>← {group.source}</span>
          <span style={styles.levelCount}>{combineSummary(group, levels)}</span>
        </button>
        {!active && (
          <button
            type="button"
            style={styles.combineUse}
            onClick={onUse}
            title={`Not in use: ${group.source} is shown as itself. Use this combine in its place.`}
          >
            use
          </button>
        )}
        <button type="button" style={styles.variantRemove} onClick={onRemove} title="Remove this combine">
          ✕
        </button>
      </div>
      {open && (
        <div style={styles.levelList}>
          <label style={styles.combineLine}>
            <span style={styles.combineLabel}>Name</span>
            <input
              value={nameDraft}
              onChange={e => setNameDraft(e.target.value)}
              onBlur={commitName}
              onKeyDown={e => { if (e.key === 'Enter') (e.target as HTMLInputElement).blur() }}
              style={styles.bucketInput}
              title="The name this combined factor carries in the figure"
            />
          </label>

          <div style={styles.combineLabel}>Buckets — pick one, then click levels</div>
          {labels.map(label => {
            const count = buckets.find(b => b.label === label)?.levels.length ?? 0
            return (
              <div key={label} style={{ ...styles.bucketRow, ...(label === current ? styles.bucketTarget : null) }}>
                <input
                  type="radio"
                  name={`combine-target-${group.source}-${group.name}`}
                  checked={label === current}
                  onChange={() => setTarget(label)}
                  title="Clicking a level puts it in this bucket"
                />
                <BucketLabel
                  label={label}
                  onRename={to => {
                    if (count === 0) {
                      setPending(p => p.map(x => (x === label ? to : x)))
                    } else {
                      write(renameBucket(buckets, label, to))
                    }
                    if (current === label) setTarget(to)
                  }}
                />
                <span style={styles.levelCount}>{count}</span>
                <button
                  type="button"
                  style={styles.variantRemove}
                  onClick={() => {
                    setPending(p => p.filter(x => x !== label))
                    const members = buckets.find(b => b.label === label)?.levels ?? []
                    if (members.length) write(assignLevels(buckets, members, null, levels))
                  }}
                  title="Remove this bucket (its levels become unused)"
                >
                  ✕
                </button>
              </div>
            )
          })}
          <div style={styles.levelBulkRow}>
            <button type="button" style={styles.levelReset} onClick={addBucket}>
              + Add bucket
            </button>
            <button
              type="button"
              style={styles.levelReset}
              onClick={() => { setPending([]); onEdit({ mapping: {} }) }}
              title="Take every level out of every bucket, to start from scratch"
            >
              Clear all
            </button>
          </div>

          <div style={styles.combineLabel}>Levels — shift-click for a range</div>
          <div style={styles.chipGrid}>
            {levels.map(raw => {
              const level = String(raw)
              const owner = bucketOf(level)
              const style =
                owner === null
                  ? styles.chipUnused
                  : owner === current
                    ? styles.chipOn
                    : styles.chipOther
              return (
                <button
                  key={level}
                  type="button"
                  style={{ ...styles.chip, ...style }}
                  onClick={e => clickLevel(level, e.shiftKey)}
                  disabled={current === null}
                  title={
                    owner === null
                      ? `${level}: in no bucket${group.unmatched === null ? ' (dropped)' : ` (→ ${group.unmatched})`}`
                      : `${level} → ${owner}`
                  }
                >
                  {level}
                  {owner !== null && owner !== current && <span style={styles.chipOwner}>{owner}</span>}
                </button>
              )
            })}
          </div>

          {unused.length > 0 && (
            <div style={styles.hint}>
              {unused.length} level{unused.length === 1 ? '' : 's'} in no bucket
              {group.unmatched === null ? ' — dropped from the figure' : ` — shown as ${group.unmatched}`}.
            </div>
          )}
          <label style={styles.levelRow} title="Levels in no bucket are dropped from the figure unless you name a catch-all">
            <input
              type="checkbox"
              checked={group.unmatched !== null}
              onChange={e => onEdit({ unmatched: e.target.checked ? 'other' : null })}
              style={{ marginRight: 6 }}
            />
            Keep the rest as
            <input
              value={group.unmatched ?? ''}
              disabled={group.unmatched === null}
              onChange={e => onEdit({ unmatched: e.target.value })}
              style={{ ...styles.bucketInput, marginLeft: 6 }}
            />
          </label>
        </div>
      )}
    </div>
  )
}

/** A bucket's name, committed on blur / Enter so a half-typed name never
 *  becomes a bucket (or merges into one) mid-keystroke. */
export function BucketLabel({ label, onRename }: { label: string; onRename: (to: string) => void }) {
  const [draft, setDraft] = useState(label)
  useEffect(() => setDraft(label), [label])
  return (
    <input
      value={draft}
      onChange={e => setDraft(e.target.value)}
      onBlur={() => {
        const to = draft.trim()
        if (to && to !== label) onRename(to)
        else setDraft(label)
      }}
      onKeyDown={e => { if (e.key === 'Enter') (e.target as HTMLInputElement).blur() }}
      style={styles.bucketInput}
      title="Renaming onto another bucket's name merges the two"
    />
  )
}

/** Which factor a row stands for — plain text, or, when the factor has
 *  combines, a dropdown of the slot's choices: the source and each combine.
 *  Choosing one hands it the slot (`combine.ts switchSlot`). The choices come
 *  from the backend (`alternatives`); nothing here works them out. */
export function SlotName({
  factor,
  factors,
  onSwitch,
}: {
  factor: FactorInfo
  factors: FactorInfo[]
  onSwitch: (source: string, choice: string) => void
}) {
  const choices = factor.alternatives ?? [factor.name]
  if (choices.length < 2) return <>{factor.display}</>
  const slot = factor.slot ?? factor.name
  const display = (name: string) => factors.find(f => f.name === name)?.display ?? name
  return (
    <select
      value={factor.name}
      onChange={e => onSwitch(slot, e.target.value)}
      onClick={e => e.stopPropagation()}
      style={styles.select}
      title={`${slot} or a combine of it — the choice replaces ${slot} everywhere`}
    >
      {choices.map(name => (
        <option key={name} value={name}>
          {name === slot ? display(name) : `${display(name)} (combined)`}
        </option>
      ))}
    </select>
  )
}

/** "+ Combine levels of…" — pick which factor to combine. A replaced source
 *  is offered too: a second combine of it is another choice in its dropdown. */
export function BucketAdder({
  factors,
  onAdd,
}: {
  factors: FactorInfo[]
  onAdd: (source: string, levels: (string | number)[]) => void
}) {
  if (factors.length === 0) return null
  return (
    <select
      value=""
      style={styles.variantAdd}
      onChange={e => {
        const factor = factors.find(f => f.name === e.target.value)
        if (factor) onAdd(factor.name, factor.levels)
      }}
    >
      <option value="">+ Combine levels of…</option>
      {factors.map(factor => (
        <option key={factor.name} value={factor.name}>
          {factor.display}
        </option>
      ))}
    </select>
  )
}

/** Numeric bounds on the measure: the outlier trim, not a level picker. */
export function RangeFilter({
  measure,
  filter,
  onChange,
}: {
  measure: string
  filter?: Filter
  onChange: (minimum: number | null, maximum: number | null) => void
}) {
  const parse = (text: string) => (text.trim() === '' ? null : Number(text))
  return (
    <div style={styles.rangeRow}>
      <span style={styles.factorName}>{measure}</span>
      <input
        type="number"
        value={filter?.minimum ?? ''}
        placeholder="min"
        style={styles.rangeInput}
        onChange={e => onChange(parse(e.target.value), filter?.maximum ?? null)}
      />
      <input
        type="number"
        value={filter?.maximum ?? ''}
        placeholder="max"
        style={styles.rangeInput}
        onChange={e => onChange(filter?.minimum ?? null, parse(e.target.value))}
      />
    </div>
  )
}

/**
 * What ticking a grouping did to the figure, in three words.
 *
 * Reads the spec, never decides anything: the role itself came from
 * `roles.role_for_new_grouping` and the nesting position from `groups.ts`.
 * The one that matters is "pooled" — a grouping that landed on FREE because
 * both channels were taken really does average its levels together, and a row
 * that looks applied while doing nothing is what this work set out to remove.
 */
export function groupingPlacement(
  spec: Spec | null,
  layers: string[],
  factors: FactorInfo[],
  name: string
): string {
  const role = spec?.roles?.[name]
  if (role === 'group') {
    const at = layers.indexOf(name)
    const inside = layers[at - 1]
    const display = (n: string) => factors.find(f => f.name === n)?.display ?? n
    const where = inside ? `grouped, around ${display(inside)}` : 'grouped'
    return spec?.color === name ? `${where}, coloured` : where
  }
  if (role === 'facet') return 'separate panels'
  if (role === 'iterate') return 'separate figures'
  if (role === 'collapse') return 'collapsed'
  return 'separate figures'
}

/** What the Summary section says the error bars are over — the chain and
 *  its sample, read off the capability report (`collapse`), never derived. */
export function sampleNote(capabilities: Capabilities | null | undefined, spec: Spec | null): string {
  const chain = capabilities?.collapse
  if (!chain || chain.order.length === 0) {
    return 'Nothing is collapsed, so there is no sample: each mark is one value and has no error bar.'
  }
  const error = spec?.aggregate?.error ?? 'sd'
  if (chain.pooled) {
    return `Pooled: every level of ${chain.order.join(' × ')} is one observation; ${error} across all of them.`
  }
  const inner = chain.order.slice(0, -1)
  const within = inner.length ? `${inner.join(', then ')} averaged within ${chain.sample} first; ` : ''
  return `${within}${error} across ${chain.sample}.`
}

/**
 * The grouping list: which factors get one mark per level combination, in
 * what order, and which one is coloured.
 *
 * Innermost first — the first row is the mark's own identity, each row below
 * wraps around it. The ↑ arrow moves a layer inward (towards the first row),
 * ↓ outward. One radio column tags the coloured layer: colour is PAINT — it
 * never splits data and never moves a mark; the layer keeps its place in the
 * nesting and its tick labels, and the legend names its levels as well
 * (user decision 2026-09-21, `roles.GroupingLayers`). That is why it is a tag
 * on a row here and not a role in the Factors dropdown.
 *
 * `maxLabelled` is the backend's cap on tick layers (a fourth level of
 * nesting cannot be read off an axis); every layer counts, coloured or not.
 */
export function GroupingList({
  factors,
  layers,
  color,
  hint,
  labelled,
  maxLabelled,
  onToggle,
  onMove,
  onColor,
  onSwitchSlot,
}: {
  factors: FactorInfo[]
  layers: string[]
  color: string | null
  hint?: string
  labelled: number
  maxLabelled: number
  onToggle: (factor: string, on: boolean) => void
  onMove: (factor: string, delta: number) => void
  onColor: (factor: string | null) => void
  /** The combine dropdown on a row name (`SlotName`). */
  onSwitchSlot: (source: string, choice: string) => void
}) {
  const groupable = factors.filter(f => f.group_available || layers.includes(f.name))
  const full = labelled >= maxLabelled
  const display = (name: string) =>
    factors.find(f => f.name === name)?.display ?? name

  return (
    <>
      <div style={styles.hint}>
        {hint ?? 'One mark per combination of these — first entry innermost.'}
        {' '}Tick the colour to paint the marks by a layer (the layout does not change).
      </div>
      {layers.map((name, index) => (
        <div key={name} style={styles.xLayerRow}>
          <span style={styles.xLayerDepth}>{index + 1}</span>
          <input
            type="checkbox"
            checked
            onChange={() => onToggle(name, false)}
            title="Stop grouping by this factor (it separates figures instead)"
            style={{ marginRight: 6 }}
          />
          <span style={styles.factorName}>
            {(() => {
              const factor = factors.find(f => f.name === name)
              return factor
                ? <SlotName factor={factor} factors={factors} onSwitch={onSwitchSlot} />
                : display(name)
            })()}
          </span>
          <label style={styles.colorTag} title="Paint the marks by this layer's level (legend added; nothing moves)">
            <input
              type="radio"
              name="grouping-colour"
              checked={color === name}
              onChange={() => onColor(name)}
              onClick={() => { if (color === name) onColor(null) }}
              style={{ marginRight: 3 }}
            />
            colour
          </label>
          <button
            type="button"
            style={{
              ...styles.navButton,
              ...(index === 0 ? styles.navButtonOff : null),
            }}
            disabled={index === 0}
            onClick={() => onMove(name, -1)}
            title="Move inward"
          >
            ↑
          </button>
          <button
            type="button"
            style={{
              ...styles.navButton,
              ...(index === layers.length - 1 ? styles.navButtonOff : null),
            }}
            disabled={index === layers.length - 1}
            onClick={() => onMove(name, 1)}
            title="Move outward"
          >
            ↓
          </button>
        </div>
      ))}
      {groupable
        .filter(f => !layers.includes(f.name))
        .map(factor => (
          <label
            key={factor.name}
            style={{ ...styles.kindRow, opacity: full ? 0.45 : 1 }}
            title={
              full
                ? `At most ${maxLabelled} labelled tick layers fit on the x axis — colour one, or separate panels.`
                : factor.group_reason ?? 'One mark per level of this factor'
            }
          >
            <input
              type="checkbox"
              checked={false}
              disabled={full || !factor.group_available}
              onChange={e => onToggle(factor.name, e.target.checked)}
              style={{ marginRight: 6 }}
            />
            <SlotName factor={factor} factors={factors} onSwitch={onSwitchSlot} />
          </label>
        ))}
      {layers.length === 0 && groupable.length === 0 && (
        <div style={styles.hint}>No factor here can group the marks.</div>
      )}
    </>
  )
}
