/**
 * The Colours section: what colour each painted level's marks are drawn in.
 * docs/claude/plot-colors.md.
 *
 * Three layers, and each row says which one it shows:
 * - a box holds THIS PLOT's colour (`PlotSpec.colors`, or `style.mark_color`
 *   for the one colour of an uncoloured figure), stored with the saved plot;
 * - an empty box shows, greyed, the colour drawn without it: the project's
 *   (`[colors]` in scistack.toml) or the palette's.
 * The swatch is the colour DRAWN (Python's, `layout.meta.colorable`) and
 * opens the native colour picker; picking writes the plot's colour. The box
 * takes typed text (COLOR_FORMATS in colorEdit.ts) as written: Python
 * parses it, so the panel never does.
 * "↑ project" makes the plot's colour the project's (every plot then paints
 * it); "✕ project" removes the project's. Both go through the backend
 * (`plot_project_color_set` → scistack_gui.config, grammar scidb.colors'),
 * which refuses text that is not a colour.
 */

import { useEffect, useMemo, useRef, useState } from 'react'
import {
  colorPlaceholder,
  colorProjectAction,
  debounced,
  pinnedCount,
  plotColor,
  PICKER_DEBOUNCE_MS,
  withPlotColor,
  COLOR_FORMATS,
  type Colorable,
  type ColorableLevel,
  type SpecColors,
} from './colorEdit'

/** One project edit, as `plot_project_color_set` takes it. */
export interface ProjectColorEdit {
  /** null = the single mark colour (`[colors] default`). */
  thing: string | null
  level?: string | null
  color?: string | null
}

interface Props {
  colorable: Colorable[]
  colors: SpecColors | undefined
  /** The plot's own single mark colour (`style.mark_color`), or null. */
  markColor: string | null
  onColors: (next: SpecColors) => void
  onMarkColor: (value: string | null) => void
  /** Resolves to an error message, or null when written. */
  onProject: (edit: ProjectColorEdit) => Promise<string | null>
  /** False for a CSV plot: no project to write to, so no project buttons. */
  projectEnabled?: boolean
}

export default function ColorsSection({
  colorable,
  colors,
  markColor,
  onColors,
  onMarkColor,
  onProject,
  projectEnabled = true,
}: Props) {
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const project = (edit: ProjectColorEdit, then?: () => void) => {
    setBusy(true)
    onProject(edit)
      .then(message => {
        setError(message)
        if (message === null && then) then()
      })
      .catch(err => setError((err as Error).message))
      .finally(() => setBusy(false))
  }

  if (!colorable.length) {
    return <div style={styles.hint}>This kind is not painted by level.</div>
  }

  return (
    <div>
      <div style={styles.hint}>
        The colour of each level's marks. Click a swatch to pick, or type
        {COLOR_FORMATS}. An empty box shows the project's or the palette's colour.
        ↑ project applies it to every plot.
      </div>
      {colorable.map(entry => (
        <FactorColors
          key={`${entry.role}:${entry.factor ?? ''}`}
          entry={entry}
          colors={colors}
          markColor={markColor}
          busy={busy}
          projectEnabled={projectEnabled}
          onColors={onColors}
          onMarkColor={onMarkColor}
          project={project}
        />
      ))}
      {error && <div style={styles.error}>{error}</div>}
    </div>
  )
}

function FactorColors({
  entry,
  colors,
  markColor,
  busy,
  projectEnabled,
  onColors,
  onMarkColor,
  project,
}: {
  entry: Colorable
  colors: SpecColors | undefined
  markColor: string | null
  busy: boolean
  projectEnabled: boolean
  onColors: (next: SpecColors) => void
  onMarkColor: (value: string | null) => void
  project: (edit: ProjectColorEdit, then?: () => void) => void
}) {
  // The single mark colour is one row, always shown; a factor's levels fold.
  const single = entry.key === null
  const [open, setOpen] = useState(single)
  const count = pinnedCount(entry)
  const key = entry.key
  return (
    <div style={styles.factor}>
      <div style={styles.factorHeader}>
        <span style={styles.role}>{entry.role}</span>
        {!single && <span style={styles.name}>{entry.name}</span>}
      </div>
      {!single && (
        <button type="button" style={styles.disclosure} onClick={() => setOpen(!open)}>
          {open ? '▾' : '▸'} {entry.levels.length} level{entry.levels.length === 1 ? '' : 's'}
          {count ? ` · ${count} pinned` : ''}
        </button>
      )}
      {open &&
        entry.levels.map(level => {
          const value = key === null ? markColor : plotColor(colors, key, level.raw)
          const set = (next: string | null) =>
            key === null ? onMarkColor(next) : onColors(withPlotColor(colors, key, level.raw, next))
          return (
            <ColorRow
              key={level.raw}
              label={level.text}
              title={level.raw}
              level={level}
              value={value}
              busy={busy}
              projectEnabled={projectEnabled}
              onChange={set}
              onProject={action =>
                action === 'save'
                  ? project({ thing: key, level: key === null ? null : level.raw, color: value }, () =>
                      set(null)
                    )
                  : project({ thing: key, level: key === null ? null : level.raw, color: null })
              }
            />
          )
        })}
      {open && entry.truncated && (
        <div style={styles.hint}>
          More levels than are listed here. Pin the rest in scistack.toml's [colors.{key}].
        </div>
      )}
    </div>
  )
}

function ColorRow({
  label,
  title,
  level,
  value,
  busy,
  projectEnabled,
  onChange,
  onProject,
}: {
  label: string
  title: string
  level: ColorableLevel
  value: string | null
  busy: boolean
  projectEnabled: boolean
  onChange: (value: string | null) => void
  onProject: (action: 'save' | 'remove') => void
}) {
  const action = projectEnabled ? colorProjectAction(value, level) : null
  // The swatch follows a drag locally at once; the spec (and a resolve) gets
  // only the last colour of a burst (`debounced`), and at once on `change`.
  const [dragging, setDragging] = useState<string | null>(null)
  // onChange is a fresh closure every render (it holds the latest spec), so
  // the debouncer reads it through a ref and is built ONCE: rebuilt per
  // render, its cleanup would cancel the pending commit at every re-render
  // the drag itself causes, and nothing would ever be written.
  const latest = useRef(onChange)
  latest.current = onChange
  const commit = useMemo(
    () =>
      debounced((hex: string) => latest.current(hex), PICKER_DEBOUNCE_MS),
    []
  )
  useEffect(() => () => commit.cancel(), [commit])
  // The picked colour shows until the figure comes back drawn in it; clearing
  // it at commit time would flash the old colour for one resolve.
  useEffect(() => setDragging(null), [level.hex])
  // The text box's uncommitted text; null = show the plot's value.
  const [draft, setDraft] = useState<string | null>(null)
  const commitDraft = () => {
    if (draft === null) return
    const text = draft.trim()
    setDraft(null)
    if (text !== (value ?? '')) onChange(text === '' ? null : text)
  }
  return (
    <div style={styles.row}>
      <input
        type="color"
        value={dragging ?? level.hex}
        title={`Pick a colour for ${title}`}
        style={styles.swatch}
        onChange={e => {
          setDragging(e.target.value)
          commit.call(e.target.value)
        }}
        onBlur={() => commit.flush()}
      />
      <span style={styles.raw} title={title}>
        {label}
      </span>
      <input
        type="text"
        value={draft ?? value ?? ''}
        placeholder={colorPlaceholder(level)}
        title={`${COLOR_FORMATS}; Enter or leaving the box applies it`}
        // Committed on Enter / blur, not per keystroke: "#ff" on the way to
        // a whole colour is not a colour, and each keystroke would be a resolve
        // that WARNs about it.
        onChange={e => setDraft(e.target.value)}
        onBlur={commitDraft}
        onKeyDown={e => {
          if (e.key === 'Enter') commitDraft()
          if (e.key === 'Escape') setDraft(null)
        }}
        style={styles.input}
        spellCheck={false}
      />
      {action && (
        <button
          type="button"
          disabled={busy}
          style={styles.projectButton}
          title={
            action === 'save'
              ? 'Make this the project colour (scistack.toml): every plot paints it'
              : 'Remove the project colour (scistack.toml): every plot uses the palette'
          }
          onClick={() => {
            commit.flush()
            onProject(action)
          }}
        >
          {action === 'save' ? '↑ project' : '✕ project'}
        </button>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  hint: { fontSize: 10, color: 'var(--ps-text-faint)', marginBottom: 6, fontStyle: 'italic' },
  error: { fontSize: 10, color: 'var(--ps-caution)', marginTop: 6, lineHeight: 1.4 },
  factor: { borderTop: '1px solid var(--ps-border)', paddingTop: 6, marginTop: 6 },
  factorHeader: { display: 'flex', flexDirection: 'column', gap: 2 },
  role: { fontSize: 9, color: 'var(--ps-text-faint-indigo)', textTransform: 'uppercase', letterSpacing: 0.6 },
  name: { fontSize: 11, color: 'var(--ps-text-secondary)' },
  row: { display: 'flex', alignItems: 'center', gap: 4, marginBottom: 3 },
  swatch: {
    flex: '0 0 22px', width: 22, height: 18, padding: 0, border: '1px solid var(--ps-border-strong)',
    borderRadius: 3, background: 'none', cursor: 'pointer',
  },
  raw: {
    fontSize: 11, color: 'var(--ps-text-secondary)', flex: '0 0 64px', overflow: 'hidden',
    textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  input: {
    flex: 1, minWidth: 0, background: 'var(--ps-control)', color: 'var(--ps-text-body)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, fontSize: 11, padding: '3px 5px',
    fontFamily: 'var(--vscode-editor-font-family, monospace)',
  },
  projectButton: {
    flex: '0 0 auto', padding: '1px 5px', background: 'var(--ps-control)', color: 'var(--ps-text-secondary-alt)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, cursor: 'pointer', fontSize: 9,
  },
  disclosure: {
    background: 'none', border: 'none', color: 'var(--ps-text-muted-purple)', cursor: 'pointer',
    fontSize: 10, padding: '2px 0', textAlign: 'left',
  },
}
