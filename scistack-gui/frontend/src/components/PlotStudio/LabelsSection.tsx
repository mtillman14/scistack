/**
 * The Labels section: the figure's titles, and what each factor and level
 * READS AS (display aliases). docs/claude/plot-text-and-labels.md.
 *
 * Two layers, and each row says which one it shows:
 * - a box holds THIS PLOT's alias (`PlotSpec.aliases`), stored with the
 *   saved plot and overriding the project's;
 * - an empty box shows, greyed, what the figure draws without it: the
 *   project's alias (`[aliases]` in scistack.toml) or the raw text.
 * "↑ project" makes a plot alias the project's (every plot then reads it);
 * "✕ project" removes a project alias. Both go through the backend
 * (`plot_project_alias_set` → scistack_gui.config, whose grammar is
 * scidb.aliases'), never written here.
 *
 * The rows come from Python (`layout.meta.labelable`): the measure and every
 * factor the figure draws as text. The panel never works out which.
 */

import { useState } from 'react'
import {
  aliasedCount,
  placeholderFor,
  plotLevel,
  plotName,
  projectAction,
  withPlotLevel,
  withPlotName,
  type Labelable,
  type LabelableText,
  type SpecAliases,
} from './aliasEdit'

/** One project edit, as `plot_project_alias_set` takes it. */
export interface ProjectAliasEdit {
  thing: string
  set_name?: boolean
  name?: string | null
  level?: string | null
  alias?: string | null
}

export interface TitleTexts {
  title?: string | null
  x_label?: string | null
  y_label?: string | null
}

interface Props {
  labelable: Labelable[]
  aliases: SpecAliases | undefined
  titles: TitleTexts
  onTitle: (key: keyof TitleTexts, value: string | null) => void
  onAliases: (next: SpecAliases) => void
  /** Resolves to an error message, or null when written. */
  onProject: (edit: ProjectAliasEdit) => Promise<string | null>
  /** False for a CSV plot: no project to write to, so no project buttons. */
  projectEnabled?: boolean
}

export default function LabelsSection({
  labelable,
  aliases,
  titles,
  onTitle,
  onAliases,
  onProject,
  projectEnabled = true,
}: Props) {
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const project = (edit: ProjectAliasEdit, then?: () => void) => {
    setBusy(true)
    onProject(edit)
      .then(message => {
        setError(message)
        if (message === null && then) then()
      })
      .catch(err => setError((err as Error).message))
      .finally(() => setBusy(false))
  }

  return (
    <div>
      <div style={styles.hint}>
        What the figure's text reads as. A box sets it for this plot; an empty box
        shows the project's alias or the raw text. ↑ project applies it to every plot.
      </div>
      {(['title', 'x_label', 'y_label'] as const).map(key => (
        <TextRow
          key={key}
          label={{ title: 'Title', x_label: 'X label', y_label: 'Y label' }[key]}
          value={titles[key] ?? null}
          placeholder="automatic"
          onChange={value => onTitle(key, value)}
        />
      ))}
      {labelable.map(entry => (
        <FactorAliases
          key={`${entry.role}:${entry.factor}`}
          entry={entry}
          aliases={aliases}
          busy={busy}
          projectEnabled={projectEnabled}
          onAliases={onAliases}
          project={project}
        />
      ))}
      {error && <div style={styles.error}>{error}</div>}
    </div>
  )
}

function FactorAliases({
  entry,
  aliases,
  busy,
  projectEnabled,
  onAliases,
  project,
}: {
  entry: Labelable
  aliases: SpecAliases | undefined
  busy: boolean
  projectEnabled: boolean
  onAliases: (next: SpecAliases) => void
  project: (edit: ProjectAliasEdit, then?: () => void) => void
}) {
  const [open, setOpen] = useState(false)
  const key = entry.key
  const name = plotName(aliases, key)
  const count = aliasedCount(entry)
  return (
    <div style={styles.factor}>
      <div style={styles.factorHeader}>
        <span style={styles.role}>{entry.role}</span>
        <AliasRow
          raw={entry.factor}
          item={entry.name}
          value={name}
          busy={busy}
          projectEnabled={projectEnabled}
          onChange={value => onAliases(withPlotName(aliases, key, value))}
          onProject={action =>
            action === 'save'
              ? project({ thing: key, set_name: true, name }, () =>
                  onAliases(withPlotName(aliases, key, null))
                )
              : project({ thing: key, set_name: true, name: null })
          }
        />
      </div>
      {entry.levels.length > 0 && (
        <button type="button" style={styles.disclosure} onClick={() => setOpen(!open)}>
          {open ? '▾' : '▸'} {entry.levels.length} level{entry.levels.length === 1 ? '' : 's'}
          {count ? ` · ${count} aliased` : ''}
        </button>
      )}
      {open &&
        entry.levels.map(level => {
          const value = plotLevel(aliases, key, level.raw)
          return (
            <AliasRow
              key={level.raw}
              raw={level.raw}
              item={level}
              value={value}
              busy={busy}
              projectEnabled={projectEnabled}
              onChange={next => onAliases(withPlotLevel(aliases, key, level.raw, next))}
              onProject={action =>
                action === 'save'
                  ? project({ thing: key, level: level.raw, alias: value }, () =>
                      onAliases(withPlotLevel(aliases, key, level.raw, null))
                    )
                  : project({ thing: key, level: level.raw, alias: null })
              }
            />
          )
        })}
      {open && entry.truncated && (
        <div style={styles.hint}>
          More levels than are listed here. Alias the rest in scistack.toml's
          [aliases.{key}.levels].
        </div>
      )}
    </div>
  )
}

function AliasRow({
  raw,
  item,
  value,
  busy,
  projectEnabled,
  onChange,
  onProject,
}: {
  raw: string
  item: LabelableText
  value: string | null
  busy: boolean
  projectEnabled: boolean
  onChange: (value: string | null) => void
  onProject: (action: 'save' | 'remove') => void
}) {
  const action = projectEnabled ? projectAction(value, item) : null
  return (
    <div style={styles.row}>
      <span style={styles.raw} title={raw}>
        {raw}
      </span>
      <input
        type="text"
        value={value ?? ''}
        placeholder={placeholderFor(item)}
        onChange={e => onChange(e.target.value === '' ? null : e.target.value)}
        style={styles.input}
      />
      {action && (
        <button
          type="button"
          disabled={busy}
          style={styles.projectButton}
          title={
            action === 'save'
              ? 'Make this the project alias (scistack.toml): every plot reads it'
              : 'Remove the project alias (scistack.toml): every plot shows the raw text'
          }
          onClick={() => onProject(action)}
        >
          {action === 'save' ? '↑ project' : '✕ project'}
        </button>
      )}
    </div>
  )
}

function TextRow({
  label,
  value,
  placeholder,
  onChange,
}: {
  label: string
  value: string | null
  placeholder: string
  onChange: (value: string | null) => void
}) {
  return (
    <div style={styles.row}>
      <span style={styles.raw}>{label}</span>
      <input
        type="text"
        value={value ?? ''}
        placeholder={placeholder}
        onChange={e => onChange(e.target.value === '' ? null : e.target.value)}
        style={styles.input}
      />
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  hint: { fontSize: 10, color: '#777', marginBottom: 6, fontStyle: 'italic' },
  error: { fontSize: 10, color: '#e0b050', marginTop: 6, lineHeight: 1.4 },
  factor: { borderTop: '1px solid #2a2a4a', paddingTop: 6, marginTop: 6 },
  factorHeader: { display: 'flex', flexDirection: 'column', gap: 2 },
  role: { fontSize: 9, color: '#7c7ca0', textTransform: 'uppercase', letterSpacing: 0.6 },
  row: { display: 'flex', alignItems: 'center', gap: 4, marginBottom: 3 },
  raw: {
    fontSize: 11, color: '#bbb', flex: '0 0 72px', overflow: 'hidden',
    textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  input: {
    flex: 1, minWidth: 0, background: '#22223a', color: '#ddd',
    border: '1px solid #3a3a5a', borderRadius: 4, fontSize: 11, padding: '3px 5px',
  },
  projectButton: {
    flex: '0 0 auto', padding: '1px 5px', background: '#22223a', color: '#aaa',
    border: '1px solid #3a3a5a', borderRadius: 4, cursor: 'pointer', fontSize: 9,
  },
  disclosure: {
    background: 'none', border: 'none', color: '#8a8aa8', cursor: 'pointer',
    fontSize: 10, padding: '2px 0', textAlign: 'left',
  },
}
