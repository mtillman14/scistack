/**
 * The "Presets" section of Plot Studio's right-hand column
 * (.claude/plan-plot-presets.md, Stage 4).
 *
 * A preset is this plot's settings without its data. Save the current
 * settings as one, then apply it to any variable's plot: everything but the
 * variable, its variants, the title, the y label and the y limits becomes the
 * preset's. Applying is one undo step (Ctrl+Z restores the settings before).
 *
 * Presentation only. What a preset keeps, whether a name clashes, how it fits
 * another variable and what "remove" does are the backend's answers
 * (`scistackplot.presets`, `scistackplotdb.presets`). Questions are asked
 * inline, because a VS Code webview blocks `window.prompt` / `window.confirm`.
 */

import { useEffect, useState } from 'react'

import { appliedSummary, madeOnLabel, type PresetInfo } from './presets'
import { NameBox, Question, railStyles as styles } from './SavedPlotsRail'
import { savedAtLabel, type RestoreNote } from './savedPlots'

export interface PresetSaveResult {
  ok: boolean
  /** A DIFFERENT preset already has the name; ask before adding a version to it. */
  exists?: PresetInfo
}

interface Props {
  presets: PresetInfo[]
  /** The preset applied last in this panel, or null. */
  applied: PresetInfo | null
  /** What the last apply could not carry over. */
  notes: RestoreNote[]
  busy: boolean
  error: string
  onSave: (name: string, overwrite: boolean) => Promise<PresetSaveResult>
  onApply: (presetId: string) => void
  onRename: (presetId: string, name: string) => Promise<boolean>
  onRemove: (presetId: string) => Promise<void>
  onDismissNotes: () => void
}

type Pending =
  | { kind: 'naming'; text: string }
  | { kind: 'replace'; name: string; existing: PresetInfo }
  | { kind: 'renaming'; presetId: string; text: string }
  | { kind: 'removing'; preset: PresetInfo }
  | null

export default function PresetsSection({
  presets,
  applied,
  notes,
  busy,
  error,
  onSave,
  onApply,
  onRename,
  onRemove,
  onDismissNotes,
}: Props) {
  const [pending, setPending] = useState<Pending>(null)
  const [showNotes, setShowNotes] = useState(false)
  useEffect(() => { setShowNotes(false) }, [notes])

  const save = async (name: string, overwrite: boolean) => {
    const result = await onSave(name, overwrite)
    if (result.ok) setPending(null)
    else if (result.exists) setPending({ kind: 'replace', name, existing: result.exists })
  }

  return (
    <div style={local.section}>
      <div style={local.heading}>Presets</div>
      <div style={styles.hint}>
        This plot's settings without its data. Apply one to plot another
        variable the same way; its title, y label and y limits are kept.
      </div>

      {pending?.kind === 'naming' ? (
        <NameBox
          initial={pending.text}
          placeholder="Preset name"
          actionLabel="Save preset"
          busy={busy}
          onSubmit={name => save(name, false)}
          onCancel={() => setPending(null)}
        />
      ) : (
        <button
          type="button"
          style={styles.primaryButton}
          onClick={() => setPending({ kind: 'naming', text: applied?.name ?? '' })}
          disabled={busy}
          title="Save these settings, without the data, to apply to other variables"
        >
          Save settings as preset…
        </button>
      )}
      {pending?.kind === 'replace' && (
        <Question
          text={`"${pending.existing.name}" is another preset. Save these settings as its next version? Its earlier versions are kept.`}
          confirmLabel="Save as its version"
          onConfirm={() => save(pending.name, true)}
          onCancel={() => setPending({ kind: 'naming', text: pending.name })}
        />
      )}

      {applied && (
        <div style={{ ...styles.notes, marginTop: 8 }}>
          <div>
            {appliedSummary(applied.name, notes)}{' '}
            {notes.length > 0 && (
              <button type="button" style={styles.linkButton} onClick={() => setShowNotes(v => !v)}>
                {showNotes ? 'Hide' : 'Details'}
              </button>
            )}
            {notes.length > 0 && ' · '}
            <button type="button" style={styles.linkButton} onClick={onDismissNotes}>
              Dismiss
            </button>
          </div>
          {showNotes && (
            <ul style={styles.noteList}>
              {notes.map((note, i) => (
                <li key={i}>
                  <code>{note.path || 'plot'}</code>: {note.message}
                </li>
              ))}
            </ul>
          )}
          <div style={styles.noteHint}>Undo (Ctrl+Z) restores the settings from before.</div>
        </div>
      )}

      {error && <div style={{ ...styles.error, marginTop: 6 }}>{error}</div>}

      {presets.length === 0 ? (
        <div style={{ ...styles.hint, marginTop: 6 }}>No presets in this project yet.</div>
      ) : (
        <div style={{ ...styles.list, marginTop: 6 }}>
          {presets.map(preset => {
            if (pending?.kind === 'renaming' && pending.presetId === preset.preset_id) {
              return (
                <div key={preset.preset_id} style={styles.row}>
                  <NameBox
                    initial={pending.text}
                    placeholder="Preset name"
                    actionLabel="Rename"
                    busy={busy}
                    onSubmit={async name => {
                      if (await onRename(preset.preset_id, name)) setPending(null)
                    }}
                    onCancel={() => setPending(null)}
                  />
                </div>
              )
            }
            const madeOn = madeOnLabel(preset)
            return (
              <div key={preset.preset_id}>
                <div
                  style={styles.row}
                  title={
                    `Apply "${preset.name}" (version ${preset.version}) to this plot` +
                    (madeOn ? `\n${madeOn}` : '') +
                    (preset.shape_warning ? `\n${preset.shape_warning}` : '')
                  }
                >
                  <button
                    type="button"
                    style={styles.rowOpen}
                    onClick={() => onApply(preset.preset_id)}
                    disabled={busy}
                  >
                    <span style={styles.rowName}>
                      {preset.shape_warning && <span style={local.warn}>⚠ </span>}
                      {preset.name}
                    </span>
                    <span style={styles.meta}>
                      {madeOn ? `${madeOn} · ` : ''}
                      {savedAtLabel(preset.saved_at)} · v{preset.version}
                    </span>
                  </button>
                  <button
                    type="button"
                    style={styles.iconButton}
                    title="Rename"
                    onClick={() => setPending({ kind: 'renaming', presetId: preset.preset_id, text: preset.name })}
                  >
                    ✎
                  </button>
                  <button
                    type="button"
                    style={styles.iconButton}
                    title="Remove from this list"
                    onClick={() => setPending({ kind: 'removing', preset })}
                  >
                    ✕
                  </button>
                </div>
                {pending?.kind === 'removing' && pending.preset.preset_id === preset.preset_id && (
                  <Question
                    text={`Remove the preset "${preset.name}" from this list? It is hidden, not deleted: every version is kept in the database.`}
                    confirmLabel="Remove"
                    onConfirm={async () => { setPending(null); await onRemove(preset.preset_id) }}
                    onCancel={() => setPending(null)}
                  />
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

const local: Record<string, React.CSSProperties> = {
  section: { marginTop: 16, paddingTop: 10, borderTop: '1px solid var(--ps-border)' },
  heading: { color: 'var(--ps-text)', fontSize: 12, fontWeight: 600, marginBottom: 4 },
  warn: { color: 'var(--ps-caution)' },
}
