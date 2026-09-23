/**
 * PathInputSettingsPanel — shown in the sidebar when a PathInput node is
 * selected. Edit its template, or add alternates.
 *
 * Edits rewrite the `scidb.PathInput(...)` declaration in source via
 * `update_path_input` (docs/claude/entity-editability-model.md). Adding
 * alternates re-renders it as `EachOf(PathInput(...), PathInput(...))` under
 * the same name — that IS "multiple templates", not a separate concept.
 *
 * **Adding an alternate is always safe; REPLACING the primary template is
 * the one edit that can detach history.** Run history is attributed to a
 * node by content-matching its template (PathInput.to_key() records the
 * value, never the bound name), so overwriting the template would orphan
 * prior runs — D7's name-history table is what keeps that from happening,
 * but only for edits made HERE. The hint below says so, because a user
 * hand-editing source instead gets no such protection.
 *
 * A PathInput declared outside the entities file (e.g. in a MATLAB script)
 * is read-only: every edit control is greyed out up front with a banner
 * pointing at the declaration (useEntityEditability), rather than taking
 * input the write would then refuse.
 *
 * A refused write is SHOWN, never silently reverted: this panel used to
 * offer inputs wired to since-removed RPCs that no-opped on every save,
 * which read as the field reverting rather than the write being refused.
 */

import { useCallback, useState } from 'react'
import { callBackend } from '../../api'
import { useScope } from '../../context/ScopeContext'
import ReadOnlyDeclarationBanner from './ReadOnlyDeclarationBanner'
import { formatLocation, isLockedForEditing } from './sourceLocation'
import { useEntityEditability, useSourceEdit } from './useSourceEdit'

interface PathInputAlternate {
  template: string
  root_folder: string | null
}

interface Props {
  id: string
  label: string
  template: string
  root_folder: string | null
  alternate_templates: PathInputAlternate[]
}

function parseTemplateKeys(template: string): string[] {
  const matches = template.match(/\{(\w+)\}/g)
  if (!matches) return []
  return [...new Set(matches.map(m => m.slice(1, -1)))]
}

export default function PathInputSettingsPanel({ id, label, template, root_folder, alternate_templates }: Props) {
  const { bumpGraph } = useScope()
  const [deepCopyError, setDeepCopyError] = useState('')
  const [templateDraft, setTemplateDraft] = useState(template)
  const [rootDraft, setRootDraft] = useState(root_folder ?? '')
  const [altDraft, setAltDraft] = useState('')
  const { submit, error, readOnlyAt, saving, clearError } = useSourceEdit()
  // Declared outside the entities file: every edit control is locked up
  // front rather than taking input the write would refuse.
  const editability = useEntityEditability('path_input', label)
  const locked = isLockedForEditing(editability)
  const inputsDisabled = saving || locked

  const write = (t: string, root: string, alts: PathInputAlternate[]) =>
    submit('update_path_input', {
      name: label,
      template: t,
      root_folder: root.trim() || null,
      alternate_templates: alts.length ? alts : null,
    })

  const templateDirty =
    templateDraft !== template || rootDraft !== (root_folder ?? '')

  const saveTemplate = () => {
    if (!templateDirty) return
    write(templateDraft, rootDraft, alternate_templates)
  }

  const addAlternate = async () => {
    const t = altDraft.trim()
    if (!t) return
    const next = [...alternate_templates, { template: t, root_folder: null }]
    if (await write(template, rootDraft, next)) setAltDraft('')
  }

  const removeAlternate = (index: number) =>
    write(template, rootDraft, alternate_templates.filter((_, i) => i !== index))

  const keys = parseTemplateKeys(template)

  const handleDeepCopy = useCallback(() => {
    callBackend('deep_copy_path_input', { node_id: id })
      .then(() => { setDeepCopyError(''); bumpGraph() })
      .catch(err => setDeepCopyError((err as Error).message))
  }, [id, bumpGraph])

  return (
    <div style={styles.root}>
      <div style={styles.name}>{label}</div>

      {locked && editability?.file && (
        <ReadOnlyDeclarationBanner file={editability.file} line={editability.line} />
      )}

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Path Template</div>
        <input
          style={locked ? styles.inputLocked : styles.input}
          value={templateDraft}
          disabled={inputsDisabled}
          placeholder="{subject}/{trial}.csv"
          onChange={e => { setTemplateDraft(e.target.value); clearError() }}
          onBlur={saveTemplate}
          onKeyDown={e => {
            if (e.key === 'Enter') saveTemplate()
            if (e.key === 'Escape') { setTemplateDraft(template); clearError() }
          }}
        />
        {!locked && (
          <div style={styles.hint}>
            Shared by name — this rewrites the{' '}
            <span style={styles.mono}>scidb.PathInput(...)</span> declaration in
            source. Replacing the template re-points existing runs at the new
            one; adding an alternate below never does.
          </div>
        )}
      </section>

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Root Folder</div>
        <input
          style={locked ? styles.inputLocked : styles.input}
          value={rootDraft}
          disabled={inputsDisabled}
          placeholder="(not set)"
          onChange={e => { setRootDraft(e.target.value); clearError() }}
          onBlur={saveTemplate}
          onKeyDown={e => {
            if (e.key === 'Enter') saveTemplate()
            if (e.key === 'Escape') { setRootDraft(root_folder ?? ''); clearError() }
          }}
        />
      </section>

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Alternate Templates</div>
        <div style={styles.hint}>
          More than one template runs as EachOf(...) — one for_each call per
          alternative, results concatenated.
        </div>

        {alternate_templates.length === 0 ? (
          <div style={styles.empty}>No alternates — single template only.</div>
        ) : (
          alternate_templates.map((alt, i) => (
            <div key={i} style={styles.altRow}>
              <div style={styles.altTemplateText}>{alt.template}</div>
              {alt.root_folder && (
                <div style={styles.altRootText}>root: {alt.root_folder}</div>
              )}
              <button
                style={styles.removeBtn}
                onClick={() => removeAlternate(i)}
                disabled={inputsDisabled}
                title="Remove this alternate"
              >
                ×
              </button>
            </div>
          ))
        )}

        <div style={styles.addRow}>
          <input
            style={locked ? styles.inputLocked : styles.input}
            placeholder="another template…"
            value={altDraft}
            disabled={inputsDisabled}
            onChange={e => { setAltDraft(e.target.value); clearError() }}
            onKeyDown={e => {
              if (e.key === 'Enter') addAlternate()
              if (e.key === 'Escape') { setAltDraft(''); clearError() }
            }}
          />
          <button
            style={locked ? styles.addBtnLocked : styles.addBtn}
            onClick={addAlternate}
            disabled={inputsDisabled}
          >
            {saving ? '…' : 'Add'}
          </button>
        </div>
      </section>

      {error && (
        <div style={styles.errorBox}>
          {error}
          {readOnlyAt && (
            <div style={styles.errorDetail}>
              Declared in <span style={styles.mono}>{formatLocation(readOnlyAt)}</span> —
              edit it there and hit 🔄 Refresh Code.
            </div>
          )}
        </div>
      )}

      <section style={styles.section}>
        <button style={styles.deepCopyBtn} onClick={handleDeepCopy} type="button">
          Deep copy (make this placement independent)
        </button>
        {deepCopyError && <div style={styles.errorText}>{deepCopyError}</div>}
      </section>

      {keys.length > 0 && (
        <section style={styles.section}>
          <div style={styles.sectionTitle}>Schema Keys</div>
          <div style={styles.keysRow}>
            {keys.map(k => (
              <span key={k} style={styles.keyPill}>{k}</span>
            ))}
          </div>
        </section>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  input: {
    width: '100%',
    background: '#1a1a2e',
    border: '1px solid #333',
    borderRadius: 3,
    color: '#ccc',
    fontFamily: 'monospace',
    fontSize: 11,
    padding: '3px 6px',
    minWidth: 0,
    boxSizing: 'border-box',
  },
  addRow: {
    display: 'flex',
    gap: 6,
    marginTop: 6,
  },
  addBtn: {
    background: '#d97706',
    border: 'none',
    borderRadius: 3,
    color: '#fff',
    fontSize: 11,
    padding: '3px 8px',
    cursor: 'pointer',
    fontWeight: 600,
  },
  // Greyed, not hidden: the values stay readable, only writing is refused.
  inputLocked: {
    width: '100%',
    background: '#15151f',
    border: '1px solid #2a2a2a',
    borderRadius: 3,
    color: '#777',
    fontFamily: 'monospace',
    fontSize: 11,
    padding: '3px 6px',
    minWidth: 0,
    boxSizing: 'border-box',
    cursor: 'not-allowed',
  },
  addBtnLocked: {
    background: '#3a3a3a',
    border: 'none',
    borderRadius: 3,
    color: '#777',
    fontSize: 11,
    padding: '3px 8px',
    cursor: 'not-allowed',
    fontWeight: 600,
  },
  removeBtn: {
    background: 'transparent',
    border: 'none',
    color: '#666',
    cursor: 'pointer',
    fontSize: 14,
    padding: '0 2px',
    lineHeight: 1,
  },
  errorBox: {
    background: 'rgba(255, 77, 79, 0.12)',
    border: '1px solid #ff4d4f',
    borderRadius: 4,
    padding: '6px 8px',
    fontSize: 11,
    color: '#ff9a9c',
    whiteSpace: 'pre-wrap',
  },
  errorDetail: {
    marginTop: 4,
    color: '#c98a8b',
  },
  root: {
    padding: '12px',
    color: '#ccc',
    fontSize: 12,
  },
  name: {
    fontFamily: 'monospace',
    fontWeight: 700,
    fontSize: 13,
    color: '#fbbf24',
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
  value: {
    display: 'block',
    width: '100%',
    background: '#1a1a2e',
    border: '1px solid #333',
    borderRadius: 3,
    color: '#e5c8a0',
    fontSize: 11,
    fontFamily: 'monospace',
    padding: '5px 6px',
    boxSizing: 'border-box',
    wordBreak: 'break-all',
  },
  hint: {
    fontSize: 10,
    color: '#666',
    marginTop: 4,
    lineHeight: 1.4,
  },
  mono: {
    fontFamily: 'monospace',
  },
  deepCopyBtn: {
    width: '100%',
    padding: '5px 0',
    background: 'transparent',
    color: '#fbbf24',
    border: '1px solid #92702a',
    borderRadius: 4,
    cursor: 'pointer',
    fontSize: 11,
    fontWeight: 600,
  },
  errorText: {
    marginTop: 6,
    fontSize: 11,
    color: '#f87171',
    whiteSpace: 'pre-wrap',
  },
  keysRow: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 5,
  },
  keyPill: {
    fontSize: 11,
    fontFamily: 'monospace',
    background: '#3d2e1a',
    border: '1px solid #92702a',
    borderRadius: 3,
    padding: '2px 6px',
    color: '#fbbf24',
  },
  empty: {
    color: '#555',
    fontStyle: 'italic',
    fontSize: 11,
    marginTop: 6,
  },
  altRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    marginTop: 6,
    borderBottom: '1px solid #1e1e3a',
    paddingBottom: 4,
  },
  altTemplateText: {
    flex: 1,
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#e5c8a0',
    wordBreak: 'break-all',
  },
  altRootText: {
    fontFamily: 'monospace',
    fontSize: 10,
    color: '#8a7a60',
    wordBreak: 'break-all',
  },
}
