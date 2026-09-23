/**
 * ReadOnlyDeclarationBanner — shown at the top of an entity settings panel
 * when the entity is declared in source outside the entities file, so the
 * GUI cannot write to it (docs/claude/entity-editability-model.md: only the
 * entities file is writable, permanently, by design).
 *
 * Shown BEFORE any edit is attempted, alongside greyed-out controls — the
 * write-time refusal in useSourceEdit is only the backstop.
 */

import { formatLocation } from './sourceLocation'

interface Props {
  file: string
  line: number | null
}

export default function ReadOnlyDeclarationBanner({ file, line }: Props) {
  return (
    <div style={styles.banner} title={line ? `${file}:${line}` : file}>
      <div style={styles.title}>Read-only — declared in source</div>
      <div>
        This is declared in{' '}
        <span style={styles.mono}>{formatLocation({ file, line })}</span>, not
        in the entities file, so the GUI can't edit it. Edit it directly in
        that file, then hit 🔄 Refresh Code.
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  banner: {
    background: 'rgba(148, 163, 184, 0.10)',
    border: '1px solid #475569',
    borderRadius: 4,
    padding: '6px 8px',
    marginBottom: 12,
    fontSize: 11,
    color: '#b8c2d0',
    lineHeight: 1.4,
  },
  title: {
    fontWeight: 700,
    marginBottom: 2,
    color: '#cbd5e1',
  },
  mono: {
    fontFamily: 'monospace',
  },
}
