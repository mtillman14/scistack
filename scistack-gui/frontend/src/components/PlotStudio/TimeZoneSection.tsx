/**
 * Data > Time zone: how datetimes holding several zones share the figure.
 * Shown only when a datetime is in play (`capabilities.time_zone.applies`).
 *
 * The choice is `PlotSpec.time_zone`; everything else — the zones the data
 * holds, whether a typed zone exists, the notes about the drawn figure — comes
 * from Python (`scistackplot.temporal`). Pure logic: timeZone.ts.
 */

import { useState } from 'react'

import {
  LOCAL,
  OTHER,
  normalizeMode,
  timeZoneOptions,
  typedZone,
  type TemporalMeta,
  type TimeZoneCapability,
} from './timeZone'

interface Props {
  mode: string | undefined
  capability: TimeZoneCapability | undefined
  meta: TemporalMeta | null
  onChange: (mode: string) => void
}

export default function TimeZoneSection({ mode, capability, meta, onChange }: Props) {
  const current = normalizeMode(mode)
  const [typing, setTyping] = useState<string | null>(null)
  const options = timeZoneOptions(capability, current)
  const commit = () => {
    const zone = typing === null ? null : typedZone(typing)
    setTyping(null)
    if (zone) onChange(zone)
  }
  return (
    <>
      <div style={styles.row}>
        <span style={styles.label}>Place at</span>
        <select
          value={typing !== null ? OTHER : current}
          onChange={e => {
            if (e.target.value === OTHER) {
              setTyping('')
            } else {
              setTyping(null)
              onChange(e.target.value)
            }
          }}
          style={styles.input}
        >
          {options.map(option => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>
      {typing !== null && (
        <div style={styles.row}>
          <span style={styles.label}>Zone</span>
          <input
            type="text"
            autoFocus
            value={typing}
            placeholder="Europe/Berlin, +02:00"
            onChange={e => setTyping(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter') commit()
              if (e.key === 'Escape') setTyping(null)
            }}
            onBlur={commit}
            style={styles.input}
          />
        </div>
      )}
      <div style={styles.note}>
        {current === LOCAL
          ? 'Each value at its own local clock time, so times of day compare across sites.'
          : `Every value with a zone converted to ${current}, so instants line up.`}
      </div>
      {capability?.problem && <div style={styles.warn}>{capability.problem}</div>}
      {(meta?.notes ?? []).map(line => (
        <div key={line} style={styles.warn}>
          {line}
        </div>
      ))}
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
