/**
 * What a Variable node prints about its variants.
 *
 * The node shows the variant the Variants popup marks CURRENT — the cards
 * scidb marks `is_default` (`_mark_defaults`), sent by the canvas build
 * (`variant_cards_service.canvas_variants`) in the cards' newest-first order.
 * Nothing here decides what is current. The heading and the ★/☆ glyph come
 * from the popup's own helpers, so the node and the popup cannot word the
 * same variant two ways.
 *
 * React-free so `npm test` can run it.
 */

import { cardHeading, statusBadge, type VariantCard } from '../Variants/variantCards.js'

export type NodeVariantCard = Pick<
  VariantCard,
  | 'card_id' | 'ordinal' | 'distinguishing' | 'record_count'
  | 'current_location_count' | 'verdict' | 'is_default' | 'is_pinned'
>

/** `data.variant` on a Variable node. */
export interface NodeVariants {
  variant_count: number
  has_default: boolean
  default_sources: string[]
  pooled_record_count: number
  current: NodeVariantCard[]
}

export interface VariableNodeText {
  /** "★ V3 of 3 · 48 records", "48 records"; null = show "empty". */
  countLine: string | null
  /** The current variant's distinguishing settings ("low hz = 30"), if several variants. */
  heading: string | null
  /** "+2 more current" when a sweep with no pin leaves several current. */
  more: string | null
  tooltip: string
}

function records(n: number): string {
  return `${n} record${n === 1 ? '' : 's'}`
}

const GLYPH: Record<string, string> = { pinned: '★ ', default: '☆ ' }

export function variableNodeText(totalRecords: number, variant?: NodeVariants): VariableNodeText {
  // No variant payload: a server older than it, or its cards failed to build
  // (WARN in scistack.log). Say the pooled number rather than nothing.
  if (!variant) {
    return {
      countLine: totalRecords > 0 ? records(totalRecords) : null,
      heading: null,
      more: null,
      tooltip: totalRecords > 0 ? `${records(totalRecords)} in all variants` : '',
    }
  }
  const all = `${variant.variant_count} variant${variant.variant_count === 1 ? '' : 's'}, ` +
    `${records(variant.pooled_record_count)} in all`
  const [head, ...others] = variant.current
  if (!head) {
    return {
      countLine: variant.pooled_record_count > 0 ? 'no current variant' : null,
      heading: null,
      more: null,
      tooltip: `${all}. None is current: the pinned selection matches no records. ` +
        'Right-click → Variants to see them.',
    }
  }
  if (variant.variant_count <= 1) {
    return { countLine: records(head.record_count), heading: null, more: null, tooltip: `${all}.` }
  }
  const badge = statusBadge(head, variant.has_default)
  const how = badge.code === 'pinned'
    ? ' (pinned)'
    : badge.code === 'default'
      ? ` (via the pin on ${variant.default_sources.join(', ')})`
      : ''
  const alsoCurrent = others.length > 0
    ? ` Also current: ${others.map(c => `V${c.ordinal}`).join(', ')}.`
    : ''
  return {
    countLine: `${GLYPH[badge.code] ?? ''}V${head.ordinal} of ${variant.variant_count} · ${records(head.record_count)}`,
    heading: cardHeading(head),
    more: others.length > 0 ? `+${others.length} more current` : null,
    tooltip: `V${head.ordinal} is current${how}. ${all}.${alsoCurrent} ` +
      'Right-click → Variants to see them.',
  }
}
