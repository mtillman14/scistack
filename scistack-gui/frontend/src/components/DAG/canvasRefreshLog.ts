/**
 * Canvas refresh diagnostics: which refresh dropped an on-screen item.
 *
 * Symptom (user, 2026-10-22, after canvas-as-document): a node or edge you
 * just placed disappears for a moment, then reappears. The canvas adds it
 * optimistically, then every refresh (PipelineDAG fetchPipeline) REPLACES
 * the node/edge lists with the server's snapshot. Suspected cause, not yet
 * confirmed: a refresh that started BEFORE the item's write (the server
 * runs each request on its own thread, and the extension's .duckdb watcher
 * now refreshes ~2 s after the GUI's own position writes) lands after the
 * optimistic add and wipes it; the next refresh brings it back.
 *
 * This module is the bookkeeping that settles it from one reproduction:
 * each refresh gets a sequence number and start time; each optimistic item
 * is remembered with when it was added and when its write was confirmed.
 * When a refresh is applied, an item missing from its snapshot is reported
 * with the verdict that separates the hypotheses:
 *   - "stale_snapshot": the refresh started before the write was confirmed
 *     (the race above — frontend/watcher fix);
 *   - "server_omitted": the refresh started after the confirm and the server
 *     still left it out (a build-side bug — document view / scope filter).
 *
 * Pure: no React, no DOM, no RPC. PipelineDAG owns the instance and sends
 * the report (report_canvas_refresh -> scistack.log).
 */

export type PendingKind = 'node' | 'edge'

export interface PendingItem {
  kind: PendingKind
  /** What else the server may return it as: an edge's connection
   *  (`edgeMatchKey`; a drawn edge over a recorded connection comes back
   *  under the history edge's `e__` id) or an entity box's type+label
   *  (`nodeMatchKey`; a second RawGait box folds into var__RawGait).
   *  Function nodes have none: a fork keeps its own id. */
  matchKey?: string
  addedAt: number
  /** When the write RPC (put_layout / put_edge) resolved; undefined while in flight. */
  confirmedAt?: number
}

export interface DroppedItem {
  kind: PendingKind
  id: string
  /** ms from the optimistic add to this refresh being applied. */
  age_ms: number
  /** ms from this refresh's start to the write's confirm (positive = the
   *  refresh started BEFORE the confirm); null while still unconfirmed. */
  confirm_after_start_ms: number | null
  verdict: 'stale_snapshot' | 'server_omitted'
}

export interface RefreshStart {
  seq: number
  trigger: string
  startedAt: number
}

export interface RefreshReport {
  seq: number
  trigger: string
  scope: string
  nodes: number
  edges: number
  get_pipeline_ms: number
  get_layout_ms: number
  total_ms: number
  /** A newer refresh had already started when this one was applied. */
  superseded_by: number | null
  dropped: DroppedItem[]
  /** Optimistic items this snapshot carried (now confirmed on screen). */
  settled: number
  /** Optimistic items it carried under another id (match key): folded. */
  folded: number
}

/** The connection a drawn edge describes, as both sides can compute it. */
export function edgeMatchKey(source: string, target: string, targetHandle?: string | null): string {
  return `edge|${source}|${target}|${targetHandle ?? ''}`
}

/** The entity a placed box stands for; null for kinds that never fold. */
export function nodeMatchKey(type: string | undefined, label: unknown): string | null {
  if (type === 'functionNode' || type === undefined || typeof label !== 'string') return null
  return `node|${type}|${label}`
}

/** Pending items are forgotten after this long even if never seen again
 *  (a refused write, a scope switch), so the map cannot grow. */
export const PENDING_TTL_MS = 60_000

export class CanvasRefreshLog {
  private seq = 0
  private pending = new Map<string, PendingItem>()

  /** A refresh is starting; returns its handle. */
  start(trigger: string, now: number): RefreshStart {
    this.seq += 1
    return { seq: this.seq, trigger, startedAt: now }
  }

  /** The newest refresh number handed out. */
  get latestSeq(): number {
    return this.seq
  }

  /** An item was drawn before the server confirmed it. */
  addPending(id: string, kind: PendingKind, now: number, matchKey?: string | null): void {
    this.pending.set(id, { kind, addedAt: now, ...(matchKey ? { matchKey } : {}) })
  }

  /** Its write RPC resolved. */
  confirm(id: string, now: number): void {
    const item = this.pending.get(id)
    if (item && item.confirmedAt === undefined) item.confirmedAt = now
  }

  /** Its write was refused or rolled back: stop tracking it. */
  forget(id: string): void {
    this.pending.delete(id)
  }

  pendingCount(): number {
    return this.pending.size
  }

  /**
   * A refresh is being applied with these node/edge ids. Returns the report
   * and forgets every pending item the snapshot carried (settled) or that
   * has outlived PENDING_TTL_MS. A dropped item stays pending, so the
   * refresh that brings it back is the one that settles it.
   */
  apply(
    start: RefreshStart,
    snapshot: {
      scope: string
      nodeIds: Iterable<string>
      edgeIds: Iterable<string>
      /** Match keys of the snapshot's nodes and edges (see PendingItem.matchKey). */
      matchKeys?: Iterable<string>
    },
    timings: { get_pipeline_ms: number; get_layout_ms: number },
    now: number,
  ): RefreshReport {
    const nodeIds = new Set(snapshot.nodeIds)
    const edgeIds = new Set(snapshot.edgeIds)
    const matchKeys = new Set(snapshot.matchKeys ?? [])
    const dropped: DroppedItem[] = []
    let settled = 0
    let folded = 0
    for (const [id, item] of [...this.pending]) {
      const present = item.kind === 'node' ? nodeIds.has(id) : edgeIds.has(id)
      if (present) {
        this.pending.delete(id)
        settled += 1
        continue
      }
      if (item.matchKey && matchKeys.has(item.matchKey)) {
        this.pending.delete(id)
        folded += 1
        continue
      }
      if (now - item.addedAt > PENDING_TTL_MS) {
        this.pending.delete(id)
        continue
      }
      // Missing from a wholesale replace = wiped from the screen, whether it
      // was added before or during this refresh. The verdict asks whether
      // the snapshot COULD have contained it (write confirmed before start).
      const confirmAfterStart =
        item.confirmedAt === undefined ? null : item.confirmedAt - start.startedAt
      dropped.push({
        kind: item.kind,
        id,
        age_ms: Math.round(now - item.addedAt),
        confirm_after_start_ms: confirmAfterStart === null ? null : Math.round(confirmAfterStart),
        verdict:
          confirmAfterStart === null || confirmAfterStart > 0
            ? 'stale_snapshot'
            : 'server_omitted',
      })
    }
    return {
      seq: start.seq,
      trigger: start.trigger,
      scope: snapshot.scope,
      nodes: nodeIds.size,
      edges: edgeIds.size,
      get_pipeline_ms: Math.round(timings.get_pipeline_ms),
      get_layout_ms: Math.round(timings.get_layout_ms),
      total_ms: Math.round(now - start.startedAt),
      superseded_by: this.seq > start.seq ? this.seq : null,
      dropped,
      settled,
      folded,
    }
  }
}
