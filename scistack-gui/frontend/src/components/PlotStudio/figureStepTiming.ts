/**
 * How long one ◀ ▶ figure step takes, as the user sees it: click → painted.
 *
 * scistack.log shows the backend's share of a step (plot_resolve, the layout
 * timings) but nothing on either side of it: the panel's debounce before the
 * request goes, and Plotly drawing the answer after it lands. On 2026-10-16 a
 * "slow arrows" report could only be explained up to "resolved bar: figure N
 * rendered"; whatever the webview did next was invisible. This tracker splits
 * one step into those phases so the panel can send it to the log
 * (`report_client_timing`).
 *
 * Rapid clicks coalesce: the step is timed from the FIRST click to the
 * painting of the figure the LAST click asked for; answers for figures the
 * user clicked past are ignored, as the cursor ignores them (figureCursor.ts).
 */

/** One step's phases (ms), as sent to `report_client_timing`. */
export interface FigureStepTiming {
  figure_index: number
  /** Arrow presses folded into this step (rapid clicking). */
  clicks: number
  /** Last click → the request leaving (the panel's debounce, and any wait
   *  behind a resolve already in flight). */
  queued_ms: number
  /** Request out → answer in (the backend's plot_resolve, plus transport). */
  round_trip_ms: number
  /** Answer in → Plotly finished drawing it. */
  draw_ms: number
  /** First click → drawn. */
  total_ms: number
}

interface Pending {
  index: number
  firstClickAt: number
  lastClickAt: number
  clicks: number
  sentAt: number | null
  answeredAt: number | null
}

export class FigureStepTimer {
  private pending: Pending | null = null

  /** An arrow moved the cursor to `index`. */
  stepped(index: number, now: number): void {
    const p = this.pending
    if (p === null) {
      this.pending = {
        index, firstClickAt: now, lastClickAt: now, clicks: 1, sentAt: null, answeredAt: null,
      }
      return
    }
    // Clicked again before the last figure was drawn: one longer step.
    p.index = index
    p.lastClickAt = now
    p.clicks += 1
    p.sentAt = null
    p.answeredAt = null
  }

  /** A resolve for `index` was sent. */
  sent(index: number, now: number): void {
    const p = this.pending
    if (p !== null && p.index === index) {
      p.sentAt = now
      p.answeredAt = null
    }
  }

  /** The resolve for `index` was answered and applied. */
  answered(index: number, now: number): void {
    const p = this.pending
    if (p !== null && p.index === index && p.sentAt !== null) p.answeredAt = now
  }

  /** Plotly drew figure `index`. Returns the finished step, or null when this
   *  draw is not the end of a timed step (a spec change, a resize, a figure
   *  the user clicked past). */
  drawn(index: number, now: number): FigureStepTiming | null {
    const p = this.pending
    if (p === null || p.index !== index || p.sentAt === null || p.answeredAt === null) {
      return null
    }
    this.pending = null
    return {
      figure_index: index,
      clicks: p.clicks,
      queued_ms: Math.round(p.sentAt - p.lastClickAt),
      round_trip_ms: Math.round(p.answeredAt - p.sentAt),
      draw_ms: Math.round(now - p.answeredAt),
      total_ms: Math.round(now - p.firstClickAt),
    }
  }

  /** Forget a step that will not finish (an error answer, a new spec). */
  reset(): void {
    this.pending = null
  }
}
