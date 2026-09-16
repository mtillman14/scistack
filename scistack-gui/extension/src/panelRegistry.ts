/**
 * PanelRegistry — the set of open webviews a push notification must reach.
 *
 * The DAG webview is a singleton the extension holds in a variable, so
 * forwarding a notification to it is one `if`. Plot Studio is not: it opens as
 * its own editor tab, `newTab` can open several at once, and each one is
 * created long after the notification handler was registered. Without a
 * registry the forwarding code has nothing to name, which is exactly how
 * `plot_save_complete` ended up delivered only to the DAG panel — the Plot tab
 * that started the save never learned it had finished, and its Save button sat
 * on "Saving…" until the tab was closed.
 *
 * Deliberately free of any `vscode` import: a panel is only "something you can
 * post a message to", which makes the routing testable under `node --test`.
 */

export interface MessageSink<Backend = unknown> {
  postMessage(msg: Record<string, unknown>): void;
  /**
   * Point the panel at a new backend process. Optional because the registry
   * only requires something to post to; a panel that talks to the server
   * itself (Plot Studio does — every `plot_*` RPC) must implement it, or a
   * server restart leaves it writing to a stream the old process took with
   * it: `Error [ERR_STREAM_DESTROYED]: Cannot call write after a stream was
   * destroyed` — seen on 2026-09-15 as "Could not open the plot panel".
   */
  updatePythonProcess?(proc: Backend): void;
}

export class PanelRegistry<Backend = unknown> {
  private sinks = new Set<MessageSink<Backend>>();

  /** Number of panels currently registered. */
  get size(): number {
    return this.sinks.size;
  }

  /**
   * Register a panel. Returns the function that removes it again — call it
   * from the panel's dispose, or a closed tab keeps receiving messages.
   */
  add(sink: MessageSink<Backend>): () => void {
    this.sinks.add(sink);
    return () => {
      this.sinks.delete(sink);
    };
  }

  /**
   * Post to every registered panel. Returns how many received it, which is
   * what the caller logs — "emitted, 0 panels" is the signature of this bug
   * and is otherwise indistinguishable from a message that was never sent.
   *
   * One panel that throws (a webview disposed between the notification and
   * this loop) must not swallow delivery to the rest, so failures are counted
   * out rather than propagated.
   */
  send(msg: Record<string, unknown>): number {
    let delivered = 0;
    for (const sink of this.sinks) {
      try {
        sink.postMessage(msg);
        delivered += 1;
      } catch {
        // A dead webview is not an error worth failing the broadcast over;
        // its own dispose will unregister it.
      }
    }
    return delivered;
  }

  /**
   * Hand every registered panel the process that replaced the last one.
   * Returns how many panels took it, for the same reason `send` counts:
   * "restarted, 0 panels rebound" while a plot tab is open is this bug.
   *
   * The DAG panel is rebound by name in `startPipeline`; plot tabs are
   * created after the fact and can only be reached through here.
   */
  rebind(proc: Backend): number {
    let rebound = 0;
    for (const sink of this.sinks) {
      if (!sink.updatePythonProcess) continue;
      try {
        sink.updatePythonProcess(proc);
        rebound += 1;
      } catch {
        // Same policy as send: one dead panel must not block the rest.
      }
    }
    return rebound;
  }
}
