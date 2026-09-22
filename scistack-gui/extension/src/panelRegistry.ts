/**
 * PanelRegistry — the set of open webviews a push notification must reach.
 *
 * The DAG webview is a singleton within its session, so forwarding a
 * notification to it is one `if`. Plot Studio is not: it opens as its own
 * editor tab, there can be several at once, and each one is created long
 * after the notification handler was registered. Without a registry the
 * forwarding code has nothing to name, which is exactly how
 * `plot_save_complete` ended up delivered only to the DAG panel — the Plot
 * tab that started the save never learned it had finished, and its Save
 * button sat on "Saving…" until the tab was closed.
 *
 * One registry per session (`Session.plots`), never one global one: a save
 * that completes in one database must not re-enable the Save button of a
 * tab plotting a different database.
 *
 * Deliberately free of any `vscode` import: a panel is only "something you
 * can post a message to", which makes the routing testable under
 * `node --test`.
 */

export interface MessageSink {
  postMessage(msg: Record<string, unknown>): void;
}

export class PanelRegistry {
  private panels = new Set<MessageSink>();

  /** Number of panels currently registered. */
  get size(): number {
    return this.panels.size;
  }

  /**
   * The registered panels, for a caller that must act on each one (closing
   * a session's plot tabs). A snapshot, because disposing a panel
   * unregisters it and would otherwise mutate the set mid-iteration.
   */
  sinks(): MessageSink[] {
    return [...this.panels];
  }

  /**
   * Register a panel. Returns the function that removes it again — call it
   * from the panel's dispose, or a closed tab keeps receiving messages.
   */
  add(sink: MessageSink): () => void {
    this.panels.add(sink);
    return () => {
      this.panels.delete(sink);
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
    for (const sink of this.panels) {
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
}
