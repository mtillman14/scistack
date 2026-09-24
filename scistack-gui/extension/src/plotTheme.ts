/**
 * PlotThemeHost — the owner of the Plot Studio's light/dark preference.
 *
 * The preference belongs to the USER, not to a figure or a database, so it is
 * kept in the extension's `globalState` and is shared by every plot tab of
 * every session. A webview cannot hold it: each tab is its own origin and
 * starts empty, so a choice kept there would be forgotten by the next tab.
 *
 * - A new tab reads it once, injected into its HTML (`initScript`).
 * - A toggle arrives as the `set_plot_theme` host-side method (plotPanel.ts,
 *   dagPanel.ts); `set` stores it and pushes `plot_theme_changed` to every
 *   OTHER open webview, so all figures switch together.
 *
 * One registry for all sessions, deliberately unlike `Session.plots`: a save
 * finishing in one database must not reach another database's tabs, but a
 * colour preference has nothing to do with which database a tab reads.
 *
 * The webview half is frontend/src/components/PlotStudio/usePlotTheme.ts.
 * Deliberately free of any `vscode` import (a `Memento` is only get/update),
 * so it runs under `node --test`.
 */

import { MessageSink, PanelRegistry } from './panelRegistry';

export type PlotThemeMode = 'dark' | 'light';

export const PLOT_THEME_STATE_KEY = 'scistack.plotTheme';
export const DEFAULT_PLOT_THEME: PlotThemeMode = 'dark';

/** The slice of `vscode.Memento` this needs. */
export interface ThemeMemento {
  get(key: string): unknown;
  update(key: string, value: unknown): PromiseLike<void>;
}

export function parsePlotThemeMode(value: unknown): PlotThemeMode | null {
  return value === 'dark' || value === 'light' ? value : null;
}

export class PlotThemeHost {
  private sinks = new PanelRegistry();

  constructor(private memento: ThemeMemento) {}

  /** The stored mode; anything unreadable is the default, not an error. */
  get mode(): PlotThemeMode {
    return parsePlotThemeMode(this.memento.get(PLOT_THEME_STATE_KEY)) ?? DEFAULT_PLOT_THEME;
  }

  /** Register a webview for changes. Call the returned function on dispose. */
  add(sink: MessageSink): () => void {
    return this.sinks.add(sink);
  }

  /**
   * Store `value` and tell every open webview but `origin` (which switched
   * itself already). Returns how many were told, for the caller's log line.
   * Throws on anything that is not a mode, so a malformed request is answered
   * with an error instead of storing garbage.
   */
  async set(value: unknown, origin?: MessageSink): Promise<{ mode: PlotThemeMode; notified: number }> {
    const mode = parsePlotThemeMode(value);
    if (!mode) throw new Error(`not a plot theme: ${JSON.stringify(value)}`);
    await this.memento.update(PLOT_THEME_STATE_KEY, mode);
    const msg = { method: 'plot_theme_changed', params: { mode } };
    let notified = 0;
    for (const sink of this.sinks.sinks()) {
      if (sink === origin) continue;
      try {
        sink.postMessage(msg);
        notified += 1;
      } catch {
        // A disposed webview unregisters itself; not worth failing the rest.
      }
    }
    return { mode, notified };
  }

  /**
   * The `<script>` body that hands a new webview the current mode. JSON, so
   * the value cannot break out of the tag whatever the store holds.
   */
  initScript(): string {
    return `window.__SCISTACK_PLOT_THEME__ = ${JSON.stringify(this.mode)};`;
  }
}

let shared: PlotThemeHost | undefined;

/** The one host for this extension, created on first use. */
export function plotThemeHost(memento: ThemeMemento): PlotThemeHost {
  shared ??= new PlotThemeHost(memento);
  return shared;
}

/**
 * Answer a webview's `set_plot_theme` request — the one handler both the plot
 * tabs and the canvas (whose fallback modal is also a Plot Studio) route to.
 */
export async function answerSetPlotTheme(
  host: PlotThemeHost,
  msg: Record<string, unknown>,
  panel: MessageSink,
  log: { appendLine(line: string): void },
): Promise<void> {
  try {
    const params = (msg.params ?? {}) as { mode?: unknown };
    const { mode, notified } = await host.set(params.mode, panel);
    log.appendLine(`plot theme: ${mode} (stored; ${notified} other webview(s) told)`);
    panel.postMessage({ id: msg.id, result: { mode } });
  } catch (err) {
    log.appendLine(`plot theme: set failed — ${err}`);
    panel.postMessage({ id: msg.id, error: { message: String(err) } });
  }
}
