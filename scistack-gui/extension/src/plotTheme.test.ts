/**
 * Tests for PlotThemeHost — run with `npm test` in extension/.
 *
 * What they lock down: the light/dark choice is stored once for the user, a
 * new tab is handed it, and a toggle in one tab reaches every other open tab
 * (of any database) but not the tab that made it.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { MessageSink } from './panelRegistry';
import {
  DEFAULT_PLOT_THEME,
  PLOT_THEME_STATE_KEY,
  PlotThemeHost,
  answerSetPlotTheme,
} from './plotTheme';

class FakeMemento {
  values = new Map<string, unknown>();
  get(key: string): unknown {
    return this.values.get(key);
  }
  update(key: string, value: unknown): Promise<void> {
    this.values.set(key, value);
    return Promise.resolve();
  }
}

class FakePanel implements MessageSink {
  received: Record<string, unknown>[] = [];
  postMessage(msg: Record<string, unknown>): void {
    this.received.push(msg);
  }
}

const silentLog = { lines: [] as string[], appendLine(line: string) { this.lines.push(line); } };

test('an unset or unreadable store reads as the default', () => {
  const memento = new FakeMemento();
  const host = new PlotThemeHost(memento);
  assert.equal(host.mode, DEFAULT_PLOT_THEME);
  memento.values.set(PLOT_THEME_STATE_KEY, 'sepia');
  assert.equal(host.mode, DEFAULT_PLOT_THEME);
});

test('a toggle is stored and reaches every other tab, not its origin', async () => {
  const memento = new FakeMemento();
  const host = new PlotThemeHost(memento);
  const origin = new FakePanel();
  const other = new FakePanel();
  const canvas = new FakePanel();
  host.add(origin);
  host.add(other);
  host.add(canvas);

  const result = await host.set('light', origin);

  assert.deepEqual(result, { mode: 'light', notified: 2 });
  assert.equal(memento.get(PLOT_THEME_STATE_KEY), 'light');
  assert.equal(host.mode, 'light');
  assert.deepEqual(origin.received, []);
  const msg = { method: 'plot_theme_changed', params: { mode: 'light' } };
  assert.deepEqual(other.received, [msg]);
  assert.deepEqual(canvas.received, [msg]);
});

test('a closed tab is no longer told', async () => {
  const host = new PlotThemeHost(new FakeMemento());
  const closed = new FakePanel();
  const unregister = host.add(closed);
  unregister();
  assert.equal((await host.set('light')).notified, 0);
  assert.deepEqual(closed.received, []);
});

test('a malformed mode is refused and stores nothing', async () => {
  const memento = new FakeMemento();
  const host = new PlotThemeHost(memento);
  await assert.rejects(() => host.set('Light'));
  assert.equal(memento.values.size, 0);
});

test('a new tab is handed the stored mode as JSON', async () => {
  const host = new PlotThemeHost(new FakeMemento());
  assert.equal(host.initScript(), 'window.__SCISTACK_PLOT_THEME__ = "dark";');
  await host.set('light');
  assert.equal(host.initScript(), 'window.__SCISTACK_PLOT_THEME__ = "light";');
});

test('the webview request is answered with its id, errors included', async () => {
  const host = new PlotThemeHost(new FakeMemento());
  const panel = new FakePanel();

  await answerSetPlotTheme(host, { id: 7, method: 'set_plot_theme', params: { mode: 'light' } }, panel, silentLog);
  assert.deepEqual(panel.received, [{ id: 7, result: { mode: 'light' } }]);

  await answerSetPlotTheme(host, { id: 8, method: 'set_plot_theme', params: {} }, panel, silentLog);
  assert.equal(panel.received[1].id, 8);
  assert.ok('error' in panel.received[1]);
  assert.equal(host.mode, 'light');
});
