/**
 * Which of a resolve's `data_warnings` to show as a popup.
 *
 * The backend owns WHAT is wrong (`scistackplot.cell.all_nan_warning`); this
 * owns only "have we already shown it". Every control change re-resolves the
 * plot, so without this an all-NaN variable would raise the same popup on
 * every click. A message is shown once per panel; a different message (other
 * variable, other row count) is new.
 */
export function unseenWarnings(shown: Set<string>, incoming: readonly string[] | undefined): string[] {
  const fresh: string[] = []
  for (const message of incoming ?? []) {
    if (shown.has(message)) continue
    shown.add(message)
    fresh.push(message)
  }
  return fresh
}
