/**
 * The Content-Security-Policy of every SciStack webview — ONE owner, so the
 * DAG tab and the plot tab cannot drift apart (they did: both carried a
 * hand-copied policy, and both lacked `blob:` below).
 *
 * `img-src blob:` is required by Plot Studio's "Copy image": Plotly.toImage
 * rasterises the figure by loading its SVG into an <img> from a `blob:` object
 * URL (a `data:` URL only on Safari). Without `blob:` the load is refused, the
 * <img> fires `error`, and Plotly rejects with that bare Event — no message, no
 * stack (scidb.log 2026-09-27: "render error in Plot Studio copy_png: (no
 * message)").
 */

/** The directives, in order, as `[name, sources]`. Exported for the tests. */
export function webviewCspDirectives(cspSource: string, nonce: string): [string, string][] {
  return [
    ['default-src', "'none'"],
    ['style-src', `${cspSource} 'unsafe-inline'`],
    ['script-src', `'nonce-${nonce}'`],
    ['img-src', `${cspSource} data: blob:`],
    ['font-src', cspSource],
  ];
}

/** The `content` of the CSP `<meta>` tag. */
export function webviewCsp(cspSource: string, nonce: string): string {
  return webviewCspDirectives(cspSource, nonce)
    .map(([name, sources]) => `${name} ${sources};`)
    .join(' ');
}
