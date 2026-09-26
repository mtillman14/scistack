/**
 * The Plot Studio needs box, violin and heatmap traces, which the *basic*
 * bundle (used by the sidebar's VariablePlot) does not carry. Same typing
 * situation as that one: react-plotly.js's factory() treats the Plotly
 * instance as `unknown`, so this only needs to satisfy the import — plus the
 * one call made on it directly, `toImage` (Copy PNG, `clipboardPng.ts`).
 */
declare module 'plotly.js-cartesian-dist-min' {
  const Plotly: {
    toImage(
      figure: { data: unknown[]; layout: Record<string, unknown> },
      options: { format: 'png' | 'svg' | 'jpeg' | 'webp'; width: number; height: number; scale?: number }
    ): Promise<string>
  }
  export default Plotly
}
