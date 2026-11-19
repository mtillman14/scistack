# Plan: running a submodule — a node hidden inside it cuts the parent's lines

## Symptom (2026-11-19, scistack-debug.log 12:38)
Run on the `load_filter_xsens` placement (pipe_34f820d0ab43, MATLAB) ->
`ValueError: generate_matlab_pipeline_command: no runnable MATLAB step`.
Both steps dropped by `derive_target_for_node`:
- loadXSENSOneFile: "disconnected: input port(s) ['xsensFilePath'] have a hidden edge…"
- filterXSENS: "no output edge drawn"

Main canvas (spec/images/AFL DAG.png) is fully wired: xsensPath_6MWT -> placement, placement -> FilteredXSENS.

## Root cause
`domain/edge_view.py` `effective_edges`, the hidden-node edge filter:
`end in hidden_nodes or strip_placement(end) in hidden_nodes`.
The submodule canvas hides bare `pathInput__xsensPath_6MWT`, `var__FilteredXSENS`,
`param__xsens_sampling_frequency` (inner copies left after extract). Viewed from the
submodule scope, main's stored segments `pathInput__xsensPath_6MWT::main -> use_…` and
`use_… -> var__FilteredXSENS::main` match via strip_placement and are dropped, so the
port trace has no outer side. A hide on one canvas leaks onto another canvas's edges.

## Fix
- Fix the hide rule: in `effective_edges` (the one owner), a scope's hidden node cuts an
  edge end only when the end lives on that scope. A `::P` suffix with P != the asking
  scope is never cut. A bare end keeps the current match.
- Logging: DEBUG for edges dropped by a hidden node (node + scope), and for edges spared
  because they live on another canvas.
- Clearer error: the "no runnable MATLAB step" error (matlab_command_format.py:1882) and
  the Python "no targets" path list each step's skip reason. Drop "likely a SciStack
  bug" wording for wiring causes.
- Check order: in `derive_target_for_node`, check `open_arguments` before
  `document_disconnected_ports`, so the port reason is the one shown.

## Tests (scistack-gui/tests/test_submodule_ports.py)
- Inner hidden copy does not cut the outer segment; derive_target_for_node finds a target for input and output side.
- Extract two steps (source/sink stay outside, hidden inside): generate_matlab_pipeline_command(sub) contains both steps; Python run_pipeline(sub) produces the output.
- All-steps-dropped error names each step and its reason.

## Separate follow-up
Stale outer edges `runConfig -> use_… in__glue_delsysEMG_config` dangle since a7e97fee
renamed glue-fed ports to the argument name. EMG submodules will fail next. Decide:
move the stale lines to the new port name, or show them as missing-port lines for the user to redraw.
