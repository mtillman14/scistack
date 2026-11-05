# No portless edges into function nodes

## Problem
A function with no input ports draws a generic target handle. An edge dropped on
it is saved with `target_handle=None`. `edge_resolver` ignores that edge on every
read, so the function runs with no inputs even if the argument is added later.

Seen on 2026-11-05: `combine_groups` failed with "Not enough input arguments"
because `manual__owci0m` was drawn while the function had 0 parameters.

## Changes
1. **Canvas:** remove the generic target handle from both function node renderers
   (`FunctionNode.tsx:494`, `FunctionNode.tsx:813`). No ports means no input
   handles. Keep the "+ argument" drop target and the generic output handle.
2. **`put_edge`:** refuse an edge into a function node with no `in__<param>`
   handle and name the reason. This covers API callers. The comment in
   `layout_service.put_edge` saying the canvas always sends a port becomes true.

Already stored portless edges are left alone (user decision 2026-11-05: converting
or hiding them would be migration code). They stay ignored until redrawn.

## Check before changing
- Whether any legitimate caller stores a portless edge into a function, such as
  submodule ports, glue or history edges.
- Whether the variant-graph node (`FunctionNode.tsx:813`) relies on the generic
  handle for its edges.
- How quickly a new port appears after a `.m` edit. On 2026-11-05 the file
  re-parsed with 1 parameter at 14:56:20 and the port was first logged at
  14:57:54. With the generic handle gone, a slow refresh leaves nothing to connect
  to. Add logging if the cause isn't clear.

## Tests
- `put_edge` refuses a portless edge into a function and still accepts
  function-to-variable output edges.
- A function node draws no target handle except its ports.

## Status
Built 2026-11-05, both bundles rebuilt. Waiting on the user running pytest and the
GUI check in docs/gui-manual-testing-todo.md.
