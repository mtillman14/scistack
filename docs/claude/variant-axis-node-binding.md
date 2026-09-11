# Which node supplies a variant axis — three names that look like one

> Written 2026-09-11, after a name collision between them produced two
> user-visible bugs at once. Companion to `plot-variant-rows.md` §5 and
> `variant-selection.md`.

A branch-param variant axis is `filterDelsys.config`. Somewhere on the pipeline
canvas is the node that supplies it. Connecting the two looks like string
matching, and for a long time it was. It is not.

## The three namespaces

Everything here turns on the fact that **a parameter has three different names**,
and they coincide only by accident.

| | example | where it comes from | changes when |
|---|---|---|---|
| the function's **argument** name | `config` | `def filterDelsys(data, config, Fs)` — the signature | the function is edited |
| the **Parameter entity**'s name | `delsys_config` | `scistack_entities.toml`, or whatever was declared | the user renames the entity |
| the **node id / label** on the canvas | `param__delsys_config` / `delsys_config` | `graph_builder.build_parameter_nodes` | follows the entity |

scidb namespaces a branch param as `{producing_fn}.{argument}`, so
`VariantAxis.param` is **always the first column**. A Parameter node is labelled
with **the second**. Matching them is comparing a function signature against an
entity name.

### Why they agreed for so long

`build_parameter_nodes` builds its node list from
`set(const_counts) | set(source_parameters)` — a union of two namespaces:

- `const_counts` keys come from recorded `__constants`, which is keyed by the
  **function's argument name**;
- `source_parameters` keys are **declared entity names**.

A project that never declared Parameters therefore has nodes labelled with
argument names, and name matching works perfectly. It breaks the first time
someone declares a Parameter with a name of their own, or wires a port from a
glue node.

## The rule: bind by PORT

An edge into a function carries

```
targetHandle = "param__" + <the function's own argument name>
```

on **both** paths that create one:

- DB-derived (`graph_builder.build_edges`): `f"{PARAM_ID_PREFIX}{const_name}"`,
  where `const_name` came from `__constants` — an argument name;
- manual (a user wiring a port on the canvas): the port they dropped onto, which
  is an argument name by construction.

That suffix is exactly `VariantAxis.param`. So:

> **The node feeding the port is the node that holds the axis** — whatever the
> node is called, and whatever type it is.

Implemented once, in `scistack_gui.services.plot_service.axis_node_bindings`,
returning `{axis column: node id}`.

### Why it lives in Python and not in the popup

It needs the canvas graph, so it cannot live in `scistackplot`. But it is still
a rule about what scidb's namespacing *means*, and the popup is TSX, where a
rule has no test — the frontend has no JS test runner, and the invariants that
are checked are checked by reading the source from pytest. Computing it in the
service means the regression test is an ordinary pytest test that wires a
deliberately mis-named node to a port and asserts the axis still binds.

The popup receives `node_bindings` on the variant-graph payload and reads it
back. It derives nothing.

### Code axes are not port-bound

`Code:<fn>` binds to a function node by **function name**. A function node is
labelled with the function's name — one namespace, no port involved, nothing to
get wrong. `axis_node_bindings` deliberately returns nothing for them.

## What the old rule broke

Measured on a real project, 2026-09-11. The user had deleted the Parameters
named `Fs` and `config` and rewired `filterDelsys`'s ports to
`delsys_sampling_frequency` and a glue node:

```
axis filterDelsys.config -> by port (filterDelsys, param__config): glue_config_filter [glueNode]
                          | by label 'config': NO NODE WITH THAT LABEL
axis filterDelsys.Fs     -> by port (filterDelsys, param__Fs): delsys_sampling_frequency [parameterNode]
                          | by label 'Fs':     NO NODE WITH THAT LABEL
```

Three symptoms, one cause, and none of them named it:

1. **Every parameter node dimmed** to "not a variant here" — the node's own
   lookup found no axis.
2. **Both axes appeared under "Not on this canvas (defined in a nested
   pipeline)"** — the fallback list assumed nesting was the only way an axis
   could fail to find a node, so it asserted a cause that had nothing to do with
   it. The heading is now "No node on this canvas".
3. **The glue node was unreachable as an axis at all** — only `ParameterNode`
   was ever consulted, so an axis fed through glue had nowhere to live even when
   the names did line up.

The third is the one that shows the rule was wrong rather than merely fragile:
even with perfect names, binding "a parameter axis" to "a Parameter node" cannot
express a port fed by anything else.

## Diagnosing it again

`axis_node_bindings` logs its whole mapping at INFO on every dialog open, naming
the node and its type per axis, and listing any axis that feeds no port:

```
[plot] axis bindings: {'filterDelsys.config': 'glue_config_filter [glueNode]'}
    — 1 axis/axes feed no port on this canvas: ['somewhere.cutoff']
```

An axis in that second list is either genuinely inside a nested pipeline, or a
port that nothing is wired to. Check the canvas before assuming the former.

## Related traps in the same family

- **`scifor`/`scidb` inputs come from edges, not name matching** — the same
  lesson, learned earlier and elsewhere. Binding by name is convenient and
  silently wrong the moment two namespaces drift.
- **`branch_params_batch` returns every upstream constant**, varying or not, so
  a constant used to become a one-level "variant" factor. Dropped in
  `attach_variants`; see `plot-variant-rows.md` §3 and `synthetic-factors.md`.
- **Windows-separator paths in `scistack.toml`** resolve to a literal
  backslash-named file on POSIX — another case of two things that look like one
  name.
