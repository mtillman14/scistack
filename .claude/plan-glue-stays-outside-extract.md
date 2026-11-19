# Plan: glue nodes stay outside an extracted submodule unless they sit inside it

Date: 2026-11-19. User decisions:
- A glue node is a situation-specific adapter, so it stays on the parent unless it
  is part of the submodule's own graph.
- An input port fed by a glue is named after the argument it feeds.
- An explicitly selected glue the rule keeps outside stays outside (no override).
- Ship with the placement-check fix (ports._same_source_ref) as one release.

## Rule (one owner: `domain.ports.split_selection`)

A selected glue node moves only when everything that feeds it and everything it
feeds move. Otherwise it stays on the parent canvas with the end that stays
outside. The rule repeats until nothing changes, so a whole glue chain off an
outside source stays out.

| Selection | Glue |
|---|---|
| PathInput -> glue -> fn -> var | out (the port sits between the glue and fn) |
| fn -> var -> glue -> fn -> var | in |
| source var / Parameter -> glue -> fn | out |
| fn -> var -> glue -> fn, last fn not selected | out |
| glue fed or feeding nothing selected | out |

The order is: sources/sinks (entities) first, then glue on what moves.
`SelectionSplit.glue` lists the glue kept outside.

## Port name

`ports.plan_boundary`: an input port whose source is a glue node is named after
the argument it feeds (the `in__` handle). Otherwise the naming is unchanged.

## Logging

- The `kept outside as sources … / sinks … / glue …` line in `scope_service`.
- The refusal message when nothing moves also names glue adapters.

## Tests (`tests/test_submodule_ports.py`)

- The split rule: the table above, with a chain.
- The load_filter_emg shape: the glue stays on main, the port is `config`, the
  connections are unchanged, and `glue -> fn.config` is in the wiring list.
- The internal glue moves.

## Not changed

Existing submodules (no migration). Move a glue out by hand.
