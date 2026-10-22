"""Recorded wirings: what history says each pipeline STEP is, as one shape.

Stage 1 of ``.claude/plan-canvas-as-document.md``. The wiring RECIPE has
lived here since 2026-09-20 (``provenance.compute_wiring_id``,
``provenance.split_call_site_outputs``, ``database.call_site_wiring_ids``);
what the GUI still reconstructed on its own was the step SHAPE around it: which
call sites form the step, which constant combinations it ran, and whether those
combinations are a full Cartesian product. Canvas adoption
(``scistack_gui/services/adoption.py``) needs exactly that to turn a recorded
step into stored canvas rows, so it is owned here, below every caller, and the
GUI imports it.

Pure: no database. Input is the output of
:func:`scidb.database.aggregate_pipeline_variants` (or
``DatabaseManager.get_aggregated_variants()``).

**Non-Cartesian combos** (plan risk 2). A step whose constants are
``cutoff ∈ {6, 10}`` and ``order ∈ {2, 4}`` but which only ever ran
``(6, 2)`` and ``(10, 4)`` is NOT the product of its values. A canvas that
states the values per Parameter would widen it to four combinations;
:meth:`RecordedWiring.missing_combinations` names the two that never ran so
the caller can hide them (user decision Q2a, 2026-10-21).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import product
from typing import Any

from .provenance import (
    constant_value_repr,
    constants_identity_key,
    split_call_site_outputs,
)


@dataclass(frozen=True)
class RecordedWiring:
    """One recorded pipeline step: a function with one wiring.

    ``constant_rows`` are the distinct constant maps its call sites ran with
    (``{argument: typed value}``), in a stable order. ``parameter_names`` maps
    an argument to the declared Parameter that fed it, where the run recorded
    one.
    """

    fn_name: str
    wiring_id: str
    call_ids: tuple
    input_params: dict
    outputs: tuple
    path_inputs: dict
    glue_chains: dict = field(default_factory=dict)
    parameter_names: dict = field(default_factory=dict)
    constant_rows: tuple = ()

    @property
    def constant_axes(self) -> dict[str, list]:
        """``{argument: [distinct values]}`` over every row, first-seen order."""
        axes: dict[str, list] = {}
        seen: dict[str, set] = {}
        for row in self.constant_rows:
            for arg, value in row.items():
                key = constant_value_repr(value)
                if key in seen.setdefault(arg, set()):
                    continue
                seen[arg].add(key)
                axes.setdefault(arg, []).append(value)
        return axes

    def missing_combinations(self) -> list[dict]:
        """Combinations of :attr:`constant_axes` that no recorded row ran.

        Empty when the rows are the full Cartesian product (the common case:
        one Parameter, or a ``for_each`` over two EachOfs). Rows that do not
        all name the same arguments (a Parameter added to the signature after
        some runs) are compared on the arguments each row has: a row
        ``{cutoff: 6}`` covers every combination with ``cutoff=6``.
        """
        axes = self.constant_axes
        if len(axes) < 2:
            return []
        names = sorted(axes)
        ran = [
            {a: constant_value_repr(v) for a, v in row.items()}
            for row in self.constant_rows
        ]
        missing: list[dict] = []
        for combo in product(*(axes[n] for n in names)):
            want = {n: constant_value_repr(v) for n, v in zip(names, combo, strict=True)}
            covered = any(
                all(want[a] == r for a, r in row.items() if a in want) for row in ran
            )
            if not covered:
                missing.append(dict(zip(names, combo, strict=True)))
        return missing

    @property
    def is_cartesian(self) -> bool:
        return not self.missing_combinations()


def recorded_wirings(
    aggregate: dict,
    claimed_wirings=(),
    path_input_names: "dict | None" = None,
) -> list[RecordedWiring]:
    """Every recorded step in *aggregate*, one :class:`RecordedWiring` per
    ``(function, wiring_id)``, sorted by function then wiring.

    ``claimed_wirings`` are the wirings GUI runs claimed at dispatch (the
    GUI's ``_node_wiring``); they decide how a call site holding several output
    types splits into wirings (:func:`provenance.split_call_site_outputs`, the
    one owner). Without them a call site is one wiring, the union of its
    outputs, which is what a script-only history gets.

    ``path_input_names`` is ``{fkey: {argument: declared PathInput name}}``.
    Omitted, it is :func:`database.call_site_path_input_names` (recorded name,
    else the spec), which is what the CLI uses; the GUI passes its own
    registry-resolved names so the ids equal its canvas's.
    """
    from .database import call_site_path_input_names

    functions = aggregate.get("functions") or {}
    if path_input_names is None:
        path_input_names = call_site_path_input_names(aggregate.get("path_inputs") or {})

    grouped: dict[tuple[str, str], dict] = {}
    for fkey, data in functions.items():
        fkey = tuple(fkey)
        fn_name, call_id = fkey
        pis = dict(path_input_names.get(fkey) or {})
        splits = split_call_site_outputs(
            fn_name,
            data.get("input_params") or {},
            data.get("outputs") or [],
            pis,
            claimed_wirings,
        )
        for wid, subset in splits:
            entry = grouped.setdefault(
                (fn_name, wid),
                {
                    "call_ids": [],
                    "input_params": dict(data.get("input_params") or {}),
                    "outputs": set(),
                    "path_inputs": pis,
                    "glue_chains": {},
                    "parameter_names": {},
                    "rows": [],
                    "row_keys": set(),
                },
            )
            if call_id not in entry["call_ids"]:
                entry["call_ids"].append(call_id)
            entry["outputs"].update(subset)
            for arg, names in (data.get("glue_chains") or {}).items():
                entry["glue_chains"].setdefault(arg, list(names))
            for arg, name in (data.get("parameter_names") or {}).items():
                entry["parameter_names"].setdefault(arg, name)
            for variant in data.get("variants") or []:
                if variant.get("output_type") not in subset:
                    continue
                row = dict(variant.get("constants") or {})
                key = constants_identity_key(row)
                if key in entry["row_keys"]:
                    continue
                entry["row_keys"].add(key)
                entry["rows"].append(row)

    out = []
    for (fn_name, wid), e in sorted(grouped.items()):
        out.append(
            RecordedWiring(
                fn_name=fn_name,
                wiring_id=wid,
                call_ids=tuple(sorted(e["call_ids"])),
                input_params=e["input_params"],
                outputs=tuple(sorted(e["outputs"])),
                path_inputs=e["path_inputs"],
                glue_chains=e["glue_chains"],
                parameter_names=e["parameter_names"],
                constant_rows=tuple(
                    sorted(e["rows"], key=constants_identity_key)
                ),
            )
        )
    return out


def non_cartesian_report(wirings: "list[RecordedWiring]") -> list[dict[str, Any]]:
    """``[{fn_name, wiring_id, axes, rows, missing}]`` for every wiring whose
    recorded combinations are not the full product of its values: the
    measurement plan Stage 1 asks for, printable as is."""
    report = []
    for w in wirings:
        missing = w.missing_combinations()
        if not missing:
            continue
        report.append(
            {
                "fn_name": w.fn_name,
                "wiring_id": w.wiring_id,
                "axes": {a: [constant_value_repr(v) for v in vs] for a, vs in w.constant_axes.items()},
                "rows": len(w.constant_rows),
                "missing": [{a: constant_value_repr(v) for a, v in m.items()} for m in missing],
            }
        )
    return report
