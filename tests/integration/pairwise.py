"""
All-pairs case generation for the Plot Studio's panes.

The panes are axes — kind, grouping, colour, variant rows, show sample,
location filter, filters, summary, y axis, layout — and the cross product is
millions of specs. Pairwise coverage guarantees every PAIR of axis values
appears together in at least one case, which is where the bugs live
(colour x variant, sample x pooled, location x filter) at a few dozen cases.

Each case is built as the JSON dict the webview sends, not a ``PlotSpec``:
the dict shape is its own class of breakage. No new dependency — the
generator below is a small greedy all-pairs builder, deterministic in the
axis order.
"""

from __future__ import annotations

import itertools
from typing import Any

# --- the generator -----------------------------------------------------------------


def all_pairs(axes: dict[str, list[Any]]) -> list[dict[str, Any]]:
    """Cases covering every pair of values across ``axes`` at least once.

    Greedy: each new case picks, axis by axis, the value that covers the
    most still-uncovered pairs with what the case already holds. Deterministic
    for a given axis order (a dict, so insertion order). The count is close
    to the product of the two largest axes — 6 x 5 axes give ~35 cases.
    """
    names = list(axes)
    uncovered: set[tuple[tuple[str, Any], tuple[str, Any]]] = set()
    for a, b in itertools.combinations(names, 2):
        for va in axes[a]:
            for vb in axes[b]:
                uncovered.add(((a, va), (b, vb)))

    def pairs_of(case: dict[str, Any]) -> set:
        items = [(n, case[n]) for n in names if n in case]
        return {(x, y) for x, y in itertools.combinations(items, 2)}

    cases: list[dict[str, Any]] = []
    while uncovered:
        # Seed with an uncovered pair so every iteration makes progress.
        (a, va), (b, vb) = sorted(uncovered, key=repr)[0]
        case: dict[str, Any] = {a: va, b: vb}
        for n in names:
            if n in case:
                continue
            best = max(
                axes[n],
                key=lambda v: (
                    sum(1 for pair in pairs_of({**case, n: v}) if pair in uncovered),
                    -axes[n].index(v),
                ),
            )
            case[n] = best
        uncovered -= pairs_of(case)
        cases.append({n: case[n] for n in names})
    return cases


# --- the axes, as the panel offers them --------------------------------------------

SCALAR_AXES: dict[str, list[str]] = {
    "kind": ["bar", "box", "violin", "scatter", "strip", "spaghetti"],
    "grouping": ["session", "session+speed", "session+group", "session+phase"],
    "color": ["none", "inner", "outer", "variant"],
    "variants": ["none", "one_row", "two_variables"],
    "show_sample": ["off", "sample", "deeper", "deeper_joined"],
    "location": ["none", "prefix", "prefix_exclude"],
    "filters": ["none", "level", "range"],
    "summary": ["mean_sd", "median_iqr", "pooled", "none"],
    "y_axis": ["auto", "scoped", "manual", "log"],
    "layout": ["flow", "n_cols", "matchers"],
}

SERIES_AXES: dict[str, list[str]] = {
    "kind": ["line", "band", "box", "bar"],
    "grouping": ["speed", "speed+session", "speed+group"],
    "color": ["none", "inner", "outer"],
    "cell_statistic": ["mean", "median"],
    "location": ["none", "prefix"],
    "filters": ["none", "level", "range"],
    "summary": ["mean_sd", "median_iqr", "pooled", "none"],
    "y_axis": ["auto", "scoped", "log"],
    "layout": ["flow", "n_cols"],
}


def scalar_spec(case: dict[str, str], *, field: str, subjects: list[str], sessions: list[str]) -> dict:
    """The JSON spec for one scalar case over the example's ``CycleSymmetry``."""
    roles = {
        field: "facet",
        "subject": "collapse",
        "session": "group",
        "speed": "iterate",
        "trial": "collapse",
        "cycle": "collapse",
    }
    groups = ["session"]
    factor_variables: list[dict] = []
    level_groups: list[dict] = []
    if case["grouping"] == "session+speed":
        roles["speed"] = "group"
        groups = ["speed", "session"]  # innermost first: speeds inside sessions
    elif case["grouping"] == "session+group":
        factor_variables.append({"variable": "Demographics", "column": "group"})
        roles["group"] = "group"
        groups = ["session", "group"]
    elif case["grouping"] == "session+phase":
        level_groups.append({
            "name": "Phase",
            "source": "session",
            "mapping": {sessions[0]: "early", **{s: "late" for s in sessions[1:]}},
            "unmatched": None,
        })
        roles["Phase"] = "group"
        groups = ["session", "Phase"]

    variant_sets: list[dict] = []
    if case["variants"] == "one_row":
        variant_sets = [{"name": "all", "selection": {}}]
    elif case["variants"] == "two_variables":
        variant_sets = [
            {"name": "symmetry", "selection": {}},
            {"name": "deviation", "selection": {}, "variable": "CycleDeviation"},
        ]

    color = None
    if case["color"] == "inner":
        color = groups[0]
    elif case["color"] == "outer":
        color = groups[-1]
    elif case["color"] == "variant" and len(variant_sets) > 1:
        roles["Variant"] = "group"
        groups = [*groups, "Variant"]
        color = "Variant"

    show_sample: list[str] = []
    join_sample = None
    if case["show_sample"] == "sample":
        show_sample = ["subject"]
    elif case["show_sample"] == "deeper":
        show_sample = ["trial"]
    elif case["show_sample"] == "deeper_joined":
        show_sample = ["trial"]
        join_sample = True

    location: dict = {"include": [], "exclude_levels": {}}
    if case["location"] in ("prefix", "prefix_exclude"):
        location["include"] = [[["subject", s]] for s in subjects]
    if case["location"] == "prefix_exclude":
        location["exclude_levels"] = {"speed": ["fast"]}

    filters: list[dict] = []
    if case["filters"] == "level":
        filters = [{"column": "session", "include": sessions[:1]}]
    elif case["filters"] == "range":
        filters = [{"column": "CycleSymmetry", "minimum": 20.0, "maximum": 180.0}]

    aggregate = {"statistic": "mean", "error": "sd", "pooled": False}
    if case["summary"] == "median_iqr":
        aggregate = {"statistic": "median", "error": "iqr", "pooled": False}
    elif case["summary"] == "pooled":
        aggregate["pooled"] = True
    elif case["summary"] == "none":
        aggregate["error"] = "none"

    y_axis: dict = {"scope": [], "minimum": None, "maximum": None}
    style: dict = {}
    if case["y_axis"] == "scoped":
        y_axis["scope"] = ["speed"]
    elif case["y_axis"] == "manual":
        y_axis.update({"minimum": 0.0, "maximum": 200.0})
    elif case["y_axis"] == "log":
        style["log_y"] = True

    facet: dict = {}
    if case["layout"] == "n_cols":
        facet = {"n_cols": 2}
    elif case["layout"] == "matchers":
        facet = {
            "rows": [{"op": "starts_with", "value": "a"}, {"op": "starts_with", "value": "k"}],
            "cols": [],
        }

    return {
        "measures": ["CycleSymmetry"],
        "roles": roles,
        "groups": groups,
        "color": color,
        "kind": case["kind"],
        "show_sample": show_sample,
        "join_sample": join_sample,
        "filters": filters,
        "location_filter": location,
        "factor_variables": factor_variables,
        "level_groups": level_groups,
        "variant_sets": variant_sets,
        "aggregate": aggregate,
        "y_axis": y_axis,
        "style": style,
        "facet": facet,
    }


def series_spec(case: dict[str, str], *, field: str, subjects: list[str], sessions: list[str]) -> dict:
    """The JSON spec for one 1-D case over ``CycleWaveform``."""
    roles = {
        field: "facet",
        "subject": "iterate",
        "session": "iterate",
        "speed": "group",
        "trial": "collapse",
        "cycle": "collapse",
    }
    groups = ["speed"]
    factor_variables: list[dict] = []
    if case["grouping"] == "speed+session":
        roles["session"] = "group"
        groups = ["speed", "session"]
    elif case["grouping"] == "speed+group":
        factor_variables.append({"variable": "Demographics", "column": "group"})
        roles["group"] = "group"
        roles["subject"] = "collapse"
        groups = ["speed", "group"]

    color = None
    if case["color"] == "inner":
        color = groups[0]
    elif case["color"] == "outer":
        color = groups[-1]

    location: dict = {"include": [], "exclude_levels": {}}
    if case["location"] == "prefix":
        location["include"] = [[["subject", subjects[0]]]]

    filters: list[dict] = []
    if case["filters"] == "level":
        filters = [{"column": "session", "include": sessions[:1]}]
    elif case["filters"] == "range":
        filters = [{"column": "CycleWaveform", "minimum": -20.0, "maximum": 20.0}]

    aggregate = {"statistic": "mean", "error": "sd", "pooled": False}
    if case["summary"] == "median_iqr":
        aggregate = {"statistic": "median", "error": "iqr", "pooled": False}
    elif case["summary"] == "pooled":
        aggregate["pooled"] = True
    elif case["summary"] == "none":
        aggregate["error"] = "none"

    y_axis: dict = {"scope": [], "minimum": None, "maximum": None}
    style: dict = {}
    if case["y_axis"] == "scoped":
        y_axis["scope"] = ["subject"]
    elif case["y_axis"] == "log":
        style["log_y"] = True

    facet: dict = {"n_cols": 3} if case["layout"] == "n_cols" else {}

    return {
        "measures": ["CycleWaveform"],
        "roles": roles,
        "groups": groups,
        "color": color,
        "kind": case["kind"],
        "cell_statistic": case["cell_statistic"],
        "filters": filters,
        "location_filter": location,
        "factor_variables": factor_variables,
        "aggregate": aggregate,
        "y_axis": y_axis,
        "style": style,
        "facet": facet,
    }


def case_id(case: dict[str, str]) -> str:
    return "-".join(f"{k}={v}" for k, v in case.items())
