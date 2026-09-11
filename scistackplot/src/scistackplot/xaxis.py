"""
Composing a nested categorical x axis.

"Stim and sham side by side, each split by session" is one axis carrying two
factors. This module turns the observed combinations of those factors into a
flat sequence of **leaf positions** plus the **spans** the higher layers cover,
and nothing else — no frames, no colours, no rendering.

That purity is the point, and it is the same bargain
:func:`~scistackplot.reduce.plan_layout` makes for the facet grid: the two
renderers draw brackets from the spans rather than each deriving them, ``codegen``
emits the resolved order rather than replaying the rules, and the whole
arrangement is testable from label lists.

Separation between groups is **spacer categories** — unique labels with no data.
That is a deliberate choice over numeric offsets: it keeps the axis categorical,
so box, violin, bar and strip all position themselves exactly as they already do
in both backends. Numeric positions would give finer control over gap widths at
the cost of re-implementing every trace type's placement.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

#: Joins a leaf's layer values into the key the frame is matched on. Chosen to
#: be unlikely in real level names; a level containing it still works, since the
#: key is only ever built and compared by this module, never parsed.
LEAF_SEPARATOR = "␟"

#: Prefix marking a spacer category. Spacers hold no data and draw no tick.
SPACER_PREFIX = " gap"


@dataclass(frozen=True)
class XGroup:
    """One higher-layer label and the leaf positions it covers.

    ``start``/``end`` are inclusive indices into :attr:`XPlan.order`, which is
    what lets a renderer centre a label over its group and draw a bracket
    beneath it without knowing anything about the data.
    """

    label: str
    #: 0 is the outermost layer. The leaf layer never appears here — it is the
    #: tick labels.
    depth: int
    start: int
    end: int

    @property
    def centre(self) -> float:
        return (self.start + self.end) / 2.0


@dataclass(frozen=True)
class XPlan:
    """A composed nested axis."""

    #: Leaf keys in drawn order, spacers included. Positions are indices here.
    order: list[str] = field(default_factory=list)
    #: Tick label per entry; "" for a spacer.
    tick_labels: list[str] = field(default_factory=list)
    #: Spans of every layer above the leaf, outermost first.
    groups: list[XGroup] = field(default_factory=list)
    #: How many factors share the axis.
    n_layers: int = 1

    @property
    def depth(self) -> int:
        """Label rows below the axis, excluding the tick labels themselves."""
        return max((group.depth for group in self.groups), default=-1) + 1

    def position_of(self, key: str) -> int | None:
        try:
            return self.order.index(key)
        except ValueError:
            return None


def leaf_key(values: Sequence[Any]) -> str:
    """The key identifying one leaf position, from its layers' values."""
    return LEAF_SEPARATOR.join(str(value) for value in values)


def is_spacer(key: str) -> bool:
    return str(key).startswith(SPACER_PREFIX)


def plan_x_axis(
    combinations: Sequence[Sequence[Any]],
    layer_orders: Sequence[Sequence[Any]],
) -> XPlan:
    """Lay out the leaves for the observed ``combinations``.

    ``combinations`` are the layer-value tuples that actually occur in the
    data — **not** the Cartesian product of the layers. Real designs are ragged
    (a sham subject with no post session) and reserving a position for a
    combination nobody ran leaves a hole in the axis that reads as missing data.

    ``layer_orders`` gives each layer's declared level order, so the axis obeys
    the same ordering rule as everything else here: zero-padded IDs sort
    ``01, 02, … 10``, and a legend does not reshuffle when a filter removes a
    level's last row.

    Gaps scale with the boundary's depth — a change of outer group opens a wider
    gap than a change of the layer just above the leaf — which is what makes
    three levels of nesting readable rather than a uniform picket fence.
    """
    layer_orders = [list(order) for order in layer_orders]
    n_layers = max(1, len(layer_orders))
    combos = [tuple(combo) for combo in combinations]
    if not combos:
        return XPlan(n_layers=n_layers)

    ranks = [
        {str(level): position for position, level in enumerate(order)}
        for order in layer_orders
    ]

    def sort_key(combo: tuple) -> tuple:
        key: list = []
        for depth, value in enumerate(combo):
            lookup = ranks[depth] if depth < len(ranks) else {}
            # Levels the declared order never mentioned sort after the ones it
            # did, in a stable way — never dropped.
            key.extend(
                (0, lookup[str(value)])
                if str(value) in lookup
                else (1, str(value))
            )
        return tuple(key)

    ordered = sorted(dict.fromkeys(combos), key=sort_key)

    order: list[str] = []
    tick_labels: list[str] = []
    # Where each group currently being accumulated started, keyed by depth.
    open_groups: dict[int, tuple[str, int]] = {}
    groups: list[XGroup] = []
    spacer_count = 0
    previous: tuple | None = None

    for combo in ordered:
        prefix = combo[:-1]
        if previous is not None:
            changed = [
                depth
                for depth in range(len(prefix))
                if str(prefix[depth]) != str(previous[depth])
            ]
            if changed:
                # Close every group at or below the shallowest change, then open
                # fresh ones. One spacer per closed layer: an outer boundary
                # closes more layers, so its gap is wider.
                shallowest = min(changed)
                for depth in sorted(open_groups, reverse=True):
                    if depth >= shallowest:
                        label, start = open_groups.pop(depth)
                        groups.append(
                            XGroup(label, depth, start, len(order) - 1)
                        )
                for _ in range(len(prefix) - shallowest):
                    order.append(f"{SPACER_PREFIX}{spacer_count}")
                    tick_labels.append("")
                    spacer_count += 1

        for depth, value in enumerate(prefix):
            if depth not in open_groups:
                open_groups[depth] = (str(value), len(order))

        order.append(leaf_key(combo))
        tick_labels.append(str(combo[-1]))
        previous = combo

    for depth, (label, start) in open_groups.items():
        groups.append(XGroup(label, depth, start, len(order) - 1))

    groups.sort(key=lambda group: (group.depth, group.start))
    return XPlan(
        order=order, tick_labels=tick_labels, groups=groups, n_layers=n_layers
    )
