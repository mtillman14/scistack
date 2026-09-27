"""
Difference bars, Stage 2 (2026-09-27): the placement math. ``diffbars``
stacks bars over everything drawn (plan D5) and finds the axis top by fixed
point. Every placement here is checked by :func:`violations`, which redraws
the result in POINTS and asserts nothing overlaps — rather than pinning
numbers, so a later refinement of the stacking cannot pass by accident.
"""

from __future__ import annotations

import logging
import math

import pandas as pd
import pytest

import scistackplot.reduce as reduce_mod
from scistackplot import LongTable, PlotKind, PlotSpec, Role, StyleOptions, YAxis, resolve
from scistackplot.diffbars import (
    BarSizes,
    DifferenceBar,
    Endpoint,
    GroupPlacement,
    Obstacles,
    PanelFrame,
    PanelProblem,
    ResolvedBar,
    _place_once,
    panel_obstacles,
    place_figure,
    place_group,
    placement_groups,
)

LAYER = "scistackplot"
EPS = 1e-6

#: A 200 x 300 pt panel over x = -0.5 .. 4.5 (five ticks).
FRAME = PanelFrame(height_pt=200.0, width_pt=300.0, x_range=(-0.5, 4.5))
SIZES = BarSizes(label_font_pt=10.0, label_boxes={"*": (6.0, 8.0), "p = 0.001": (48.0, 12.0)})


@pytest.fixture(autouse=True)
def _clear_plan_cache():
    reduce_mod.clear_plan_cache()
    yield
    reduce_mod.clear_plan_cache()


# --- helpers ------------------------------------------------------------------


def _end(position: float) -> Endpoint:
    return Endpoint(slot=int(position), position=float(position), values={"x": str(position)}, x_text=str(position))


def _pair(a: float, b: float, label: str = "*") -> ResolvedBar:
    bar = DifferenceBar(match={}, a={"x": str(a)}, b={"x": str(b)}, label=label)
    left, right = sorted((a, b))
    return ResolvedBar(bar, _end(left), _end(right))


def _boxes(tops: dict[float, float], half: float = 0.4) -> Obstacles:
    """Bar-like marks: one flat box per tick."""
    return Obstacles.build([(x - half, x + half, top, top, 0.0) for x, top in tops.items()])


def _problem(bars, obstacles, frame: PanelFrame = FRAME) -> PanelProblem:
    return PanelProblem(bars=list(bars), obstacles=obstacles, frame=frame)


def violations(
    result: GroupPlacement,
    problems: list[PanelProblem],
    sizes: BarSizes = SIZES,
    *,
    log: bool = False,
) -> list[str]:
    """Everything wrong with a placement, measured in points on the final axis."""
    axis = (lambda v: math.log10(v)) if log else (lambda v: v)
    bottom, top = axis(result.limits[0]), axis(result.limits[1])
    found: list[str] = []
    half_line = sizes.line_pt / 2.0
    for problem, placed in zip(problems, result.placed):
        frame = problem.frame
        per_y = frame.height_pt / (top - bottom)
        per_x = frame.per_unit_x
        obstacles = problem.obstacles.to_axis(log)

        def pt_y(value: float) -> float:
            return (axis(value) - bottom) * per_y

        def under(a: float, b: float) -> float | None:
            """Highest obstacle over [a, b] in pt (reach included)."""
            found_top = obstacles.top_over(a, b, per_unit_y=per_y, per_unit_x=per_x)
            return None if found_top is None else (found_top - bottom) * per_y

        rects = []  # (bar index, name, x0, x1, y0, y1) in pt
        for index, bar in enumerate(placed):
            y = pt_y(bar.y)
            width, _ = sizes.label_box(bar.bar.label)
            label = (
                bar.middle * per_x - width / 2,
                bar.middle * per_x + width / 2,
                pt_y(bar.label_y),
                pt_y(bar.label_top),
            )
            line = (bar.left * per_x, bar.right * per_x, y - half_line, y + half_line)
            legs = [
                (bar.left * per_x, bar.left * per_x, pt_y(bar.left_foot), y),
                (bar.right * per_x, bar.right * per_x, pt_y(bar.right_foot), y),
            ]
            if label[3] > frame.height_pt + EPS:
                found.append(f"bar {index}: label top {label[3]:.2f} above the axis")
            if label[2] < line[3] - EPS:
                found.append(f"bar {index}: label overlaps its own line")
            # Clearance over the marks, by the gap.
            span_top = under(bar.left, bar.right)
            if span_top is not None and line[2] < span_top + sizes.gap_pt - EPS:
                found.append(
                    f"bar {index}: line {line[2]:.2f} pt within the gap of a mark at {span_top:.2f}"
                )
            label_under = under(label[0] / per_x, label[1] / per_x)
            if label_under is not None and label[2] < label_under - EPS:
                found.append(f"bar {index}: label sits on a mark")
            for x_pt, foot, _ in ((leg[0], leg[2], leg[3]) for leg in legs):
                mark = under(x_pt / per_x, x_pt / per_x)
                if mark is not None and foot < mark + sizes.gap_pt - EPS:
                    found.append(f"bar {index}: leg at {x_pt / per_x:g} reaches into a mark")
                if foot > y + EPS:
                    found.append(f"bar {index}: leg foot above its line")
            rects.append((index, "line", *line))
            rects.append((index, "label", *label))
            rects.extend((index, "leg", *leg) for leg in legs)

        for i, first in enumerate(rects):
            for second in rects[i + 1 :]:
                if first[0] == second[0]:
                    continue
                if _intersect(first[2:], second[2:]):
                    found.append(
                        f"bar {first[0]} {first[1]} overlaps bar {second[0]} {second[1]}"
                    )
    return found


def _intersect(a, b) -> bool:
    """Closed rectangles (x0, x1, y0, y1), with a hair of tolerance."""
    return (
        a[0] <= b[1] + EPS
        and b[0] <= a[1] + EPS
        and a[2] < b[3] - EPS
        and b[2] < a[3] - EPS
    )


def _place(bars, obstacles, *, limits=(0.0, 10.0), pinned=False, sizes=SIZES, **extra):
    problems = [_problem(bars, obstacles)]
    return place_group(problems, limits=limits, pinned_top=pinned, sizes=sizes, **extra), problems


# --- one panel ----------------------------------------------------------------


def test_one_bar_clears_both_marks_and_raises_the_top():
    result, problems = _place([_pair(0, 1)], _boxes({0: 6.0, 1: 9.8}))
    assert violations(result, problems) == []
    (bar,) = result.placed[0]
    assert bar.y > 9.8
    assert bar.left_foot < bar.right_foot  # each leg drops to its own mark
    assert result.limits[1] > 10.0 and result.converged


def test_a_bar_clears_a_tall_tick_between_its_ends():
    result, problems = _place([_pair(0, 2)], _boxes({0: 2.0, 1: 9.5, 2: 2.0}))
    assert violations(result, problems) == []
    assert result.placed[0][0].y > 9.5


def test_bars_sharing_an_end_tick_stack():
    result, problems = _place([_pair(0, 1), _pair(1, 2)], _boxes({0: 5.0, 1: 5.0, 2: 5.0}))
    assert violations(result, problems) == []
    first, second = result.placed[0]
    assert second.y > first.label_top


def test_disjoint_bars_share_a_level():
    result, problems = _place([_pair(0, 1), _pair(3, 4)], _boxes({0: 5, 1: 5, 3: 5, 4: 5}))
    assert violations(result, problems) == []
    first, second = result.placed[0]
    assert first.y == pytest.approx(second.y)


def test_a_wide_bar_arches_over_the_narrow_ones():
    bars = [_pair(0, 4), _pair(0, 1), _pair(2, 3), _pair(1, 3)]
    result, problems = _place(bars, _boxes({x: 4.0 + x for x in range(5)}))
    assert violations(result, problems) == []
    by_span = {(b.left, b.right): b for b in result.placed[0]}
    assert by_span[(0.0, 4.0)].y > max(b.label_top for k, b in by_span.items() if k != (0.0, 4.0))


def test_a_label_wider_than_its_span_clears_the_neighbours():
    marks = _boxes({0: 1.0, 1: 1.0, 2: 9.0})
    result, problems = _place([_pair(0, 1, "p = 0.001")], marks)
    assert violations(result, problems) == []


def test_a_sloped_segment_into_the_span_is_cleared():
    """A spaghetti line from (0.8, 10) down to (1.2, 0) crosses x = 1 at 5,
    while the point AT tick 1 is only 0."""
    marks = Obstacles.build([(0.8, 1.2, 10.0, 0.0, 0.5), (1.2, 1.2, 0.0, 0.0, 2.0), (2, 2, 1.0, 1.0, 2.0)])
    result, problems = _place([_pair(1, 2)], marks, limits=(0.0, 12.0))
    assert violations(result, problems) == []
    assert result.placed[0][0].y > 5.0


def test_marker_reach_is_cleared_in_points():
    marks = Obstacles.build([(0, 0, 5.0, 5.0, 10.0), (1, 1, 5.0, 5.0, 10.0)])
    result, problems = _place([_pair(0, 1)], marks)
    assert violations(result, problems) == []


def test_the_top_is_a_fixed_point():
    """Placing again on the final top needs exactly that top (these bars
    stack four levels, more than the data's headroom, so they raise it)."""
    bars = [_pair(0, 1), _pair(1, 2), _pair(0, 2), _pair(2, 4)]
    result, problems = _place(bars, _boxes({x: 8.0 for x in range(5)}))
    assert result.converged and result.iterations < 60
    _, _, need = _place_once(problems[0], bottom=0.0, top=result.limits[1], sizes=SIZES, pinned=False)
    assert result.limits[1] > 10.0
    assert need == pytest.approx(result.limits[1], rel=1e-6)


def test_bars_that_fit_the_headroom_keep_the_data_top():
    """One bar over marks at 6 and 9 on 0..10 needs ~9.75: the top stays."""
    result, problems = _place([_pair(0, 1)], _boxes({0: 6.0, 1: 9.0}))
    assert result.limits == (0.0, 10.0)
    _, _, need = _place_once(problems[0], bottom=0.0, top=10.0, sizes=SIZES, pinned=False)
    assert need <= 10.0
    assert violations(result, problems) == []


def test_a_typed_top_wins_and_the_bars_above_it_are_unfit(caplog):
    bars = [_pair(0, 1), _pair(0, 2), _pair(0, 3)]
    with caplog.at_level(logging.WARNING, logger=LAYER):
        result, problems = _place(bars, _boxes({x: 9.0 for x in range(4)}), pinned=True)
    assert result.limits == (0.0, 10.0)
    assert result.unfit[0]  # not everything fits in one unit
    assert violations(result, problems) == []


def test_a_top_floor_raises_the_range():
    result, _ = _place([_pair(0, 1)], _boxes({0: 1.0, 1: 1.0}), top_floor=50.0)
    assert result.limits[1] == pytest.approx(50.0)


def test_a_short_panel_gives_most_of_its_height_to_the_bars():
    short = PanelFrame(height_pt=40.0, width_pt=300.0, x_range=(-0.5, 4.5))
    bars = [_pair(0, 1), _pair(1, 2)]
    problems = [_problem(bars, _boxes({x: 9.0 for x in range(3)}), short)]
    result = place_group(problems, limits=(0.0, 10.0), pinned_top=False, sizes=SIZES)
    assert result.bar_fraction > 0.5
    assert violations(result, problems) == []


def test_bars_taller_than_the_panel_warn_that_the_top_never_settles(caplog):
    """Three stacked bars need ~40 pt; the panel has 40 pt in all: no top holds them."""
    short = PanelFrame(height_pt=40.0, width_pt=300.0, x_range=(-0.5, 4.5))
    bars = [_pair(0, 1), _pair(1, 2), _pair(0, 2)]
    problems = [_problem(bars, _boxes({x: 9.0 for x in range(3)}), short)]
    with caplog.at_level(logging.WARNING, logger=LAYER):
        result = place_group(problems, limits=(0.0, 10.0), pinned_top=False, sizes=SIZES)
    assert not result.converged
    assert "did not settle" in caplog.text


def test_log_axis_places_in_log_units():
    marks = _boxes({0: 100.0, 1: 1000.0})
    problems = [_problem([_pair(0, 1)], marks)]
    result = place_group(problems, limits=(1.0, 2000.0), pinned_top=False, sizes=SIZES, log=True)
    assert violations(result, problems, log=True) == []
    assert result.placed[0][0].y > 1000.0


def test_a_flat_range_still_places(caplog):
    with caplog.at_level(logging.WARNING, logger=LAYER):
        result, problems = _place([_pair(0, 1)], _boxes({0: 5.0, 1: 5.0}), limits=(5.0, 5.0))
    assert "is empty" in caplog.text
    assert result.placed[0]


def test_shared_panels_get_one_top():
    tall = _problem([_pair(0, 1), _pair(0, 2)], _boxes({0: 9.0, 1: 9.0, 2: 9.0}))
    plain = _problem([], Obstacles.empty())
    result = place_group([tall, plain], limits=(0.0, 10.0), pinned_top=False, sizes=SIZES)
    assert result.placed[1] == []
    assert result.limits[1] > 10.0
    assert violations(result, [tall, plain]) == []


# --- from a resolved figure ---------------------------------------------------


@pytest.fixture
def gait_table() -> LongTable:
    rows = []
    for subject in ("01", "02", "03"):
        for side in ("L", "R"):
            for number, session in enumerate(("pre", "post", "follow")):
                rows.append(
                    {
                        "subject": subject,
                        "side": side,
                        "session": session,
                        "Step": 1.0 + number * (2.0 if side == "R" else 1.0) + int(subject) / 10,
                    }
                )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "side", "session"],
        measures=["Step"],
        name="Step",
        schema_levels=["subject", "side", "session"],
        level_order={"session": ["pre", "post", "follow"]},
    )


def _gait(*bars: DifferenceBar, kind: PlotKind = PlotKind.BAR, **extra) -> PlotSpec:
    return PlotSpec(
        measures=["Step"],
        kind=kind,
        roles={"side": Role.FACET, "session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        difference_bars=list(bars),
        **extra,
    )


def _side_bar(side: str, a: str, b: str) -> DifferenceBar:
    return DifferenceBar(match={"side": side}, a={"session": a}, b={"session": b})


def _frames(figure) -> dict[int, PanelFrame]:
    return {
        i: PanelFrame(height_pt=180.0, width_pt=200.0, x_range=(-0.5, 2.5))
        for i in range(len(figure.panels))
    }


def test_bar_obstacles_reach_the_error_bar_tops(gait_table):
    figure = resolve(_gait(), gait_table)[0]
    panel = next(p for p in figure.panels if p.key["side"] == "R")
    obstacles = panel_obstacles(figure, panel)
    high = panel.frame[figure.encoding.y_high].max()
    assert obstacles.y1.max() == pytest.approx(high)


def test_box_obstacles_reach_the_highest_value(gait_table):
    figure = resolve(_gait(kind=PlotKind.BOX), gait_table)[0]
    panel = next(p for p in figure.panels if p.key["side"] == "R")
    assert panel_obstacles(figure, panel).y1.max() == pytest.approx(panel.frame["__y"].max())


def test_sample_points_are_obstacles_too(gait_table):
    figure = resolve(_gait(show_sample=["subject"]), gait_table)[0]
    panel = next(p for p in figure.panels if p.key["side"] == "R")
    obstacles = panel_obstacles(figure, panel)
    assert len(obstacles) >= len(panel.frame) + len(panel.sample)
    assert obstacles.y1.max() >= panel.sample["__y"].max()


def test_place_figure_raises_a_shared_axis_for_every_panel(gait_table):
    figure = resolve(_gait(_side_bar("R", "pre", "follow")), gait_table)[0]
    assert placement_groups(figure) == [[0, 1]]
    before = figure.y_limits
    placement = place_figure(figure, _frames(figure), SIZES)
    assert placement.count == 1
    tops = {placement.limits[i][1] for i in (0, 1)}
    assert len(tops) == 1 and tops.pop() > before[1]


def test_place_figure_honours_a_typed_max(gait_table, caplog):
    spec = _gait(_side_bar("R", "pre", "follow"), y_axis=YAxis(maximum=5.2))
    figure = resolve(spec, gait_table)[0]
    with caplog.at_level(logging.WARNING, logger=LAYER):
        placement = place_figure(figure, _frames(figure), SIZES)
    assert all(high == 5.2 for _, high in placement.limits.values())
    assert placement.count == 0 and sum(len(u) for u in placement.unfit.values()) == 1
    assert "do not fit under the Max you set" in caplog.text


def test_place_figure_on_a_spaghetti_has_no_violations(gait_table):
    spec = PlotSpec(
        measures=["Step"],
        kind=PlotKind.SPAGHETTI,
        roles={"side": Role.FACET, "session": Role.GROUP, "subject": Role.GROUP},
        groups=["subject", "session"],
        difference_bars=[_side_bar("R", "pre", "post"), _side_bar("R", "pre", "follow")],
    )
    figure = resolve(spec, gait_table)[0]
    frames = _frames(figure)
    placement = place_figure(figure, frames, SIZES)
    index = next(i for i, p in enumerate(figure.panels) if p.key["side"] == "R")
    problem = _problem([], panel_obstacles(figure, figure.panels[index]), frames[index])
    result = GroupPlacement(
        placed=[placement.bars[index]],
        unfit=[[]],
        limits=placement.limits[index],
        iterations=1,
        converged=True,
        bar_fraction=0.0,
    )
    assert len(placement.bars[index]) == 2
    assert violations(result, [problem]) == []


def test_a_panel_without_bars_and_unshared_keeps_its_range(gait_table):
    spec = _gait(_side_bar("R", "pre", "follow"), y_axis=YAxis(scope=["side"]))
    figure = resolve(spec, gait_table)[0]
    left = next(i for i, p in enumerate(figure.panels) if p.key["side"] == "L")
    placement = place_figure(figure, _frames(figure), SIZES)
    assert placement.limits[left] == tuple(figure.panels[left].y_limits)


def test_placing_is_logged(gait_table, caplog):
    figure = resolve(_gait(_side_bar("R", "pre", "follow"), style=StyleOptions()), gait_table)[0]
    with caplog.at_level(logging.DEBUG, logger=LAYER):
        placement = place_figure(figure, _frames(figure), SIZES)
    assert "labels measured" in caplog.text
    assert placement.notes and "1 placed, 0 unfit" in placement.notes[0]
