"""Figure size presets: one vocabulary, and both exports honour the numbers."""

from __future__ import annotations

import pytest

from scistackplot import (
    ASPECT_PRESETS,
    CUSTOM_ASPECT,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    aspect_name,
    describe_size,
    generate_plot_function,
    height_for,
    presets_payload,
    render_plotly,
    resolve,
)
from scistackplot.figsize import RATIO_TOLERANCE, preset


def test_the_default_size_is_a_named_preset():
    """8 x 6 must not open as "custom" — the dropdown shows what it is."""
    style = StyleOptions()
    assert aspect_name(style.width, style.height) == "4:3"


@pytest.mark.parametrize("item", [p for p in ASPECT_PRESETS if p.ratio is not None])
def test_every_preset_round_trips_through_height_for(item):
    """Pick a ratio, name a width, read the ratio back off the stored size."""
    for width in (3.5, 7.2, 8.0, 13.333):
        assert aspect_name(width, height_for(width, item.name)) == item.name


def test_presets_do_not_collide_within_tolerance():
    ratios = sorted(p.ratio for p in ASPECT_PRESETS if p.ratio is not None)
    gaps = [b - a for a, b in zip(ratios, ratios[1:])]
    assert min(gaps) > 2 * RATIO_TOLERANCE


def test_custom_is_last_and_has_no_ratio():
    assert ASPECT_PRESETS[-1].name == CUSTOM_ASPECT
    assert preset(CUSTOM_ASPECT).ratio is None


def test_height_for_custom_keeps_the_given_height():
    assert height_for(8.0, CUSTOM_ASPECT, height=5.25) == 5.25
    with pytest.raises(ValueError):
        height_for(8.0, CUSTOM_ASPECT)


def test_height_for_refuses_a_non_positive_width():
    with pytest.raises(ValueError):
        height_for(0, "16:9")


def test_an_unlisted_ratio_is_custom():
    assert aspect_name(8.0, 5.0) == CUSTOM_ASPECT  # 1.6 — neither golden nor 3:2
    assert aspect_name(0, 6) == CUSTOM_ASPECT


def test_presets_payload_is_json_shaped_and_ordered():
    payload = presets_payload()
    assert [p["name"] for p in payload] == [p.name for p in ASPECT_PRESETS]
    assert all({"name", "ratio", "label", "hint"} <= set(p) for p in payload)


@pytest.fixture
def wide_spec():
    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.X, "session": Role.COLOR, "trial": Role.FREE},
        kind=PlotKind.BOX,
        style=StyleOptions(width=7.2, height=height_for(7.2, "16:9")),
    )


def test_plotly_meta_states_the_export_size(scalar_table, wide_spec):
    """The preview fills its pane; meta is how the panel says what the SAVE is."""
    meta = render_plotly(resolve(wide_spec, scalar_table)[0])["layout"]["meta"]
    assert meta["figure_size"] == describe_size(7.2, 4.05)
    assert meta["figure_size"]["aspect"] == "16:9"


def test_matplotlib_draws_at_the_spec_size(scalar_table, wide_spec):
    matplotlib = pytest.importorskip("matplotlib")
    from scistackplot import render_matplotlib

    figure = render_matplotlib(resolve(wide_spec, scalar_table)[0])
    try:
        assert tuple(figure.get_size_inches()) == pytest.approx((7.2, 4.05))
    finally:
        matplotlib.pyplot.close(figure)


def test_generated_code_carries_the_size(scalar_table, wide_spec):
    source = generate_plot_function(wide_spec, scalar_table)
    assert "set_size_inches(7.2, 4.05)" in source


def test_size_survives_the_spec_round_trip(wide_spec):
    again = PlotSpec.from_dict(wide_spec.to_dict())
    assert (again.style.width, again.style.height) == (7.2, 4.05)
