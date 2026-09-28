"""What a colour may be written as: ``colors.parse_color``, the ONE owner.

Stage 1 of ``.claude/plan-custom-mark-colors.md``; docs/claude/plot-colors.md.
Every accepted form lands on one canonical lowercase ``#rrggbb``, so a pin
typed as ``rgb(0, 114, 178)`` and one picked as ``#0072B2`` are the same pin.
"""

from __future__ import annotations

import pytest

from scistackplot.colors import ColorError, parse_color, try_color


@pytest.mark.parametrize(
    "text, expected",
    [
        ("#0072B2", "#0072b2"),
        ("#0072b2", "#0072b2"),
        ("0072B2", "#0072b2"),
        ("  #0072B2  ", "#0072b2"),
        ("#abc", "#aabbcc"),
        ("rgb(0, 114, 178)", "#0072b2"),
        ("RGB(0,114,178)", "#0072b2"),
        ("rgb( 255 , 0 , 0 )", "#ff0000"),
    ],
)
def test_hex_and_rgb_forms_are_canonical(text, expected):
    assert parse_color(text) == expected


def test_named_colours_resolve_through_matplotlib():
    pytest.importorskip("matplotlib")
    assert parse_color("red") == "#ff0000"
    assert parse_color("tab:blue") == "#1f77b4"


@pytest.mark.parametrize(
    "text, fragment",
    [
        ("", "empty"),
        ("   ", "empty"),
        ("#0072B2FF", "alpha"),
        ("#abcd", "alpha"),
        ("rgb(0, 300, 0)", "0-255"),
        ("rgb(0, 0.5, 0)", "0-255"),
        ("C0", "cycle"),
        ("not a colour at all", "colour"),
    ],
)
def test_refusals_say_why(text, fragment):
    with pytest.raises(ColorError, match=fragment):
        parse_color(text)


def test_non_text_is_refused():
    with pytest.raises(ColorError):
        parse_color(3)
    with pytest.raises(ColorError):
        parse_color(None)


def test_try_color_warns_and_returns_none(caplog):
    assert try_color("#zzzzzz", "somewhere") is None
    assert try_color("#123456", "somewhere") == "#123456"
