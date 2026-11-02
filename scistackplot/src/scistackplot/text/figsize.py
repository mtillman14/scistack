"""Figure size presets — the one vocabulary for "what shape is the saved figure".

``StyleOptions.width``/``height`` (inches) are what the matplotlib export and
the generated code draw with; the interactive preview fills whatever pane it
is in and does not use them. This module is the layer that turns a user-facing
choice ("16:9, seven inches wide") into those two numbers, so the GUI, the
backend and the renderer cannot each carry their own list of ratios.

The presets are RATIOS, not sizes, because in practice the width is dictated
by where the figure goes (a journal's single column is 3.5 in, a double 7.2 in,
a slide is whatever the deck is) and the ratio is the free choice. So the
contract is: pick a ratio, name a width, the height follows
(:func:`height_for`). :func:`aspect_name` runs the other way — given a stored
size, which preset is it — so a reopened spec's dropdown shows the ratio it
was saved with rather than falling back to "custom".
"""

from __future__ import annotations

from dataclasses import dataclass

#: The preset that means "both numbers were typed by hand".
CUSTOM = "custom"


@dataclass(frozen=True)
class AspectPreset:
    name: str
    #: width / height. ``None`` only for :data:`CUSTOM`.
    ratio: float | None
    #: What the dropdown says, and why one would pick it.
    label: str
    hint: str

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "ratio": self.ratio,
            "label": self.label,
            "hint": self.hint,
        }


_GOLDEN = (1 + 5**0.5) / 2

#: Ordered as the dropdown lists them. Landscape first, the current default
#: (8 x 6 = 4:3) near the top, portrait last, custom always at the end.
ASPECT_PRESETS: tuple[AspectPreset, ...] = (
    AspectPreset("4:3", 4 / 3, "4:3", "The 8 x 6 in default; older slide decks"),
    AspectPreset("16:9", 16 / 9, "16:9", "Widescreen slides"),
    AspectPreset("3:2", 3 / 2, "3:2", "Photo proportions; a common single-panel figure"),
    AspectPreset("golden", _GOLDEN, "Golden (1.618:1)", "The classic textbook figure"),
    AspectPreset("2:1", 2.0, "2:1", "Wide strip — time series, long signals"),
    AspectPreset("1:1", 1.0, "1:1", "Square — heatmaps, correlation matrices"),
    AspectPreset("3:4", 3 / 4, "3:4 (portrait)", "Tall — stacked facet rows"),
    AspectPreset("9:16", 9 / 16, "9:16 (portrait)", "Tall widescreen"),
    AspectPreset(CUSTOM, None, "Custom", "Type both width and height"),
)

_BY_NAME = {preset.name: preset for preset in ASPECT_PRESETS}

#: How far a stored width/height may sit from a preset's ratio and still be
#: reported as that preset. Two decimals of height on an 8 in width is
#: ~0.001 in ratio; this is loose enough for a rounded height and tight enough
#: that 4:3 (1.333) and 3:2 (1.5) never collide.
RATIO_TOLERANCE = 0.01


def preset(name: str) -> AspectPreset:
    """The preset called ``name``; ``KeyError`` for an unknown one."""
    return _BY_NAME[name]


def height_for(width: float, name: str, *, height: float | None = None) -> float:
    """Height in inches for ``width`` at preset ``name``.

    ``custom`` has no ratio, so the caller must supply the ``height`` it
    already holds; asking for custom without one is a programming error, not
    a case to paper over with a default.
    """
    ratio = preset(name).ratio
    if ratio is None:
        if height is None:
            raise ValueError("height_for: the custom preset needs an explicit height")
        return height
    if width <= 0:
        raise ValueError(f"height_for: width must be positive, got {width!r}")
    return round(width / ratio, 2)


def aspect_name(width: float, height: float) -> str:
    """Which preset ``width`` x ``height`` is, or :data:`CUSTOM`.

    Compared by ratio within :data:`RATIO_TOLERANCE`, so the height rounding
    in :func:`height_for` round-trips. A non-positive size is custom: it is
    not a ratio at all, and the renderer will refuse it before it matters.
    """
    if width <= 0 or height <= 0:
        return CUSTOM
    ratio = width / height
    for candidate in ASPECT_PRESETS:
        if candidate.ratio is not None and abs(candidate.ratio - ratio) <= RATIO_TOLERANCE:
            return candidate.name
    return CUSTOM


def describe_size(width: float, height: float) -> dict:
    """``{width, height, aspect}`` — the shape a figure will be saved at.

    Emitted by the plotly renderer in ``layout.meta`` so the panel can state
    the export size next to a preview that deliberately does not honour it.
    """
    return {"width": width, "height": height, "aspect": aspect_name(width, height)}


def presets_payload() -> list[dict]:
    """The presets as JSON, in dropdown order."""
    return [item.to_dict() for item in ASPECT_PRESETS]
