"""A wide variable's TEXT fields, plotted as their own measure.

One melted value column holds one shape, so a wide variable with numeric AND
text fields (``Gait``: ``StepLength``, ``Cadence``, ``Outcome``, ``Side``)
cannot melt them all under one ``ColName`` factor. The catalog lists it twice
(user decision, 2026-11-01): ``Gait`` holds the numeric fields, as it always
has, and ``Gait (text)`` holds the text fields, drawn as counts, %s or
categories (``scistackplot.reduce.categorical``). Every field is reachable; none is
dropped. See docs/claude/categorical-measures.md.

This module is the one owner of the NAME: how a text measure is spelled, and
which variable a measure name loads. Everything that turns a measure name back
into a scidb variable — the source's loaders, the endpoint generator's
``for_each`` input, the GUI — asks :func:`base_variable`.
"""

from __future__ import annotations

#: Appended to a variable's name for the measure holding its text fields.
TEXT_SUFFIX = " (text)"


def text_measure_name(variable: str) -> str:
    """The measure name of ``variable``'s text fields: ``"Gait (text)"``."""
    return f"{variable}{TEXT_SUFFIX}"


def is_text_measure(name: str) -> bool:
    """Whether ``name`` is a text-fields measure rather than a variable."""
    return name.endswith(TEXT_SUFFIX) and len(name) > len(TEXT_SUFFIX)


def base_variable(name: str) -> str:
    """The scidb variable a measure name loads: ``"Gait (text)"`` → ``"Gait"``,
    any other name unchanged."""
    return name[: -len(TEXT_SUFFIX)] if is_text_measure(name) else name
