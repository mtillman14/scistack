"""Display aliases in the EXPORTED code: the same text the preview draws.

Stage 5 of ``.claude/plan-plot-text-sizes-and-aliases.md``. The generated
``plot_`` function carries the merged aliases (the plot's over the project's)
as a literal and relabels what it drew — ticks, legend entries, facet titles,
axis titles — while every level in ``df``, every order and offset stays raw,
exactly as ``ResolvedPlot.text`` does for the renderers.
"""

from __future__ import annotations

import io
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    Aggregation,
    Alias,
    AliasError,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    generate_plot_function,
    resolve,
)

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

pytest.importorskip("seaborn")

SUBJECTS = ["01", "02", "03"]
SESSIONS = ["pre", "post"]

#: Raw text that must never survive once everything is aliased. Trials are
#: collapsed in every figure here (and "1".."4" are numeric tick text).
RAW_TEXT = {*SUBJECTS, *SESSIONS, "subject", "session", "StepLength"}


def _everything() -> dict[str, Alias]:
    aliases = {
        name: Alias(name=f"«{name}»", levels={level: f"«{level}»" for level in values})
        for name, values in {"subject": SUBJECTS, "session": SESSIONS}.items()
    }
    aliases["StepLength"] = Alias(name="«StepLength»")
    return aliases


def _bar(**kwargs) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        color="session",
        kind=PlotKind.BAR,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _export(spec: PlotSpec, table: LongTable, frame: pd.DataFrame):
    source = generate_plot_function(spec, table)
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    function = next(v for k, v in namespace.items() if k.startswith("plot_") and callable(v))
    figure = function(frame.copy(), "figure.png")
    figure.savefig(io.BytesIO(), format="png")
    return source, figure


def _preview(spec: PlotSpec, table: LongTable):
    from scistackplot import render_matplotlib

    figure = render_matplotlib(resolve(spec, table)[0])
    figure.savefig(io.BytesIO(), format="png")
    return figure


def _texts(figure) -> dict[str, list[str]]:
    visible = [ax for ax in figure.axes if ax.get_visible()]
    legends = list(figure.legends) + [ax.get_legend() for ax in visible if ax.get_legend()]
    return {
        "xticks": [t.get_text() for ax in visible[:1] for t in ax.get_xticklabels() if t.get_text()],
        "legend": sorted(t.get_text() for lg in legends for t in lg.get_texts() if t.get_text()),
        "ylabels": sorted({ax.get_ylabel() for ax in visible if ax.get_ylabel()}),
        "xlabel": [visible[0].get_xlabel()] if visible else [],
        "all": [
            text
            for ax in visible
            for text in [
                *(t.get_text() for t in ax.get_xticklabels()),
                ax.get_xlabel(),
                ax.get_ylabel(),
            ]
            if text
        ]
        + [t.get_text() for lg in legends for t in [*lg.get_texts(), lg.get_title()] if t.get_text()],
    }


def _raw_survivors(texts: list[str]) -> list[str]:
    survivors = []
    for text in texts:
        parts = text
        for separator in (" · ", " | ", " / ", "="):
            parts = parts.replace(separator, "\x00")
        if any(part.strip() in RAW_TEXT for part in parts.split("\x00")):
            survivors.append(text)
    return survivors


# --- nothing to do -----------------------------------------------------------


def test_no_aliases_emit_nothing(scalar_table):
    source = generate_plot_function(_bar(), scalar_table)
    assert "_alias" not in source and "_legend_text" not in source


# --- parity with the preview ------------------------------------------------


def test_a_single_axis_reads_as_the_preview(scalar_table, scalar_frame):
    spec = _bar(aliases=_everything())
    source, exported = _export(spec, scalar_table, scalar_frame)
    preview = _preview(spec, scalar_table)
    try:
        a, b = _texts(exported), _texts(preview)
        assert a["xticks"] == b["xticks"]
        assert set(a["xticks"]) == {"«post»", "«pre»"}
        # The colour is on x: seaborn calls that hue redundant and would draw
        # no legend; the export states legend=True so it matches the preview.
        assert sorted(a["legend"]) == sorted(b["legend"]) == ["«post»", "«pre»"]
        assert a["xlabel"] == b["xlabel"] == ["«session»"]
        assert a["ylabels"] == b["ylabels"] == ["«StepLength»"]
    finally:
        plt.close(exported)
        plt.close(preview)
    # The data stays raw: the literal is only ever read by `_alias` / the
    # relabel, never used to rewrite a column.
    assert "_aliases = {" in source
    assert ".map(_alias" not in source and ".replace(_aliases" not in source


def test_facet_titles_read_as_the_preview(scalar_table, scalar_frame):
    spec = _bar(
        roles={"subject": Role.FACET, "session": Role.GROUP, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
        aliases=_everything(),
    )
    _source, exported = _export(spec, scalar_table, scalar_frame)
    preview = _preview(spec, scalar_table)
    try:
        assert _texts(exported)["ylabels"] == _texts(preview)["ylabels"] == ["«01»", "«02»", "«03»"]
    finally:
        plt.close(exported)
        plt.close(preview)


def test_legend_entries_and_dash_ids_are_aliased():
    """A band coloured by session with one dash per subject: the colour is
    not on x, so the exported code draws a legend, and every entry the
    preview lists reads the same there (levels, dash ids, the names)."""
    table, frame = _series()
    spec = _bar(
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        kind=PlotKind.BAND,
        aliases=_everything(),
    )
    _source, exported = _export(spec, table, frame)
    try:
        legend = set(_texts(exported)["legend"])
        assert {"«pre»", "«post»", "«01»", "«02»", "«03»"} <= legend
        assert not legend & RAW_TEXT
    finally:
        plt.close(exported)


def test_the_project_layer_is_baked_in(scalar_table, scalar_frame):
    """A literal, not a run-time read of scistack.toml (lineage)."""
    project = {"session": {"levels": {"pre": "Before"}}}
    table = replace(scalar_table, aliases_source=lambda: project)
    source, exported = _export(_bar(), table, scalar_frame)
    try:
        assert "'Before'" in source
        assert "tomllib" not in source and "aliases_source" not in source
        assert "Before" in _texts(exported)["xticks"]
    finally:
        plt.close(exported)
    project["session"] = {"levels": {"pre": "Changed"}}
    assert "'Before'" in source, "the exported step changes only when re-exported"


def test_a_clash_is_refused_at_export_as_in_the_preview(scalar_table):
    spec = _bar(aliases={"session": Alias(levels={"post": "pre"})})
    with pytest.raises(AliasError):
        generate_plot_function(spec, scalar_table)


# --- the guard, on exported figures ------------------------------------------


def _series():
    rows = []
    rng = np.random.default_rng(4)
    for subject in SUBJECTS:
        for session in SESSIONS:
            for trial in ["1", "2", "3"]:
                rows.append({"subject": subject, "session": session, "trial": trial,
                             "StepLength": list(rng.normal(size=6))})
    frame = pd.DataFrame(rows)
    table = LongTable.from_frame(
        frame, factors=["subject", "session", "trial"], measures=["StepLength"],
        schema_levels=["subject", "session", "trial"], name="StepLength",
        level_order={"session": ["pre", "post"]},
    )
    return table, frame


@pytest.mark.parametrize("name", ["nested bar", "faceted box", "dashed band", "coloured sample"])
def test_no_raw_level_survives_in_the_exported_figure(scalar_table, scalar_frame, name):
    nested = {"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE}
    series_table, series_frame = _series()
    spec, table, frame = {
        "nested bar": (_bar(roles=nested, groups=["session", "subject"]), scalar_table, scalar_frame),
        "faceted box": (
            _bar(roles={"subject": Role.FACET, "session": Role.GROUP, "trial": Role.COLLAPSE},
                 kind=PlotKind.BOX),
            scalar_table,
            scalar_frame,
        ),
        "dashed band": (
            _bar(roles=nested, groups=["subject", "session"], kind=PlotKind.BAND),
            series_table,
            series_frame,
        ),
        "coloured sample": (
            _bar(
                aggregate=Aggregation(error=ErrorBand.SD),
                show_sample=["subject"],
                sample_color="subject",
            ),
            scalar_table,
            scalar_frame,
        ),
    }[name]
    _source, exported = _export(replace(spec, aliases=_everything()), table, frame)
    try:
        texts = _texts(exported)["all"]
        assert any("«" in text for text in texts), f"{name}: nothing aliased was drawn"
        assert _raw_survivors(texts) == [], f"{name}: {_raw_survivors(texts)}"
    finally:
        plt.close(exported)
