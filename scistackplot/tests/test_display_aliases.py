"""Display aliases in figures: the plot's over the project's, applied once.

Stage 4 of ``.claude/plan-plot-text-sizes-and-aliases.md``;
docs/claude/plot-text-and-labels.md. The load-bearing test is the GUARD at
the bottom: every level and name aliased, every kind of text collected from
both renderers, and no raw level may survive — the check that no drawing
site still turns a level into text by itself.
"""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    Aggregation,
    Alias,
    AliasError,
    ErrorBand,
    FactorVariable,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    display_text,
    reconcile,
    render_plotly,
    resolve,
    restore_spec,
)
from scistackplot.aliases import PLOT, PROJECT, RAW, DisplayText, merge

SUBJECTS = ["01", "02", "03"]
SESSIONS = ["pre", "post"]
TRIALS = ["1", "2", "3", "4"]


def _with_project(table: LongTable, project: dict) -> LongTable:
    """``table`` carrying a project layer, the way ScidbSource attaches one."""
    return replace(table, aliases_source=lambda: project)


def _bar(**kwargs) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        # subject collapsed: a GROUP holder missing from `groups` would be
        # placed by the data's nesting and silently nest the axis.
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        color="session",
        kind=PlotKind.BAR,
    )
    base.update(kwargs)
    return PlotSpec(**base)


# --- merging -------------------------------------------------------------------


def test_the_plot_wins_field_by_field():
    project = {"session": Alias(name="Session", levels={"pre": "Before", "post": "After"})}
    plot = {"session": Alias(levels={"pre": "Baseline"})}
    names, levels = merge(project, plot)
    assert names == {"session": ("Session", PROJECT)}  # the plot said nothing
    assert levels["session"] == {"pre": ("Baseline", PLOT), "post": ("After", PROJECT)}


def test_an_empty_plot_entry_shows_the_raw_text():
    project = {"session": Alias(name="Session", levels={"pre": "Before"})}
    plot = {"session": Alias(name="", levels={"pre": ""})}
    names, levels = merge(project, plot)
    assert names == {}
    assert levels == {}


def test_a_grouping_column_is_found_by_its_qualified_name():
    spec = _bar(factor_variables=[FactorVariable("Demographics", "Sex")])
    table = _with_project(
        LongTable.from_frame(
            pd.DataFrame({"Sex": ["F", "M"], "StepLength": [1.0, 2.0]}),
            factors=["Sex"],
            measures=["StepLength"],
        ),
        {"Demographics.Sex": {"name": "Sex", "levels": {"F": "Female"}}, "Sex": {"levels": {"F": "bare", "M": "Male"}}},
    )
    text = display_text(spec, table)
    assert text.level("Sex", "F") == "Female"  # qualified entry first
    assert text.level("Sex", "M") == "Male"  # then the bare one
    assert text.name("Sex", "x") == "Sex"


def test_text_is_raw_by_default():
    text = DisplayText()
    assert text.level("session", "pre") == "pre"
    assert text.level_origin("session", "pre") == RAW
    assert text.name("session", "fallback") == "fallback"
    assert text.dash_id("01 | 1") == "01 | 1"


def test_dash_ids_alias_part_by_part():
    text = DisplayText(
        levels={"subject": {"01": ("P1", PLOT)}, "trial": {"1": ("T1", PLOT)}},
        dash_layers=("subject", "trial"),
    )
    assert text.dash_id("01 | 1") == "P1 | T1"
    assert text.dash_id("01") == "01", "wrong arity: raw, never mis-assigned"


# --- one-to-one ----------------------------------------------------------------


def test_two_levels_reading_the_same_are_refused_with_their_origins(scalar_table):
    table = _with_project(scalar_table, {"session": {"levels": {"pre": "Visit"}}})
    spec = _bar(aliases={"session": Alias(levels={"post": "Visit"})})
    with pytest.raises(AliasError) as caught:
        resolve(spec, table)
    message = str(caught.value)
    assert "'pre' (project)" in message and "'post' (plot)" in message


def test_an_alias_equal_to_another_raw_level_clashes(scalar_table):
    spec = _bar(aliases={"session": Alias(levels={"post": "pre"})})
    with pytest.raises(AliasError):
        resolve(spec, scalar_table)


def test_an_alias_for_an_absent_level_is_inert(scalar_table):
    spec = _bar(aliases={"session": Alias(levels={"follow-up": "pre"})})
    (figure,) = resolve(spec, scalar_table)
    assert figure.text.level("session", "pre") == "pre"


def test_the_clash_is_a_role_error_for_every_existing_caller(scalar_table):
    from scistackplot import RoleError

    assert issubclass(AliasError, RoleError)


# --- in a resolved figure -----------------------------------------------------


def test_names_and_levels_reach_the_labels(scalar_table):
    spec = _bar(
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        aliases={
            "session": Alias(name="Visit", levels={"pre": "Before"}),
            "subject": Alias(name="Participant", levels={"01": "P1"}),
            "StepLength": Alias(name="Step length (cm)"),
        },
    )
    first = resolve(spec, scalar_table)[0]
    assert first.labels.x == "Visit"
    assert first.labels.color == "Visit"
    assert first.labels.y == "Step length (cm)"
    assert first.labels.title == "Participant=P1"
    # Identity stays raw.
    assert first.figure_key == {"subject": "01"}
    assert "pre" in [str(v) for v in first.x_order]


def test_style_labels_still_win_over_name_aliases(scalar_table):
    spec = _bar(
        aliases={"session": Alias(name="Visit"), "StepLength": Alias(name="Step")},
        style=replace(PlotSpec(measures=["m"]).style, x_label="X!", y_label="Y!"),
    )
    (figure,) = resolve(spec, scalar_table)
    assert (figure.labels.x, figure.labels.y) == ("X!", "Y!")


def test_a_nested_axis_aliases_its_text_not_its_order(scalar_table):
    spec = _bar(
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        aliases={"session": Alias(levels={"pre": "Before"}), "subject": Alias(levels={"01": "P1"})},
    )
    (figure,) = resolve(spec, scalar_table)
    plan = figure.x_plan
    assert "Before" in plan.tick_labels and "pre" not in plan.tick_labels
    assert "P1" in [group.label for group in plan.groups]
    assert any("pre" in key for key in plan.order), "the composed keys stay raw"


def test_facet_titles_are_aliased_but_the_panel_identity_is_not(scalar_table):
    from scistackplot.render.base import panel_y_title

    spec = _bar(
        roles={"subject": Role.FACET, "session": Role.GROUP, "trial": Role.COLLAPSE},
        aliases={"subject": Alias(levels={"01": "P1"})},
    )
    (figure,) = resolve(spec, scalar_table)
    first = next(p for p in figure.panels if p.key.get("subject") == "01")
    assert first.title == "01"
    assert panel_y_title(figure, first, leftmost=True) == "P1"


def test_the_project_layer_is_read_per_resolve(scalar_table):
    project = {"session": {"levels": {"pre": "Before"}}}
    table = _with_project(scalar_table, project)
    (first,) = resolve(_bar(), table)
    assert first.text.level("session", "pre") == "Before"
    project["session"] = {"levels": {"pre": "Baseline"}}
    (second,) = resolve(_bar(), table)
    assert second.text.level("session", "pre") == "Baseline"


def test_an_alias_edit_does_not_re_plan(scalar_table, monkeypatch):
    """`aliases` is plan-irrelevant: text changes, the reduction does not."""
    from scistackplot import reduce

    calls = []
    real = reduce._build_plan
    monkeypatch.setattr(reduce, "_build_plan", lambda *a, **k: calls.append(1) or real(*a, **k))
    resolve(_bar(), scalar_table)
    (figure,) = resolve(_bar(aliases={"session": Alias(levels={"pre": "Before"})}), scalar_table)
    assert len(calls) == 1
    assert figure.text.level("session", "pre") == "Before"


def test_aliases_round_trip_and_restore():
    spec = _bar(aliases={"session": Alias(name="Visit", levels={"01": "One"}), "x": Alias(name="")})
    raw = spec.to_dict()
    assert raw["aliases"] == {"session": {"name": "Visit", "levels": {"01": "One"}}, "x": {"name": ""}}
    assert PlotSpec.from_dict(raw).aliases == spec.aliases
    restored = restore_spec(raw)
    assert restored.notes == [] and restored.spec.aliases == spec.aliases


def test_reconcile_reports_stale_aliases_and_keeps_them(scalar_table):
    spec = _bar(
        aliases={
            "sesion": Alias(name="Typo"),
            "session": Alias(levels={"pre": "Before", "gone": "Gone"}),
            "StepLength": Alias(name="Step"),
        }
    )
    checked = reconcile(spec, scalar_table)
    paths = sorted(note.path for note in checked.notes)
    assert paths == ["aliases.sesion", "aliases.session.levels"]
    assert checked.spec.aliases == spec.aliases


# --- the guard -----------------------------------------------------------------


def _series_table() -> LongTable:
    rows = []
    rng = np.random.default_rng(4)
    for subject in SUBJECTS:
        for session in SESSIONS:
            for trial in TRIALS[:3]:
                rows.append({"subject": subject, "session": session, "trial": trial,
                             "StepLength": list(rng.normal(size=6))})
    return LongTable.from_frame(
        pd.DataFrame(rows), factors=["subject", "session", "trial"], measures=["StepLength"],
        schema_levels=["subject", "session", "trial"], name="StepLength",
        level_order={"session": ["pre", "post"]},
    )


def _scenarios(scalar_table):
    nested = {"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE}
    return {
        "nested bar": (
            _bar(roles=nested, groups=["session", "subject"]),
            scalar_table,
        ),
        "faceted box": (
            _bar(
                roles={"subject": Role.FACET, "session": Role.GROUP, "trial": Role.COLLAPSE},
                kind=PlotKind.BOX,
            ),
            scalar_table,
        ),
        "iterated bar": (
            _bar(roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE}),
            scalar_table,
        ),
        "dashed band": (
            _bar(roles=nested, groups=["subject", "session"], kind=PlotKind.BAND),
            _series_table(),
        ),
        "coloured sample": (
            _bar(
                roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
                kind=PlotKind.BAR,
                aggregate=Aggregation(error=ErrorBand.SD),
                show_sample=["subject"],
                sample_color="subject",
            ),
            scalar_table,
        ),
    }


# Trials are collapsed in every scenario (never drawn), and "1".."4" are also
# what a numeric x axis ticks read — so they are not in the raw set.
RAW_TEXT = {*SUBJECTS, *SESSIONS, "subject", "session", "trial", "StepLength"}


def _everything_aliased() -> dict[str, Alias]:
    levels = {"subject": SUBJECTS, "session": SESSIONS, "trial": TRIALS}
    aliases = {
        name: Alias(name=f"«{name}»", levels={level: f"«{level}»" for level in values})
        for name, values in levels.items()
    }
    aliases["StepLength"] = Alias(name="«StepLength»")
    return aliases


def _mpl_texts(figure) -> list[str]:
    texts: list[str] = [t.get_text() for t in figure.texts]
    if figure._suptitle is not None:  # noqa: SLF001
        texts.append(figure._suptitle.get_text())  # noqa: SLF001
    legends = list(figure.legends)
    for ax in figure.axes:
        if not ax.get_visible():
            continue
        texts.extend(t.get_text() for t in ax.get_xticklabels())
        texts.extend(t.get_text() for t in ax.texts)
        texts.extend([ax.get_xlabel(), ax.get_ylabel(), ax.get_title()])
        if ax.get_legend() is not None:
            legends.append(ax.get_legend())
    for legend in legends:
        texts.extend(t.get_text() for t in legend.get_texts())
        texts.append(legend.get_title().get_text())
    return [t for t in texts if t]


def _plotly_texts(payload: dict) -> list[str]:
    layout = payload["layout"]
    texts: list[str] = []
    for key, axis in layout.items():
        if (key.startswith("xaxis") or key.startswith("yaxis")) and isinstance(axis, dict):
            texts.extend(str(t) for t in axis.get("ticktext") or [])
            texts.append(str((axis.get("title") or {}).get("text") or ""))
    texts.extend(str(note.get("text", "")) for note in layout.get("annotations", []))
    texts.append(str((layout.get("title") or {}).get("text") or ""))
    texts.append(str(((layout.get("legend") or {}).get("title") or {}).get("text") or ""))
    texts.extend(str(trace.get("name", "")) for trace in payload["data"] if trace.get("showlegend"))
    return [t for t in texts if t]


def _raw_survivors(texts: list[str]) -> list[str]:
    """Texts that ARE a raw level or name, or a join of them (a panel title,
    a dash id, a legend title) with nothing aliased in it."""
    survivors = []
    for text in texts:
        parts = [p.strip() for p in text.replace(" | ", "|").replace(" · ", "|").replace(" / ", "|").replace("=", "|").split("|")]
        if any(part in RAW_TEXT for part in parts):
            survivors.append(text)
    return survivors


@pytest.mark.parametrize(
    "name", ["nested bar", "faceted box", "iterated bar", "dashed band", "coloured sample"]
)
def test_no_raw_level_survives_in_either_renderer(scalar_table, name):
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from scistackplot import render_matplotlib

    spec, table = _scenarios(scalar_table)[name]
    spec = replace(spec, aliases=_everything_aliased())
    for figure in resolve(spec, table):
        drawn = render_matplotlib(figure)
        try:
            mpl = _mpl_texts(drawn)
        finally:
            plt.close(drawn)
        plotly = _plotly_texts(render_plotly(figure))
        assert any("«" in t for t in mpl), f"{name}: nothing aliased was drawn at all"
        assert _raw_survivors(mpl) == [], f"{name} (matplotlib): {_raw_survivors(mpl)}"
        assert _raw_survivors(plotly) == [], f"{name} (plotly): {_raw_survivors(plotly)}"


# --- what the GUI's Labels section is offered (Stage 6) -----------------------


def test_labelable_lists_the_measure_then_every_drawn_factor(scalar_table):
    table = _with_project(scalar_table, {"session": {"levels": {"pre": "Before"}}})
    spec = _bar(
        roles={"subject": Role.FACET, "session": Role.GROUP, "trial": Role.COLLAPSE},
        aliases={"session": Alias(name="Visit", levels={"post": "After"})},
    )
    (figure,) = resolve(spec, table)
    entries = {entry["factor"]: entry for entry in figure.labelable}
    assert figure.labelable[0]["role"] == "measure"
    assert figure.labelable[0]["factor"] == "StepLength"
    assert "trial" not in entries, "collapsed: never drawn as text"
    session = entries["session"]
    assert session["key"] == "session"
    assert session["name"] == {"raw": "session", "text": "Visit", "origin": "plot"}
    levels = {level["raw"]: level for level in session["levels"]}
    assert levels["pre"] == {"raw": "pre", "text": "Before", "origin": "project"}
    assert levels["post"] == {"raw": "post", "text": "After", "origin": "plot"}
    assert entries["subject"]["role"] == "panels"
    assert {level["origin"] for level in entries["subject"]["levels"]} == {"raw"}


def test_labelable_reaches_the_preview_payload(scalar_table):
    (figure,) = resolve(_bar(), scalar_table)
    meta = render_plotly(figure)["layout"]["meta"]
    assert meta["labelable"] == figure.labelable
    assert [entry["factor"] for entry in meta["labelable"]] == ["StepLength", "session"]


def test_a_grouping_column_is_offered_under_its_qualified_key():
    frame = pd.DataFrame({"Sex": ["F", "M", "F"], "StepLength": [1.0, 2.0, 3.0]})
    table = LongTable.from_frame(frame, factors=["Sex"], measures=["StepLength"])
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"Sex": Role.GROUP},
        groups=["Sex"],
        kind=PlotKind.SCATTER,
        factor_variables=[FactorVariable("Demographics", "Sex")],
    )
    (figure,) = resolve(spec, table)
    (sex,) = [entry for entry in figure.labelable if entry["factor"] == "Sex"]
    assert sex["key"] == "Demographics.Sex", "the GUI writes where the lookup reads first"
