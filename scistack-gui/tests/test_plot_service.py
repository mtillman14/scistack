"""
Plot Studio backend (services/plot_service.py).

The service is an adapter: policy lives in scistackplot/scistackplotdb, and
these tests check the adaptation — that the panel gets a usable spec in one
round trip, that a bad spec comes back as a message instead of a 500, that the
JSON crossing the webview boundary is actually serializable, and that both
transports reach the same code.
"""

import json
from pathlib import Path

import pytest

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scistack_gui.app import create_app
from scistack_gui.services import plot_service


@pytest.fixture(autouse=True)
def _clear_source_cache():
    plot_service.invalidate()
    yield
    plot_service.invalidate()


# --- describe --------------------------------------------------------------


def test_describe_lists_plottable_variables(populated_db):
    result = plot_service.describe(populated_db)
    names = {m["name"] for m in result["catalog"]["measures"]}

    assert {"RawSignal", "FilteredSignal"} <= names


def test_describe_opens_a_variable_with_a_usable_default_spec(populated_db):
    result = plot_service.describe(populated_db, "RawSignal")

    assert result["eligible"] is True
    assert result["spec"]["measures"] == ["RawSignal"]
    # 1-D data with replicates defaults to a mean line + error band.
    assert result["capabilities"]["shape"] == "1d"
    assert result["spec"]["kind"] in result["capabilities"]["available"]


def test_describe_falls_back_to_the_first_plottable_measure(populated_db):
    """The palette command and the CSV entry point name no variable."""
    result = plot_service.describe(populated_db)
    assert result["variable"] is not None


def test_describe_is_json_serializable(populated_db):
    json.dumps(plot_service.describe(populated_db, "RawSignal"))


def test_unknown_variable_raises_a_key_error(populated_db):
    with pytest.raises(KeyError):
        plot_service.describe(populated_db, "NoSuchVariable")


# --- resolve ---------------------------------------------------------------


def test_resolve_returns_plotly_payloads(populated_db):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is True
    assert result["figures"]
    figure = result["figures"][0]["figure"]
    assert "data" in figure and "layout" in figure
    json.dumps(result)  # must survive the webview boundary


def test_iterate_role_produces_one_payload_per_level(populated_db):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}
    result = plot_service.resolve_figures(populated_db, spec)

    assert len(result["figures"]) == 2  # subjects 1 and 2
    assert {f["label"] for f in result["figures"]} == {"subject=1", "subject=2"}


def test_invalid_spec_returns_a_message_not_an_exception(populated_db):
    """A role conflict is user-correctable state, so the panel shows it.

    Two factors on COLOUR, not on x: x became multi-assignment when nested
    grouping landed, and colour is where single-assignment still means
    something.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "color", "session": "color"}
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is False
    assert "one factor" in result["error"]
    assert result["figures"] == []


def test_nesting_a_1d_measures_x_axis_is_refused_with_a_message(populated_db):
    """RawSignal is 1-D: its x axis is the sample index, so it has no
    categorical axis to nest groups on. Still a message, not an exception."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "x", "session": "x"}
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is False
    assert "categorical axis" in result["error"]
    assert result["figures"] == []


def test_describe_offers_the_variables_that_can_be_plotted_together(populated_db):
    """The Variants section's variable dropdown is built from this.

    Regression: the section was gated on the project HAVING pipeline variants,
    so a project that never edited a function could not reach the control at
    all — the data was here, and nothing rendered it.
    """
    described = plot_service.describe(populated_db, "RawSignal")

    assert described["stackable_with"] == ["FilteredSignal"]


def test_describe_says_why_a_variable_is_not_offered(populated_db):
    """The picker draws EVERY variable node on the canvas, so one it cannot
    offer has to say why in place.

    The reasons were computed from the beginning and only ever logged, so a
    variable a user expected to plot alongside was simply absent from the
    dropdown — indistinguishable from a missing feature. The three criteria
    (shape, dict-vs-value, schema level) are strict enough that "I expected to
    see that one" is the likely case, not the rare one.
    """
    described = plot_service.describe(populated_db, "RawSignal")
    refused = described["stackable_refused"]

    # Whatever else the fixture holds, a refusal must carry a reason and must
    # never contradict the offers.
    assert set(refused) & set(described["stackable_with"]) == set()
    assert "RawSignal" not in refused, "a variable is not refused against itself"
    assert all(reason for reason in refused.values()), (
        "a refusal with an empty reason is worse than no refusal — the node "
        "would render un-clickable with nothing to explain it"
    )


def test_variant_graph_carries_the_default_selection_for_a_new_row(populated_db):
    """A row added by "+ Add variant" must arrive pinned to ONE variant.

    The picker seeds its selection from this. Left to an empty selection, the
    new row would mean "every variant of this variable" and clicking "+" would
    silently add all of them to the figure — the thing the opening pin exists
    to prevent, reintroduced one button later.

    It comes from ``scistackplot.default_selection``, the same rule the panel
    opens on, so a row added later and the row that was already there cannot
    disagree about what "one variant" means.
    """
    from scistackplot import default_selection

    graph = plot_service.variant_graph(populated_db, "FilteredSignal")
    source = plot_service.get_source(populated_db)

    assert "default_selection" in graph
    assert graph["default_selection"] == default_selection(
        source.get_table(["FilteredSignal"])
    )


def test_two_series_over_two_variables_resolve_together(populated_db):
    """Raw vs Filtered: the whole point of todo #1, end to end through the
    service the panel actually calls."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["variant_sets"] = [
        {"name": "Raw", "selection": {}, "variable": "RawSignal"},
        {"name": "Filtered", "selection": {}, "variable": "FilteredSignal"},
    ]
    # RawSignal is 1-D, so `default_roles` already put `subject` on colour —
    # the comparison is what this figure is about, so it takes the channel and
    # subjects become replicates.
    spec["roles"] = {**spec["roles"], "subject": "free", "Variant": "color"}

    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is True, result["error"]
    names = {trace.get("name") for trace in result["figures"][0]["figure"]["data"]}
    assert {"Raw", "Filtered"} <= names

    reported = plot_service.capabilities_for(populated_db, spec)
    assert [s["variable"] for s in reported["variants"]["sets"]] == [
        "RawSignal",
        "FilteredSignal",
    ]
    # Both rows contributed; a row matching nothing is the failure that looks
    # like success.
    assert all(s["row_count"] > 0 for s in reported["variants"]["sets"])


def test_resolve_describes_the_whole_fanout_it_did_not_send(populated_db):
    """Labels and count for every figure, payload for the one being shown.

    The navigator has to name figures it is not displaying, and the labels are
    cheap next to the payloads — a 1-D measure across thirty subjects is
    megabytes per figure, crossing the webview boundary on every interaction.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}
    result = plot_service.resolve_figures(populated_db, spec, figure_index=1)

    assert result["figure_count"] == 2
    assert len(result["figure_labels"]) == 2
    assert len(result["figures"]) == 1
    assert result["figure_index"] == 1
    assert result["figures"][0]["label"] == result["figure_labels"][1]
    assert result["figures"][0]["index"] == 1


def test_resolve_without_an_index_still_returns_every_figure(populated_db):
    """None is the library/test caller's answer, and stays the default."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}
    result = plot_service.resolve_figures(populated_db, spec)

    assert len(result["figures"]) == result["figure_count"] == 2


def test_an_out_of_range_index_clamps(populated_db):
    """A cursor outlives the fan-out it was pointing into.

    Narrowing a filter shrinks the figure set while the panel's index is still
    a moment behind, so this is a normal transient — not a bad request.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    assert plot_service.resolve_figures(populated_db, spec, figure_index=99)[
        "figure_index"
    ] == 1
    assert plot_service.resolve_figures(populated_db, spec, figure_index=-4)[
        "figure_index"
    ] == 0


def test_iterating_a_nested_key_iterates_its_ancestors(populated_db):
    """Schema is [subject, session]: one figure per session is really four.

    And the panel is told, because a user who asked for two figures and
    received four would think something was broken.
    """
    # `subject` must be FREE for promotion to apply: RawSignal is 1-D, so the
    # default spec puts subject on COLOUR, and an ancestor the user assigned a
    # channel is deliberately left alone.
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "session": "iterate", "subject": "free"}
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["figure_count"] == 4
    assert all("subject=" in label for label in result["figure_labels"])
    assert result["notes"] and "subject" in result["notes"][0]


def test_an_ancestor_on_a_channel_is_not_promoted(populated_db):
    """`subject=colour, session=separate figures` is a legitimate figure.

    This is the default state for a 1-D measure, so it is also the common one:
    promotion must not quietly turn two figures into four.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    assert spec["roles"]["subject"] == "color"
    spec["roles"] = {**spec["roles"], "session": "iterate"}
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["figure_count"] == 2
    assert result["notes"] == []


def test_the_fanout_rolls_over_at_a_subject_boundary(populated_db):
    """Subject-major order: after subject 1's last session comes subject 2's first."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "session": "iterate", "subject": "free"}
    labels = plot_service.resolve_figures(populated_db, spec)["figure_labels"]

    assert [label.split(",")[0] for label in labels] == [
        "subject=1",
        "subject=1",
        "subject=2",
        "subject=2",
    ]


def test_a_failed_resolve_still_answers_the_navigator(populated_db):
    """The panel reads these keys unconditionally; a role error must not KeyError it."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "x", "session": "x"}
    result = plot_service.resolve_figures(populated_db, spec, figure_index=3)

    assert result["ok"] is False
    assert result["figure_count"] == 0
    assert result["figure_labels"] == [] and result["notes"] == []


def test_max_points_downsamples_for_transport(populated_db):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["kind"] = "line"
    result = plot_service.resolve_figures(populated_db, spec, max_points=5)

    assert result["figures"][0]["downsampled_from"] is not None


# --- capabilities ----------------------------------------------------------


def test_capabilities_track_role_changes(populated_db):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]

    with_replicates = plot_service.capabilities_for(populated_db, spec)
    assert with_replicates["has_replicates"] is True
    assert "band" in with_replicates["available"]

    spec["roles"] = {key: "color" if key == "session" else "aggregate"
                     for key in spec["roles"]}
    collapsed = plot_service.capabilities_for(populated_db, spec)
    assert collapsed["has_replicates"] is False
    assert "band" not in collapsed["available"]


# --- export ----------------------------------------------------------------


def test_export_generates_a_plot_function_and_call(populated_db):
    pytest.importorskip("seaborn")
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    result = plot_service.export_code(populated_db, spec)

    assert result["function_name"].startswith("plot_")
    assert "for_each(" in result["foreach_source"]
    assert "outputs=[RawSignalFigure]" in result["foreach_source"]
    compile(result["source"], "<generated>", "exec")


def test_export_never_calls_back_into_this_package(populated_db):
    """
    Generated code must run on seaborn alone.

    The docstring does mention scistackplot — the byline, and the embedded
    ``scistackplot-spec:`` block the GUI reads back to repopulate its controls.
    What must never appear is an import or a call, which is what would make an
    exported pipeline depend on this package at runtime.
    """
    pytest.importorskip("seaborn")
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    source = plot_service.export_code(populated_db, spec)["function_source"]

    assert "import scistackplot" not in source
    assert "scistackplot.render" not in source
    assert "sns." in source


def test_add_to_pipeline_writes_the_function_and_declares_the_output(
    client_with_variable_file, populated_db, tmp_path
):
    pytest.importorskip("seaborn")
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    result = plot_service.add_to_pipeline(populated_db, spec)

    assert result.get("error") is None, result.get("error")
    written = tmp_path / "scistack_plots.py"
    assert written.exists()
    assert f"def {result['function_name']}(" in written.read_text()


def test_add_to_pipeline_refuses_to_clobber_an_existing_function(
    client_with_variable_file, populated_db
):
    pytest.importorskip("seaborn")
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    plot_service.add_to_pipeline(populated_db, spec)
    second = plot_service.add_to_pipeline(populated_db, spec)

    assert second["ok"] is False
    assert "already defines" in second["error"]


# --- saving an image -------------------------------------------------------


def test_save_figure_writes_a_png(populated_db, tmp_path):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    target = tmp_path / "figure.png"

    result = plot_service.save_figure(populated_db, spec, str(target))

    assert result["ok"] is True
    assert result["files"] == [str(target)]
    assert target.exists() and target.stat().st_size > 0


def test_save_figure_uses_full_resolution(populated_db, tmp_path):
    """
    The interactive view is downsampled for transport; a saved figure must not
    be. Nothing in save_figure may pass max_points.
    """
    import inspect

    # Drop the docstring, which mentions max_points precisely to explain why the
    # body must not use it.
    body = inspect.getsource(plot_service.save_figure).split('"""', 2)[-1]
    assert "max_points" not in body


def test_save_figure_honours_the_suffix(populated_db, tmp_path):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    target = tmp_path / "figure.svg"

    result = plot_service.save_figure(populated_db, spec, str(target))
    assert result["files"] == [str(target)]
    assert target.exists()


def test_save_figure_writes_one_file_per_iterated_figure(populated_db, tmp_path):
    """A fanned-out spec must not silently save only the first figure."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    result = plot_service.save_figure(populated_db, spec, str(tmp_path / "emg.png"))

    assert len(result["files"]) == 2
    assert {p.name for p in tmp_path.glob("emg_*.png")} == {
        "emg_subject_1.png",
        "emg_subject_2.png",
    }


def test_save_figure_saves_only_the_requested_figure(populated_db, tmp_path):
    """"Save current figure": the whole point of Stage 3.

    Saving every figure at full resolution is what crossed the 30s client
    timeout — a save is more work than the interactive resolve beside it, and
    that was already taking 25-27s on the user's data."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    result = plot_service.save_figure(
        populated_db, spec, str(tmp_path / "emg.png"), figure_index=1
    )

    assert result["ok"] is True
    # Named for the figure it holds, not "emg.png" — a fan-out saved one frame
    # at a time must not produce files that cannot be told apart.
    assert [p.name for p in tmp_path.glob("*.png")] == ["emg_subject_2.png"]
    assert result["files"] == [str(tmp_path / "emg_subject_2.png")]


def test_saving_one_figure_builds_only_that_figure(populated_db, tmp_path, monkeypatch):
    """Not merely writing one file — building one. Resolving all of them and
    saving one would cost exactly what this exists to avoid."""
    import scistackplot

    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    calls = []
    original = scistackplot.resolve

    def spy(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(scistackplot, "resolve", spy)

    plot_service.save_figure(
        populated_db, spec, str(tmp_path / "emg.png"), figure_index=0
    )

    assert calls == [], "the whole fan-out was resolved to save one figure"


def test_an_out_of_range_figure_index_clamps(populated_db, tmp_path):
    """The panel's cursor can be a moment behind a fan-out that just shrank;
    `resolve_one` clamps rather than rejecting, and saving inherits that."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    result = plot_service.save_figure(
        populated_db, spec, str(tmp_path / "emg.png"), figure_index=99
    )

    assert result["ok"] is True
    assert len(result["files"]) == 1


def test_save_reports_progress_per_file(populated_db, tmp_path):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    seen = []
    plot_service.save_figure(
        populated_db,
        spec,
        str(tmp_path / "emg.png"),
        on_progress=lambda stage, done, total, detail: seen.append(
            (stage, done, total)
        ),
    )

    assert [s for s in seen if s[0] == "writing"] == [
        ("writing", 1, 2),
        ("writing", 2, 2),
    ]


def test_save_reports_the_resolve_before_any_file_exists(populated_db, tmp_path):
    """Resolving is the expensive half; it has to be reported while it happens.

    A save of two figures at full resolution spent ~25 minutes in `resolve`
    (scidb.log 2026-09-11 12:28 -> 12:54) and reported nothing until the first
    PNG existed. The panel was indistinguishable from a hung one.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    seen = []
    plot_service.save_figure(
        populated_db,
        spec,
        str(tmp_path / "emg.png"),
        on_progress=lambda stage, done, total, detail: seen.append(
            (stage, done, total)
        ),
    )

    # Every figure announces itself as it STARTS, before the file it becomes.
    assert [s for s in seen if s[0] == "resolving"] == [
        ("resolving", 1, 2),
        ("resolving", 2, 2),
    ]
    assert seen[0][0] == "resolving"
    assert seen.index(("resolving", 2, 2)) < seen.index(("writing", 2, 2))


def test_an_indexed_save_reports_its_one_figure_too(populated_db, tmp_path):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    seen = []
    plot_service.save_figure(
        populated_db,
        spec,
        str(tmp_path / "emg.png"),
        figure_index=1,
        on_progress=lambda stage, done, total, detail: seen.append(
            (stage, done, total)
        ),
    )

    assert seen == [("resolving", 1, 1), ("writing", 1, 1)]


def test_a_bad_spec_is_a_message_on_the_indexed_save_too(populated_db, tmp_path):
    """Both save paths share the refusal, like both resolve paths do."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "x", "session": "x"}

    result = plot_service.save_figure(
        populated_db, spec, str(tmp_path / "x.png"), figure_index=0
    )

    assert result["ok"] is False
    assert result["files"] == []


def test_save_figure_reports_a_bad_spec_instead_of_raising(populated_db, tmp_path):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "x", "session": "x"}

    result = plot_service.save_figure(populated_db, spec, str(tmp_path / "x.png"))
    assert result["ok"] is False
    assert result["files"] == []


def test_saving_into_a_folder_names_the_files_after_the_figures(
    populated_db, tmp_path
):
    """The fan-out case: a folder, not a filename.

    Thirty figures have thirty names that are the figure labels, not anything
    the user chose — asking for a filename only raised "which of the thirty is
    that?". The stem falls back to the measure, so the names match exactly what
    the filename-plus-suffix scheme produced.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}
    folder = tmp_path / "figures"
    folder.mkdir()

    result = plot_service.save_figure(populated_db, spec, str(folder))

    assert result["ok"] is True
    assert result["directory"] == str(folder)
    assert {p.name for p in folder.glob("*.png")} == {
        "RawSignal_subject_1.png",
        "RawSignal_subject_2.png",
    }


def test_a_suffixless_path_is_a_folder_even_before_it_exists(
    populated_db, tmp_path
):
    """The folder picker returns existing directories, but the browser prompt
    does not — a path the user typed has to work the first time."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    folder = tmp_path / "not" / "yet"

    result = plot_service.save_figure(populated_db, spec, str(folder))

    assert result["ok"] is True
    assert result["directory"] == str(folder)
    assert [p.name for p in folder.glob("*")] == ["RawSignal.png"]


def test_an_existing_folder_with_a_dot_is_not_read_as_a_format(
    populated_db, tmp_path
):
    """`~/analysis.v2` is a real folder, and `.v2` is not an image format.

    The existence check is what keeps a chosen destination from turning into a
    guess — without it this writes `analysis.png` beside the folder instead of
    a figure inside it.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    folder = tmp_path / "analysis.v2"
    folder.mkdir()

    result = plot_service.save_figure(populated_db, spec, str(folder))

    assert result["ok"] is True
    assert [p.name for p in folder.glob("*")] == ["RawSignal.png"]


def test_a_filename_still_means_a_file(populated_db, tmp_path):
    """The rule is additive: every caller that passed a filename before keeps
    the behaviour it had."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    target = tmp_path / "chosen.png"

    result = plot_service.save_figure(populated_db, spec, str(target))

    assert result["files"] == [str(target)]


# --- image formats ----------------------------------------------------------


@pytest.mark.parametrize("fmt", ["png", "svg", "pdf", "eps"])
def test_every_offered_format_is_written(populated_db, tmp_path, fmt):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]

    result = plot_service.save_figure(
        populated_db, spec, str(tmp_path / "fig"), image_format=fmt
    )

    assert result["ok"] is True
    written = Path(result["files"][0])
    assert written.suffix == f".{fmt}"
    assert written.exists() and written.stat().st_size > 0


def test_the_format_wins_over_the_paths_suffix(populated_db, tmp_path):
    """The dropdown is the answer; the filename's extension is a leftover.

    They agree in the GUI (the default name is built from the dropdown, and the
    dialog is filtered to it), so this only decides a conflict that the UI does
    not produce — but it has to decide it the same way every time.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]

    result = plot_service.save_figure(
        populated_db, spec, str(tmp_path / "fig.png"), image_format="svg"
    )

    assert [Path(f).name for f in result["files"]] == ["fig.svg"]


def test_an_unwritable_format_is_refused_before_anything_is_done(
    populated_db, tmp_path, monkeypatch
):
    """Caught at the very top — before the resolve, not just before savefig.

    Order matters here, not merely the refusal. Discovered at `savefig`, a bad
    format costs the twelve minutes of rendering first and (inside a job)
    arrives as a crashed thread rather than a message; discovered after the
    resolve, it still costs all of it. Nothing about the format depends on the
    spec, so nothing about the spec should be loaded to check it.

    `.fig` is the real case: MATLAB's own format, which matplotlib cannot write.
    """
    import scistackplot

    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    touched: list[str] = []
    monkeypatch.setattr(
        scistackplot, "resolve", lambda *a, **k: touched.append("resolve")
    )
    monkeypatch.setattr(
        scistackplot, "resolve_one", lambda *a, **k: touched.append("resolve_one")
    )
    monkeypatch.setattr(
        scistackplot, "render_matplotlib", lambda item: touched.append("render")
    )
    folder = tmp_path / "out"

    result = plot_service.save_figure(
        populated_db, spec, str(folder), image_format="fig"
    )

    assert result["ok"] is False
    assert "fig" in result["error"]
    # The message names what IS available rather than only what is not.
    assert "png" in result["error"]
    assert touched == [], "the spec was resolved before the format was checked"
    # And nothing was created on the way to refusing — not even the folder.
    assert not folder.exists()


def test_the_offered_formats_are_what_matplotlib_can_write(populated_db):
    """One list, asked of matplotlib — so the dropdown cannot offer a format
    the save would refuse."""
    import matplotlib
    from matplotlib.backend_bases import FigureCanvasBase

    offered = plot_service.describe(populated_db, "RawSignal")["image_formats"]

    assert offered == sorted(FigureCanvasBase.get_supported_filetypes())
    assert "png" in offered and "svg" in offered
    assert "fig" not in offered, "matplotlib has no MATLAB .fig writer"


def test_save_figure_creates_missing_parent_directories(populated_db, tmp_path):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    target = tmp_path / "new" / "nested" / "figure.png"

    assert plot_service.save_figure(populated_db, spec, str(target))["ok"] is True
    assert target.exists()


# --- the CSV path (same protocol, no database) -----------------------------


def test_csv_source_needs_no_database(tmp_path):
    csv = tmp_path / "gait.csv"
    csv.write_text(
        "subject,session,StepLength\n"
        "s01,pre,1.1\ns01,post,1.3\ns02,pre,1.0\ns02,post,1.2\n"
    )

    described = plot_service.describe(None, "StepLength", csv_path=str(csv))
    assert described["eligible"] is True

    resolved = plot_service.resolve_figures(
        None, described["spec"], csv_path=str(csv)
    )
    assert resolved["ok"] is True
    assert resolved["figures"]


def test_csv_padded_ids_keep_numeric_order(tmp_path):
    """Ten subjects: lexicographic order would put s10 second."""
    csv = tmp_path / "wide.csv"
    rows = "\n".join(f"s{n:02d},{n}" for n in range(1, 11))
    csv.write_text(f"subject,Mass\n{rows}\n")

    described = plot_service.describe(None, "Mass", csv_path=str(csv))
    levels = described["table"]["factors"][0]["levels"]
    assert levels == [f"s{n:02d}" for n in range(1, 11)]


# --- the background save job (Stage 4) -------------------------------------
#
# Saving N figures at full resolution is N times the interactive resolve, and
# that resolve already ran to 25-27s against a fixed 30s client timeout. No
# timeout value makes "save thirty subjects" a request/response operation, so
# it became a job that reports progress.


def _drain(messages, kind, timeout=60.0):
    """Wait for a terminal save message, then return every message seen.

    Polls rather than joining the thread: `start_save_job` deliberately does
    not hand its thread back (nothing in the product has a use for it), and the
    notification IS the completion signal the GUI relies on — so waiting on it
    tests the thing the panel actually depends on.
    """
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any((m.get("type") == kind) for m in messages):
            return messages
        time.sleep(0.02)
    raise AssertionError(
        f"no {kind!r} within {timeout}s; saw {[m.get('type') for m in messages]}"
    )


@pytest.fixture
def captured_pushes(monkeypatch):
    """Collect every push_message the job emits, over either transport."""
    from scistack_gui.api import ws as ws_mod

    messages: list = []
    monkeypatch.setattr(ws_mod, "push_message", messages.append)
    return messages


def test_a_save_job_writes_every_figure_and_reports_each(
    populated_db, tmp_path, captured_pushes
):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec = {**spec, "roles": {**spec["roles"], "subject": "iterate"}}

    started = plot_service.start_save_job(populated_db, spec, str(tmp_path / "emg.png"))
    assert "job_id" in started

    messages = _drain(captured_pushes, "plot_save_complete")
    progress = [m for m in messages if m["type"] == "plot_save_progress"]
    done = next(m for m in messages if m["type"] == "plot_save_complete")

    # Two progress messages per figure — one when it starts resolving (the
    # expensive half) and one when its file exists — in order, all carrying
    # the job id.
    assert [(m["stage"], m["done"], m["total"]) for m in progress] == [
        ("resolving", 1, 2),
        ("resolving", 2, 2),
        ("writing", 1, 2),
        ("writing", 2, 2),
    ]
    assert {m["job_id"] for m in messages} == {started["job_id"]}
    assert len(done["files"]) == 2
    assert {p.name for p in tmp_path.glob("*.png")} == {
        "emg_subject_1.png",
        "emg_subject_2.png",
    }


def test_a_save_job_does_not_hold_the_database_while_rendering(
    populated_db, tmp_path, captured_pushes, per_request_policy, monkeypatch
):
    """The user's stated requirement for this stage: a long save must leave the
    .duckdb file free so MATLAB and the rest of the GUI can work meanwhile."""
    import scistackplot

    db_mod = per_request_policy
    seen = []
    original = scistackplot.render_matplotlib

    def spy(item):
        seen.append((db_mod._db_refcount, db_mod._db_open))
        return original(item)

    monkeypatch.setattr(scistackplot, "render_matplotlib", spy)

    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec = {**spec, "roles": {**spec["roles"], "subject": "iterate"}}

    plot_service.start_save_job(populated_db, spec, str(tmp_path / "emg.png"))
    _drain(captured_pushes, "plot_save_complete")

    assert seen, "render was never reached"
    assert all(refcount == 0 for refcount, _ in seen), (
        "the save job held the DuckDB connection while rendering"
    )
    assert all(not is_open for _, is_open in seen)


def test_a_failing_save_job_announces_itself(
    populated_db, tmp_path, captured_pushes, monkeypatch
):
    """A thread that dies quietly leaves the panel waiting for a completion
    that can never arrive."""
    import scistackplot

    def boom(item):
        raise RuntimeError("renderer exploded")

    monkeypatch.setattr(scistackplot, "render_matplotlib", boom)

    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    plot_service.start_save_job(populated_db, spec, str(tmp_path / "emg.png"))

    messages = _drain(captured_pushes, "plot_save_failed")
    failed = next(m for m in messages if m["type"] == "plot_save_failed")
    assert "renderer exploded" in failed["error"]


def test_a_failed_save_job_releases_the_database(
    populated_db, tmp_path, captured_pushes, per_request_policy, monkeypatch
):
    """A leaked refcount never falls back to zero, so the connection is never
    closed again and the file stays locked for the life of the process."""
    import scistackplot

    db_mod = per_request_policy
    monkeypatch.setattr(
        scistackplot,
        "render_matplotlib",
        lambda item: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    plot_service.start_save_job(populated_db, spec, str(tmp_path / "emg.png"))
    _drain(captured_pushes, "plot_save_failed")

    assert db_mod._db_refcount == 0


def test_an_invalid_spec_ends_the_job_rather_than_hanging(
    populated_db, tmp_path, captured_pushes
):
    """A refusal is not a crash, but it still has to END the job — the panel
    cannot tell a refusal from silence."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec = {**spec, "roles": {**spec["roles"], "subject": "x", "session": "x"}}

    plot_service.start_save_job(populated_db, spec, str(tmp_path / "emg.png"))

    messages = _drain(captured_pushes, "plot_save_failed")
    failed = next(m for m in messages if m["type"] == "plot_save_failed")
    assert failed["error"]
    assert not list(tmp_path.glob("*.png"))


def test_save_reaches_both_transports(
    client, populated_db, tmp_path, captured_pushes
):
    from scistack_gui.server import METHODS

    assert "plot_save_start" in METHODS

    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    response = client.post(
        "/api/plot/save",
        json={"spec": spec, "path": str(tmp_path / "http.png")},
    )

    assert response.status_code == 200
    assert "job_id" in response.json()
    # Drained, not left running: a daemon thread still writing into tmp_path
    # after the test returns is a flake waiting to happen.
    _drain(captured_pushes, "plot_save_complete")


def test_there_is_one_save_method_and_it_is_a_job():
    """The synchronous save is GONE, not deprecated — beta, clean break.

    It existed on the theory that one figure fits a round trip. One
    full-resolution figure is ~12 minutes (scidb.log 2026-09-11), so it did not,
    and leaving it reachable would leave the timeout reachable.
    """
    from scistack_gui.server import METHODS

    assert "plot_save_figure" not in METHODS
    assert "plot_save_all" not in METHODS

    # Asked of the route table, not of a request: the SPA catch-all
    # (`@app.get("/{full_path:path}")` in app.py) matches every path for GET, so
    # a POST to a deleted API route answers 405 rather than 404 and "not 404"
    # would pass just as well against a route that still existed.
    #
    # The table comes from the OpenAPI schema, which is public API and already
    # resolves prefixes and nesting for us. Two earlier versions of this walked
    # route objects instead and both broke on library upgrades, in CI only
    # (2026-09-13): `client.app.routes` was not the app's routes on a newer
    # starlette, and `create_app().routes` stopped being flat when FastAPI
    # changed include_router() to keep one `_IncludedRouter` per call with the
    # real routes nested inside, so every /api route vanished from the scan and
    # the assertion read as "the endpoint is gone" when nothing was gone.
    # No include_in_schema=False anywhere in scistack_gui/api, so the schema is
    # the complete set of HTTP routes.
    paths = create_app().openapi()["paths"]
    posts = {path for path, operations in paths.items() if "post" in operations}
    # Name what was inspected, so a future mismatch says which routes existed
    # instead of only "not in set()".
    detail = f"{len(paths)} path(s), {len(posts)} with POST:\n" + "\n".join(
        f"    {p} {sorted(ops)}" for p, ops in sorted(paths.items())
    )
    assert "/api/plot/save" in posts, detail
    assert "/api/plot/save-all" not in posts, detail


def test_saving_one_figure_is_a_job_too(
    client, populated_db, tmp_path, captured_pushes
):
    """The regression this stage exists for.

    "Save current figure" was a request/response RPC against a 30s client
    timeout, and the work behind it is minutes. It now returns a job id like
    every other save, and reports one file when that figure is on disk.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec = {**spec, "roles": {**spec["roles"], "subject": "iterate"}}

    started = plot_service.start_save_job(
        populated_db, spec, str(tmp_path / "emg.png"), figure_index=1
    )
    assert "job_id" in started

    messages = _drain(captured_pushes, "plot_save_complete")
    done = next(m for m in messages if m["type"] == "plot_save_complete")

    assert [Path(f).name for f in done["files"]] == ["emg_subject_2.png"]
    # One figure resolved, not the fan-out: the whole point of the index.
    assert [
        (m["stage"], m["done"], m["total"])
        for m in messages
        if m["type"] == "plot_save_progress"
    ] == [("resolving", 1, 1), ("writing", 1, 1)]


def test_a_save_job_reports_the_folder_it_filled(
    populated_db, tmp_path, captured_pushes
):
    """What ends the panel's "Saving…" state, and what it says afterwards.

    Thirty full paths sharing one directory is not a message anyone reads, and
    printing them pushed the one fact that matters — it finished — off the end.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec = {**spec, "roles": {**spec["roles"], "subject": "iterate"}}
    folder = tmp_path / "out"
    folder.mkdir()

    plot_service.start_save_job(populated_db, spec, str(folder))

    done = next(
        m
        for m in _drain(captured_pushes, "plot_save_complete")
        if m["type"] == "plot_save_complete"
    )
    assert done["directory"] == str(folder)
    assert len(done["files"]) == 2


def test_a_client_supplied_job_id_is_honoured(
    populated_db, tmp_path, captured_pushes
):
    """The panel adopts the id BEFORE the request leaves.

    A quick save can complete while the response is still in flight; a panel
    that learned the id from the response would drop the completion and sit on
    "Saving…" forever with the file already written.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]

    started = plot_service.start_save_job(
        populated_db, spec, str(tmp_path / "emg.png"), job_id="ps-abc123"
    )

    assert started["job_id"] == "ps-abc123"
    messages = _drain(captured_pushes, "plot_save_complete")
    assert {m["job_id"] for m in messages} == {"ps-abc123"}


# --- both transports reach the same code -----------------------------------


def test_http_route_and_rpc_handler_share_the_service(client, populated_db):
    from scistack_gui.server import METHODS

    response = client.post("/api/plot/describe", json={"variable": "RawSignal"})
    assert response.status_code == 200
    http_result = response.json()

    rpc_result = METHODS["plot_describe"]({"variable": "RawSignal"})
    assert rpc_result["spec"] == http_result["spec"]


def test_figure_index_reaches_the_service_over_both_transports(
    client, populated_db, tmp_path, captured_pushes
):
    """`figure_index` had to be added in four places — the service, the job, the
    RPC handler and the pydantic request model. A transport that drops it
    silently saves the whole fan-out, which is the slow path this exists to
    avoid."""
    from scistack_gui.server import METHODS

    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec = {**spec, "roles": {**spec["roles"], "subject": "iterate"}}

    client.post(
        "/api/plot/save",
        json={"spec": spec, "path": str(tmp_path / "http.png"), "figure_index": 0},
    )
    over_http = next(
        m
        for m in _drain(captured_pushes, "plot_save_complete")
        if m["type"] == "plot_save_complete"
    )

    # One job at a time: the drain watches the shared list, so the second save
    # starts from an empty one rather than matching the first one's completion.
    captured_pushes.clear()
    METHODS["plot_save_start"](
        {"spec": spec, "path": str(tmp_path / "rpc.png"), "figure_index": 0}
    )
    over_rpc = next(
        m
        for m in _drain(captured_pushes, "plot_save_complete")
        if m["type"] == "plot_save_complete"
    )

    # One figure each, and the SAME figure — so the same label lands in both
    # names and neither transport quietly saved the fan-out.
    assert [Path(f).name for f in over_http["files"]] == ["http_subject_1.png"]
    assert [Path(f).name for f in over_rpc["files"]] == ["rpc_subject_1.png"]


# --- cache invalidation after a run ----------------------------------------
#
# ScidbSource caches whole variable frames. A run that writes records makes
# them stale, and a stale frame means the panel plots PRE-RUN data — the
# reported bug, where an edited function's new records were written correctly
# but the figure kept showing the old ones. `plot_invalidate` existed and was
# wired end to end; nothing called it.


def test_source_is_cached_between_calls(populated_db):
    first = plot_service.get_source(populated_db)
    assert plot_service.get_source(populated_db) is first


def test_sources_are_keyed_by_path_not_object_identity(populated_db):
    """`id(db)` was the old key. CPython reuses ids after GC, so a new manager
    could land on a dead entry and inherit another database's frames."""
    plot_service.get_source(populated_db)

    assert ("db", str(populated_db.dataset_db_path)) in plot_service._sources
    assert not any(isinstance(k[1], int) for k in plot_service._sources)


def test_invalidate_drops_the_cached_source(populated_db):
    first = plot_service.get_source(populated_db)
    plot_service.invalidate(populated_db)

    assert plot_service.get_source(populated_db) is not first


def test_invalidate_accepts_a_bare_path(populated_db):
    """The MATLAB run threads have released their connection by the time they
    finish, so they invalidate by path rather than by manager."""
    first = plot_service.get_source(populated_db)
    plot_service.invalidate(populated_db.dataset_db_path)

    assert plot_service.get_source(populated_db) is not first


def test_invalidate_is_a_noop_for_an_unbuilt_source(populated_db):
    """Called after every run, including runs where no panel was ever open."""
    assert plot_service.invalidate(populated_db) == {"ok": True}


def test_run_completion_invalidates_the_cache(populated_db, monkeypatch):
    """The wiring itself: finishing a run must drop the cache, not just push
    dag_updated. These two travelled separately and only one was ever sent."""
    from scistack_gui.api import run as run_api

    cached = plot_service.get_source(populated_db)
    messages = []
    monkeypatch.setattr(run_api, "push_message", messages.append)

    run_api._notify_records_changed()

    assert {"type": "dag_updated"} in messages
    assert plot_service.get_source(populated_db) is not cached


def test_notify_still_updates_the_dag_if_invalidation_fails(
    populated_db, monkeypatch
):
    """A cache we failed to drop is a stale figure, not a failed run — the
    records are already written, so this must never bury the result."""
    from scistack_gui.api import run as run_api

    messages = []
    monkeypatch.setattr(run_api, "push_message", messages.append)
    monkeypatch.setattr(
        plot_service,
        "invalidate",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    run_api._notify_records_changed()  # must not raise

    assert {"type": "dag_updated"} in messages


# --- panel styles ----------------------------------------------------------
#
# Source-scanned rather than rendered: the GUI has no frontend test runner, and
# this class of defect is a property of the style OBJECTS, which are readable
# text (same approach as the handle-id guards in test_edge_resolver.py).


def _plot_studio_style(name: str) -> str:
    """The body of one entry in PlotStudio.tsx's ``styles`` map."""
    import re
    from pathlib import Path

    source = (
        Path(__file__).parent.parent
        / "frontend/src/components/PlotStudio/PlotStudio.tsx"
    ).read_text()
    match = re.search(rf"\n  {name}: \{{(.*?)\n?  \}},", source, re.DOTALL)
    assert match, f"PlotStudio.tsx has no {name!r} style"
    return match.group(1)


def test_collapsed_controls_keep_the_scroll_property():
    """
    The controls rail must still scroll after being collapsed and reopened.

    React removes the style properties a re-render drops. When the collapsed
    state added the ``overflow`` SHORTHAND on top of a base that set the
    ``overflowY`` LONGHAND, reopening cleared ``overflow`` — which owns
    overflow-y — while ``overflowY: 'auto'``, unchanged between the two
    objects, was skipped as nothing to re-apply. The rail came back
    unscrollable, and only ever after a collapse/expand cycle.

    So: both states name the same overflow keys, and neither uses the
    shorthand.
    """
    open_style = _plot_studio_style("controls")
    hidden_style = _plot_studio_style("controlsHidden")

    for name, style in (("controls", open_style), ("controlsHidden", hidden_style)):
        assert "overflow:" not in style, (
            f"PlotStudio.tsx styles.{name} uses the `overflow` shorthand; the "
            "two states are merged, so clearing it on reopen also clears the "
            "other state's overflowY. Use overflowX/overflowY."
        )
        assert "overflowY" in style, (
            f"PlotStudio.tsx styles.{name} must state overflowY explicitly — a "
            "property present in only one of the two merged states is the "
            "property React silently drops."
        )

    assert "overflowY: 'auto'" in open_style


# --- variant graph ---------------------------------------------------------
#
# What the variant-selection popup draws itself from. The GUI must not have to
# parse "Code:bandpass" or "bandpass.low_hz" back into a producing function —
# those are scidb's namespacing conventions, and a TypeScript copy of them
# would be the first thing to break when they change.


def test_variant_graph_reports_axes_and_versions(populated_db):
    graph = plot_service.variant_graph(populated_db, "FilteredSignal")

    assert set(graph) >= {"axes", "versions", "chain_functions"}
    for axis in graph["axes"]:
        assert axis["kind"] in ("code", "param")
        assert "levels" in axis


def test_variant_graph_answers_for_canvas_functions(populated_db):
    """Nodes outside the plotted measure's chain still get a version list — the
    popup mirrors the whole pipeline, so every node has to be able to speak."""
    graph = plot_service.variant_graph(
        populated_db, "FilteredSignal", functions=["never_ran_anywhere"]
    )

    assert "never_ran_anywhere" not in graph["versions"]


def test_variant_graph_is_json_serializable(populated_db):
    json.dumps(plot_service.variant_graph(populated_db, "FilteredSignal"))


def test_variant_graph_on_a_csv_is_empty_not_an_error(tmp_path):
    """A CSV carries no provenance, so it has no variants — the popup should
    still open and say so rather than failing."""
    csv = tmp_path / "flat.csv"
    csv.write_text("subject,value\n01,1.0\n02,2.0\n")

    graph = plot_service.variant_graph(None, "value", csv_path=str(csv))

    assert graph["axes"] == []
    assert graph["versions"] == {}


def test_variant_graph_reaches_both_transports(client, populated_db):
    from scistack_gui.server import METHODS

    response = client.post(
        "/api/plot/variant-graph", json={"variable": "FilteredSignal"}
    )
    assert response.status_code == 200

    rpc_result = METHODS["plot_variant_graph"]({"variable": "FilteredSignal"})
    assert rpc_result["axes"] == response.json()["axes"]


def _component_body(path: str, name: str) -> str:
    import re
    from pathlib import Path

    source = (Path(__file__).parent.parent / path).read_text()
    # `export` optional: VariantParameterNode is exported so the variant popup
    # can reuse the same widget for a glue node that supplies an axis, rather
    # than growing a second copy of the checkbox list.
    match = re.search(rf"\n(?:export )?function {name}\(", source)
    assert match, f"{path} has no {name!r} component"
    rest = source[match.end() :]
    # Up to the next top-level declaration — enough to cover the component.
    # `const ` and `export ` are terminators too, not just `const styles`: a
    # component followed by a top-level const (VariantDagPopup's `nodeTypes`)
    # would otherwise capture the entire rest of the file, and the assertion
    # below would fail on code that is not the component's.
    end = re.search(r"\n(function |export |const )", rest)
    return rest[: end.start()] if end else rest


@pytest.mark.parametrize(
    ("path", "component"),
    [
        ("frontend/src/components/DAG/ParameterNode.tsx", "VariantParameterNode"),
        ("frontend/src/components/DAG/FunctionNode.tsx", "VariantFunctionNode"),
        # Step one of "+ Add variant". Same risk, one step earlier: it is a
        # node rendered in the popup, drawn from the same canvas graph, and a
        # backend call from it would make choosing what to PLOT write execution
        # state.
        (
            "frontend/src/components/PlotStudio/VariantDagPopup.tsx",
            "PickableVariableNode",
        ),
    ],
)
def test_variant_mode_nodes_never_call_the_backend(path, component):
    """The trap this whole feature is built around.

    The canvas and the variant popup draw the same nodes with the same widgets,
    and they mean opposite things: a ParameterNode checkbox on the canvas is
    EXECUTION state (it excludes a value from future for_each fan-outs), while
    in the popup it selects which already-computed records a FIGURE draws.

    If the popup's controls ever reached a backend call, looking at a plot would
    quietly rewrite the run configuration — far worse than two similar-looking
    widgets. In selection mode the node components must therefore be pure: they
    write to the PlotSpec through the context and touch nothing else.
    """
    body = _component_body(path, component)

    assert "callBackend" not in body, (
        f"{component} calls the backend — variant selection is display state "
        f"and must never write execution state (hide/unhide/run)."
    )


# --- axis -> canvas node binding ------------------------------------------


@pytest.fixture
def swept_db(populated_db):
    """The same pipeline run at a SECOND ``low_hz``, so the axis has two levels.

    One value is a constant and Stage 2 drops it — a column that cannot separate
    two records is not an axis. Two values make it a real one, which is the
    precondition for anything below to be testable at all.
    """
    import numpy as np
    from scidb import for_each

    from conftest import FilteredSignal, RawSignal, bandpass_filter

    assert np  # imported for the fixture's own seeding contract
    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 40},
        outputs=[FilteredSignal],
        subject=[1, 2],
        session=["pre", "post"],
    )
    plot_service.invalidate()
    return populated_db


def test_a_param_axis_binds_to_the_node_feeding_its_port(swept_db):
    source = plot_service.get_source(swept_db)
    axes = source.variant_graph("FilteredSignal")["axes"]
    params = [a for a in axes if a["kind"] == "param"]
    assert params, "two low_hz values must produce a branch-param axis"

    bindings = plot_service.axis_node_bindings(swept_db, axes)

    assert set(bindings) == {a["column"] for a in params}


def test_binding_survives_a_parameter_renamed_away_from_the_argument(swept_db):
    """The reported bug, as a regression test.

    A Parameter node is labelled with the Parameter ENTITY's name; the axis is
    namespaced by the producing function's ARGUMENT name. Matching those two
    strings works only while they happen to agree. Here they deliberately do
    not: a differently-named node is wired to the `low_hz` PORT, exactly as the
    real project wired `delsys_sampling_frequency` into `filterDelsys`'s `Fs`.

    Binding by port must be unaffected. Binding by label loses the axis, dims
    the node to "not a variant here", and reports the axis as living in a
    nested pipeline.
    """
    from scistack_gui import pipeline_store

    source = plot_service.get_source(swept_db)
    axes = source.variant_graph("FilteredSignal")["axes"]
    column = next(a["column"] for a in axes if a["kind"] == "param")
    argument = next(a["param"] for a in axes if a["column"] == column)
    function = next(a["function"] for a in axes if a["column"] == column)

    from scistack_gui.services.pipeline_service import get_pipeline_graph

    graph = get_pipeline_graph(swept_db, "main")
    target = next(
        n["id"]
        for n in graph["nodes"]
        if n["type"] == "functionNode" and n["data"]["label"] == function
    )
    pipeline_store.write_manual_edge(
        swept_db,
        {
            "id": "manual__renamed",
            "source": "param__nothing_like_the_argument_name",
            "target": target,
            "targetHandle": f"param__{argument}",
        },
    )

    bindings = plot_service.axis_node_bindings(swept_db, axes)

    assert bindings[column] == "param__nothing_like_the_argument_name", (
        "the axis must follow the PORT, not the name of whatever feeds it"
    )


def test_an_axis_feeding_no_port_on_this_canvas_is_unbound(swept_db):
    """Not an error: the popup lists it beneath the graph so it stays
    reachable. What matters is that it is absent rather than mis-bound."""
    invented = [
        {
            "column": "somewhere_else.cutoff",
            "kind": "param",
            "function": "not_on_this_canvas",
            "param": "cutoff",
            "levels": ["1", "2"],
        }
    ]

    assert plot_service.axis_node_bindings(swept_db, invented) == {}


def test_code_axes_are_not_port_bound(swept_db):
    """They bind to a function node by function NAME — one namespace, no port
    involved — so they must not appear in this mapping at all."""
    code = [
        {
            "column": "Code:bandpass_filter",
            "kind": "code",
            "function": "bandpass_filter",
            "param": None,
            "levels": ["v1", "v2"],
        }
    ]

    assert plot_service.axis_node_bindings(swept_db, code) == {}


# --- building only the figure being looked at ------------------------------


def test_figure_index_builds_one_figure_and_labels_them_all(populated_db):
    """The panel shows one figure at a time; it must not PAY for the others.

    Reducing every figure to serialize one made a fan-out cost N times what the
    user was looking at, on every control change — and building a figure is
    where the cost is (panels, aggregation, downsampling over the whole group).
    The labels the navigator needs come from the group keys, which never
    required the figures.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    result = plot_service.resolve_figures(populated_db, spec, figure_index=1)

    assert result["ok"] is True
    assert len(result["figures"]) == 1, "only the requested figure is built"
    assert result["figures"][0]["index"] == 1
    assert result["figure_count"] == 2
    assert result["figure_labels"] == ["subject=1", "subject=2"], (
        "every label, including the figure that was not built — the navigator "
        "has to name where it would step to"
    )


def test_an_out_of_range_figure_index_is_clamped_not_rejected(populated_db):
    """The fan-out shrinks whenever a filter narrows the data, and the panel's
    cursor is a moment behind the spec it is already re-resolving."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    result = plot_service.resolve_figures(populated_db, spec, figure_index=99)

    assert result["ok"] is True
    assert result["figure_index"] == 1
    assert result["figures"][0]["label"] == "subject=2"


def test_one_figure_matches_what_resolving_all_of_them_gives(populated_db):
    """The deferred path must not be a second implementation. Same spec, same
    figure — otherwise the panel and an export disagree about what it drew."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "iterate"}

    one = plot_service.resolve_figures(populated_db, spec, figure_index=1)
    every = plot_service.resolve_figures(populated_db, spec)

    assert one["figure_labels"] == every["figure_labels"]
    assert one["figures"][0]["key"] == every["figures"][1]["key"]
    assert one["figures"][0]["row_count"] == every["figures"][1]["row_count"]


def test_an_invalid_spec_is_a_message_on_the_deferred_path_too(populated_db):
    """Both resolve paths share `_invalid_spec`, so a role conflict cannot come
    back as a message on one and an exception on the other."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "color", "session": "color"}

    result = plot_service.resolve_figures(populated_db, spec, figure_index=0)

    assert result["ok"] is False
    assert "one factor" in result["error"]
    assert result["figures"] == []


# --- connection lifecycle (Stage 2) ----------------------------------------
#
# Plot work is CPU-bound and long: the 2026-09-11 log shows a resolve holding
# the DuckDB file lock from 12:27:39 to 12:28:10 while it reduced and rendered
# in pandas, blocking MATLAB for 31s over a database it needed for well under a
# second of that. These tests pin the narrowing down, in both directions —
# the lock IS dropped before the expensive part, and the standalone process is
# NOT made to close a connection it holds for life.


@pytest.fixture
def per_request_policy():
    """Run the body under the JSON-RPC server's connection policy."""
    from scistack_gui import db as db_mod

    previous = db_mod.connection_policy()
    was_open = db_mod._db_open
    db_mod.set_connection_policy("per_request")
    try:
        yield db_mod
    finally:
        db_mod.set_connection_policy(previous)
        db_mod._db_refcount = 0
        # Restore the entry state, and only that. Reopening unconditionally
        # would resurrect a manager pointing at a previous test's deleted
        # tmp_path, creating an empty database file as a side effect.
        if was_open and not db_mod._db_open and db_mod._db is not None:
            db_mod._db.reopen()
            db_mod._db_open = True


def test_every_self_managed_method_exists(populated_db):
    """A typo here is silent: the name simply never matches, the method keeps
    the blanket hold, and the narrowing quietly does nothing."""
    from scistack_gui.server import METHODS, SELF_MANAGED_DB_METHODS

    assert SELF_MANAGED_DB_METHODS <= set(METHODS)


def test_resolve_drops_the_connection_before_reducing(
    populated_db, per_request_policy, monkeypatch
):
    """The property the whole stage exists for.

    `resolve_one` runs after the frames are loaded, and by then nobody should
    be holding the database — that is the window MATLAB gets back.
    """
    import scistackplot

    db_mod = per_request_policy
    seen = {}
    original = scistackplot.resolve_one

    def spy(*args, **kwargs):
        seen["refcount"] = db_mod._db_refcount
        seen["open"] = db_mod._db_open
        return original(*args, **kwargs)

    monkeypatch.setattr(scistackplot, "resolve_one", spy)

    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    result = plot_service.resolve_figures(populated_db, spec, figure_index=0)

    assert result["ok"] is True
    assert seen["refcount"] == 0, "the reduce phase still holds the DuckDB lock"
    assert seen["open"] is False, "the DuckDB file lock is still held while reducing"


def test_the_load_phase_does_hold_the_connection(
    populated_db, per_request_policy, monkeypatch
):
    """The other half: narrowing must not become 'never acquires at all', which
    would pass the test above while failing on a database MATLAB has open."""
    db_mod = per_request_policy
    seen = {}
    original = plot_service._table_for

    def spy(source, spec):
        seen["refcount"] = db_mod._db_refcount
        return original(source, spec)

    monkeypatch.setattr(plot_service, "_table_for", spy)

    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    plot_service.resolve_figures(populated_db, spec, figure_index=0)

    assert seen["refcount"] == 1


def test_persistent_policy_never_closes_the_connection(populated_db):
    """Standalone/FastAPI holds one connection for the life of the process and
    no request acquires. An acquire/release pair there would drop the refcount
    to zero and close the connection that every later request expects to find
    open — `get_db` hands back the manager without reopening it."""
    from scistack_gui import db as db_mod

    assert db_mod.connection_policy() == "persistent"
    was_open, was_ref = db_mod._db_open, db_mod._db_refcount
    db_mod._db_open = True
    db_mod._db_refcount = 0
    try:
        with db_mod.db_connection("test"):
            pass

        assert db_mod._db_open is True
        assert db_mod._db_refcount == 0
    finally:
        db_mod._db_open, db_mod._db_refcount = was_open, was_ref


def test_a_csv_spec_takes_no_connection(per_request_policy):
    """There is no database in the CSV path; acquiring the project's DuckDB
    lock to plot a loose file would block MATLAB for no reason at all."""
    db_mod = per_request_policy
    db_mod._db_refcount = 0

    with db_mod.db_connection("test", needed=False):
        assert db_mod._db_refcount == 0


def test_an_unknown_policy_is_refused():
    from scistack_gui import db as db_mod

    with pytest.raises(ValueError, match="Unknown connection policy"):
        db_mod.set_connection_policy("whenever")


# --- location tree (the schema location picker's data) ----------------------


def test_location_tree_reports_every_location(populated_db):
    """The picker's payload: four states, counts, and a verdict."""
    tree = plot_service.location_tree(populated_db, "FilteredSignal")

    assert tree["variable"] == "FilteredSignal"
    assert (tree["green"], tree["total"]) == (4, 4)  # 2 subjects x 2 sessions
    assert tree["verdict"] == "green"
    assert tree["basis"] == "expected"
    assert [r["value"] for r in tree["roots"]] == ["1", "2"]
    assert [c["key"] for c in tree["roots"][0]["children"]] == ["session", "session"]


def test_location_tree_defaults_to_the_variant_a_panel_opens_on(populated_db):
    """No spec is open on the canvas path, so the default must be the SAME rule
    the panel opens on — or the two entry points show different variants."""
    from scistackplot import default_selection

    tree = plot_service.location_tree(populated_db, "FilteredSignal")
    source = plot_service.get_source(populated_db)
    expected = default_selection(source.get_table(["FilteredSignal"]))

    assert tree["selection"] == expected


def test_location_tree_accepts_a_column_keyed_selection(populated_db):
    """Callers hold the plotting layer's vocabulary, not scidb's.

    ``bandpass_filter.low_hz`` is already scidb's namespacing and passes
    through; the translation that matters is tested in scistackplotdb.
    """
    tree = plot_service.location_tree(
        populated_db, "FilteredSignal", selection={"bandpass_filter.low_hz": 20}
    )

    assert tree["variant"] == {"bandpass_filter.low_hz": 20}
    assert tree["green"] == 4


def test_location_tree_problems_only_prunes(populated_db):
    tree = plot_service.location_tree(
        populated_db, "FilteredSignal", problems_only=True
    )

    assert tree["roots"] == []
    assert tree["total"] == 4  # counts still describe the whole tree


def test_location_tree_is_json_serializable(populated_db):
    """It crosses the webview boundary, and `asdict` would drop the header
    numbers — see LocationTree.to_dict."""
    payload = plot_service.location_tree(populated_db, "FilteredSignal")

    restored = json.loads(json.dumps(payload))
    assert restored["total"] == 4
    assert restored["roots"][0]["path"] == [["subject", "1"]]


def test_location_tree_on_a_csv_says_so_rather_than_failing(populated_db, tmp_path):
    """A CSV carries no provenance: the popup must open and explain, not error."""
    csv = tmp_path / "flat.csv"
    csv.write_text("subject,value\n1,2.0\n")

    tree = plot_service.location_tree(populated_db, "value", csv_path=str(csv))

    assert tree["roots"] == []
    assert tree["total"] == 0
    assert any("no provenance" in note for note in tree["notes"])


def test_both_transports_reach_the_same_function(populated_db):
    """The JSON-RPC handler and the HTTP route must not diverge."""
    from scistack_gui.server import _h_plot_location_tree

    rpc = _h_plot_location_tree({"variable": "FilteredSignal"})
    direct = plot_service.location_tree(populated_db, "FilteredSignal")

    assert rpc == direct


# --- the location picker REPLACED the per-key pickers (D5) -------------------


def _frontend_source(path: str) -> str:
    from pathlib import Path

    return (Path(__file__).parent.parent / path).read_text()


def test_schema_keys_no_longer_write_row_filters():
    """D5: the picker REPLACED the flat per-key LevelPickers, not joined them.

    Two controls answering one question is how the Variants/Factors duplication
    went wrong (docs/claude/plot-variant-rows.md §3), and here it would be worse
    than duplication: a `Filter` per column can only express a Cartesian
    product, so a spec carrying both would have a schema-key selection that
    silently disagreed with the location filter it sits beside.

    Asserted on the source because the alternative is a browser: the section
    renders one button, and `setLevelFilter` must be reachable only from the
    Filters section below it.
    """
    source = _frontend_source("frontend/src/components/PlotStudio/PlotStudio.tsx")

    section = source.split('<Section title="Schema keys">', 1)
    assert len(section) == 2, "the Schema keys section is gone entirely"
    body = section[1].split("</Section>", 1)[0]

    assert "setLevelFilter" not in body, (
        "the Schema keys section still writes a per-column Filter — D5 makes it "
        "one button opening the location picker, which writes location_filter"
    )
    # The ELEMENT, not the word: the section's comment explains why the pickers
    # were removed, and that explanation should not trip its own assertion.
    assert "<LevelPicker" not in body, (
        "the per-key LevelPickers are supposed to be removed, not hidden"
    )
    assert "setLocationPickerOpen" in body, "the section should open the picker"

    # `setLevelFilter` itself survives — the Filters section (non-schema
    # factors: a struct's fields, a joined group variable) still needs it.
    assert "const setLevelFilter" in source


def test_the_picker_only_ever_reads():
    """Looking at data integrity must not be able to change anything.

    The same trap the variant popup is built around, one control further on:
    the picker draws a tree of checkboxes, and the canvas has checkboxes that
    mean EXECUTION state (unticking a constant value excludes it from future
    for_each fan-outs). These select what a figure DRAWS. If this component
    ever reached a second backend method, inspecting a study could quietly
    rewrite the run configuration or the database.

    One call, one method — everything else it does is arithmetic on the reply.
    """
    import re

    source = _frontend_source(
        "frontend/src/components/PlotStudio/SchemaLocationPicker.tsx"
    )

    called = set(re.findall(r"callBackend\(\s*'([^']+)'", source))
    assert called == {"plot_location_tree"}, (
        f"the location picker calls {sorted(called)}; it is a read-only view and "
        f"must reach exactly one read method"
    )


def test_the_pickers_state_colours_come_from_python():
    """The four states are scidb's, drawn here — never recomputed in the webview.

    A second definition of "amber" in TSX would be a rule with no test and no
    log, and it is the one a user would actually see. The component may map a
    state to a glyph and a colour; it may not decide which state a location is
    in, so the only place a state name may appear is in those lookup tables.
    """
    source = _frontend_source(
        "frontend/src/components/PlotStudio/SchemaLocationPicker.tsx"
    )

    import re

    assert "node.state" in source, "a leaf's state is read off the payload"
    assert "tree.verdict" in source, "the header verdict is read off the payload"
    # ASSIGNING a state name is the tell. Comparing against one is not — the
    # lookbehind keeps `node.state === 'grey'` (which is how the excluded label
    # is drawn) from reading as a derivation.
    for state in ("green", "amber", "red", "grey"):
        assigned = re.search(rf"(?<![=!<>])=\s*'{state}'", source)
        assert assigned is None, (
            f"assigning {state!r} looks like the picker deciding a state for "
            f"itself; states come from scidb.locations.location_states"
        )
