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


def _pooled_spec(db, variable: str = "RawSignal") -> dict:
    """`describe()`'s spec with the schema keys in ONE figure — subject the
    coloured grouping layer, session collapsed (the sample), a band over it,
    one range of y limits — the shape most tests here were written against
    (one figure, two colours). The opening default is one figure per record
    with per-panel limits (`default_assignment`), and has its own tests
    (`TestTheOpeningDefault`)."""
    spec = plot_service.describe(db, variable)["spec"]
    spec = _with_roles(spec, color="subject", subject="group", session="collapse")
    spec["kind"] = "band"
    spec["y_axis"] = {**(spec.get("y_axis") or {}), "scope": []}
    return spec


def _with_roles(spec: dict, color: str | None = "keep", **roles: str) -> dict:
    """``spec`` with ``roles`` merged in, the grouping order and colour kept
    consistent: a factor leaving the grouping leaves `groups` and drops the
    colour if it held it — what the panel's `setRole` does."""
    merged = {**spec["roles"], **roles}
    groups = [n for n in (spec.get("groups") or []) if merged.get(n) == "group"]
    for name, role in roles.items():
        if role == "group" and name not in groups:
            groups.insert(0, name)
    if color == "keep":
        color = spec.get("color")
    if color is not None and merged.get(color) != "group":
        color = None
    return {**spec, "roles": merged, "groups": groups, "color": color}


def _invalid_spec(spec: dict) -> dict:
    """A spec `validate` refuses: a colour naming a factor that is not a
    grouping layer — the role conflict the panel shows as a message."""
    return {**spec, "color": "session", "roles": {**spec["roles"], "session": "collapse"}}


class TestTheOpeningDefault:
    """What the panel shows first, and why it is cheap: one record."""

    def test_every_schema_key_separates_figures(self, populated_db):
        spec = plot_service.describe(populated_db, "RawSignal")["spec"]
        assert spec["roles"]["subject"] == "iterate"
        assert spec["roles"]["session"] == "iterate"

    def test_the_first_figure_is_one_record(self, populated_db):
        spec = plot_service.describe(populated_db, "RawSignal")["spec"]
        result = plot_service.resolve_figures(populated_db, spec, figure_index=0)
        assert result["figure_count"] == 4  # 2 subjects x 2 sessions
        assert result["figures"][0]["row_count"] == 10  # one 10-sample record
        assert result["figures"][0]["label"].startswith("subject=1, session=")

    def test_y_limits_are_scoped_to_every_panel_factor(self, populated_db):
        spec = plot_service.describe(populated_db, "RawSignal")["spec"]
        assert spec["y_axis"]["scope"] == ["subject", "session"]

    def test_the_opening_figure_resolves_without_a_role_change(self, populated_db):
        """The default must be a figure, not an error — for a produced variable
        with a pinned variant as well as a raw one."""
        for variable in ("RawSignal", "FilteredSignal"):
            spec = plot_service.describe(populated_db, variable)["spec"]
            result = plot_service.resolve_figures(populated_db, spec, figure_index=0)
            assert result["ok"] is True, (variable, result.get("error"))
            assert result["figures"][0]["row_count"] > 0


# --- describe --------------------------------------------------------------


def test_describe_lists_plottable_variables(populated_db):
    result = plot_service.describe(populated_db)
    names = {m["name"] for m in result["catalog"]["measures"]}

    assert {"RawSignal", "FilteredSignal"} <= names


def test_describe_opens_a_variable_with_a_usable_default_spec(populated_db):
    result = plot_service.describe(populated_db, "RawSignal")

    assert result["eligible"] is True
    assert result["spec"]["measures"] == ["RawSignal"]
    # 1-D data opens on one record, so there are no replicates to summarise
    # yet and the kind is a plain line (see TestTheOpeningDefault).
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
    spec = _pooled_spec(populated_db)
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is True
    assert result["figures"]
    figure = result["figures"][0]["figure"]
    assert "data" in figure and "layout" in figure
    json.dumps(result)  # must survive the webview boundary


def test_iterate_role_produces_one_payload_per_level(populated_db):
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")
    result = plot_service.resolve_figures(populated_db, spec)

    assert len(result["figures"]) == 2  # subjects 1 and 2
    assert {f["label"] for f in result["figures"]} == {"subject=1", "subject=2"}


def test_invalid_spec_returns_a_message_not_an_exception(populated_db):
    """A role conflict is user-correctable state, so the panel shows it.

    A colour naming a factor that does not group: colour is a tag on a
    grouping layer, and a stale one is the conflict a saved spec can carry.
    """
    spec = _pooled_spec(populated_db)
    spec = _invalid_spec(spec)
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is False
    assert "not a grouping layer" in result["error"]
    assert result["figures"] == []


def test_a_kind_without_its_sample_is_refused_with_a_message(populated_db):
    """A band needs a sample; with nothing collapsed there is none. Still a
    message, not an exception."""
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, session="iterate")
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is False
    assert "Needs a sample" in result["error"]
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

    The assertion below is also an EQUIVALENCE check, and is worth keeping in
    that shape: the service computes this over ``variant_table`` (no data
    columns) while the comparison uses ``get_table`` (the whole variable). The
    two are documented to produce the same variant structure, and if they ever
    stop doing so, the cheap path would silently open figures on a different
    variant than the expensive one.
    """
    from scistackplot import default_selection

    graph = plot_service.variant_graph(populated_db, "FilteredSignal")
    source = plot_service.get_source(populated_db)

    assert "default_selection" in graph
    assert graph["default_selection"] == default_selection(
        source.get_table(["FilteredSignal"])
    )


def test_variant_graph_does_not_load_the_variable(populated_db):
    """Opening a picker must not read the data it is picking over.

    `default_selection` reads `default_pin`, `latest_column` and the variant
    factors' levels and never touches a measure column, which is exactly what
    `variant_table` answers from a query selecting no data columns. Through
    `get_table` it meant loading the whole variable — 174 M samples on the case
    that produced `.claude/plot-at-scale-plan.md` §7, where the schema-location
    picker timed out on a 419-location variable.

    It was survivable while the popup only opened over the variable the panel
    had already cached. The grouping picker asks it for a variable nobody has
    plotted, so it is not survivable any more.
    """
    source = plot_service.get_source(populated_db)
    assert "FilteredSignal" not in source._frames, "precondition: nothing cached yet"

    plot_service.variant_graph(populated_db, "FilteredSignal")

    # `_frames` is the FULL-frame cache; `variant_table` fills its own memo
    # under a separate key and never populates this one.
    assert "FilteredSignal" not in source._frames, (
        "variant_graph loaded the variable's data to read its variant levels"
    )


def test_two_series_over_two_variables_resolve_together(populated_db):
    """Raw vs Filtered: the whole point of todo #1, end to end through the
    service the panel actually calls."""
    spec = _pooled_spec(populated_db)
    spec["variant_sets"] = [
        {"name": "Raw", "selection": {}, "variable": "RawSignal"},
        {"name": "Filtered", "selection": {}, "variable": "FilteredSignal"},
    ]
    # The comparison is what this figure is about, so the variant takes the
    # colour and the subjects become the sample.
    spec = _with_roles(spec, color="Variant", subject="collapse", Variant="group")

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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")
    result = plot_service.resolve_figures(populated_db, spec, figure_index=1)

    assert result["figure_count"] == 2
    assert len(result["figure_labels"]) == 2
    assert len(result["figures"]) == 1
    assert result["figure_index"] == 1
    assert result["figures"][0]["label"] == result["figure_labels"][1]
    assert result["figures"][0]["index"] == 1


def test_resolve_without_an_index_still_returns_every_figure(populated_db):
    """None is the library/test caller's answer, and stays the default."""
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")
    result = plot_service.resolve_figures(populated_db, spec)

    assert len(result["figures"]) == result["figure_count"] == 2


def test_an_out_of_range_index_clamps(populated_db):
    """A cursor outlives the fan-out it was pointing into.

    Narrowing a filter shrinks the figure set while the panel's index is still
    a moment behind, so this is a normal transient — not a bad request.
    """
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

    assert plot_service.resolve_figures(populated_db, spec, figure_index=99)[
        "figure_index"
    ] == 1
    assert plot_service.resolve_figures(populated_db, spec, figure_index=-4)[
        "figure_index"
    ] == 0


def test_iterating_a_nested_key_does_not_drag_its_ancestors_along(populated_db):
    """Schema is [subject, session]: one figure per session IS two figures,
    every subject's session side by side. The old model promoted an unassigned
    ancestor to a fan-out; every role is explicit now, so nothing is silent
    and nothing is promoted."""
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, session="iterate", subject="group")
    spec["kind"] = "line"  # a band needs a sample; nothing is collapsed here
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is True, result["error"]
    assert result["figure_count"] == 2
    assert all(label.startswith("session=") for label in result["figure_labels"])
    assert result["notes"] == []


def test_an_ancestor_on_a_channel_is_not_promoted(populated_db):
    """`subject=colour, session=separate figures` is a legitimate figure.

    It was the opening state for a 1-D measure until 2026-09-13 and is still
    one click from it: promotion must not quietly turn two figures into four.
    """
    spec = _pooled_spec(populated_db)
    assert spec["color"] == "subject"
    spec = _with_roles(spec, session="iterate")
    spec["kind"] = "line"  # a band needs a sample; with session iterated there is none
    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is True, result["error"]
    assert result["figure_count"] == 2
    assert result["notes"] == []


def test_the_fanout_rolls_over_at_a_subject_boundary(populated_db):
    """Subject-major order: after subject 1's last session comes subject 2's first."""
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, session="iterate", subject="iterate")
    spec["kind"] = "line"
    labels = plot_service.resolve_figures(populated_db, spec)["figure_labels"]

    assert [label.split(",")[0] for label in labels] == [
        "subject=1",
        "subject=1",
        "subject=2",
        "subject=2",
    ]


def test_a_failed_resolve_still_answers_the_navigator(populated_db):
    """The panel reads these keys unconditionally; a role error must not KeyError it."""
    spec = _pooled_spec(populated_db)
    spec = _invalid_spec(spec)
    result = plot_service.resolve_figures(populated_db, spec, figure_index=3)

    assert result["ok"] is False
    assert result["figure_count"] == 0
    assert result["figure_labels"] == [] and result["notes"] == []


def test_max_points_downsamples_for_transport(populated_db):
    spec = _pooled_spec(populated_db)
    spec["kind"] = "line"
    result = plot_service.resolve_figures(populated_db, spec, max_points=5)

    assert result["figures"][0]["downsampled_from"] is not None


# --- capabilities ----------------------------------------------------------


def test_capabilities_track_role_changes(populated_db):
    spec = _pooled_spec(populated_db)

    with_sample = plot_service.capabilities_for(populated_db, spec)
    assert with_sample["has_sample"] is True
    assert "band" in with_sample["available"]

    spec = _with_roles(spec, **{key: "group" for key in spec["roles"]})
    no_sample = plot_service.capabilities_for(populated_db, spec)
    assert no_sample["has_sample"] is False
    assert "band" not in no_sample["available"]


# --- export ----------------------------------------------------------------


def test_export_generates_a_plot_function_and_call(populated_db):
    pytest.importorskip("seaborn")
    spec = _pooled_spec(populated_db)
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
    spec = _pooled_spec(populated_db)
    source = plot_service.export_code(populated_db, spec)["function_source"]

    assert "import scistackplot" not in source
    assert "scistackplot.render" not in source
    assert "sns." in source


def test_add_to_pipeline_writes_the_function_and_declares_the_output(
    client_with_variable_file, populated_db, tmp_path
):
    pytest.importorskip("seaborn")
    spec = _pooled_spec(populated_db)
    result = plot_service.add_to_pipeline(populated_db, spec)

    assert result.get("error") is None, result.get("error")
    written = tmp_path / "scistack_plots.py"
    assert written.exists()
    assert f"def {result['function_name']}(" in written.read_text()


def test_add_to_pipeline_refuses_to_clobber_an_existing_function(
    client_with_variable_file, populated_db
):
    pytest.importorskip("seaborn")
    spec = _pooled_spec(populated_db)
    plot_service.add_to_pipeline(populated_db, spec)
    second = plot_service.add_to_pipeline(populated_db, spec)

    assert second["ok"] is False
    assert "already defines" in second["error"]


# --- saving an image -------------------------------------------------------


def test_save_figure_writes_a_png(populated_db, tmp_path):
    spec = _pooled_spec(populated_db)
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
    spec = _pooled_spec(populated_db)
    target = tmp_path / "figure.svg"

    result = plot_service.save_figure(populated_db, spec, str(target))
    assert result["files"] == [str(target)]
    assert target.exists()


def test_save_figure_writes_one_file_per_iterated_figure(populated_db, tmp_path):
    """A fanned-out spec must not silently save only the first figure."""
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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

    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

    result = plot_service.save_figure(
        populated_db, spec, str(tmp_path / "emg.png"), figure_index=99
    )

    assert result["ok"] is True
    assert len(result["files"]) == 1


def test_save_reports_progress_per_file(populated_db, tmp_path):
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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
    spec = _pooled_spec(populated_db)
    spec = _invalid_spec(spec)

    result = plot_service.save_figure(
        populated_db, spec, str(tmp_path / "x.png"), figure_index=0
    )

    assert result["ok"] is False
    assert result["files"] == []


def test_save_figure_reports_a_bad_spec_instead_of_raising(populated_db, tmp_path):
    spec = _pooled_spec(populated_db)
    spec = _invalid_spec(spec)

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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")
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
    spec = _pooled_spec(populated_db)
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
    spec = _pooled_spec(populated_db)
    folder = tmp_path / "analysis.v2"
    folder.mkdir()

    result = plot_service.save_figure(populated_db, spec, str(folder))

    assert result["ok"] is True
    assert [p.name for p in folder.glob("*")] == ["RawSignal.png"]


def test_a_filename_still_means_a_file(populated_db, tmp_path):
    """The rule is additive: every caller that passed a filename before keeps
    the behaviour it had."""
    spec = _pooled_spec(populated_db)
    target = tmp_path / "chosen.png"

    result = plot_service.save_figure(populated_db, spec, str(target))

    assert result["files"] == [str(target)]


# --- image formats ----------------------------------------------------------


@pytest.mark.parametrize("fmt", ["png", "svg", "pdf", "eps"])
def test_every_offered_format_is_written(populated_db, tmp_path, fmt):
    spec = _pooled_spec(populated_db)

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
    spec = _pooled_spec(populated_db)

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

    spec = _pooled_spec(populated_db)
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


def test_describe_offers_the_figure_size_presets(populated_db):
    """The aspect dropdown is scistackplot's list, in its order — the GUI
    carries no second copy that could drift from what `aspect_name` reports."""
    from scistackplot import ASPECT_PRESETS, CUSTOM_ASPECT

    payload = plot_service.describe(populated_db, "RawSignal")
    offered = payload["figure_presets"]

    assert [p["name"] for p in offered] == [p.name for p in ASPECT_PRESETS]
    assert offered[-1]["name"] == CUSTOM_ASPECT and offered[-1]["ratio"] is None
    # The opening spec's size is one of them, so the dropdown never opens on
    # "custom" for a figure nobody has customised.
    style = payload["spec"].get("style") or {}
    assert style.get("width") == 8.0 and style.get("height") == 6.0


def test_save_figure_creates_missing_parent_directories(populated_db, tmp_path):
    spec = _pooled_spec(populated_db)
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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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

    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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

    spec = _pooled_spec(populated_db)
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

    spec = _pooled_spec(populated_db)
    plot_service.start_save_job(populated_db, spec, str(tmp_path / "emg.png"))
    _drain(captured_pushes, "plot_save_failed")

    assert db_mod._db_refcount == 0


def test_an_invalid_spec_ends_the_job_rather_than_hanging(
    populated_db, tmp_path, captured_pushes
):
    """A refusal is not a crash, but it still has to END the job — the panel
    cannot tell a refusal from silence."""
    spec = _pooled_spec(populated_db)
    spec = _invalid_spec(spec)

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

    spec = _pooled_spec(populated_db)
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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")
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
    spec = _pooled_spec(populated_db)

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

    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

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
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

    result = plot_service.resolve_figures(populated_db, spec, figure_index=99)

    assert result["ok"] is True
    assert result["figure_index"] == 1
    assert result["figures"][0]["label"] == "subject=2"


def test_one_figure_matches_what_resolving_all_of_them_gives(populated_db):
    """The deferred path must not be a second implementation. Same spec, same
    figure — otherwise the panel and an export disagree about what it drew."""
    spec = _pooled_spec(populated_db)
    spec = _with_roles(spec, subject="iterate")

    one = plot_service.resolve_figures(populated_db, spec, figure_index=1)
    every = plot_service.resolve_figures(populated_db, spec)

    assert one["figure_labels"] == every["figure_labels"]
    assert one["figures"][0]["key"] == every["figures"][1]["key"]
    assert one["figures"][0]["row_count"] == every["figures"][1]["row_count"]


def test_an_invalid_spec_is_a_message_on_the_deferred_path_too(populated_db):
    """Both resolve paths share `_invalid_spec`, so a role conflict cannot come
    back as a message on one and an exception on the other."""
    spec = _pooled_spec(populated_db)
    spec = _invalid_spec(spec)

    result = plot_service.resolve_figures(populated_db, spec, figure_index=0)

    assert result["ok"] is False
    assert "not a grouping layer" in result["error"]
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
    """The property the narrow hold exists for: nobody holds the database
    while `resolve_one` runs — that is the window MATLAB gets back.

    This flipped twice on 2026-09-13. A DuckDB-SQL reducer briefly queried the
    database from inside `resolve`; released first, it found the connection
    closed and silently fell back to pandas for 375 s, so the hold was widened
    to cover the reduction. Then the SQL reducer lost every measurement to
    numpy over the loaded cells and was removed — the reduction is pure memory
    again (`scistackplot.NumpyReducer`), and the hold is back to the load. See
    `_load`'s docstring and .claude/plan-plot-minimal-load-examples.md §8.
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

    spec = _pooled_spec(populated_db)
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

    spec = _pooled_spec(populated_db)
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
    ever reached a WRITE method, inspecting a study could quietly rewrite the
    run configuration or the database.

    TWO read methods now, not one: the same component serves the plotting tab
    (one variable under a variant) and the processing tab (one node's inputs,
    intersected). Both compute and return; neither writes. Everything else the
    component does is arithmetic on the reply.

    The method name is read out of the whole call expression rather than from
    the first token after ``callBackend(``, because it is chosen by a ternary —
    a regex anchored on the opening quote saw NO calls at all and passed the
    emptiness off as "reaches nothing".
    """
    import re

    source = _frontend_source(
        "frontend/src/components/PlotStudio/SchemaLocationPicker.tsx"
    )

    # Everything quoted in the call expression, up to the params object (`{`)
    # or the closing paren of a bare call — which is where the method name is,
    # ternary or not.
    called = {
        name
        for chunk in re.findall(r"callBackend\(([\s\S]*?)[{)]", source)
        for name in re.findall(r"'([^']+)'", chunk)
    }
    assert called == {"plot_location_tree", "node_location_tree"}, (
        f"the location picker calls {sorted(called)}; it is a read-only view "
        f"and must reach only read methods"
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


# --- the reduction never reaches the database ---------------------------------
#
# 2026-09-13: a DuckDB-SQL reducer briefly ran queries from inside `resolve`;
# under the per-request policy it found the connection closed and silently fell
# back to pandas (375 s of y_limits). The suite runs under the persistent policy,
# where `db_connection` is a no-op, which is why nothing caught it. The SQL
# reducer is gone (numpy won every measurement) — these pin that a resolve and
# a save do their reduction with the connection released and touch nothing that
# would fail on a closed one.


def _nothing_touched_a_closed_connection(caplog) -> None:
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "Connection already closed" not in text
    assert "pandas fallback" not in text, text
    assert "FAILED" not in text, text


def test_resolve_reduces_with_the_connection_released(
    populated_db, per_request_policy, monkeypatch, caplog
):
    import logging

    import scistackplot

    spec = _pooled_spec(populated_db)
    assert per_request_policy._db_open is False

    seen: dict = {}
    original = scistackplot.resolve

    def spy(*args, **kwargs):
        seen["open_during_resolve"] = per_request_policy._db_open
        return original(*args, **kwargs)

    monkeypatch.setattr(scistackplot, "resolve", spy)

    with caplog.at_level(logging.INFO):
        result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is True
    assert seen["open_during_resolve"] is False
    _nothing_touched_a_closed_connection(caplog)
    assert per_request_policy._db_open is False
    assert per_request_policy._db_refcount == 0


def test_save_reduces_and_renders_with_the_connection_released(
    populated_db, per_request_policy, monkeypatch, caplog, tmp_path
):
    """The save's promise to MATLAB: the file is free for everything after the
    load — the reduction and the minutes matplotlib spends drawing."""
    import logging

    import scistackplot

    spec = _pooled_spec(populated_db)

    seen: dict = {}
    original_resolve = scistackplot.resolve
    original_render = scistackplot.render_matplotlib

    def spy_resolve(*args, **kwargs):
        seen["open_during_resolve"] = per_request_policy._db_open
        return original_resolve(*args, **kwargs)

    def spy_render(*args, **kwargs):
        seen["open_during_render"] = per_request_policy._db_open
        return original_render(*args, **kwargs)

    monkeypatch.setattr(scistackplot, "resolve", spy_resolve)
    monkeypatch.setattr(scistackplot, "render_matplotlib", spy_render)

    with caplog.at_level(logging.INFO):
        result = plot_service.save_figure(populated_db, spec, str(tmp_path / "f.png"))

    assert result["ok"] is True, result
    assert seen["open_during_resolve"] is False
    assert seen["open_during_render"] is False
    _nothing_touched_a_closed_connection(caplog)


def test_an_invalid_spec_releases_the_hold(populated_db, per_request_policy):
    """The early return inside the hold must still release it."""
    spec = _pooled_spec(populated_db)
    spec = _invalid_spec(spec)

    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is False
    assert per_request_policy._db_open is False
    assert per_request_policy._db_refcount == 0


# --- a 1-D measure drawn by a scalar kind ------------------------------------


def test_capabilities_offer_the_scalar_kinds_for_a_1d_variable(populated_db):
    """RawSignal holds a 10-sample vector per record. Collapsing each vector to
    its centre makes the scalar kinds meaningful for it, and the panel learns
    that from the report alone."""
    spec = _pooled_spec(populated_db)

    report = plot_service.capabilities_for(populated_db, spec)

    assert report["cell_collapse"]["applies"] is True
    assert report["cell_collapse"]["active"] is False, "a band draws the samples"
    assert "violin" in report["available"]
    assert "band" in report["available"], "and the 1-D kinds stay available"


def test_selecting_a_scalar_kind_reports_a_scalar_figure(populated_db):
    spec = _pooled_spec(populated_db)
    spec["kind"] = "violin"

    report = plot_service.capabilities_for(populated_db, spec)

    assert report["shape"] == "scalar", "what the figure is"
    assert report["raw_shape"] == "1d", "what the data is"
    assert report["cell_collapse"]["active"] is True
    # The panel must still be able to go back.
    assert "line" in report["available"]


def test_a_collapsed_1d_variable_resolves_to_one_point_per_record(populated_db):
    spec = _pooled_spec(populated_db)
    spec["kind"] = "violin"
    spec["cell_statistic"] = "median"

    result = plot_service.resolve_figures(populated_db, spec)

    assert result["ok"] is True
    rows = sum(
        len(trace.get("y") or [])
        for figure in result["figures"]
        for trace in figure["figure"]["data"]
    )
    assert 0 < rows <= 4, "four records, not forty samples"


def test_the_kind_list_carries_the_roles_a_scalar_kind_would_open_with(populated_db):
    """The opening spec iterates every schema key, which leaves no sample.
    The panel applies these synchronously when the user picks a violin, so the
    first click draws a distribution instead of doing nothing visible.

    This fixture has exactly TWO factors: the scalar default groups `session`
    (the deepest key) and iterates `subject` — one point per figure, nothing
    to distribute — so a distribution kind also collapses the key just
    outside the grouped one.
    """
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]

    report = plot_service.capabilities_for(populated_db, spec)
    kinds = {info["kind"]: info for info in report["kinds"]}

    assert kinds["violin"]["collapses"] is True
    assignment = kinds["violin"]["assignment"]
    assert assignment, "a shape change, so a suggestion"
    assert assignment["roles"]["session"] == "group"
    assert assignment["roles"]["subject"] == "collapse"
    assert assignment["groups"] == ["session"]
    assert kinds["band"]["assignment"] is None, "no shape change, no suggestion"


def test_a_violin_is_clickable_from_the_opening_state(populated_db):
    """And the loop closes: the opening roles leave no replicates, so the kind
    that would supply them has to be selectable anyway — it is judged against
    the roles it brings with it, not against the ones on screen."""
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]

    report = plot_service.capabilities_for(populated_db, spec)

    assert report["has_sample"] is False
    assert "violin" in report["available"]
    assert "band" not in report["available"], "BAND brings no roles of its own"


def test_the_export_of_a_collapsed_1d_variable_runs(populated_db):
    pytest.importorskip("seaborn")
    spec = _pooled_spec(populated_db)
    spec["kind"] = "violin"

    result = plot_service.export_code(populated_db, spec)

    assert "np.asarray" in result["function_source"]
    assert ".explode(" not in result["function_source"]
    compile(result["source"], "<generated>", "exec")


# --- grouping: which variables, then one variable's columns ----------------
#
# `describe` used to answer both at once, so opening the panel paid every wide
# variable's per-column queries whether or not anyone looked. On the user's
# project that was ~8 s (scidb.log 2026-09-15): 4.2 s for FilteredDelsys and
# 5.4 s for RawEMG, two variables that offer no groupings at all. The columns
# are now `plot_grouping_columns`, asked once per variable the user opens.


@pytest.fixture
def grouping_db(populated_db):
    """``populated_db`` plus a subject-level wide sheet to group by.

    Defined inside the fixture rather than in conftest so it lands in the
    per-test subclass cleanup (`BaseVariable._all_subclasses` is restored after
    every test) and no existing test gains a variable it was not written
    against.
    """
    from scidb import BaseVariable

    class Demographics(BaseVariable):
        schema_version = 1

    rows = {
        1: {"Age": 64.0, "Sex": "F", "Site": "Boston"},
        2: {"Age": 71.0, "Sex": "M", "Site": "Boston"},
    }
    for subject, row in rows.items():
        Demographics.save(row, subject=subject)
    return populated_db


@pytest.fixture
def column_query_spy(monkeypatch):
    """Records every per-column query the source makes."""
    import scistackplotdb.source as source_module

    calls: list[tuple[str, str]] = []
    real_sample = source_module.sample_column_value
    real_levels = source_module.column_levels

    def sample(db, variable, column):
        calls.append(("sample", variable, column))
        return real_sample(db, variable, column)

    def levels(db, variable, column, *, limit):
        calls.append(("levels", variable, column))
        return real_levels(db, variable, column, limit=limit)

    monkeypatch.setattr(source_module, "sample_column_value", sample)
    monkeypatch.setattr(source_module, "column_levels", levels)
    return calls


def test_describe_ships_variables_never_their_columns(grouping_db):
    described = plot_service.describe(grouping_db, "RawSignal")

    assert "groupable_with" not in described, (
        "describe must no longer enumerate columns — that is what cost seconds"
    )
    by_name = {o["variable"]: o for o in described["groupable_variables"]}
    assert by_name["Demographics"]["kind"] == "columns"
    assert by_name["Demographics"]["column_count"] == 3
    # WHICH of them qualify is deliberately absent: it is the click's question.
    assert "offer" not in by_name["Demographics"]


def test_opening_the_panel_makes_no_column_query(grouping_db, column_query_spy):
    """The end-to-end pin on the ~8 s. A per-column query here is one paid on
    every panel open, by every user, whether or not they ever group anything."""
    plot_service.describe(grouping_db, "RawSignal")

    assert not [
        call for call in column_query_spy if call[1] == "Demographics"
    ], f"describe queried the sheet's columns: {column_query_spy}"


def test_grouping_graph_is_the_source_s_answer(grouping_db):
    """The service renders what scistackplotdb decided (CLAUDE.md NOTE 3); it
    does not get a vote on what may group a figure."""
    source = plot_service.get_source(grouping_db)

    assert plot_service.grouping_graph(grouping_db, "RawSignal") == (
        source.groupable_variables("RawSignal")
    )


def test_grouping_columns_answers_for_one_variable(grouping_db):
    report = plot_service.grouping_columns(grouping_db, "RawSignal", "Demographics")

    assert {o["label"] for o in report["offered"]} == {"Demographics.Sex"}
    assert "not offered yet" in report["rejected"]["Demographics.Age"]
    assert "one value" in report["rejected"]["Demographics.Site"]


def test_grouping_columns_is_the_only_thing_that_reads_columns(
    grouping_db, column_query_spy
):
    """The cost moved, it did not vanish — and it must land here, on the
    variable the user actually opened, and on no other."""
    plot_service.describe(grouping_db, "RawSignal")
    assert not column_query_spy

    plot_service.grouping_columns(grouping_db, "RawSignal", "Demographics")

    assert {call[1] for call in column_query_spy} == {"Demographics"}


def test_grouping_default_variant_is_the_latest_rule(grouping_db):
    """One rule for "latest", shared with the Variants section — never a second
    implementation that could disagree with it."""
    source = plot_service.get_source(grouping_db)
    answer = plot_service.grouping_default_variant(grouping_db, "Demographics")

    assert answer["variable"] == "Demographics"
    assert answer["selection"] == source.default_variant_for("Demographics")


def test_the_grouping_calls_change_nothing(grouping_db):
    """Read-only, like every picker call. The Variants popup is held to this by
    `test_variant_mode_nodes_never_call_the_backend`; the grouping picker DOES
    call the backend (it has to fetch columns), so the property that matters is
    narrower and is asserted here instead: looking must not alter what a run
    would do, or what the figure holds."""
    before = plot_service.describe(grouping_db, "RawSignal")

    plot_service.grouping_graph(grouping_db, "RawSignal")
    plot_service.grouping_columns(grouping_db, "RawSignal", "Demographics")
    plot_service.grouping_default_variant(grouping_db, "Demographics")

    assert plot_service.describe(grouping_db, "RawSignal") == before


def test_the_grouping_methods_are_registered_and_self_managed():
    """A plot method that takes the connection inside the service MUST be in
    `SELF_MANAGED_DB_METHODS`, or the server holds the file lock across the
    whole request — which is what blocked MATLAB for 31 s once already."""
    from scistack_gui import server

    for method in (
        "plot_grouping_graph",
        "plot_grouping_columns",
        "plot_grouping_default_variant",
    ):
        assert method in server.METHODS, f"{method} is not dispatchable"
        assert method in server.SELF_MANAGED_DB_METHODS, (
            f"{method} takes its own connection but is not declared self-managed"
        )


# --- the grouping picker's own boundary ------------------------------------


def test_the_grouping_popup_calls_only_read_rpcs():
    """The variant popup's node components may call NOTHING
    (`test_variant_mode_nodes_never_call_the_backend`). The grouping popup is
    different in kind: it has to fetch a variable's columns, which is the whole
    point of moving that cost off the panel open.

    So the property is narrower and it is this one — every RPC it makes is a
    READ. If a grouping dialog ever reached a write, choosing what to look at
    would quietly rewrite the run configuration, which is the same failure the
    variant guard exists to prevent, arriving through a door left open on
    purpose.
    """
    import re
    from pathlib import Path

    source = (
        Path(__file__).parent.parent
        / "frontend/src/components/PlotStudio/GroupingDagPopup.tsx"
    ).read_text()
    called = set(re.findall(r"callBackend\(\s*'([a-z_]+)'", source))

    assert called, "the popup should fetch something — it lists columns"
    assert called <= {"plot_grouping_columns", "plot_variant_graph"}, (
        f"unexpected RPC(s) from the grouping popup: "
        f"{called - {'plot_grouping_columns', 'plot_variant_graph'}}"
    )


def test_the_shared_picker_shell_is_read_only():
    """`DagPicker` draws the canvas for both pickers, so a write from there
    would reach the variant popup too — behind the guard that was written
    before the shell existed."""
    import re
    from pathlib import Path

    source = (
        Path(__file__).parent.parent
        / "frontend/src/components/PlotStudio/DagPicker.tsx"
    ).read_text()
    called = set(re.findall(r"callBackend\(\s*'([a-z_]+)'", source))

    assert called == {"get_pipeline", "get_layout"}, called


# --- saving the plot's data (CSV) --------------------------------------------
#
# "Save data" writes scistackplot.plot_data — the rows the figure is drawn
# from — so these check the adaptation: the file IS that frame, a spec that
# cannot be exported is a message, and the job path reports like an image save.
# The numbers themselves are pinned in scistackplot/tests/test_plot_data.py.


def _scalar_box_spec(db) -> dict:
    """RawSignal drawn as a box: the kind implies the per-record cell
    collapse, so the drawn measure is scalar and its data can be saved.
    subject grouped (coloured), session collapsed — the sample."""
    spec = _pooled_spec(db)
    spec["kind"] = "box"
    return spec


def test_save_plot_data_writes_what_plot_data_returns(populated_db, tmp_path):
    import pandas as pd
    from scistackplot import plot_data

    spec = _scalar_box_spec(populated_db)
    target = tmp_path / "data.csv"

    result = plot_service.save_plot_data(populated_db, spec, str(target))

    assert result["ok"] is True, result
    assert result["files"] == [str(target)]
    _, spec_obj, table = plot_service._load(
        populated_db, spec, csv_path=None, label="test"
    )
    expected = plot_data(spec_obj, table)
    written = pd.read_csv(target, dtype=str)
    assert list(written.columns) == [str(c) for c in expected.columns]
    assert result["columns"] == list(written.columns)
    assert len(written) == len(expected) == result["rows"] == 4  # 2 subjects x 2 sessions
    assert pd.to_numeric(written["RawSignal"]).to_numpy() == pytest.approx(
        pd.to_numeric(expected["RawSignal"]).to_numpy()
    )


def test_save_plot_data_into_a_folder_names_the_file(populated_db, tmp_path):
    result = plot_service.save_plot_data(
        populated_db, _scalar_box_spec(populated_db), str(tmp_path)
    )
    assert result["ok"] is True, result
    assert result["files"] == [str(tmp_path / "RawSignal_data.csv")]


def test_save_plot_data_adds_the_csv_suffix(populated_db, tmp_path):
    result = plot_service.save_plot_data(
        populated_db, _scalar_box_spec(populated_db), str(tmp_path / "nested" / "stats")
    )
    assert result["ok"] is True, result
    assert (tmp_path / "nested" / "stats.csv").exists()


def test_a_1d_plot_cannot_save_its_data_and_says_why(populated_db, tmp_path):
    target = tmp_path / "data.csv"
    result = plot_service.save_plot_data(populated_db, _pooled_spec(populated_db), str(target))
    assert result["ok"] is False
    assert "scalar" in result["error"]
    assert not target.exists()


def test_an_unknown_depth_is_a_message(populated_db, tmp_path):
    result = plot_service.save_plot_data(
        populated_db, _scalar_box_spec(populated_db), str(tmp_path / "d.csv"), depth="nope"
    )
    assert result["ok"] is False
    assert "nope" in result["error"]


def test_the_capability_report_offers_data_export(populated_db):
    report = plot_service.capabilities_for(populated_db, _scalar_box_spec(populated_db))
    assert report["data_export"]["available"] is True
    assert report["data_export"]["default"] == "session"
    refused = plot_service.capabilities_for(populated_db, _pooled_spec(populated_db))
    assert refused["data_export"]["available"] is False


def test_a_data_save_job_reports_like_an_image_save(
    populated_db, tmp_path, captured_pushes
):
    target = tmp_path / "data.csv"
    started = plot_service.start_save_job(
        populated_db, _scalar_box_spec(populated_db), str(target), what="data"
    )
    messages = _drain(captured_pushes, "plot_save_complete")
    done = next(m for m in messages if m["type"] == "plot_save_complete")
    assert done["files"] == [str(target)]
    assert {m["job_id"] for m in messages} == {started["job_id"]}
    stages = [m["stage"] for m in messages if m["type"] == "plot_save_progress"]
    assert stages == ["resolving", "writing"]
    assert target.exists()


def test_a_refused_data_save_job_ends_with_a_failure(
    populated_db, tmp_path, captured_pushes
):
    plot_service.start_save_job(
        populated_db, _pooled_spec(populated_db), str(tmp_path / "d.csv"), what="data"
    )
    messages = _drain(captured_pushes, "plot_save_failed")
    failed = next(m for m in messages if m["type"] == "plot_save_failed")
    assert "scalar" in failed["error"]


def test_an_unknown_save_kind_is_refused_before_a_thread_starts(populated_db, tmp_path):
    with pytest.raises(ValueError, match="image"):
        plot_service.start_save_job(
            populated_db, _scalar_box_spec(populated_db), str(tmp_path / "x"), what="movie"
        )


def test_fields_as_columns_reaches_plot_data(populated_db, tmp_path, monkeypatch):
    """The checkbox's value arrives at scistackplot.plot_data unchanged (the
    reshape itself is pinned in test_plot_data.py's struct tests)."""
    import scistackplot

    seen = []
    original = scistackplot.plot_data

    def spy(spec, table, **kwargs):
        seen.append(kwargs.get("fields_as_columns"))
        return original(spec, table, **kwargs)

    monkeypatch.setattr(scistackplot, "plot_data", spy)
    spec = _scalar_box_spec(populated_db)
    plot_service.save_plot_data(populated_db, spec, str(tmp_path / "a.csv"))
    plot_service.save_plot_data(
        populated_db, spec, str(tmp_path / "b.csv"), fields_as_columns=False
    )
    assert seen == [True, False], "default on, as the panel's checkbox"
