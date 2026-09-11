"""
Plot Studio backend (services/plot_service.py).

The service is an adapter: policy lives in scistackplot/scistackplotdb, and
these tests check the adaptation — that the panel gets a usable spec in one
round trip, that a bad spec comes back as a message instead of a 500, that the
JSON crossing the webview boundary is actually serializable, and that both
transports reach the same code.
"""

import json

import pytest

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

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


def test_save_figure_reports_a_bad_spec_instead_of_raising(populated_db, tmp_path):
    spec = plot_service.describe(populated_db, "RawSignal")["spec"]
    spec["roles"] = {**spec["roles"], "subject": "x", "session": "x"}

    result = plot_service.save_figure(populated_db, spec, str(tmp_path / "x.png"))
    assert result["ok"] is False
    assert result["files"] == []


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


# --- both transports reach the same code -----------------------------------


def test_http_route_and_rpc_handler_share_the_service(client, populated_db):
    from scistack_gui.server import METHODS

    response = client.post("/api/plot/describe", json={"variable": "RawSignal"})
    assert response.status_code == 200
    http_result = response.json()

    rpc_result = METHODS["plot_describe"]({"variable": "RawSignal"})
    assert rpc_result["spec"] == http_result["spec"]


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
