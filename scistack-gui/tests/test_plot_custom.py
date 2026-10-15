"""
Plot Studio opened on a plot_ node (services/custom_plot_service.py).

Custom plot kinds (.claude/plan-custom-plot-kinds.md): the user's own plot_
function draws the figure, and the node's edges bind its inputs. These tests
check the GUI half: the wiring is read through the run's owners, the panel
gets a custom spec and a capability report whose only kind is the function,
the preview is the function's drawing, Save writes it, and every refusal is a
message rather than a 500.
"""

import json

import pytest

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")
matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")

from scistack_gui import registry as _registry
from scistack_gui.services import custom_plot_service, layout_service, plot_service

NODE = "fn__plot_signals__a1b2c3"


@pytest.fixture(autouse=True)
def _clear_source_cache():
    plot_service.invalidate()
    yield
    plot_service.invalidate()


CALLS: list[dict] = []


def plot_signals(signal, filtered, filename, ax, context=None):
    """A panel-level plot_ function: both inputs onto one axes."""
    CALLS.append({"signal": signal, "filtered": filtered, "filename": filename})
    for frame, column in ((signal, "RawSignal"), (filtered, "FilteredSignal")):
        for cell in frame[column]:
            ax.plot(cell)


def plot_whole(signal, filename):
    """A figure-level plot_ function: returns its own figure."""
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()
    for cell in signal["RawSignal"]:
        ax.plot(cell)
    return fig


def plot_broken(signal, ax):
    raise ValueError("this plot_ function is broken")


def _place(db, function, node=NODE, wires=(("RawSignal", "signal"), ("FilteredSignal", "filtered"))):
    _registry._functions[function] = globals()[function]
    layout_service.put_layout(node, 0.0, 0.0, node_type="functionNode", label=function)
    for variable, param in wires:
        layout_service.put_edge(
            db,
            f"e_{variable}_{param}_{node}",
            f"var__{variable}",
            node,
            None,
            f"in__{param}",
        )


@pytest.fixture
def plot_node(populated_db):
    CALLS.clear()
    _place(populated_db, "plot_signals")
    return populated_db


def test_the_node_wiring_comes_from_its_edges(plot_node):
    wiring = custom_plot_service.node_wiring(plot_node, NODE)
    assert wiring.function == "plot_signals"
    assert wiring.variables == {"signal": "RawSignal", "filtered": "FilteredSignal"}


def test_describe_opens_on_the_function(plot_node):
    result = custom_plot_service.describe_function(plot_node, NODE)
    json.dumps(result)
    spec = result["spec"]
    assert spec["kind"] == "custom"
    assert spec["custom"]["function"] == "plot_signals"
    assert spec["custom"]["node"] == NODE
    caps = result["capabilities"]
    assert [k["kind"] for k in caps["kinds"]] == ["custom"]
    assert caps["custom"]["level"] == "panel"
    assert caps["custom"]["reason"] is None
    assert result["plot_node"]["function"] == "plot_signals"
    # The unwired output path is passed None, and said so.
    assert any("filename" in note for note in result["plot_node"]["notes"])


def test_resolve_previews_the_functions_drawing(plot_node):
    spec = custom_plot_service.describe_function(plot_node, NODE)["spec"]
    result = plot_service.resolve_figures(plot_node, spec, figure_index=0)
    assert result["ok"] is True, result.get("error")
    figure = result["figures"][0]
    assert figure["figure"] is None
    assert figure["custom_preview"]["kind"] == "image"
    assert figure["custom_preview"]["meta"]["preview"]["mode"] == "export"
    assert CALLS and CALLS[-1]["filename"] is None
    # The opening default iterates every key: one record per input per call.
    assert len(CALLS[-1]["signal"]) == 1
    json.dumps(result)


def test_a_pane_preview_is_drawn_at_the_pane_size(plot_node):
    spec = custom_plot_service.describe_function(plot_node, NODE)["spec"]
    result = plot_service.resolve_figures(
        plot_node, spec, figure_index=0, preview={"mode": "pane", "width_px": 360, "height_px": 216}
    )
    payload = result["figures"][0]["custom_preview"]
    assert (payload["width_in"], payload["height_in"]) == pytest.approx((5.0, 3.0))


def test_capabilities_route_to_the_function(plot_node):
    spec = custom_plot_service.describe_function(plot_node, NODE)["spec"]
    caps = plot_service.capabilities_for(plot_node, spec)
    assert caps["available"] == ["custom"]
    assert caps["data_export"]["available"] is False


def test_save_writes_the_functions_figure(plot_node, tmp_path):
    spec = custom_plot_service.describe_function(plot_node, NODE)["spec"]
    out = tmp_path / "custom.png"
    result = plot_service.save_figure(plot_node, spec, str(out), figure_index=0, dpi=50)
    assert result["ok"] is True, result.get("error")
    assert result["files"] and result["files"][0].endswith(".png")


def test_a_figure_level_function_opens_and_saves(populated_db, tmp_path):
    node = "fn__plot_whole__d4e5f6"
    _place(populated_db, "plot_whole", node=node, wires=(("RawSignal", "signal"),))
    described = custom_plot_service.describe_function(populated_db, node)
    assert described["capabilities"]["custom"]["level"] == "figure"
    result = plot_service.resolve_figures(populated_db, described["spec"], figure_index=0)
    assert result["ok"] is True, result.get("error")
    assert result["figures"][0]["custom_preview"]["kind"] == "image"


def test_a_function_that_raises_is_a_message(populated_db):
    node = "fn__plot_broken__a9b8c7"
    _place(populated_db, "plot_broken", node=node, wires=(("RawSignal", "signal"),))
    spec = custom_plot_service.describe_function(populated_db, node)["spec"]
    result = plot_service.resolve_figures(populated_db, spec, figure_index=0)
    assert result["ok"] is False
    assert "plot_broken raised ValueError" in result["error"]


def test_a_node_with_no_variable_wired_is_refused(populated_db):
    node = "fn__plot_signals__ffffff"
    _place(populated_db, "plot_signals", node=node, wires=())
    with pytest.raises(custom_plot_service.CustomPlotError, match="no variable input"):
        custom_plot_service.describe_function(populated_db, node)


def test_only_plot_functions_open(populated_db):
    node = "fn__bandpass_filter__abcdef"
    layout_service.put_layout(node, 0.0, 0.0, node_type="functionNode", label="bandpass_filter")
    with pytest.raises(custom_plot_service.CustomPlotError, match="not a plot_ function"):
        custom_plot_service.node_wiring(populated_db, node)


def test_export_and_save_data_are_refused_for_a_custom_figure(plot_node, tmp_path):
    spec = custom_plot_service.describe_function(plot_node, NODE)["spec"]
    with pytest.raises(ValueError, match="plot_ function"):
        plot_service.export_code(plot_node, spec)
    result = plot_service.save_plot_data(plot_node, spec, str(tmp_path / "data.csv"))
    assert result["ok"] is False
