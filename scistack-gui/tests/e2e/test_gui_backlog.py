"""Browser regression tests for older "pytest passes, GUI unchecked" items
(grug follow-up 5). Each names its item in docs/gui-manual-testing-todo.md.

Only what the browser GUI can reach. Extension-only steps (VS Code commands,
opening a file in the editor, separate VS Code tabs) stay on the manual list.

The project is a real one (scistack.toml + an entities file), so the palette
can delete a declaration and undo can bring it back.

Opt-in: see gui_e2e.py.
"""

from __future__ import annotations

import textwrap

import pytest

from gui_e2e import require_playwright  # noqa: E402

require_playwright()

from gui_e2e import click, gui_server, node, open_canvas, page, screenshot, seed  # noqa: E402,F401 (page is a fixture)

STEPS = textwrap.dedent(
    '''
    import numpy as np

    from scidb import BaseVariable


    class BacklogRaw(BaseVariable):
        pass


    class BacklogOut(BaseVariable):
        pass


    def backlog_double(signal):
        return np.asarray(signal, dtype=float) * 2.0
    '''
)

#: SpareVar is declared only here and is on no canvas, so it is deletable.
ENTITIES = 'variables = ["SpareVar"]\n'

CONFIG = textwrap.dedent(
    '''
    modules = ["backlog_steps.py"]
    entities_file = "scistack_entities.toml"
    '''
)

SEED = textwrap.dedent(
    '''
    import sys

    import numpy as np

    sys.path.insert(0, {project!r})
    from scidb import configure_database, for_each
    from backlog_steps import BacklogOut, BacklogRaw, backlog_double

    db = configure_database({db!r}, ["subject"])
    for s in ("1", "2"):
        BacklogRaw.save(np.arange(5.0), subject=s)
    for_each(backlog_double, {{"signal": BacklogRaw}}, [BacklogOut], subject=[])
    db.close()
    print("SEEDED")
    '''
)

STATUSES = {"present", "missing_unexpected", "present_unexpected", "missing_expected", "excluded"}


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    project = tmp_path_factory.mktemp("backlog")
    (project / "backlog_steps.py").write_text(STEPS)
    (project / "scistack_entities.toml").write_text(ENTITIES)
    (project / "scistack.toml").write_text(CONFIG)
    db = project / "backlog.duckdb"
    seed(project, SEED.format(project=str(project), db=str(db)))
    with gui_server(project, [str(db), "--project", str(project)]) as srv:
        yield srv


def test_palette_search_narrows_the_list(page, server):
    """Item 0zzzzzzzzzzzzzzzz (palette search): typing narrows every row to
    the query; Esc clears it."""
    open_canvas(page, server)
    click(page, server, page.get_by_test_id("palette-tab-function"), "palette Functions tab")
    search = page.get_by_test_id("palette-search")
    search.fill("backlog")
    rows = page.get_by_test_id("palette-item")
    rows.first.wait_for()
    labels = [r.get_attribute("data-label") or "" for r in rows.all() if r.is_visible()]
    assert labels, "no palette row matched 'backlog'"
    assert all("backlog" in label.lower() for label in labels), labels
    search.press("Escape")
    assert search.input_value() == ""


def test_palette_delete_then_undo_restores_the_file(page, server):
    """Item 0zzzzzzzzzzzzzzzz steps 4-5: 🗑 on a declaration-only variable
    removes exactly that entry from the entities file; Ctrl/Cmd+Z puts the
    file back byte for byte."""
    entities = server["project"] / "scistack_entities.toml"
    before = entities.read_text()
    open_canvas(page, server)
    click(page, server, page.get_by_test_id("palette-tab-variable"), "palette Variables tab")
    page.get_by_test_id("palette-search").fill("SpareVar")
    row = page.locator('[data-testid="palette-item"][data-label="SpareVar"]').first
    row.hover()
    row.get_by_test_id("palette-delete").click()
    page.get_by_test_id("delete-declaration-confirm").click()
    row.wait_for(state="detached")
    assert "SpareVar" not in entities.read_text(), entities.read_text()

    page.locator("body").click(position={"x": 5, "y": 5})  # focus the page, not an input
    page.keyboard.press("ControlOrMeta+z")
    page.wait_for_function(
        "() => document.querySelector('[data-testid=\"palette-item\"][data-label=\"SpareVar\"]') !== null"
    )
    assert entities.read_text() == before


def test_expected_locations_lists_every_location(page, server):
    """Item 0zzzzzzzzzzzzzzzzzzzzzzz step 1: right-click a variable →
    Expected locations… shows one row per location, each with a known status.
    Both seeded subjects were computed, so both are present."""
    open_canvas(page, server)
    node(page, "variable", "BacklogOut").click(button="right")
    page.get_by_test_id("menu-expected-locations").click()
    popup = page.get_by_test_id("expected-locations-popup")
    rows = popup.get_by_test_id("expected-row")
    rows.first.wait_for()
    statuses = [r.get_attribute("data-status") for r in rows.all()]
    assert set(statuses) <= STATUSES, statuses
    assert statuses.count("present") == 2, statuses
    assert page.smoke_errors == [], page.smoke_errors


def test_coverage_has_a_row_per_subject(page, server):
    """Item 0zzzzzzzzzzzzzzzzzzzzzzz step 6: header ✅ Coverage by subject has
    one row per subject value."""
    open_canvas(page, server)
    page.get_by_test_id("coverage-button").click()
    popup = page.get_by_test_id("coverage-popup")
    popup.get_by_label("subject").check()
    rows = popup.get_by_test_id("coverage-row")
    rows.first.wait_for()
    assert rows.count() == 2, rows.count()
    assert page.smoke_errors == [], page.smoke_errors


def test_node_panel_header_and_back_to_palette(page, server):
    """Item 0zzzzzzzzzzzzzzzz (node panels) steps 1-2: selecting a node shows
    the shared header with its kind; ← Palette returns to the palette."""
    open_canvas(page, server)
    node(page, "function", "backlog_double").click()
    assert page.get_by_test_id("panel-kind").is_visible()
    page.get_by_test_id("panel-back-to-palette").click()
    page.get_by_test_id("palette-search").wait_for()


def test_plot_studio_rail_groups_all_render(page, server):
    """The Plot Studio split (grug plotstudio_split): the always-shown rail
    groups render after the move to PlotStudioRail.tsx + the state hooks."""
    open_canvas(page, server)
    node(page, "variable", "BacklogOut").click()
    with page.expect_response(lambda r: "/api/plot/describe" in r.url) as described:
        click(page, server, page.get_by_test_id("open-plot-studio"), "Open Plot Studio")
    resp = described.value
    body = resp.text()
    assert resp.status == 200, body[:2000]
    # The rail renders only for an eligible variable; say why when it is not.
    try:
        payload = resp.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict) and payload.get("eligible") is False:
        pytest.fail(f"BacklogOut is not plottable: {payload.get('reason')!r}")
    # Rail groups exist only in the Advanced view; a fresh studio opens in
    # Step by step.
    page.get_by_role("group", name="View").get_by_role("button", name="Advanced", exact=True).click()
    for title in ("Data", "Chart", "Structure", "Appearance"):
        group = page.get_by_test_id(f"rail-group-{title}")
        try:
            group.wait_for(timeout=20_000)
        except Exception as exc:
            shot = screenshot(page, server, f"rail-group-{title}-missing")
            text = page.locator("body").inner_text()[-1500:]
            raise AssertionError(
                f"rail group {title!r} never appeared. describe: {body[:800]}\n"
                f"page text (end): {text}\nscreenshot: {shot}\nserver log: {server['log']}"
            ) from exc
    assert page.smoke_errors == [], page.smoke_errors
