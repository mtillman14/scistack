"""GUI smoke paths in a real browser (grug stage gui_smoke_e2e,
docs/claude/grugbrain-principles.md).

pytest covers the backend in depth, but only these tests load the BUILT
frontend bundle against the real server. They are the few key paths grug asks
for, not a UI test suite:

* the canvas renders the seeded pipeline, with no uncaught page error;
* ▶ Run on a function node finishes with ``run_done`` success (read off the
  WebSocket, so it does not depend on run-log markup). The server runs on a
  random non-default port, so this also proves the WebSocket follows the
  page's port (``frontend/src/hooks/wsUrl.ts``);
* Open Plot Studio on a variable reaches ``/api/plot/describe``.

Opt-in: see gui_e2e.py. Run from the workspace root:

    pytest scistack-gui/tests/e2e -v -rs
"""

from __future__ import annotations

import textwrap

import pytest

from gui_e2e import require_playwright  # noqa: E402

require_playwright()

from gui_e2e import gui_server, node, open_canvas, page, seed, wait_for_frame  # noqa: E402,F401 (page is a fixture)

MODULE = textwrap.dedent(
    '''
    import numpy as np

    from scidb import BaseVariable


    class SmokeRaw(BaseVariable):
        pass


    class SmokeOut(BaseVariable):
        pass


    def smoke_double(signal):
        return np.asarray(signal, dtype=float) * 2.0
    '''
)

SEED = textwrap.dedent(
    '''
    import sys

    import numpy as np

    sys.path.insert(0, {project!r})
    from scidb import configure_database, for_each
    from smoke_steps import SmokeOut, SmokeRaw, smoke_double

    db = configure_database({db!r}, ["subject"])
    for s in ("1", "2"):
        SmokeRaw.save(np.arange(5.0), subject=s)
    for_each(smoke_double, {{"signal": SmokeRaw}}, [SmokeOut], subject=[])
    db.close()
    print("SEEDED")
    '''
)


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    project = tmp_path_factory.mktemp("smoke")
    (project / "smoke_steps.py").write_text(MODULE)
    db = project / "smoke.duckdb"
    seed(project, SEED.format(project=str(project), db=str(db)))
    with gui_server(project, [str(db), "--module", str(project / "smoke_steps.py")]) as srv:
        yield srv


def test_canvas_renders_without_page_errors(page, server):
    open_canvas(page, server)
    for kind, label in (("variable", "SmokeRaw"), ("variable", "SmokeOut"), ("function", "smoke_double")):
        assert node(page, kind, label).is_visible(), f"{kind} {label} not on the canvas (log: {server['log']})"
    assert page.smoke_errors == [], page.smoke_errors


def test_run_one_function_finishes(page, server):
    open_canvas(page, server)
    node(page, "function", "smoke_double").get_by_test_id("run-button").first.click()
    done = wait_for_frame(page, "run_done")
    assert done is not None, f"no run_done on the WebSocket (log: {server['log']})"
    assert done.get("success") is True, done
    assert page.smoke_errors == [], page.smoke_errors


def test_plot_studio_opens_on_a_variable(page, server):
    open_canvas(page, server)
    node(page, "variable", "SmokeOut").click()
    with page.expect_response(lambda r: "/api/plot/describe" in r.url) as described:
        page.get_by_test_id("open-plot-studio").click()
    assert described.value.status == 200, described.value.text()[:2000]
    assert page.smoke_errors == [], page.smoke_errors
