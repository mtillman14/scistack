"""
Endpoint-presentation tests (plan-endpoint-presentation.md, Part B item 1):
endpoint_kind tagging on graph nodes, the artifacts manifest endpoint, the
project-dir-guarded artifact file route, show-mode runs, and the report
trigger.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import scistack_gui.db as _gui_db
from conftest import FilteredSignal, RawSignal, bandpass_filter
from fastapi.testclient import TestClient
from scidb.database import _local
from scistack_gui import registry as _registry
from scistack_gui.app import create_app

from scidb import BaseVariable, configure_database, for_each


class StatSummary(BaseVariable):
    schema_version = 1


def stat_signal_mean(filtered):
    """stat_ endpoint: as_table default pools the combo's rows into a
    DataFrame; be robust to either shape."""
    if isinstance(filtered, pd.DataFrame):
        vals = np.concatenate([np.ravel(v) for v in filtered["FilteredSignal"]])
    else:
        vals = np.ravel(filtered)
    return {"mean": float(np.mean(vals)), "n": int(vals.size)}


@pytest.fixture
def endpoint_client(tmp_path):
    """bandpass (processing) + a FINALIZED stat_ endpoint run."""
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _gui_db._db = None

    db = configure_database(tmp_path / "endpoints.duckdb", ["subject", "session"])
    for subj in [1, 2]:
        for sess in ["pre", "post"]:
            RawSignal.save(np.random.randn(10), subject=subj, session=sess)

    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[FilteredSignal],
        subject=[1, 2],
        session=["pre", "post"],
    )
    for_each(
        stat_signal_mean,
        inputs={"filtered": FilteredSignal},
        outputs=[StatSummary],
        subject=[1, 2],
        session=["pre", "post"],
        finalized=True,
    )

    _gui_db._db = db
    _gui_db._db_path = tmp_path / "endpoints.duckdb"
    _registry._functions["bandpass_filter"] = bandpass_filter
    _registry._functions["stat_signal_mean"] = stat_signal_mean

    from scistack_gui import pipeline_store

    pipeline_store._ensure_tables(db)

    with TestClient(create_app()) as c:
        yield c, db, tmp_path

    db.close()


class TestEndpointKindTagging:
    def test_stat_node_tagged_processing_node_not(self, endpoint_client):
        client, _, _ = endpoint_client
        nodes = client.get("/api/pipeline").json()["nodes"]
        by_label = {
            n["data"]["label"]: n for n in nodes if n.get("type") == "functionNode"
        }
        assert by_label["stat_signal_mean"]["data"]["endpoint_kind"] == "stat"
        assert "endpoint_kind" not in by_label["bandpass_filter"]["data"]


class TestFunctionInfoEndpointKind:
    def test_drop_path_info_carries_endpoint_kind(self, endpoint_client):
        """Regression (2026-07-19): a freshly DRAGGED endpoint node builds
        its data from /api/function/{name}/params — without endpoint_kind
        there, the badge/Show button only appeared after the first run
        created DB history and a refetch re-tagged the node."""
        client, _, _ = endpoint_client
        info = client.get("/api/function/stat_signal_mean/params").json()
        assert info["endpoint_kind"] == "stat"
        info = client.get("/api/function/bandpass_filter/params").json()
        assert info["endpoint_kind"] is None


class TestArtifactsEndpoint:
    def test_finalized_stat_records_in_manifest(self, endpoint_client):
        client, _, _ = endpoint_client
        data = client.get("/api/endpoints/stat_signal_mean/artifacts").json()
        assert data["figures"] == []
        assert len(data["stats"]) == 4  # one per subject×session combo
        entry = data["stats"][0]
        assert entry["fn"] == "stat_signal_mean"
        assert entry["result_parsed"] is True
        assert "mean" in entry["result"] and "n" in entry["result"]
        assert set(entry["schema"]) == {"subject", "session"}

    def test_unknown_fn_is_empty_manifest(self, endpoint_client):
        client, _, _ = endpoint_client
        data = client.get("/api/endpoints/no_such_fn/artifacts").json()
        assert data["figures"] == [] and data["stats"] == []


class TestArtifactFileRoute:
    def test_serves_file_inside_project_dir(self, endpoint_client):
        client, _, tmp_path = endpoint_client
        artifact = tmp_path / "fig.png"
        artifact.write_bytes(b"\x89PNG\r\n\x1a\nfake")

        r = client.get("/api/artifacts/file", params={"path": str(artifact)})

        assert r.status_code == 200
        assert r.content.startswith(b"\x89PNG")

    def test_path_outside_project_dir_is_403(self, endpoint_client):
        client, _, _ = endpoint_client
        r = client.get("/api/artifacts/file", params={"path": __file__})
        assert r.status_code == 403

    def test_missing_file_inside_project_dir_is_404(self, endpoint_client):
        client, _, tmp_path = endpoint_client
        r = client.get(
            "/api/artifacts/file", params={"path": str(tmp_path / "nope.png")}
        )
        assert r.status_code == 404


class TestShowMode:
    def test_show_requires_target(self, endpoint_client):
        client, _, _ = endpoint_client
        r = client.post("/api/pipelines/main/run", json={"mode": "show"})
        assert r.status_code == 400

    def test_show_renders_drafts_without_new_records(self, endpoint_client):
        """show = draft-run one endpoint: rendered payloads come back, the
        endpoint writes NO new records. skip_computed=False forces the
        recompute so the assertion is deterministic even though a finalized
        run already exists."""
        client, db, _ = endpoint_client
        from scistack_gui.services.execution_service import run_pipeline

        n_stat_before = db._duck.fetchall(
            "SELECT COUNT(*) FROM _record WHERE type = 'StatSummary'"
        )[0][0]

        result = run_pipeline(
            db, "main", mode="show", target="stat_signal_mean", skip_computed=False
        )

        assert len(result["rendered"]) > 0
        assert "mean" in str(result["rendered"][0])
        n_stat_after = db._duck.fetchall(
            "SELECT COUNT(*) FROM _record WHERE type = 'StatSummary'"
        )[0][0]
        assert n_stat_after == n_stat_before  # draft: endpoint saves nothing

    def test_show_rejects_processing_target(self, endpoint_client):
        client, db, _ = endpoint_client
        from scistack_gui.services.execution_service import run_pipeline

        with pytest.raises(ValueError, match="endpoints"):
            run_pipeline(db, "main", mode="show", target="bandpass_filter")


class TestReport:
    def test_report_writes_index_html(self, endpoint_client):
        client, _, tmp_path = endpoint_client
        from pathlib import Path

        r = client.post("/api/report")

        assert r.status_code == 200
        index = Path(r.json()["index_path"])
        assert index.name == "index.html"
        assert index.is_file()
        assert str(tmp_path) in str(index)  # written inside the project dir
        # And the file route can serve it (how the frontend opens it).
        served = client.get("/api/artifacts/file", params={"path": str(index)})
        assert served.status_code == 200
