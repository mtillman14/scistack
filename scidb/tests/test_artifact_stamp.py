"""Tests for endpoint artifact provenance stamping (D4).

Every endpoint artifact — plot_ figures (PNG/SVG/PDF) and stat_ PDF reports —
gets an embedded JSON provenance blob: the artifact's own record_id (record
mode) or draft:true (draft mode), plus function, consumed input record_ids,
schema combo, database name, and timestamp. Drafts embed the FULL blob minus
the record_id. Unsupported/unparseable formats fall back to a
``<artifact>.provenance.json`` sidecar.
"""

import json as _json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pytest
from scidb.store.database import _local

import scifor as _scifor
from scidb import (
    BaseVariable,
    PathOutput,
    configure_database,
    for_each,
    read_artifact_stamp,
    stamp_artifact,
)

SCHEMA = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "stamps.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


class RawSignal(BaseVariable):
    pass


class PlotFigure(BaseVariable):
    pass


class StatResult(BaseVariable):
    pass


BLOB = {
    "scidb_stamp": 1,
    "record_id": "abc123",
    "function": "plot_demo",
    "inputs": {"signal": ["rid1", "rid2"]},
    "schema": {"subject": "S01"},
    "database": "demo.duckdb",
    "timestamp": "2026-07-06T12:00:00",
}


def _make_figure_file(path):
    fig, ax = plt.subplots()
    ax.plot([0.0, 1.0, 2.0])
    fig.savefig(str(path))
    plt.close(fig)


# ---------------------------------------------------------------------------
# 1. Unit round-trips per format (no DB)
# ---------------------------------------------------------------------------


class TestStampRoundTrips:
    @pytest.mark.parametrize("ext", ["png", "svg", "pdf"])
    def test_stamp_and_read_back(self, tmp_path, ext):
        path = tmp_path / f"fig.{ext}"
        _make_figure_file(path)
        original = path.read_bytes()

        assert stamp_artifact(path, BLOB) is True, f"in-file embed failed for {ext}"
        assert read_artifact_stamp(path) == BLOB
        # No sidecar when embedding succeeded.
        assert not (tmp_path / f"fig.{ext}.provenance.json").exists()

        stamped = path.read_bytes()
        if ext == "pdf":
            # Incremental update: every original byte untouched.
            assert stamped.startswith(original)
        assert len(stamped) > len(original)

    def test_png_still_loads_after_stamp(self, tmp_path):
        path = tmp_path / "fig.png"
        _make_figure_file(path)
        stamp_artifact(path, BLOB)
        img = plt.imread(str(path))
        assert img.size > 0

    def test_restamp_is_idempotent(self, tmp_path):
        """Stamping twice keeps exactly one (the latest) blob."""
        path = tmp_path / "fig.png"
        _make_figure_file(path)
        stamp_artifact(path, BLOB)
        updated = dict(BLOB, record_id="def456")
        stamp_artifact(path, updated)
        assert read_artifact_stamp(path)["record_id"] == "def456"


# ---------------------------------------------------------------------------
# 2. Fallbacks
# ---------------------------------------------------------------------------


class TestStampFallbacks:
    def test_unsupported_extension_writes_sidecar(self, tmp_path):
        path = tmp_path / "fig.jpg"
        path.write_bytes(b"\xff\xd8\xff not really a jpeg")
        assert stamp_artifact(path, BLOB) is False
        sidecar = tmp_path / "fig.jpg.provenance.json"
        assert sidecar.exists()
        assert read_artifact_stamp(path) == BLOB  # reader finds the sidecar

    def test_garbage_png_falls_back_to_sidecar(self, tmp_path):
        path = tmp_path / "fig.png"
        path.write_bytes(b"not a png at all")
        assert stamp_artifact(path, BLOB) is False
        assert (tmp_path / "fig.png.provenance.json").exists()

    def test_unparseable_pdf_falls_back_to_sidecar(self, tmp_path):
        path = tmp_path / "fig.pdf"
        path.write_bytes(b"%PDF-1.5 but no xref or trailer here")
        assert stamp_artifact(path, BLOB) is False
        assert (tmp_path / "fig.pdf.provenance.json").exists()
        assert read_artifact_stamp(path) == BLOB

    def test_missing_file_warns_not_crashes(self, tmp_path):
        assert stamp_artifact(tmp_path / "nope.png", BLOB) is False
        assert not (tmp_path / "nope.png.provenance.json").exists()
        assert read_artifact_stamp(tmp_path / "nope.png") is None

    def test_unstamped_file_reads_none(self, tmp_path):
        path = tmp_path / "fig.png"
        _make_figure_file(path)
        assert read_artifact_stamp(path) is None


# ---------------------------------------------------------------------------
# 3. plot_ integration: record and draft stamps
# ---------------------------------------------------------------------------


class TestPlotStamping:
    def _seed(self):
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0, 2.0]), subject=subj, trial="1")

    def test_record_stamp_matches_db(self, db, tmp_path):
        self._seed()

        def plot_sig(signal, filename):
            fig, ax = plt.subplots()
            ax.plot(np.asarray(signal).ravel())
            return fig

        for_each(
            plot_sig,
            {
                "signal": RawSignal,
                "filename": PathOutput(str(tmp_path / "{subject}_{trial}.png")),
            },
            [PlotFigure],
            finalized=True,
            subject=["S01", "S02"],
            trial=["1"],
        )

        rec = PlotFigure.load(subject="S01", trial="1")
        blob = read_artifact_stamp(tmp_path / "S01_1.png")
        assert blob is not None, "recorded figure must carry an embedded stamp"
        assert blob["record_id"] == rec.record_id
        assert blob["function"] == "plot_sig"
        assert blob["schema"] == {"subject": "S01", "trial": "1"}
        assert blob["database"] == "stamps.duckdb"
        assert "draft" not in blob
        # inputs name the exact consumed RawSignal record
        raw = RawSignal.load(subject="S01", trial="1")
        assert blob["inputs"] == {"signal": [raw.record_id]}

    def test_draft_stamp_is_full_minus_record_id(self, db, tmp_path):
        """Drafts embed everything a finalized run would, minus record_id."""
        self._seed()

        def plot_sig(signal, filename):
            fig, ax = plt.subplots()
            ax.plot(np.asarray(signal).ravel())
            return fig

        for_each(
            plot_sig,
            {
                "signal": RawSignal,
                "filename": PathOutput(str(tmp_path / "{subject}_{trial}.png")),
            },
            [PlotFigure],  # finalized default False -> draft
            subject=["S01", "S02"],
            trial=["1"],
        )

        assert db.list_versions(PlotFigure) == []
        blob = read_artifact_stamp(tmp_path / "S02_1.png")
        assert blob is not None, "draft figures must be stamped too"
        assert blob.get("draft") is True
        assert "record_id" not in blob
        assert blob["function"] == "plot_sig"
        assert blob["schema"] == {"subject": "S02", "trial": "1"}
        assert blob["database"] == "stamps.duckdb"
        raw = RawSignal.load(subject="S02", trial="1")
        assert blob["inputs"] == {"signal": [raw.record_id]}

    def test_rerender_updates_record_id(self, db, tmp_path):
        self._seed()

        def plot_sig(signal, filename):
            fig, ax = plt.subplots()
            ax.plot(np.asarray(signal).ravel())
            return fig

        kwargs = {
            "inputs": {
                "signal": RawSignal,
                "filename": PathOutput(str(tmp_path / "{subject}_{trial}.png")),
            },
            "outputs": [PlotFigure],
            "finalized": True,
            "subject": ["S01"],
            "trial": ["1"],
        }
        for_each(plot_sig, **kwargs)
        first = read_artifact_stamp(tmp_path / "S01_1.png")["record_id"]

        def plot_sig(signal, filename):  # noqa: F811 — changed body
            fig, ax = plt.subplots()
            ax.plot(np.asarray(signal).ravel() * 2.0)
            ax.set_title("v2")
            return fig

        for_each(plot_sig, **kwargs)
        second = read_artifact_stamp(tmp_path / "S01_1.png")["record_id"]
        assert second != first, "re-rendered artifact must point at its new record"


# ---------------------------------------------------------------------------
# 4. stat_ integration: PDF report stamping (incl. aggregation inputs)
# ---------------------------------------------------------------------------


class TestStatReportStamping:
    def test_record_stamp_on_pdf_report_with_aggregated_inputs(self, db, tmp_path):
        RawSignal.save(1.0, subject="S01", trial="1")
        RawSignal.save(2.0, subject="S02", trial="1")
        report = tmp_path / "report.pdf"

        def stat_summary(df, filename):
            fig = plt.figure()  # minimal real PDF, no csv-stats dependency
            fig.savefig(str(filename))
            plt.close(fig)
            return {"n_rows": len(df)}

        for_each(
            stat_summary,
            {"df": RawSignal, "filename": PathOutput(str(report))},
            [StatResult],
            finalized=True,
        )  # grand aggregation

        rec = StatResult.load()
        blob = read_artifact_stamp(report)
        assert blob is not None, "stat PDF report must carry an embedded stamp"
        assert blob["record_id"] == rec.record_id
        assert blob["function"] == "stat_summary"
        assert "draft" not in blob
        # Aggregation: BOTH contributing records, grouped under the param name
        # (indexed __upstream keys df_0/df_1 collapse back to "df").
        raw_rids = {
            RawSignal.load(subject=s, trial="1").record_id for s in ["S01", "S02"]
        }
        assert set(blob["inputs"].keys()) == {"df"}
        assert set(blob["inputs"]["df"]) == raw_rids
        # The stored record and the stamped file agree on the report path.
        assert _json.loads(rec.data)["report_path"] == str(report)

    def test_stat_draft_produces_no_artifact_to_stamp(self, db, tmp_path):
        """stat_ drafts resolve PathOutput to None: no PDF, no stamp, no sidecar."""
        RawSignal.save(1.0, subject="S01", trial="1")
        report = tmp_path / "draft_report.pdf"

        def stat_summary(df, filename):
            assert filename is None
            return {"n_rows": len(df)}

        for_each(
            stat_summary,
            {"df": RawSignal, "filename": PathOutput(str(report))},
            [StatResult],
        )

        assert not report.exists()
        assert not (tmp_path / "draft_report.pdf.provenance.json").exists()
        assert db.list_versions(StatResult) == []
