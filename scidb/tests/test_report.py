"""Tests for the endpoint report surface (`scidb report` / Inspector.report).

Seeds a real pipeline: two branch_param variants of a processed signal ->
recorded plot_ (one PNG per variant via the {low_hz} placeholder) +
recorded stat_ (one JSON record per variant, one carrying a PDF report) +
a plain processing output (must NOT appear) + a draft endpoint run (must
NOT appear). Discovery, stamp verification, missing-file handling, the
written folder, and the CLI subcommand are all exercised against it.
"""

import json as _json
import re as _re

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
    stamp_artifact,
)

SCHEMA = ["subject", "session"]


class RawSignal(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


class PlotOut(BaseVariable):
    pass


class StatOut(BaseVariable):
    pass


class DraftOut(BaseVariable):
    pass


def bandpass(signal, low_hz):
    return signal * low_hz


@pytest.fixture
def seeded(tmp_path):
    """db + artifact dir with the full endpoint fixture pipeline."""
    _scifor.set_schema([])
    db = configure_database(tmp_path / "report.duckdb", SCHEMA)
    art = tmp_path / "figs"
    art.mkdir()

    for subj in ["S01", "S02"]:
        RawSignal.save(np.array([1.0, 2.0]), subject=subj, session="1")
    for low_hz in [20, 30]:
        for_each(
            bandpass,
            {"signal": RawSignal, "low_hz": low_hz},
            [Filtered],
            subject=["S01", "S02"],
            session=["1"],
        )

    def plot_sig(signal, filename):
        fig, ax = plt.subplots()
        ax.plot(np.asarray(signal).ravel())
        return fig

    for_each(
        plot_sig,
        {
            "signal": Filtered,
            "filename": PathOutput(str(art / "{subject}_{low_hz}.png")),
        },
        [PlotOut],
        subject=["S01", "S02"],
        session=["1"],
    )

    report_pdf = art / "stat_{low_hz}.pdf"

    def stat_mean(df, filename):
        fig = plt.figure()
        fig.savefig(str(filename))
        plt.close(fig)
        # Cells are ARRAYS (the seed saves np.array([1.0, 2.0])): sum each
        # cell — float(cell) would raise and silently [skip] every combo.
        vals = [float(np.asarray(v).sum()) for v in df["Filtered"]]
        return {
            "mean": sum(vals) / len(vals),
            "n": len(vals),
            "assumptions": {"normality": "n/a"},
        }

    for_each(
        stat_mean,
        {"df": Filtered, "filename": PathOutput(str(report_pdf))},
        [StatOut],
    )  # grand aggregation, per variant

    # A DRAFT endpoint run: must never appear in the report.
    def stat_draft_only(df):
        return {"n": len(df)}

    for_each(stat_draft_only, {"df": Filtered}, [DraftOut], save=False)

    yield db, art
    _scifor.set_schema([])
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


# ---------------------------------------------------------------------------
# 1. Discovery
# ---------------------------------------------------------------------------


class TestDiscovery:
    def test_finds_exactly_the_endpoint_records(self, seeded):
        db, art = seeded
        data = db.inspect.report()

        # plot_: 2 subjects x 2 variants = 4 figures.
        assert len(data.figures) == 4
        assert all(f.fn == "plot_sig" for f in data.figures)
        assert all(f.artifact_exists for f in data.figures)
        # stat_: one per variant group = 2 entries; parsed results.
        stat_fns = {s.fn for s in data.stats}
        assert stat_fns == {"stat_mean"}
        assert len(data.stats) == 2
        assert all(s.result_parsed and "mean" in s.result for s in data.stats)
        # Processing outputs and drafts are absent.
        vars_present = {f.variable for f in data.figures} | {
            s.variable for s in data.stats
        }
        assert "Filtered" not in vars_present
        assert "DraftOut" not in vars_present

    def test_variant_identity_present(self, seeded):
        db, _ = seeded
        data = db.inspect.report()
        lows = sorted(s.branch_params.get("bandpass.low_hz") for s in data.stats)
        assert lows == [20, 30]

    def test_fn_and_var_filters(self, seeded):
        db, _ = seeded
        only_stats = db.inspect.report(fn="stat_mean")
        assert only_stats.figures == [] and len(only_stats.stats) == 2
        only_plots = db.inspect.report(variable=PlotOut)
        assert len(only_plots.figures) == 4 and only_plots.stats == []

    def test_root_level_records_appear(self, seeded):
        """Grand-aggregation stats save at NULL schema_id — the LEFT JOIN
        regression (the stage-2 skip-gate trap)."""
        db, _ = seeded
        data = db.inspect.report(fn="stat_mean")
        assert len(data.stats) == 2
        assert all(s.schema == {} for s in data.stats)


# ---------------------------------------------------------------------------
# 2. Stamp verification + missing artifacts
# ---------------------------------------------------------------------------


class TestArtifactVerification:
    def test_stamps_verify_ok(self, seeded):
        db, _ = seeded
        data = db.inspect.report()
        assert all(f.stamp_ok is True for f in data.figures), [
            (f.artifact_path, f.stamp_ok) for f in data.figures
        ]

    def test_overwritten_artifact_flags_stale(self, seeded):
        db, art = seeded
        victim = next(iter(sorted(art.glob("S01_*.png"))))
        stamp_artifact(
            victim,
            {"scidb_stamp": 1, "record_id": "not-the-one", "function": "elsewhere"},
        )
        data = db.inspect.report()
        stale = [f for f in data.figures if f.artifact_path == str(victim)]
        assert stale and stale[0].stamp_ok is False
        assert any("STALE" in w for w in data.warnings)

    def test_missing_artifact_warns_but_reports(self, seeded, tmp_path):
        db, art = seeded
        victim = next(iter(sorted(art.glob("S02_*.png"))))
        victim.unlink()
        data = db.inspect.report()
        gone = [f for f in data.figures if f.artifact_path == str(victim)]
        assert gone and gone[0].artifact_exists is False
        assert any("not found" in w for w in data.warnings)
        # write_report still succeeds and renders the entry's metadata.
        index = db.inspect.write_report(tmp_path / "out")
        html_text = index.read_text()
        assert gone[0].record_id[:12] in html_text


# ---------------------------------------------------------------------------
# 3. write_report output
# ---------------------------------------------------------------------------


class TestWriteReport:
    def test_folder_contents(self, seeded, tmp_path):
        db, _ = seeded
        out = tmp_path / "out"
        index = db.inspect.write_report(out)

        assert index == out / "index.html" and index.is_file()
        html_text = index.read_text()
        # Self-contained: no external requests.
        assert not _re.search(r"https?://", html_text)
        # Every record represented.
        data = _json.loads((out / "manifest.json").read_text())
        for entry in data["figures"] + data["stats"]:
            assert entry["record_id"][:12] in html_text
        # Variant identity labels present.
        assert "bandpass.low_hz" in html_text
        # Artifact copies: 4 figures + 2 stat PDFs, record_id-prefixed.
        copies = list((out / "artifacts").iterdir())
        assert len(copies) == 6
        # Embedded images (default): PNGs inline as data URIs.
        assert "data:image/png;base64," in html_text

    def test_stats_csv(self, seeded, tmp_path):
        import pandas as pd

        db, _ = seeded
        out = tmp_path / "out"
        db.inspect.write_report(out)
        df = pd.read_csv(out / "stats.csv")
        assert len(df) == 2
        assert "mean" in df.columns and "n" in df.columns
        assert "test_family" in df.columns
        assert sorted(df["bandpass.low_hz"]) == [20, 30]

    def test_no_copy_and_no_embed(self, seeded, tmp_path):
        db, _ = seeded
        out = tmp_path / "out"
        db.inspect.write_report(out, copy_artifacts=False, embed=False)
        assert not (out / "artifacts").exists()
        html_text = (out / "index.html").read_text()
        assert "data:image/png" not in html_text
        assert '<img src="' in html_text  # links to original absolute paths

    def test_all_versions_includes_superseded(self, seeded, tmp_path):
        db, art = seeded

        # Same fn name + same inputs (incl. the SAME PathOutput template) but
        # a changed body: the new records SUPERSEDE the old ones in the same
        # variant, rather than coexisting as a new variant.
        def stat_mean(df, filename):
            fig = plt.figure()
            fig.savefig(str(filename))
            plt.close(fig)
            vals = [float(np.asarray(v).sum()) for v in df["Filtered"]]
            return {"mean": sum(vals) / len(vals), "n": len(vals), "version": 2}

        for_each(
            stat_mean,
            {"df": Filtered, "filename": PathOutput(str(art / "stat_{low_hz}.pdf"))},
            [StatOut],
        )

        latest = db.inspect.report(fn="stat_mean")
        everything = db.inspect.report(fn="stat_mean", all_versions=True)
        assert len(everything.stats) > len(latest.stats)


# ---------------------------------------------------------------------------
# 4. CLI
# ---------------------------------------------------------------------------


class TestReportCli:
    def test_report_json(self, seeded, capsys):
        from scidb.inspect.cli import main

        db, _ = seeded
        db_file = str(db.dataset_db_path)
        db.close()  # CLI opens its own read-only connection
        if hasattr(_local, "database"):
            delattr(_local, "database")

        rc = main(["--db", db_file, "report", "--json"])
        out = capsys.readouterr().out
        assert rc == 0, out
        payload = _json.loads(out)
        assert len(payload["figures"]) == 4
        assert len(payload["stats"]) == 2

    def test_report_writes_folder(self, seeded, tmp_path, capsys):
        from scidb.inspect.cli import main

        db, _ = seeded
        db_file = str(db.dataset_db_path)
        db.close()
        if hasattr(_local, "database"):
            delattr(_local, "database")

        out_dir = tmp_path / "cli-report"
        rc = main(["--db", db_file, "report", "-o", str(out_dir)])
        assert rc == 0
        assert (out_dir / "index.html").is_file()
        assert "Report written" in capsys.readouterr().out

    def test_bad_fn_filter_is_clean(self, seeded, capsys):
        from scidb.inspect.cli import main

        db, _ = seeded
        db_file = str(db.dataset_db_path)
        db.close()
        if hasattr(_local, "database"):
            delattr(_local, "database")

        rc = main(["--db", db_file, "report", "--fn", "plot_nonexistent", "--json"])
        out = capsys.readouterr().out
        assert rc == 0
        payload = _json.loads(out)
        assert payload["figures"] == [] and payload["stats"] == []
