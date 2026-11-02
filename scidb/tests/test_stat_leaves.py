"""Tests for statistics leaf nodes: stat_ detection, JSON record storage,
the finalized draft/record flag (D3), and csv-stats integration (D5).

A function whose name starts with ``stat_`` is a statistics leaf: it returns
a dict (e.g. a csv-stats result) or a JSON string, normalized to a canonical
JSON string. ``finalized=True`` stores it as a queryable record with lineage;
the DEFAULT (``finalized=False``) is DRAFT mode — the result is
pretty-printed, any PathOutput resolves to None (disabling e.g. csv-stats'
PDF side effect), and nothing is written to the database.

stat_ functions receive the LONG-FORMAT table (as_table defaults on): schema
key columns (subject, session, ...) arrive as ordinary columns — exactly the
group_column / repeated_measures_column contract of csv-stats.
"""

from scidb.provenance import query as provenance_query
import json as _json
import warnings as _warnings

import matplotlib

matplotlib.use("Agg")  # headless: csv-stats imports matplotlib for reports
import numpy as np
import pytest
from scidb.store.database import _local

import scifor as _scifor
from scidb import (
    BaseVariable,
    PathOutput,
    branch_param,
    configure_database,
    for_each,
)

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "stats.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


class StepLen(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


class TTestResult(BaseVariable):
    pass


class GroupMean(BaseVariable):
    pass


class Out(BaseVariable):
    pass


def _seed_step_lengths(db):
    """5 subjects x pre/post scalars with a consistent post-pre shift."""
    pre = {"S01": 1.0, "S02": 2.0, "S03": 3.0, "S04": 4.0, "S05": 5.0}
    for subj, val in pre.items():
        StepLen.save(val, subject=subj, session="pre")
        StepLen.save(val + 1.0 + 0.1 * int(subj[1:]), subject=subj, session="post")


def bandpass(signal, low_hz):
    return signal * low_hz


# ---------------------------------------------------------------------------
# 1. Record path: dict -> canonical JSON record
# ---------------------------------------------------------------------------


class TestStatRecord:
    def test_dict_result_stored_as_json_record(self, db):
        _seed_step_lengths(db)

        def stat_summary(df):
            assert "subject" in df.columns and "session" in df.columns, (
                f"stat_ fn must receive schema columns; got {list(df.columns)}"
            )
            return {
                "test": "demo",
                "n_rows": np.int64(len(df)),  # numpy -> native
                "mean_val": np.float64(2.5),
                "date": "2026-07-06 12:00:00",  # must be stripped
                "nested": {"alpha": np.float64(0.05)},
            }

        result = for_each(stat_summary, {"df": StepLen}, [TTestResult], finalized=True)
        assert len(result) == 1

        rec = TTestResult.load()
        assert isinstance(rec.data, str)
        parsed = _json.loads(rec.data)
        assert parsed["test"] == "demo"
        assert parsed["n_rows"] == 10 and isinstance(parsed["n_rows"], int)
        assert parsed["nested"]["alpha"] == 0.05
        assert "date" not in parsed, (
            "wall-clock date must be stripped for reproducibility"
        )

        prov = provenance_query.upstream_provenance(db, rec.record_id)
        nodes = [n for n in prov if n["variable_type"] == "TTestResult"]
        assert len(nodes) == 1 and nodes[0]["function_name"] == "stat_summary"


# ---------------------------------------------------------------------------
# 2. Draft default: print, don't record
# ---------------------------------------------------------------------------


class TestStatDraft:
    def test_draft_prints_and_records_nothing(self, db, capsys, caplog):
        import logging

        _seed_step_lengths(db)

        def stat_summary(df):
            return {"test": "demo", "p_value": 0.04}

        with caplog.at_level(logging.INFO, logger="scidb"):
            result = for_each(stat_summary, {"df": StepLen}, [TTestResult])

        assert result is not None and len(result) == 1
        assert db.list_versions(TTestResult) == []
        # The draft payload is the deliverable — it stays on stdout.
        out = capsys.readouterr().out
        assert "[stat draft] stat_summary" in out
        assert '"p_value": 0.04' in out
        # The not-recorded notice + how-to hint is an INFO log record.
        messages = [r.getMessage() for r in caplog.records]
        assert any("[draft]" in m and "finalized=True" in m for m in messages)


# ---------------------------------------------------------------------------
# 3. PathOutput handling (report artifact)
# ---------------------------------------------------------------------------


class TestStatPathOutput:
    def test_draft_resolves_pathoutput_to_none(self, db, tmp_path):
        _seed_step_lengths(db)
        received = {}

        def stat_summary(df, filename):
            received["filename"] = filename
            return {"test": "demo"}

        for_each(
            stat_summary,
            {"df": StepLen, "filename": PathOutput(str(tmp_path / "report.pdf"))},
            [TTestResult],
        )

        assert received["filename"] is None

    def test_record_passes_path_and_embeds_report_path(self, db, tmp_path):
        import matplotlib.pyplot as plt

        _seed_step_lengths(db)
        received = {}

        def stat_summary(df, filename):
            received["filename"] = filename
            fig = plt.figure()  # write a real report PDF (as csv-stats would)
            fig.savefig(str(filename))
            plt.close(fig)
            return {"test": "demo"}

        for_each(
            stat_summary,
            {"df": StepLen, "filename": PathOutput(str(tmp_path / "report.pdf"))},
            [TTestResult],
            finalized=True,
        )

        assert isinstance(received["filename"], str)
        assert received["filename"].endswith("report.pdf")
        rec = TTestResult.load()
        parsed = _json.loads(rec.data)
        assert parsed["report_path"].endswith("report.pdf")

        # The report artifact carries its embedded provenance stamp (D4).
        from scidb import read_artifact_stamp

        blob = read_artifact_stamp(parsed["report_path"])
        assert blob is not None and blob["record_id"] == rec.record_id


# ---------------------------------------------------------------------------
# 4. Return-type contract
# ---------------------------------------------------------------------------


class TestStatReturnContract:
    def test_non_dict_return_rejected(self, db, caplog):
        import logging

        _seed_step_lengths(db)

        def stat_bad(df):
            return [1, 2, 3]

        # scifor catches per-combo exceptions, skips the combo, and logs the
        # failure (WARN first occurrence + summary) on the "scifor" logger.
        with caplog.at_level(logging.INFO, logger="scifor"):
            result = for_each(stat_bad, {"df": StepLen}, [TTestResult], finalized=True)
        assert result is None or len(result) == 0
        assert any("must return a dict" in r.getMessage() for r in caplog.records)

    def test_json_string_return_passes_through(self, db):
        _seed_step_lengths(db)

        def stat_ready(df):
            return _json.dumps({"test": "prejson", "p": 0.5})

        for_each(stat_ready, {"df": StepLen}, [TTestResult], finalized=True)
        parsed = _json.loads(TTestResult.load().data)
        assert parsed["test"] == "prejson"

    def test_invalid_string_rejected(self, db, caplog):
        import logging

        _seed_step_lengths(db)

        def stat_broken(df):
            return "not json at all"

        with caplog.at_level(logging.INFO, logger="scifor"):
            result = for_each(
                stat_broken, {"df": StepLen}, [TTestResult], finalized=True
            )
        assert result is None or len(result) == 0
        assert any("not valid JSON" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# 5. D1 integration: one stat per upstream variant group
# ---------------------------------------------------------------------------


class TestStatWithVariants:
    def test_one_stat_record_per_variant_group(self, db):
        """The stage-1 + stage-2 payoff: multiverse stats in one line."""
        for sess in ["1", "2"]:
            StepLen.save(1.0, subject="S01", session=sess)
        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": StepLen, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1", "2"],
            )

        def stat_group_mean(df):
            vals = [float(v) for v in df["Filtered"]]
            return {"mean": sum(vals) / len(vals), "n": len(vals)}

        result = for_each(
            stat_group_mean, {"df": Filtered}, [GroupMean], finalized=True
        )
        assert len(result) == 2, "One stat call per upstream variant group"

        mean20 = _json.loads(GroupMean.load(**branch_param("bandpass", low_hz=20)).data)
        mean30 = _json.loads(GroupMean.load(**branch_param("bandpass", low_hz=30)).data)
        assert mean20["mean"] == 20.0
        assert mean30["mean"] == 30.0


# ---------------------------------------------------------------------------
# 6. skip_computed: records skip; drafts never do
# ---------------------------------------------------------------------------


class TestStatSkipComputed:
    def test_finalized_run_skips_second_time(self, db):
        _seed_step_lengths(db)
        calls = {"n": 0}

        def stat_counting(df):
            calls["n"] += 1
            return {"n_rows": len(df)}

        kwargs = {
            "inputs": {"df": StepLen},
            "outputs": [TTestResult],
            "finalized": True,
            "skip_computed": True,
        }
        for_each(stat_counting, **kwargs)
        assert calls["n"] == 1
        for_each(stat_counting, **kwargs)
        assert calls["n"] == 1, "Second identical finalized run skips"

    def test_draft_never_skips(self, db):
        _seed_step_lengths(db)
        calls = {"n": 0}

        def stat_counting(df):
            calls["n"] += 1
            return {"n_rows": len(df)}

        kwargs = {
            "inputs": {"df": StepLen},
            "outputs": [TTestResult],
            "skip_computed": True,
        }  # draft: finalized default False
        for_each(stat_counting, **kwargs)
        for_each(stat_counting, **kwargs)
        assert calls["n"] == 2, "Drafts leave no record, so nothing skips"


# ---------------------------------------------------------------------------
# 7. finalized on a non-endpoint function: warned and ignored
# ---------------------------------------------------------------------------


class TestFinalizedNonEndpoint:
    def test_warns_and_records_normally(self, db):
        StepLen.save(1.0, subject="S01", session="pre")

        def double(x):
            return x * 2

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            for_each(
                double,
                {"x": StepLen},
                [Out],
                finalized=True,
                subject=["S01"],
                session=["pre"],
            )

        flagged = [w for w in caught if "only applies to endpoint" in str(w.message)]
        assert flagged, "Expected a finalized-ignored warning"
        assert len(db.list_versions(Out)) >= 1, "Processing fn records normally"


# ---------------------------------------------------------------------------
# 8. csv-stats end-to-end (skipped where csv-stats isn't installed)
# ---------------------------------------------------------------------------


class TestCsvStatsIntegration:
    def test_paired_ttest_through_for_each(self, db, tmp_path):
        # csvstats itself is an empty package; its ttest module pulls the heavy
        # deps (statsmodels/pingouin). Skip on ANY import failure — including a
        # broken dep install (plain ImportError, not ModuleNotFoundError).
        try:
            from csvstats.ttest import ttest_dep
        except Exception as exc:
            pytest.skip(f"csv-stats unavailable: {exc}")

        _seed_step_lengths(db)
        report = tmp_path / "step_ttest.pdf"

        def stat_step_ttest(df, filename):
            return ttest_dep(
                df,
                "session",
                "StepLen",
                repeated_measures_column="subject",
                filename=filename,
            )

        result = for_each(
            stat_step_ttest,
            {"df": StepLen, "filename": PathOutput(str(report))},
            [TTestResult],
            finalized=True,
        )
        assert len(result) == 1

        parsed = _json.loads(TTestResult.load().data)
        assert "t_statistic" in parsed
        assert "p_value" in parsed
        assert "date" not in parsed
        assert parsed["report_path"].endswith("step_ttest.pdf")
        assert report.exists(), "csv-stats PDF report written in record mode"

    def test_draft_skips_pdf(self, db, tmp_path):
        try:
            from csvstats.ttest import ttest_dep
        except Exception as exc:
            pytest.skip(f"csv-stats unavailable: {exc}")

        _seed_step_lengths(db)
        report = tmp_path / "draft_ttest.pdf"

        def stat_step_ttest(df, filename):
            return ttest_dep(
                df,
                "session",
                "StepLen",
                repeated_measures_column="subject",
                filename=filename,
            )

        for_each(
            stat_step_ttest,
            {"df": StepLen, "filename": PathOutput(str(report))},
            [TTestResult],
        )

        assert not report.exists(), "filename=None must disable the PDF"
        assert db.list_versions(TTestResult) == []
