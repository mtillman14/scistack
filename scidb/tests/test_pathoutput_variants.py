"""Tests for PathOutput branch_param placeholders and the collision guard.

PathOutput templates may reference branch_params with the same ``{}`` syntax
as schema keys — ``{low_hz}`` (bare, suffix-matched like Variant()),
``{bandpass.low_hz}`` (namespaced), or ``{variant}`` (8-char digest of the
whole group signature) — so each variant group writes its OWN artifact file
instead of clobbering a shared path. A collision guard hard-errors when one
resolved path is shared by combos that differ in variant identity.
"""

import json as _json
import re as _re
import warnings as _warnings

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pytest
from scidb.database import _local

import scifor as _scifor
from scidb import (
    BaseVariable,
    PathOutput,
    branch_param,
    configure_database,
    for_each,
    read_artifact_stamp,
)

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "paths.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


class RawSignal(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


class Chained(BaseVariable):
    pass


class StatOut(BaseVariable):
    pass


class PlotOut(BaseVariable):
    pass


def bandpass(signal, low_hz):
    return signal * low_hz


def _make_two_groups(db, subjects=("S01",), sessions=("1", "2")):
    for subj in subjects:
        for sess in sessions:
            RawSignal.save(np.array([1.0, 2.0]), subject=subj, session=sess)
    for low_hz in [20, 30]:
        for_each(
            bandpass,
            {"signal": RawSignal, "low_hz": low_hz},
            [Filtered],
            subject=list(subjects),
            session=list(sessions),
        )


def _plot_fig(signal, filename):
    fig, ax = plt.subplots()
    ax.plot(np.asarray(signal).ravel())
    return fig


# ---------------------------------------------------------------------------
# 1. Bare-name placeholder, aggregation: one artifact per variant group
# ---------------------------------------------------------------------------


class TestAggregationPlaceholders:
    def test_stat_pdf_per_group_with_correct_stamps(self, db, tmp_path):
        """The stage-3 test we couldn't write: two groups -> two stamped PDFs."""
        _make_two_groups(db)

        def stat_summary(df, filename):
            fig = plt.figure()
            fig.savefig(str(filename))
            plt.close(fig)
            return {"n_rows": len(df)}

        for_each(
            stat_summary,
            {
                "df": Filtered,
                "filename": PathOutput(str(tmp_path / "report_{low_hz}.pdf")),
            },
            [StatOut],
            finalized=True,
            subject=["S01"],
        )

        pdf20 = tmp_path / "report_20.pdf"
        pdf30 = tmp_path / "report_30.pdf"
        assert pdf20.exists() and pdf30.exists()

        rec20 = StatOut.load(subject="S01", **branch_param("bandpass", low_hz=20))
        rec30 = StatOut.load(subject="S01", **branch_param("bandpass", low_hz=30))
        blob20 = read_artifact_stamp(pdf20)
        blob30 = read_artifact_stamp(pdf30)
        assert blob20["record_id"] == rec20.record_id
        assert blob30["record_id"] == rec30.record_id
        assert blob20["record_id"] != blob30["record_id"]
        # Each record's report_path names ITS group's file.
        assert _json.loads(rec20.data)["report_path"].endswith("report_20.pdf")
        assert _json.loads(rec30.data)["report_path"].endswith("report_30.pdf")

    def test_namespaced_placeholder_resolves(self, db, tmp_path):
        _make_two_groups(db)
        seen = []

        def stat_summary(df, filename):
            seen.append(filename)
            return {"n": len(df)}

        for_each(
            stat_summary,
            {
                "df": Filtered,
                "filename": PathOutput(str(tmp_path / "r_{bandpass.low_hz}.pdf")),
            },
            [StatOut],
            finalized=True,
            subject=["S01"],
        )

        assert sorted(str(p).rsplit("/", 1)[-1] for p in seen) == [
            "r_20.pdf",
            "r_30.pdf",
        ]


# ---------------------------------------------------------------------------
# 2. Full iteration: one file per (location x variant)
# ---------------------------------------------------------------------------


class TestFullIterationPlaceholders:
    def test_plot_file_per_variant(self, db, tmp_path):
        _make_two_groups(db, sessions=("1",))

        def plot_sig(signal, filename):
            return _plot_fig(signal, filename)

        for_each(
            plot_sig,
            {
                "signal": Filtered,
                "filename": PathOutput(
                    str(tmp_path / "{subject}_{session}_{low_hz}.png")
                ),
            },
            [PlotOut],
            finalized=True,
            subject=["S01"],
            session=["1"],
        )

        f20 = tmp_path / "S01_1_20.png"
        f30 = tmp_path / "S01_1_30.png"
        assert f20.exists() and f30.exists()
        # Stamps carry the right per-variant records.
        assert (
            read_artifact_stamp(f20)["record_id"]
            != read_artifact_stamp(f30)["record_id"]
        )


# ---------------------------------------------------------------------------
# 3. Ambiguity: bare name matching two namespaced keys errors
# ---------------------------------------------------------------------------


class TestAmbiguity:
    def test_ambiguous_bare_name_errors(self, db, tmp_path):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")

        def alpha(signal, cut):
            return signal * cut

        def beta(signal, cut):
            return signal + cut

        for_each(
            alpha,
            {"signal": RawSignal, "cut": 2},
            [Filtered],
            subject=["S01"],
            session=["1"],
        )
        # Chained: bp inherits alpha.cut AND adds beta.cut.
        for_each(
            beta,
            {"signal": Filtered, "cut": 3},
            [Chained],
            subject=["S01"],
            session=["1"],
        )

        def stat_summary(df, filename):
            return {"n": len(df)}

        with pytest.raises(ValueError, match="ambiguous"):
            for_each(
                stat_summary,
                {"df": Chained, "filename": PathOutput(str(tmp_path / "r_{cut}.pdf"))},
                [StatOut],
                finalized=True,
                subject=["S01"],
            )


# ---------------------------------------------------------------------------
# 4. {variant} digest
# ---------------------------------------------------------------------------


class TestVariantToken:
    def test_variant_token_distinct_and_stable(self, db, tmp_path):
        _make_two_groups(db)
        seen: list = []

        def stat_summary(df, filename):
            seen.append(str(filename).rsplit("/", 1)[-1])
            return {"n": len(df)}

        # finalized=True: a draft stat_ resolves PathOutput to None by design,
        # so paths are only observable in record mode.
        kwargs = {
            "inputs": {
                "df": Filtered,
                "filename": PathOutput(str(tmp_path / "r_{variant}.pdf")),
            },
            "outputs": [StatOut],
            "finalized": True,
            "subject": ["S01"],
        }

        for_each(stat_summary, **kwargs)
        first = sorted(seen)
        assert len(first) == 2 and first[0] != first[1]
        for name in first:
            assert _re.fullmatch(r"r_[0-9a-f]{8}\.pdf", name), name

        seen.clear()
        for_each(stat_summary, **kwargs)
        assert sorted(seen) == first, "digest must be stable across runs"

    def test_variant_token_with_no_variants(self, db, tmp_path):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        seen = []

        def stat_summary(df, filename):
            seen.append(str(filename).rsplit("/", 1)[-1])
            return {"n": len(df)}

        for_each(
            stat_summary,
            {
                "df": RawSignal,
                "filename": PathOutput(str(tmp_path / "r_{variant}.pdf")),
            },
            [StatOut],
            finalized=True,
            subject=["S01"],
        )
        assert len(seen) == 1
        assert _re.fullmatch(r"r_[0-9a-f]{8}\.pdf", seen[0])


# ---------------------------------------------------------------------------
# 5. Collision guard
# ---------------------------------------------------------------------------


class TestCollisionGuard:
    def test_two_groups_one_path_errors_before_rendering(self, db, tmp_path):
        _make_two_groups(db)

        def stat_summary(df, filename):
            fig = plt.figure()
            fig.savefig(str(filename))
            plt.close(fig)
            return {"n": len(df)}

        with pytest.raises(ValueError, match="low_hz"):
            for_each(
                stat_summary,
                {
                    "df": Filtered,
                    "filename": PathOutput(str(tmp_path / "report_{subject}.pdf")),
                },
                [StatOut],
                finalized=True,
                subject=["S01"],
            )
        assert not (tmp_path / "report_S01.pdf").exists(), (
            "guard must fire before any file is written"
        )

    def test_single_group_no_error(self, db, tmp_path):
        """Existing single-variant pipelines are unaffected."""
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        for_each(
            bandpass,
            {"signal": RawSignal, "low_hz": 20},
            [Filtered],
            subject=["S01"],
            session=["1"],
        )

        def stat_summary(df, filename):
            return {"n": len(df)}

        for_each(
            stat_summary,
            {
                "df": Filtered,
                "filename": PathOutput(str(tmp_path / "report_{subject}.pdf")),
            },
            [StatOut],
            subject=["S01"],
        )  # no raise

    def test_schema_key_omission_is_not_an_error(self, db, tmp_path):
        """Collisions across SCHEMA keys only (no variant difference) are the
        user's business (pre-existing overwrite behavior), not guarded."""
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        def plot_sig(signal, filename):
            return _plot_fig(signal, filename)

        # {subject} only, iterating session too -> session collision, no variants.
        for_each(
            plot_sig,
            {
                "signal": RawSignal,
                "filename": PathOutput(str(tmp_path / "{subject}.png")),
            },
            [PlotOut],
            subject=["S01"],
            session=["1", "2"],
        )  # no raise
        assert (tmp_path / "S01.png").exists()


# ---------------------------------------------------------------------------
# 6. Missing-key warning
# ---------------------------------------------------------------------------


class TestMissingPlaceholder:
    def test_unmatched_placeholder_warns_and_keeps_literal(self, db, tmp_path):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        seen = []

        def stat_summary(df, filename):
            seen.append(str(filename).rsplit("/", 1)[-1])
            return {"n": len(df)}

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            for_each(
                stat_summary,
                {
                    "df": RawSignal,
                    "filename": PathOutput(str(tmp_path / "r_{nope}.pdf")),
                },
                [StatOut],
                finalized=True,
                subject=["S01"],
            )

        assert seen == ["r_{nope}.pdf"], "literal placeholder text stays"
        flagged = [
            w for w in caught if "did not match any branch_param" in str(w.message)
        ]
        assert flagged


# ---------------------------------------------------------------------------
# 7. Hygiene: injected keys leak nowhere
# ---------------------------------------------------------------------------


class TestPlaceholderHygiene:
    def test_injected_keys_absent_from_results_and_records(self, db, tmp_path):
        _make_two_groups(db)

        def stat_summary(df, filename):
            return {"n": len(df)}

        result = for_each(
            stat_summary,
            {"df": Filtered, "filename": PathOutput(str(tmp_path / "r_{low_hz}.pdf"))},
            [StatOut],
            finalized=True,
            subject=["S01"],
            introspect=True,
        )

        assert "low_hz" not in result.columns
        rec = StatOut.load(subject="S01", **branch_param("bandpass", low_hz=20))
        # The bare injected name must not appear as an (unnamespaced)
        # branch_param — only the real inherited namespaced key.
        assert "low_hz" not in rec.branch_params
        assert rec.branch_params.get("bandpass.low_hz") == 20
        assert "low_hz" not in rec.metadata


# ---------------------------------------------------------------------------
# 8. Draft mode: placeholders + stamps per group
# ---------------------------------------------------------------------------


class TestDraftPlaceholders:
    def test_draft_plot_files_per_group_with_draft_stamps(self, db, tmp_path):
        _make_two_groups(db, sessions=("1",))

        def plot_sig(signal, filename):
            return _plot_fig(signal, filename)

        for_each(
            plot_sig,
            {
                "signal": Filtered,
                "filename": PathOutput(str(tmp_path / "{subject}_{low_hz}.png")),
            },
            [PlotOut],
            subject=["S01"],
            session=["1"],
        )  # draft

        assert db.list_versions(PlotOut) == []
        for low_hz in [20, 30]:
            blob = read_artifact_stamp(tmp_path / f"S01_{low_hz}.png")
            assert blob is not None and blob.get("draft") is True
            assert "record_id" not in blob


# ---------------------------------------------------------------------------
# 9. Sanitization
# ---------------------------------------------------------------------------


class TestSanitization:
    def test_path_separator_in_value_becomes_dash(self, db, tmp_path):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")

        def tag(signal, label):
            return signal

        for_each(
            tag,
            {"signal": RawSignal, "label": "a/b"},
            [Filtered],
            subject=["S01"],
            session=["1"],
        )

        seen = []

        def stat_summary(df, filename):
            seen.append(str(filename).rsplit("/", 1)[-1])
            return {"n": len(df)}

        for_each(
            stat_summary,
            {"df": Filtered, "filename": PathOutput(str(tmp_path / "r_{label}.pdf"))},
            [StatOut],
            finalized=True,
            subject=["S01"],
        )
        assert seen == ["r_a-b.pdf"]


class TestVariantTokenIsOneDigest:
    """``{variant}`` digests the group's canonical signature
    (``bindings.variant_signature``) in BOTH iteration modes.

    It used to digest two different texts: ``json.dumps(merged_bp)`` under
    full iteration and ``"|".join(per_input_signatures)`` under aggregation.
    One PathOutput template therefore wrote to two different directories for
    the same variant group depending on how many schema keys the call
    iterated — a silent file-location split, found 2026-09-20 while giving
    the signature recipe one owner.
    """

    def _names(self, out_dir, **iterate):
        out_dir.mkdir(parents=True, exist_ok=True)
        seen: list = []

        def stat_summary(df, filename):
            seen.append(str(filename).rsplit("/", 1)[-1])
            return {"n": len(df)}

        for_each(
            stat_summary,
            inputs={
                "df": Filtered,
                "filename": PathOutput(str(out_dir / "r_{variant}.pdf")),
            },
            outputs=[StatOut],
            finalized=True,
            **iterate,
        )
        return sorted(seen)

    def test_full_iteration_and_aggregation_agree(self, db, tmp_path):
        # One session, so the aggregating call's group and the fully-iterated
        # call's group hold exactly the same record — only the mode differs.
        _make_two_groups(db, subjects=("S01",), sessions=("1",))
        agg = self._names(tmp_path / "agg", subject=["S01"])
        full = self._names(tmp_path / "full", subject=["S01"], session=["1"])
        assert len(agg) == 2 and len(full) == 2
        assert agg == full, (
            "the same variant group resolved {variant} to different digests "
            f"under aggregation {agg} and full iteration {full}"
        )
