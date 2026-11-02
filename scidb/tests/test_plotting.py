"""Tests for plotting leaf nodes: plot_ detection, figure save, path record,
share_limits, and the finalized draft/record flag (D3).

A function whose name starts with ``plot_`` is treated as a plotting leaf:
scidb.for_each saves its returned matplotlib Figure to the combo's PathOutput
path. With ``finalized=True`` the path string is stored as a normal
(queryable) record with lineage; the DEFAULT (``finalized=False``) is DRAFT
mode — the figure file is still rendered (the user needs to look at it) but
nothing is written to the database. ``share_limits`` lets every plot in a
group share one axis range.
"""

import matplotlib

matplotlib.use("Agg")  # headless backend for tests
import matplotlib.pyplot as plt
import numpy as np
import pytest
from scidb.store.database import _local

from scidb import BaseVariable, PathOutput, configure_database, for_each

SCHEMA = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "plots.duckdb"
    db = configure_database(db_path, SCHEMA)
    yield db
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


class RawSignal(BaseVariable):
    schema_version = 1


class PlotFigure(BaseVariable):
    """Stores the saved plot's file path (a string)."""

    schema_version = 1


def _seed(db):
    # subject 1: small-amplitude trials; subject 2: large-amplitude trials.
    data = {
        ("1", "1"): np.array([0.0, 1.0, 2.0]),
        ("1", "2"): np.array([1.0, 2.0, 3.0]),
        ("1", "3"): np.array([0.5, 1.5, 2.5]),
        ("2", "1"): np.array([0.0, 50.0, 100.0]),
        ("2", "2"): np.array([10.0, 60.0, 110.0]),
        ("2", "3"): np.array([5.0, 55.0, 105.0]),
    }
    for (subj, trial), arr in data.items():
        RawSignal.save(arr, subject=subj, trial=trial)


def test_plot_saves_files_and_registers_paths(db, tmp_path):
    plots_dir = tmp_path / "plots"
    plots_dir.mkdir()
    _seed(db)

    def plot_timeseries(signal, filename):
        fig, ax = plt.subplots()
        ax.plot(np.asarray(signal).ravel())
        return fig

    for_each(
        plot_timeseries,
        inputs={
            "signal": RawSignal,
            "filename": PathOutput(str(plots_dir / "{subject}_{trial}.png")),
        },
        outputs=[PlotFigure],
        finalized=True,
        subject=["1", "2"],
        trial=["1", "2", "3"],
        db=db,
    )

    # (a) Every PNG exists on disk.
    for subj in ["1", "2"]:
        for trial in ["1", "2", "3"]:
            assert (plots_dir / f"{subj}_{trial}.png").exists()

    # (b) The stored record holds the path string.
    rec = PlotFigure.load(subject="1", trial="2")
    path = rec.data if hasattr(rec, "data") else rec
    assert isinstance(path, str)
    assert path.endswith("1_2.png")

    # (c) The recorded figure carries its embedded provenance stamp (D4).
    from scidb import read_artifact_stamp

    blob = read_artifact_stamp(path)
    assert blob is not None and blob["record_id"] == rec.record_id


def test_plot_draft_renders_but_records_nothing(db, tmp_path):
    """Default finalized=False: figure files rendered, NO database writes."""
    plots_dir = tmp_path / "plots"
    plots_dir.mkdir()
    _seed(db)

    def plot_timeseries(signal, filename):
        fig, ax = plt.subplots()
        ax.plot(np.asarray(signal).ravel())
        return fig

    result = for_each(
        plot_timeseries,
        inputs={
            "signal": RawSignal,
            "filename": PathOutput(str(plots_dir / "{subject}_{trial}.png")),
        },
        outputs=[PlotFigure],
        subject=["1", "2"],
        trial=["1", "2", "3"],
        db=db,
    )

    # Figures ARE rendered (the draft's whole point is looking at them)...
    for subj in ["1", "2"]:
        for trial in ["1", "2", "3"]:
            assert (plots_dir / f"{subj}_{trial}.png").exists()
    # ...the in-memory result table is still returned...
    assert result is not None and len(result) == 6
    # ...but nothing was recorded.
    assert len(db.list_versions(PlotFigure)) == 0


def test_share_limits_per_subject(db, tmp_path):
    plots_dir = tmp_path / "plots"
    plots_dir.mkdir()
    _seed(db)

    captured = {}

    def plot_timeseries(signal, filename, subject, trial, signal_limits=None):
        captured[(subject, trial)] = signal_limits
        fig, ax = plt.subplots()
        ax.plot(np.asarray(signal).ravel())
        if signal_limits is not None:
            ax.set_ylim(*signal_limits)
        return fig

    for_each(
        plot_timeseries,
        inputs={
            "signal": RawSignal,
            "filename": PathOutput(str(plots_dir / "{subject}_{trial}.png")),
        },
        outputs=[PlotFigure],
        share_limits={"signal": ["subject"]},
        subject=["1", "2"],
        trial=["1", "2", "3"],
        db=db,
    )

    # (c) All trials within a subject got identical limits, spanning that
    #     subject's data across trials; subjects differ.
    s1 = {captured[("1", t)] for t in ["1", "2", "3"]}
    s2 = {captured[("2", t)] for t in ["1", "2", "3"]}
    assert len(s1) == 1 and len(s2) == 1
    lim1 = s1.pop()
    lim2 = s2.pop()
    assert lim1 == (0.0, 3.0)  # subject 1 spans 0..3 across trials
    assert lim2 == (0.0, 110.0)  # subject 2 spans 0..110 across trials
    assert lim1 != lim2


def test_second_run_skips_rerender(db, tmp_path):
    plots_dir = tmp_path / "plots"
    plots_dir.mkdir()
    _seed(db)

    calls = {"n": 0}

    def plot_timeseries(signal, filename):
        calls["n"] += 1
        fig, ax = plt.subplots()
        ax.plot(np.asarray(signal).ravel())
        return fig

    kwargs = {
        "inputs": {
            "signal": RawSignal,
            "filename": PathOutput(str(plots_dir / "{subject}_{trial}.png")),
        },
        "outputs": [PlotFigure],
        "finalized": True,
        "skip_computed": True,
        "subject": ["1", "2"],
        "trial": ["1", "2", "3"],
        "db": db,
    }

    for_each(plot_timeseries, **kwargs)
    assert calls["n"] == 6
    # Second identical run: skip_computed should skip every combo (no re-render).
    for_each(plot_timeseries, **kwargs)
    assert calls["n"] == 6
