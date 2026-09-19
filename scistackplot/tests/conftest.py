"""Shared fixtures.

The tables mirror the shapes the design doc calls out: a scalar measure, a 1-D
measure, and a variant-bearing table. Subject IDs are deliberately zero-padded
strings — that is the ordering trap, not decoration.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import LongTable

SUBJECTS = ["01", "02", "03"]
SESSIONS = ["pre", "post"]
TRIALS = ["1", "2", "3", "4"]


@pytest.fixture
def scalar_frame() -> pd.DataFrame:
    rows = []
    rng = np.random.default_rng(0)
    for subject in SUBJECTS:
        for session in SESSIONS:
            for trial in TRIALS:
                rows.append(
                    {
                        "subject": subject,
                        "session": session,
                        "trial": trial,
                        "StepLength": float(rng.normal(1.2, 0.1)),
                    }
                )
    return pd.DataFrame(rows)


@pytest.fixture
def scalar_table(scalar_frame) -> LongTable:
    return LongTable.from_frame(
        scalar_frame,
        factors=["subject", "session", "trial"],
        measures=["StepLength"],
        name="StepLength",
        # Every scidb-backed table carries its schema depth, so the shared
        # fixtures do too: depth orders the collapse chain and the default
        # grouping (roles.collapse_order, PlotSpec.ordered_groups), and the
        # fan-out runs in this order.
        schema_levels=["subject", "session", "trial"],
    )


@pytest.fixture
def series_frame() -> pd.DataFrame:
    rows = []
    rng = np.random.default_rng(1)
    for subject in SUBJECTS:
        for session in SESSIONS:
            for trial in TRIALS:
                rows.append(
                    {
                        "subject": subject,
                        "session": session,
                        "trial": trial,
                        "Signal": list(rng.normal(0.0, 1.0, size=10)),
                    }
                )
    return pd.DataFrame(rows)


@pytest.fixture
def series_table(series_frame) -> LongTable:
    return LongTable.from_frame(
        series_frame,
        factors=["subject", "session", "trial"],
        measures=["Signal"],
        name="Signal",
        schema_levels=["subject", "session", "trial"],
    )


@pytest.fixture
def variant_frame() -> pd.DataFrame:
    """Two pipeline variants of the same measure — the silent-pooling trap."""
    rows = []
    for subject in SUBJECTS:
        for low_hz in ["20", "40"]:
            rows.append(
                {
                    "subject": subject,
                    "bandpass.low_hz": low_hz,
                    "Peak": 1.0 + 0.1 * int(low_hz),
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture
def variant_table(variant_frame) -> LongTable:
    return LongTable.from_frame(
        variant_frame,
        factors=["subject", "bandpass.low_hz"],
        measures=["Peak"],
        variant_factors=["bandpass.low_hz"],
        name="Peak",
        schema_levels=["subject"],
    )


@pytest.fixture
def wide_subject_table() -> LongTable:
    """Ten zero-padded subjects: 1, 10, 2 sorts wrong; 01, 02, ... 10 sorts right."""
    frame = pd.DataFrame(
        {
            "subject": [f"{n:02d}" for n in range(1, 11)],
            "Mass": [70.0 + n for n in range(1, 11)],
        }
    )
    return LongTable.from_frame(
        frame, factors=["subject"], measures=["Mass"], name="Mass"
    )


@pytest.fixture
def bilateral_table() -> LongTable:
    """
    The layout case the grid controls exist for: left/right x muscle group.

    Four fields whose names carry two independent facts (side and group), which
    is why a facet grid is described by rules over the names rather than by two
    separate factors — the data has only one factor here.
    """
    rng = np.random.default_rng(3)
    rows = [
        {"subject": subject, "trial": trial, "ColName": muscle,
         "RawEMG": list(rng.normal(0.0, 1.0, size=6))}
        for subject in SUBJECTS
        for trial in TRIALS[:2]
        for muscle in ["LHAM", "RHAM", "LQUAD", "RQUAD"]
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "trial", "ColName"],
        measures=["RawEMG"],
        field_factors=["ColName"],
        name="RawEMG",
        schema_levels=["subject", "trial"],
    )


@pytest.fixture
def struct_table() -> LongTable:
    """
    A dict/struct variable, melted: one column of field names, one of values.

    This is what a scidb ``multi_column`` variable looks like after
    ScidbSource melts it — an EMG record whose keys are muscle names and whose
    values are 1-D traces.
    """
    rng = np.random.default_rng(2)
    rows = []
    for subject in SUBJECTS:
        for trial in TRIALS[:2]:
            for muscle in ["RHAM", "RTA", "LMG"]:
                rows.append(
                    {
                        "subject": subject,
                        "trial": trial,
                        "ColName": muscle,
                        "RawEMG": list(rng.normal(0.0, 1.0, size=6)),
                    }
                )
    frame = pd.DataFrame(rows)
    return LongTable.from_frame(
        frame,
        factors=["subject", "trial", "ColName"],
        measures=["RawEMG"],
        field_factors=["ColName"],
        name="RawEMG",
        schema_levels=["subject", "trial"],
    )


@pytest.fixture
def matrix_table() -> LongTable:
    """A 2-D measure: one matrix per subject. A heatmap's axes come from the
    matrix, so nothing may group it."""
    frame = pd.DataFrame(
        {
            "subject": SUBJECTS,
            "Coherence": [np.zeros((3, 3)) + i for i in range(len(SUBJECTS))],
        }
    )
    return LongTable.from_frame(
        frame,
        factors=["subject"],
        measures=["Coherence"],
        name="Coherence",
        schema_levels=["subject"],
    )
