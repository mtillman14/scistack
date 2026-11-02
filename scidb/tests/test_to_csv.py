"""Tests for BaseVariable.to_csv() flat-table export.

to_csv() loads a variable across all matching schema_ids and writes one row
per record: one column per schema key plus a single value column named after
the variable class. Scalar-only — vectors and tables raise ValueError.
"""

import numpy as np
import pandas as pd
import pytest
from conftest import ArrayValue, CustomDataFrameValue, ScalarValue
from scidb.model.exceptions import NotFoundError

from scidb import BaseVariable, Merge, for_each


def _read(path):
    return pd.read_csv(path)


# --- Happy path -----------------------------------------------------------


def test_trial_level_scalar_export(db, tmp_path):
    """Trial-level scalar -> one row per subject/trial, both schema columns."""
    ScalarValue.save(0.5, subject=1, trial=1)
    ScalarValue.save(0.6, subject=1, trial=2)
    ScalarValue.save(0.7, subject=2, trial=1)

    out = tmp_path / "scalars.csv"
    ScalarValue.to_csv(str(out))

    df = _read(out)
    assert set(df.columns) == {"subject", "trial", "ScalarValue"}
    assert len(df) == 3
    # Value column is named after the class; values round-trip.
    row = df[(df.subject == 1) & (df.trial == 2)].iloc[0]
    assert row["ScalarValue"] == pytest.approx(0.6)


def test_subject_level_scalar_has_no_trial_column(db, tmp_path):
    """A subject-level variable yields only the schema keys it actually uses."""
    ScalarValue.save(10, subject=1)
    ScalarValue.save(20, subject=2)

    out = tmp_path / "subj.csv"
    ScalarValue.to_csv(str(out))

    df = _read(out)
    assert set(df.columns) == {"subject", "ScalarValue"}
    assert "trial" not in df.columns
    assert len(df) == 2


def test_metadata_filter_restricts_rows(db, tmp_path):
    ScalarValue.save(1.0, subject=1, trial=1)
    ScalarValue.save(2.0, subject=1, trial=2)
    ScalarValue.save(3.0, subject=2, trial=1)

    out = tmp_path / "subj1.csv"
    ScalarValue.to_csv(str(out), subject=1)

    df = _read(out)
    assert len(df) == 2
    assert set(df.subject.unique()) == {1}


class _Side(BaseVariable):
    schema_version = 1


def test_where_filter_restricts_rows(db, tmp_path):
    ScalarValue.save(1.0, subject=1, trial=1)
    ScalarValue.save(2.0, subject=1, trial=2)
    ScalarValue.save(3.0, subject=2, trial=1)
    _Side.save("L", subject=1, trial=1)
    _Side.save("R", subject=1, trial=2)
    _Side.save("L", subject=2, trial=1)

    out = tmp_path / "left.csv"
    ScalarValue.to_csv(str(out), where=_Side == "L")

    df = _read(out)
    assert len(df) == 2
    assert set(zip(df.subject, df.trial, strict=False)) == {(1, 1), (2, 1)}


# --- Validation errors ----------------------------------------------------


def test_filename_must_end_with_csv(db, tmp_path):
    ScalarValue.save(1.0, subject=1, trial=1)
    with pytest.raises(ValueError, match=r"\.csv"):
        ScalarValue.to_csv(str(tmp_path / "out.txt"))


def test_vector_variable_raises(db, tmp_path):
    ArrayValue.save(np.array([1.0, 2.0, 3.0]), subject=1, trial=1)
    with pytest.raises(ValueError, match="scalar"):
        ArrayValue.to_csv(str(tmp_path / "arr.csv"))


def test_multirow_table_variable_raises(db, tmp_path):
    # A multi-row table per schema_id cannot fit one CSV row.
    CustomDataFrameValue.save(
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}), subject=1, trial=1
    )
    with pytest.raises(ValueError, match="one row per schema_id|Multi-row"):
        CustomDataFrameValue.to_csv(str(tmp_path / "tbl.csv"))


def test_no_match_raises_not_found(db, tmp_path):
    ScalarValue.save(1.0, subject=1, trial=1)
    with pytest.raises(NotFoundError):
        ScalarValue.to_csv(str(tmp_path / "none.csv"), subject=999)


def test_extra_variable_arg_points_to_merge(db, tmp_path):
    """Passing another variable as a positional arg errors, suggesting Merge."""
    ScalarValue.save(1.0, subject=1, trial=1)

    class _Other(BaseVariable):
        schema_version = 1

    with pytest.raises(ValueError, match="Merge"):
        ScalarValue.to_csv(str(tmp_path / "x.csv"), _Other)


# --- Variant (branch-param) support --------------------------------------


class _RawScalar(BaseVariable):
    schema_version = 1


class _ScaledScalar(BaseVariable):
    schema_version = 1


def _scale_by(signal, low_hz):
    """Scalar pipeline whose low_hz arg creates branch-param variants."""
    return float(signal) * low_hz


def test_branch_param_selects_variant(db, tmp_path):
    """Non-schema kwargs are forwarded to load() as Variant/branch-param filters."""
    _RawScalar.save(2.0, subject=1, trial=1)
    for low_hz in (20, 50):
        for_each(
            _scale_by,
            {"signal": _RawScalar, "low_hz": low_hz},
            [_ScaledScalar],
            subject=[1],
            trial=[1],
        )

    out = tmp_path / "variant.csv"
    _ScaledScalar.to_csv(str(out), low_hz=20)

    df = _read(out)
    assert len(df) == 1
    assert df["_ScaledScalar"].iloc[0] == pytest.approx(40.0)


# --- Multi-column (single-row table) support -----------------------------


def test_single_row_table_exports_multiple_columns(db, tmp_path):
    """One row per schema_id may have multiple columns (a single-row table)."""
    CustomDataFrameValue.save(
        pd.DataFrame({"speed": [1.2], "cadence": [110]}), subject=1, trial=1
    )
    CustomDataFrameValue.save(
        pd.DataFrame({"speed": [1.5], "cadence": [120]}), subject=1, trial=2
    )

    out = tmp_path / "table.csv"
    CustomDataFrameValue.to_csv(str(out))

    df = _read(out)
    assert set(df.columns) == {"subject", "trial", "speed", "cadence"}
    assert len(df) == 2
    row = df[(df.subject == 1) & (df.trial == 2)].iloc[0]
    assert row["speed"] == pytest.approx(1.5)
    assert row["cadence"] == 120


def test_column_selection_exports_selected_columns(db, tmp_path):
    """MyVar["col"].to_csv() exports only the selected column(s)."""
    CustomDataFrameValue.save(
        pd.DataFrame({"speed": [1.2], "cadence": [110]}), subject=1, trial=1
    )

    out = tmp_path / "speed.csv"
    CustomDataFrameValue["speed"].to_csv(str(out))

    df = _read(out)
    assert set(df.columns) == {"subject", "trial", "speed"}
    assert "cadence" not in df.columns
    assert df["speed"].iloc[0] == pytest.approx(1.2)


# --- Merge support --------------------------------------------------------


class _Speed(BaseVariable):
    schema_version = 1


def test_merge_of_scalars_writes_wide_table(db, tmp_path):
    """Merge(A, B) joins scalar variables column-wise on shared schema keys."""
    ScalarValue.save(0.65, subject=1, trial=1)
    ScalarValue.save(0.72, subject=1, trial=2)
    _Speed.save(1.2, subject=1, trial=1)
    _Speed.save(1.5, subject=1, trial=2)

    out = tmp_path / "merged.csv"
    Merge(ScalarValue, _Speed).to_csv(str(out))

    df = _read(out)
    assert set(df.columns) == {"subject", "trial", "ScalarValue", "_Speed"}
    assert len(df) == 2
    row = df[df.trial == 1].iloc[0]
    assert row["ScalarValue"] == pytest.approx(0.65)
    assert row["_Speed"] == pytest.approx(1.2)


def test_merge_with_where_filter(db, tmp_path):
    """where= is forwarded to every Merge constituent's load()."""
    ScalarValue.save(0.65, subject=1, trial=1)
    ScalarValue.save(0.72, subject=1, trial=2)
    _Speed.save(1.2, subject=1, trial=1)
    _Speed.save(1.5, subject=1, trial=2)
    _Side.save("L", subject=1, trial=1)
    _Side.save("R", subject=1, trial=2)

    out = tmp_path / "merged_filtered.csv"
    Merge(ScalarValue, _Speed).to_csv(str(out), where=_Side == "L")

    df = _read(out)
    assert set(df.columns) == {"subject", "trial", "ScalarValue", "_Speed"}
    assert len(df) == 1
    assert df.iloc[0]["trial"] == 1


def test_merge_broadcasts_coarser_level(db, tmp_path):
    """A subject-level constituent broadcasts across a trial-level one."""
    ScalarValue.save(99.0, subject=1)  # subject-level covariate
    _Speed.save(1.2, subject=1, trial=1)
    _Speed.save(1.5, subject=1, trial=2)

    out = tmp_path / "broadcast.csv"
    Merge(ScalarValue, _Speed).to_csv(str(out), subject=1)

    df = _read(out)
    assert set(df.columns) == {"subject", "trial", "ScalarValue", "_Speed"}
    assert len(df) == 2
    # The subject-level value is repeated across both trials.
    assert (df["ScalarValue"] == 99.0).all()
