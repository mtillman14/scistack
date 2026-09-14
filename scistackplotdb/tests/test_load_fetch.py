"""The payload fetch: one numpy buffer per cell, never a Python float per sample.

Stage 2 of ``.claude/plan-plot-minimal-load-examples.md``. Measured on the real
database (§7): one ``DOUBLE[]`` column of 17.4 M samples took 4.7 s through
``fetchall`` and 0.26 s through ``.df()``; that difference was 86 of the 91 s
``plot_describe`` spent before the client gave up on it.

What is pinned here is the CONTRACT, not the speed — on an 8-sample fixture the
two paths are indistinguishable by clock:

* a loaded 1-D cell is an ``np.ndarray`` of float64, a 2-D cell a 2-D ndarray, a
  scalar column numeric;
* no data column of the variable is ever selected through ``_fetchall``;
* schema keys are text (zero-padded ``"01"`` survives) and a NULL key is None,
  not ``"nan"`` and not ``"1.0"``;
* the "loaded" log line says ``ndarray`` — the word a regression to boxing
  would change.
"""

from __future__ import annotations

import logging

import numpy as np
import pytest
from scidb import BaseVariable

from scistackplotdb.load import load_variable


class Duped(BaseVariable):
    """Same payload saved at several trials — the distribute=False fingerprint."""

    schema_version = 1


class Matrix(BaseVariable):
    """A 2-D cell, saved by this module so the seeded fixture stays as it is."""


@pytest.fixture
def with_matrix(seeded):
    rng = np.random.default_rng(3)
    Matrix.save(rng.normal(size=(3, 4)), subject="01", session="pre", trial="1")
    Matrix.save(rng.normal(size=(3, 4)), subject="01", session="pre", trial="2")
    return seeded


class TestCellsArriveAsNumpy:
    def test_a_1d_cell_is_a_float_ndarray(self, seeded):
        frame = load_variable(seeded, "Signal").frame
        column = [c for c in frame.columns if c not in ("record_id", "subject", "session", "trial")][0]
        cell = frame[column].iloc[0]
        assert isinstance(cell, np.ndarray), type(cell)
        assert cell.dtype == np.float64
        assert cell.shape == (8,)

    def test_a_dict_variables_fields_are_each_ndarrays(self, seeded):
        frame = load_variable(seeded, "Emg").frame
        for muscle in ("RHAM", "RTA", "LMG"):
            assert isinstance(frame[muscle].iloc[0], np.ndarray), muscle

    def test_a_2d_cell_is_a_2d_ndarray(self, with_matrix):
        frame = load_variable(with_matrix, "Matrix").frame
        column = [c for c in frame.columns if c not in ("record_id", "subject", "session", "trial")][0]
        cell = frame[column].iloc[0]
        assert isinstance(cell, np.ndarray), type(cell)
        assert cell.shape == (3, 4), cell.shape
        assert cell.dtype == np.float64

    def test_a_scalar_column_is_numeric(self, seeded):
        frame = load_variable(seeded, "StepLength").frame
        column = [c for c in frame.columns if c not in ("record_id", "subject", "session", "trial")][0]
        assert np.issubdtype(frame[column].dtype, np.number), frame[column].dtype

    def test_the_loaded_line_says_ndarray_not_boxed(self, seeded, caplog):
        with caplog.at_level(logging.INFO, logger="scistackplotdb"):
            load_variable(seeded, "Signal")
        loaded = [r.getMessage() for r in caplog.records if r.getMessage().startswith("loaded Signal")]
        assert loaded, "no 'loaded' line"
        assert "ndarray" in loaded[0], loaded[0]
        assert "boxed" not in loaded[0], loaded[0]


class TestNaNSamplesSurviveTheFetch:
    """The parity fixture's NaN cells lost their NaNs on the first `.df()` run:
    scidb's single-record INSERT binds a NaN as a NULL list element, DuckDB
    hands that cell back as a masked array, and `np.asarray` drops the mask.
    """

    @pytest.fixture
    def with_nan(self, seeded):
        from conftest import Signal

        Signal.save(
            np.array([1.0, np.nan, 3.0, np.nan, 5.0, 6.0, 7.0, 8.0]),
            subject="01", session="pre", trial="9",
        )
        Signal.save(np.full(8, np.nan), subject="01", session="post", trial="9")
        return seeded

    def _cells(self, db):
        frame = load_variable(db, "Signal").frame
        column = [c for c in frame.columns if c not in ("record_id", "subject", "session", "trial")][0]
        return {(r.subject, r.session, r.trial): r._asdict()[column] for r in frame.itertuples()}

    def test_a_nan_inside_a_cell_is_a_nan(self, with_nan):
        cell = self._cells(with_nan)[("01", "pre", "9")]
        assert not isinstance(cell, np.ma.MaskedArray), "mask leaked into the frame"
        assert cell.dtype == np.float64
        assert np.isnan(cell).sum() == 2
        np.testing.assert_array_equal(cell[[0, 2]], [1.0, 3.0])

    def test_an_all_nan_cell_is_all_nan(self, with_nan):
        cell = self._cells(with_nan)[("01", "post", "9")]
        assert not isinstance(cell, np.ma.MaskedArray)
        assert cell.shape == (8,) and np.isnan(cell).all()


class TestNoDataColumnPassesThroughFetchall:
    def test_payload_columns_are_never_in_a_fetchall(self, seeded, monkeypatch):
        """Assert on the SQL, not the clock: the payload must only ever go
        through ``_fetchdf``. ``_fetchall`` is where a sample becomes a Python
        float."""
        seen: list[str] = []
        original = seeded._duck._fetchall

        def spy(sql, params=None):
            seen.append(sql)
            return original(sql, params)

        monkeypatch.setattr(seeded._duck, "_fetchall", spy)
        load_variable(seeded, "Emg")

        for sql in seen:
            for muscle in ("RHAM", "RTA", "LMG"):
                assert f'"{muscle}"' not in sql, f"payload column through _fetchall: {sql}"

    def test_the_load_query_goes_through_fetchdf(self, seeded, monkeypatch):
        seen: list[str] = []
        original = seeded._duck._fetchdf

        def spy(sql, params=None):
            seen.append(sql)
            return original(sql, params)

        monkeypatch.setattr(seeded._duck, "_fetchdf", spy)
        load_variable(seeded, "Emg")

        selects = [s for s in seen if "SELECT t.record_id" in s]
        assert len(selects) == 1, seen
        for muscle in ("RHAM", "RTA", "LMG"):
            assert f'"{muscle}"' in selects[0]


class TestSchemaKeysStayText:
    def test_zero_padded_keys_survive(self, seeded):
        frame = load_variable(seeded, "StepLength").frame
        assert set(frame["subject"].unique()) == {"01", "02", "03"}
        assert all(isinstance(v, str) for v in frame["subject"])

    def test_a_null_key_is_none_not_a_string(self, seeded):
        """Mass is saved at subject level: session and trial are NULL for every
        row. A DataFrame fetch could spell that NaN; a stringify pass that did
        not know would write "nan" and the level would appear occupied."""
        loaded = load_variable(seeded, "Mass")
        frame = loaded.frame
        assert frame["session"].isna().all()
        assert frame["trial"].isna().all()
        assert all(v is None for v in frame["session"])
        assert loaded.levels == ["subject"]

    def test_keys_are_cast_to_text_in_the_query(self, seeded, monkeypatch):
        """The cast happens in SQL so a numeric key with a NULL in the column
        cannot arrive as float64 and stringify as "1.0"."""
        seen: list[str] = []
        original = seeded._duck._fetchdf

        def spy(sql, params=None):
            seen.append(sql)
            return original(sql, params)

        monkeypatch.setattr(seeded._duck, "_fetchdf", spy)
        load_variable(seeded, "StepLength")
        query = [s for s in seen if "SELECT t.record_id" in s][0]
        assert 'CAST(s."subject" AS VARCHAR)' in query


class TestNormalizeCell:
    """The per-cell pass, on the shapes a DuckDB build might hand over."""

    def test_an_ndarray_is_untouched(self):
        from scistackplotdb.load import _normalize_cell

        cell = np.arange(4.0)
        assert _normalize_cell(cell) is cell

    def test_an_object_array_of_rows_is_stacked(self):
        from scistackplotdb.load import _normalize_cell

        rows = np.empty(2, dtype=object)
        rows[0] = np.array([1.0, 2.0])
        rows[1] = np.array([3.0, 4.0])
        out = _normalize_cell(rows)
        assert out.shape == (2, 2) and out.dtype == np.float64

    def test_ragged_rows_become_a_list_of_rows_and_still_classify_2d(self):
        from scistackplot.shape import Shape, classify_value

        from scistackplotdb.load import _normalize_cell

        rows = np.empty(2, dtype=object)
        rows[0] = np.array([1.0, 2.0])
        rows[1] = np.array([3.0])
        out = _normalize_cell(rows)
        assert isinstance(out, list) and len(out) == 2
        assert classify_value(out) is Shape.MATRIX_2D

    def test_a_masked_cell_is_filled_with_nan(self):
        from scistackplotdb.load import _normalize_cell

        masked = np.ma.masked_array([1.0, 0.0, 3.0], mask=[False, True, False])
        out = _normalize_cell(masked)
        assert type(out) is np.ndarray
        np.testing.assert_array_equal(out, [1.0, np.nan, 3.0])

    def test_masked_rows_of_a_2d_cell_are_filled_too(self):
        from scistackplotdb.load import _normalize_cell

        rows = np.empty(2, dtype=object)
        rows[0] = np.ma.masked_array([1.0, 0.0], mask=[False, True])
        rows[1] = np.array([3.0, 4.0])
        out = _normalize_cell(rows)
        assert type(out) is np.ndarray and out.shape == (2, 2)
        np.testing.assert_array_equal(out, [[1.0, np.nan], [3.0, 4.0]])

    def test_a_python_list_becomes_an_ndarray(self):
        from scistackplotdb.load import _normalize_cell

        out = _normalize_cell([1.0, 2.0, float("nan")])
        assert isinstance(out, np.ndarray) and out.dtype == np.float64

    def test_scalars_and_none_pass_through(self):
        from scistackplotdb.load import _normalize_cell

        assert _normalize_cell(None) is None
        assert _normalize_cell(1.5) == 1.5

    def test_a_null_cell_becomes_none(self):
        """A DataFrame fetch spells a NULL list cell `pd.NA` (probe_list_cells.py,
        duckdb 1.5.5); downstream tests for `is None`."""
        import pandas as pd

        from scistackplotdb.load import _normalize_cell

        assert _normalize_cell(pd.NA) is None

    def test_same_length_cells_stay_one_ndarray_each(self, seeded):
        """The failure that found `_object_column`: every Signal cell has 8
        samples, and a pandas that infers a dtype over same-length arrays
        reinterprets the column. Each cell must still be its own 1-D array."""
        frame = load_variable(seeded, "Signal").frame
        column = [c for c in frame.columns if c not in ("record_id", "subject", "session", "trial")][0]
        assert frame[column].dtype == object
        for cell in frame[column]:
            assert isinstance(cell, np.ndarray) and cell.shape == (8,)


class TestIdenticalContentWarning:
    """A ``distribute=False`` run that should have distributed leaves records
    whose payloads are byte-identical across the key it failed to split on.
    A plot cannot show that: N identical traces draw exactly on top of each
    other and read as a single line in whichever colour was drawn last, and
    filtering to any one value of that factor shows the same curve again.

    So ``load_variable`` says it out loud. The check is diagnostic only — it
    must never change the frame, and must never be able to break a load.
    """

    def test_warns_when_records_share_content(self, db, caplog):
        same = np.array([1.0, 2.0, 3.0])
        for trial in ("1", "2", "3"):
            Duped.save(same, subject="01", session="pre", trial=trial)

        with caplog.at_level(logging.WARNING, logger="scistackplotdb"):
            frame = load_variable(db, "Duped")

        assert len(frame.frame) == 3  # the frame is untouched
        warnings = [
            r.getMessage()
            for r in caplog.records
            if "IDENTICAL data" in r.getMessage()
        ]
        assert warnings, "expected an identical-content warning"
        assert "trial" in warnings[0]

    def test_silent_when_every_record_differs(self, seeded, caplog):
        with caplog.at_level(logging.WARNING, logger="scistackplotdb"):
            load_variable(seeded, "Signal")
        assert not [
            r for r in caplog.records if "IDENTICAL data" in r.getMessage()
        ]
