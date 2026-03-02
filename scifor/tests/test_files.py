"""Tests for scifor.files — MatFile and CsvFile."""

import pytest
import os
import tempfile
import pandas as pd
import numpy as np

from scifor.files import MatFile, CsvFile, _template_keys


# ---------------------------------------------------------------------------
# _template_keys helper
# ---------------------------------------------------------------------------

def test_template_keys_simple():
    keys = _template_keys("data/{subject}/{session}.csv")
    assert keys == {"subject", "session"}


def test_template_keys_no_placeholders():
    keys = _template_keys("data/file.csv")
    assert keys == set()


# ---------------------------------------------------------------------------
# CsvFile
# ---------------------------------------------------------------------------

class TestCsvFile:
    def test_save_and_load(self, tmp_path):
        template = str(tmp_path / "{subject}" / "{session}.csv")
        csv = CsvFile(template)
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        csv.save(df, subject=1, session="pre")
        loaded = csv.load(subject=1, session="pre")
        pd.testing.assert_frame_equal(loaded, df)

    def test_save_creates_directories(self, tmp_path):
        template = str(tmp_path / "nested" / "dir" / "{subject}.csv")
        csv = CsvFile(template)
        df = pd.DataFrame({"x": [1]})
        csv.save(df, subject=1)
        assert os.path.exists(str(tmp_path / "nested" / "dir" / "1.csv"))

    def test_extra_kwargs_ignored_on_load(self, tmp_path):
        template = str(tmp_path / "{subject}.csv")
        csv = CsvFile(template)
        df = pd.DataFrame({"x": [1]})
        csv.save(df, subject=1)
        # db= and other extra kwargs should not raise
        loaded = csv.load(subject=1, db=None, __fn="something")
        pd.testing.assert_frame_equal(loaded, df)

    def test_extra_kwargs_ignored_on_save(self, tmp_path):
        template = str(tmp_path / "{subject}.csv")
        csv = CsvFile(template)
        df = pd.DataFrame({"x": [1]})
        # db=, __fn=, etc. should not raise
        csv.save(df, subject=1, db=None, __fn="something")
        loaded = csv.load(subject=1)
        pd.testing.assert_frame_equal(loaded, df)

    def test_repr(self):
        csv = CsvFile("data/{subject}.csv")
        assert "CsvFile" in repr(csv)


# ---------------------------------------------------------------------------
# MatFile
# ---------------------------------------------------------------------------

class TestMatFile:
    def test_save_and_load_dict(self, tmp_path):
        pytest.importorskip("scipy")
        template = str(tmp_path / "{subject}.mat")
        mat = MatFile(template)
        data = {"arr": np.array([1.0, 2.0, 3.0])}
        mat.save(data, subject=1)
        loaded = mat.load(subject=1)
        assert "arr" in loaded

    def test_save_array_wrapped_in_data_key(self, tmp_path):
        pytest.importorskip("scipy")
        template = str(tmp_path / "{subject}.mat")
        mat = MatFile(template)
        arr = np.array([10.0, 20.0])
        mat.save(arr, subject=1)
        loaded = mat.load(subject=1)
        assert "data" in loaded

    def test_save_creates_directories(self, tmp_path):
        pytest.importorskip("scipy")
        template = str(tmp_path / "a" / "b" / "{subject}.mat")
        mat = MatFile(template)
        mat.save({"x": np.array([1.0])}, subject=1)
        assert os.path.exists(str(tmp_path / "a" / "b" / "1.mat"))

    def test_extra_kwargs_ignored(self, tmp_path):
        pytest.importorskip("scipy")
        template = str(tmp_path / "{subject}.mat")
        mat = MatFile(template)
        data = {"v": np.array([1.0])}
        # Extra kwargs should not raise
        mat.save(data, subject=1, db=None, __fn="fn")
        mat.load(subject=1, db=None)

    def test_repr(self):
        mat = MatFile("data/{subject}.mat")
        assert "MatFile" in repr(mat)
