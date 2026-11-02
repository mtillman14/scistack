"""Tests for Merge input wrapper.

These run with NO database, which is what sends a Merge down the per-combo
path (``PerComboLoaderMerge``: each constituent's own ``.load()``) that the
mocks implement. Any db — even a mock — switches scidb to bulk-loading the
constituents from it instead (``scidb.foreach._load_input``). Saving, though,
needs a database, and since 2026-09-30 a failed save raises OutputSaveError
instead of being logged and ignored. So the for_each calls here pass
``save=False``: they assert what the function RECEIVED, never what was saved.
"""

import numpy as np
import pandas as pd
import pytest

from scidb import ColumnSelection, Fixed, Merge, for_each

# --- Mock variable classes ---


class MockDataFrameVar:
    """Mock variable that returns DataFrame data."""

    saved_data = []
    _data = None
    _metadata = None

    def __init__(self, data):
        self.data = data
        self.metadata = self._metadata

    @classmethod
    def load(cls, **metadata):
        if cls._data is None:
            raise ValueError("No data configured")
        inst = cls(cls._data)
        inst.metadata = metadata
        return inst

    @classmethod
    def save(cls, data, **metadata):
        cls.saved_data.append({"data": data, "metadata": metadata})

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls._data = None
        cls._metadata = None


class GaitData(MockDataFrameVar):
    saved_data = []
    _data = None
    _metadata = None

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls._data = None
        cls._metadata = None


class ForceData(MockDataFrameVar):
    saved_data = []
    _data = None
    _metadata = None

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls._data = None
        cls._metadata = None


class PareticSide(MockDataFrameVar):
    saved_data = []
    _data = None
    _metadata = None

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls._data = None
        cls._metadata = None


class StepLength(MockDataFrameVar):
    saved_data = []
    _data = None
    _metadata = None

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls._data = None
        cls._metadata = None


class StepWidth(MockDataFrameVar):
    saved_data = []
    _data = None
    _metadata = None

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls._data = None
        cls._metadata = None


class CadenceRate(MockDataFrameVar):
    saved_data = []
    _data = None
    _metadata = None

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls._data = None
        cls._metadata = None


class MockOutput(MockDataFrameVar):
    saved_data = []
    _data = None
    _metadata = None

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls._data = None
        cls._metadata = None


@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset all mock state before each test."""
    for cls in [
        GaitData,
        ForceData,
        PareticSide,
        StepLength,
        StepWidth,
        CadenceRate,
        MockOutput,
    ]:
        cls.reset()
    yield


# === Test Merge class construction ===


class TestMergeClass:
    def test_init_with_two_var_types(self):
        m = Merge(GaitData, ForceData)
        assert len(m.tables) == 2

    def test_init_with_three_var_types(self):
        m = Merge(StepLength, StepWidth, CadenceRate)
        assert len(m.tables) == 3

    def test_init_with_one_raises(self):
        with pytest.raises(ValueError, match="at least 2"):
            Merge(GaitData)

    def test_init_with_zero_raises(self):
        with pytest.raises(ValueError, match="at least 2"):
            Merge()

    def test_nested_merge_raises(self):
        with pytest.raises(TypeError, match="Cannot nest Merge"):
            Merge(GaitData, Merge(StepLength, StepWidth))

    def test_name_property(self):
        m = Merge(GaitData, ForceData)
        assert m.__name__ == "Merge(GaitData, ForceData)"

    def test_name_with_fixed(self):
        m = Merge(GaitData, Fixed(PareticSide, session="BL"))
        assert "Fixed(PareticSide, session=BL)" in m.__name__
        assert m.__name__.startswith("Merge(")

    def test_name_with_column_selection(self):
        cs = ColumnSelection(GaitData, ["force"])
        m = Merge(cs, PareticSide)
        name = m.__name__
        assert "Merge(" in name
        assert "PareticSide" in name


# === Test Merge in for_each ===


class TestMergeInForEach:
    def test_merge_two_dataframes(self):
        """Two DataFrame variables merge all columns into one DataFrame."""
        GaitData._data = pd.DataFrame({"side": ["L", "R"], "angle": [10.0, 20.0]})
        ForceData._data = pd.DataFrame({"fx": [1.0, 2.0], "fy": [3.0, 4.0]})

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(GaitData, ForceData)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        assert len(received) == 1
        df = received[0]
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["side", "angle", "fx", "fy"]
        assert len(df) == 2

    def test_merge_dataframe_and_array(self):
        """DataFrame + 1D array of same length: array becomes a column."""
        GaitData._data = pd.DataFrame(
            {"side": ["L", "R", "L"], "force": [1.0, 2.0, 3.0]}
        )
        PareticSide._data = np.array(["paretic", "nonparetic", "paretic"])

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(GaitData, PareticSide)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        assert len(received) == 1
        df = received[0]
        assert isinstance(df, pd.DataFrame)
        assert "side" in df.columns
        assert "force" in df.columns
        assert "PareticSide" in df.columns
        assert len(df) == 3

    def test_merge_dataframe_and_scalar(self):
        """DataFrame + scalar: scalar is broadcast as a column."""
        GaitData._data = pd.DataFrame({"side": ["L", "R"], "force": [1.0, 2.0]})
        PareticSide._data = "left"

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(GaitData, PareticSide)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert isinstance(df, pd.DataFrame)
        assert list(df["PareticSide"]) == ["left", "left"]

    def test_merge_two_arrays(self):
        """Two arrays produce a two-column DataFrame."""
        StepLength._data = np.array([0.65, 0.72, 0.68])
        StepWidth._data = np.array([0.12, 0.15, 0.13])

        received = []

        def analyze(kinematics):
            received.append(kinematics.copy())
            return "result"

        for_each(
            analyze,
            inputs={"kinematics": Merge(StepLength, StepWidth)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["StepLength", "StepWidth"]
        np.testing.assert_array_almost_equal(
            df["StepLength"].values, [0.65, 0.72, 0.68]
        )

    def test_merge_two_scalars(self):
        """Two scalars produce a single-row, two-column DataFrame."""
        StepLength._data = 0.65
        StepWidth._data = 0.12

        received = []

        def analyze(kinematics):
            received.append(kinematics.copy())
            return "result"

        for_each(
            analyze,
            inputs={"kinematics": Merge(StepLength, StepWidth)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df["StepLength"].iloc[0] == 0.65
        assert df["StepWidth"].iloc[0] == 0.12

    def test_merge_three_variables(self):
        """Three variables merge into one DataFrame."""
        StepLength._data = np.array([0.65, 0.72])
        StepWidth._data = np.array([0.12, 0.15])
        CadenceRate._data = np.array([110.0, 115.0])

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(StepLength, StepWidth, CadenceRate)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert list(df.columns) == ["StepLength", "StepWidth", "CadenceRate"]
        assert len(df) == 2

    def test_merge_result_is_dataframe(self):
        """Verify the function receives a pd.DataFrame, not a BaseVariable."""
        StepLength._data = np.array([1.0, 2.0])
        StepWidth._data = np.array([3.0, 4.0])

        received_types = []

        def analyze(data):
            received_types.append(type(data))
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(StepLength, StepWidth)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        assert received_types[0] is pd.DataFrame

    def test_merge_multiple_iterations(self):
        """Merge works across multiple iterations."""
        call_count = [0]

        class TrackingA:
            @classmethod
            def load(cls, **meta):
                inst = type("obj", (), {"data": np.array([meta["subject"] * 1.0])})()
                return inst

        class TrackingB:
            @classmethod
            def load(cls, **meta):
                inst = type("obj", (), {"data": np.array([meta["subject"] * 2.0])})()
                return inst

        received = []

        def process(data):
            received.append(data.copy())
            call_count[0] += 1
            return "result"

        for_each(
            process,
            inputs={"data": Merge(TrackingA, TrackingB)},
            outputs=[MockOutput],
            subject=[1, 2, 3],
            save=False,
        )

        assert call_count[0] == 3
        # Check subject=1 iteration
        assert received[0]["TrackingA"].iloc[0] == 1.0
        assert received[0]["TrackingB"].iloc[0] == 2.0
        # Check subject=3 iteration
        assert received[2]["TrackingA"].iloc[0] == 3.0
        assert received[2]["TrackingB"].iloc[0] == 6.0

    def test_merge_with_constants(self):
        """Merge alongside constant inputs."""
        StepLength._data = np.array([1.0])
        StepWidth._data = np.array([2.0])

        received = []

        def process(data, smoothing):
            received.append((data.copy(), smoothing))
            return "result"

        for_each(
            process,
            inputs={"data": Merge(StepLength, StepWidth), "smoothing": 0.5},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df, smoothing = received[0]
        assert isinstance(df, pd.DataFrame)
        assert smoothing == 0.5

    def test_merge_with_list_data(self):
        """List data becomes a column."""
        StepLength._data = [0.65, 0.72, 0.68]
        StepWidth._data = [0.12, 0.15, 0.13]

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(StepLength, StepWidth)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert list(df.columns) == ["StepLength", "StepWidth"]
        assert len(df) == 3


# === Test Merge composability ===


class TestMergeComposability:
    def test_merge_with_fixed(self):
        """Fixed override within Merge."""
        loaded_meta = []

        class VarA:
            @classmethod
            def load(cls, **meta):
                loaded_meta.append(("VarA", meta))
                return type("obj", (), {"data": np.array([1.0])})()

        class VarB:
            @classmethod
            def load(cls, **meta):
                loaded_meta.append(("VarB", meta))
                return type("obj", (), {"data": np.array([2.0])})()

        def process(data):
            return "result"

        for_each(
            process,
            inputs={"data": Merge(VarA, Fixed(VarB, session="BL"))},
            outputs=[MockOutput],
            subject=[1],
            session=["A"],
            save=False,
        )

        # VarA should get session="A", VarB should get session="BL"
        assert loaded_meta[0] == ("VarA", {"subject": 1, "session": "A"})
        assert loaded_meta[1] == ("VarB", {"subject": 1, "session": "BL"})

    def test_merge_with_column_selection(self):
        """ColumnSelection within Merge extracts specific columns."""
        GaitData._data = pd.DataFrame(
            {
                "side": ["L", "R"],
                "force": [1.0, 2.0],
                "angle": [10.0, 20.0],
            }
        )
        PareticSide._data = np.array(["paretic", "nonparetic"])

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        cs = ColumnSelection(GaitData, ["force"])
        for_each(
            analyze,
            inputs={"data": Merge(cs, PareticSide)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert "force" in df.columns
        assert "side" not in df.columns  # Not selected
        assert "angle" not in df.columns  # Not selected
        assert "PareticSide" in df.columns

    def test_merge_with_fixed_and_column_selection(self):
        """Fixed wrapping ColumnSelection within Merge."""
        loaded_meta = []

        class TrackingGait:
            @classmethod
            def load(cls, **meta):
                loaded_meta.append(meta)
                return type(
                    "obj",
                    (),
                    {
                        "data": pd.DataFrame(
                            {
                                "side": ["L", "R"],
                                "force": [1.0, 2.0],
                                "angle": [10.0, 20.0],
                            }
                        )
                    },
                )()

        PareticSide._data = np.array(["paretic", "nonparetic"])

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={
                "data": Merge(
                    Fixed(ColumnSelection(TrackingGait, ["force"]), session="BL"),
                    PareticSide,
                ),
            },
            outputs=[MockOutput],
            subject=[1],
            session=["A"],
            save=False,
        )

        # TrackingGait should be loaded with session="BL" override
        assert loaded_meta[0]["session"] == "BL"
        df = received[0]
        assert list(df.columns) == ["force", "PareticSide"]


# === Test Merge errors ===


class TestMergeErrors:
    def test_constituent_load_fails(self, caplog):
        """When a constituent fails to load, iteration is skipped.

        Per-iteration [skip] lines are DEBUG records on the "scifor" logger
        (not stdout) since the logging redesign."""
        import logging

        StepLength._data = np.array([1.0])

        class FailingVar:
            @classmethod
            def load(cls, **meta):
                raise ValueError("data missing")

        def process(data):
            return "result"

        with caplog.at_level(logging.DEBUG, logger="scifor"):
            for_each(
                process,
                inputs={"data": Merge(StepLength, FailingVar)},
                outputs=[MockOutput],
                subject=[1],
            )

        assert len(MockOutput.saved_data) == 0
        assert any("[skip]" in r.getMessage() for r in caplog.records)

    def test_constituent_returns_multiple(self):
        """Multiple matches from a constituent should raise ValueError."""
        StepLength._data = np.array([1.0])

        class MultiVar:
            @classmethod
            def load(cls, **meta):
                # Return a list (multiple matches)
                return [
                    type("obj", (), {"data": np.array([1.0])})(),
                    type("obj", (), {"data": np.array([2.0])})(),
                ]

        def process(data):
            return "result"

        # The error is caught by for_each's [skip] handler
        for_each(
            process,
            inputs={"data": Merge(StepLength, MultiVar)},
            outputs=[MockOutput],
            subject=[1],
        )

        assert len(MockOutput.saved_data) == 0

    def test_column_name_conflict(self):
        """Overlapping column names should cause an error."""
        GaitData._data = pd.DataFrame({"force": [1.0], "angle": [2.0]})
        ForceData._data = pd.DataFrame({"force": [3.0], "velocity": [4.0]})

        def process(data):
            return "result"

        # Column "force" appears in both — should skip with error
        for_each(
            process,
            inputs={"data": Merge(GaitData, ForceData)},
            outputs=[MockOutput],
            subject=[1],
        )

        assert len(MockOutput.saved_data) == 0

    def test_row_count_mismatch(self):
        """Different row counts should cause an error."""
        StepLength._data = np.array([1.0, 2.0, 3.0])
        StepWidth._data = np.array([1.0, 2.0])

        def process(data):
            return "result"

        for_each(
            process,
            inputs={"data": Merge(StepLength, StepWidth)},
            outputs=[MockOutput],
            subject=[1],
        )

        assert len(MockOutput.saved_data) == 0

    def test_fixed_wrapping_merge_raises(self):
        """Fixed(Merge(...)) should raise TypeError."""
        with pytest.raises(TypeError, match="Fixed cannot wrap a Merge"):
            for_each(
                lambda data: "result",
                inputs={"data": Fixed(Merge(StepLength, StepWidth), session="A")},
                outputs=[MockOutput],
                subject=[1],
            )


# === Test dry run ===


class TestMergeDryRun:
    def test_dry_run_shows_merge(self, capsys):
        """Dry run output shows merge constituents."""
        for_each(
            lambda data: "result",
            inputs={"data": Merge(GaitData, PareticSide)},
            outputs=[MockOutput],
            dry_run=True,
            subject=[1],
        )

        output = capsys.readouterr().out
        assert "merge data:" in output
        assert "GaitData" in output
        assert "PareticSide" in output

    def test_dry_run_merge_with_fixed(self, capsys):
        """Dry run shows Fixed details within Merge."""
        for_each(
            lambda data: "result",
            inputs={"data": Merge(GaitData, Fixed(PareticSide, session="BL"))},
            outputs=[MockOutput],
            dry_run=True,
            subject=[1],
            session=["A"],
        )

        output = capsys.readouterr().out
        assert "merge data:" in output
        assert "session=BL" in output

    def test_format_inputs_with_merge(self, capsys):
        """Summary line shows Merge."""
        for_each(
            lambda data: "result",
            inputs={"data": Merge(GaitData, ForceData)},
            outputs=[MockOutput],
            dry_run=True,
            subject=[1],
        )

        output = capsys.readouterr().out
        assert "Merge(GaitData, ForceData)" in output


# === Edge cases ===


class TestMergeEdgeCases:
    def test_scalar_broadcast(self):
        """Scalar is broadcast to match multi-row DataFrame."""
        GaitData._data = pd.DataFrame({"val": [1.0, 2.0, 3.0]})
        PareticSide._data = "left"

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(GaitData, PareticSide)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert len(df) == 3
        assert list(df["PareticSide"]) == ["left", "left", "left"]

    def test_2d_array_columns(self):
        """2D numpy array creates multiple columns."""
        arr = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        GaitData._data = arr
        PareticSide._data = np.array(["p", "np", "p"])

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(GaitData, PareticSide)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert "GaitData_0" in df.columns
        assert "GaitData_1" in df.columns
        assert "PareticSide" in df.columns
        assert len(df) == 3

    def test_column_order_preserved(self):
        """Columns appear in constituent order."""
        GaitData._data = pd.DataFrame({"b_col": [1], "a_col": [2]})
        ForceData._data = pd.DataFrame({"d_col": [3], "c_col": [4]})

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(GaitData, ForceData)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert list(df.columns) == ["b_col", "a_col", "d_col", "c_col"]

    def test_all_scalars_produce_single_row(self):
        """All scalar constituents produce a 1-row DataFrame."""
        StepLength._data = 0.65
        StepWidth._data = 0.12
        CadenceRate._data = 110.0

        received = []

        def analyze(data):
            received.append(data.copy())
            return "result"

        for_each(
            analyze,
            inputs={"data": Merge(StepLength, StepWidth, CadenceRate)},
            outputs=[MockOutput],
            subject=[1],
            save=False,
        )

        df = received[0]
        assert len(df) == 1
        assert df["StepLength"].iloc[0] == 0.65
        assert df["CadenceRate"].iloc[0] == 110.0
