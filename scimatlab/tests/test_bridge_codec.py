"""MATLAB custom to_db/from_db: the surrogate's codec flag.

``+scidb/+internal/codec.m`` decides whether a MATLAB class overrides
``to_db``/``from_db`` and ``ensure_registered`` passes it as
``register_matlab_variable(..., codec=True)``. MATLAB runs the conversion; the
flag makes Python store the resulting table in the columnar ``"custom": True``
form a Python codec class uses (DatabaseManager._has_custom_serialization).

Runs entirely in Python without MATLAB: the values handed to the bridge are
the tables MATLAB's to_db would produce.
"""

import json
import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np
import pandas as pd
import pytest
from scidb import BaseVariable
from scidb.database import DatabaseManager, configure_database
from scimatlab.bridge import register_matlab_variable, save_batch_bridge

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    import scifor as _scifor

    _scifor.set_schema([])
    db = configure_database(tmp_path / "codec.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


def _dtype(db, name):
    rows = db._duck._fetchall(
        "SELECT dtype FROM _variables WHERE variable_name = ?", [name]
    )
    return json.loads(rows[0][0])


def _traj_table(i):
    return pd.DataFrame({"t": np.arange(3, dtype=float) + i, "label": f"m{i}"})


class TestCodecFlag:
    def test_codec_true_marks_custom_serialization(self, db):
        cls = register_matlab_variable("CodecFlagA", codec=True)
        assert DatabaseManager._has_custom_serialization(cls)

    def test_default_registration_has_no_codec(self, db):
        cls = register_matlab_variable("CodecFlagB")
        assert not DatabaseManager._has_custom_serialization(cls)

    def test_flag_updates_an_existing_surrogate(self, db):
        cls = register_matlab_variable("CodecFlagC")
        assert register_matlab_variable("CodecFlagC", codec=True) is cls
        assert DatabaseManager._has_custom_serialization(cls)
        register_matlab_variable("CodecFlagC", codec=False)
        assert not DatabaseManager._has_custom_serialization(cls)

    def test_none_never_clears_a_flag(self, db):
        """The GUI registers MATLAB variables without knowing the codec."""
        cls = register_matlab_variable("CodecFlagD", codec=True)
        register_matlab_variable("CodecFlagD")
        assert DatabaseManager._has_custom_serialization(cls)

    def test_python_class_is_never_modified(self, db):
        class CodecFlagPy(BaseVariable):
            pass

        assert register_matlab_variable("CodecFlagPy", codec=True) is CodecFlagPy
        assert "to_db" not in CodecFlagPy.__dict__

    def test_declared_only_class_takes_the_flag(self, db):
        declared = type(
            "CodecFlagDecl", (BaseVariable,), {"_declared_only": True}
        )
        assert register_matlab_variable("CodecFlagDecl", codec=True) is declared
        assert DatabaseManager._has_custom_serialization(declared)


class TestCodecStorage:
    def test_direct_save_stores_custom_and_loads_table(self, db):
        cls = register_matlab_variable("CodecStoreA", codec=True)
        db.save_variable(cls, _traj_table(0), subject="1", session="A")
        assert _dtype(db, "CodecStoreA") == {"custom": True}
        loaded = cls.load(subject="1", session="A")
        pd.testing.assert_frame_equal(loaded.data, _traj_table(0))

    def test_save_batch_bridge_vertcat_rows(self, db):
        """MATLAB save_from_table: per-row to_db tables, vertcat + heights."""
        register_matlab_variable("CodecStoreB", codec=True)
        frames = [_traj_table(i) for i in range(3)]
        bulk = pd.concat(frames, ignore_index=True)
        rids = save_batch_bridge(
            "CodecStoreB",
            bulk,
            ["subject"],
            [np.array([1, 2, 3])],
            {"session": "A"},
            None,
            np.array([3, 3, 3]),
        ).split("\n")
        assert len(rids) == 3 and all(rids)
        assert _dtype(db, "CodecStoreB") == {"custom": True}
        cls = BaseVariable.get_subclass_by_name("CodecStoreB")
        for i in range(3):
            loaded = cls.load(subject=i + 1, session="A")
            pd.testing.assert_frame_equal(loaded.data, frames[i])

    def test_one_row_save_batch_bridge(self, db):
        """A one-row MATLAB save_from_table sends 0-d arrays (py.numpy.array
        of a 1x1 column); they must still count as one row."""
        register_matlab_variable("OneRowScalar")
        (rid,) = save_batch_bridge(
            "OneRowScalar",
            np.array(4.5),
            ["subject"],
            [np.array(1.0)],
            {"session": "A"},
            None,
            None,
        ).split("\n")
        assert rid
        cls = BaseVariable.get_subclass_by_name("OneRowScalar")
        assert cls.load(subject=1, session="A").data == 4.5

    def test_one_row_scalar_metadata_matches_direct_save(self, db):
        """number_to_python sends a one-row whole-number column as py.int,
        the same type save()'s metadata uses, so both routes give one record
        id (canonical_hash encodes int 1 as "1" and float 1.0 as "1.0")."""
        cls = register_matlab_variable("OneRowParity")
        rid_direct = db.save_variable(cls, 4.5, subject=1, session="A")
        (rid_batch,) = save_batch_bridge(
            "OneRowParity", np.array(4.5), ["subject"], [1], {"session": "A"},
            None, None,
        ).split("\n")
        assert rid_batch == rid_direct

    def test_int64_column_matches_direct_save(self, db):
        cls = register_matlab_variable("ColumnParity")
        rid_direct = db.save_variable(cls, 1.5, subject=2, session="A")
        rids = save_batch_bridge(
            "ColumnParity", np.array([0.5, 1.5]), ["subject"],
            [np.array([1, 2], dtype="int64")], {"session": "A"}, None, None,
        ).split("\n")
        assert rids[1] == rid_direct

    def test_integer_records_take_the_scalar_fast_path(self, db):
        """An integer record loads back as np.int64 (BIGINT .iloc); it must
        still be packed into scalar_data instead of the per-item path."""
        from scimatlab.bridge import load_and_extract

        cls = register_matlab_variable("IntScalarLoad")
        save_batch_bridge(
            "IntScalarLoad", np.array([100, 200], dtype="int64"), ["subject"],
            [np.array([1, 2], dtype="int64")], {"session": "A"}, None, None,
        )
        bulk = load_and_extract(cls, {"subject": 1, "session": "A"})
        assert "scalar_data" in bulk
        assert list(bulk["scalar_data"]) == [100.0]

    def test_numpy_scalar_to_python(self, db):
        from scimatlab.bridge import numpy_scalar_to_python

        ok, val = numpy_scalar_to_python(np.int64(100))
        assert ok and val == 100 and type(val) is int
        ok, val = numpy_scalar_to_python(np.bool_(True))
        assert ok and val is True
        ok, val = numpy_scalar_to_python(np.float32(1.5))
        assert ok and type(val) is float
        assert numpy_scalar_to_python(100) == [False, None]
        assert numpy_scalar_to_python(np.array([1, 2])) == [False, None]

    def test_is_dataframe(self, db):
        from scimatlab.bridge import is_dataframe

        assert is_dataframe(pd.DataFrame({"a": [1]})) is True
        assert is_dataframe(pd.DataFrame()) is True
        assert is_dataframe({"a": [1]}) is False
        assert is_dataframe(np.array([1])) is False

    def test_value_without_matlab_to_db_raises(self, db):
        cls = register_matlab_variable("CodecStoreC", codec=True)
        with pytest.raises(TypeError, match="without MATLAB's to_db"):
            db.save_variable(cls, {"v": 1.0}, subject="1", session="A")
