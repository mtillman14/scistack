"""Integration tests for introspect= flag on load() and for_each()."""

import json

import pytest
from scidb.store.database import _local

import scifor as _scifor
from scidb import BaseVariable, Fixed, configure_database, for_each

DEFAULT_SCHEMA_KEYS = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db_path = tmp_path / "test.duckdb"
    db = configure_database(db_path, DEFAULT_SCHEMA_KEYS)
    yield db
    _scifor.set_schema([])
    db.close()


@pytest.fixture(autouse=True)
def clear_global_db():
    yield
    if hasattr(_local, "database"):
        delattr(_local, "database")


@pytest.fixture
def ScalarVar(db):
    class ScalarVar(BaseVariable):
        schema_version = 1

    db.register(ScalarVar)
    return ScalarVar


# ---------------------------------------------------------------------------
# load(introspect=True) — non-df path
# ---------------------------------------------------------------------------


class TestLoadIntrospectNonDf:
    def test_returns_basevariable_unchanged(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        var = ScalarVar.load(subject=1, trial=1, introspect=True)
        assert isinstance(var, BaseVariable)
        assert var.data == 42

    def test_existing_attributes_populated(self, db, ScalarVar):
        record_id = ScalarVar.save(42, subject=1, trial=1)
        var = ScalarVar.load(subject=1, trial=1, introspect=True)
        assert var.record_id == record_id
        assert var.metadata is not None
        assert var.branch_params == {}
        assert var.content_hash is not None

    def test_where_attribute_is_none_without_filter(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        var = ScalarVar.load(subject=1, trial=1, introspect=True)
        assert var.where is None

    def test_where_attribute_echoes_filter(self, db, ScalarVar):
        # BaseVariable.to_db() stores under column "value"
        ScalarVar.save(10, subject=1, trial=1)
        filt = ScalarVar["value"] > 5
        var = ScalarVar.load(subject=1, trial=1, introspect=True, where=filt)
        assert var.where is filt

    def test_version_mode_default_latest(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        var = ScalarVar.load(subject=1, trial=1, introspect=True)
        assert var.version_mode == "latest"

    def test_version_mode_all(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        ScalarVar.save(99, subject=1, trial=1)
        results = ScalarVar.load(version="all", subject=1, trial=1, introspect=True)
        assert isinstance(results, list)
        for r in results:
            assert r.version_mode == "all"

    def test_multi_result_returns_list_of_basevariable(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        ScalarVar.save(20, subject=2, trial=1)
        results = ScalarVar.load(trial=1, introspect=True)
        assert isinstance(results, list)
        assert all(isinstance(r, BaseVariable) for r in results)
        assert all(r.where is None for r in results)
        assert all(r.version_mode == "latest" for r in results)

    def test_where_and_version_mode_absent_without_introspect(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        var = ScalarVar.load(subject=1, trial=1)
        assert not hasattr(var, "where")
        assert not hasattr(var, "version_mode")

    def test_branch_params_populated_from_for_each_pipeline(self, db, ScalarVar):
        # branch_params are set by for_each() pipeline constants, not direct saves.
        # Save raw input, run two for_each passes with different constants.
        class Raw(BaseVariable):
            schema_version = 1

        class Processed(BaseVariable):
            schema_version = 1

        db.register(Raw)
        db.register(Processed)

        Raw.save(1.0, subject=1, trial=1)

        def scale(signal, factor):
            return signal * factor

        for_each(
            scale,
            inputs={"signal": Raw, "factor": 2.0},
            outputs=[Processed],
            subject=[1],
            trial=[1],
        )
        for_each(
            scale,
            inputs={"signal": Raw, "factor": 3.0},
            outputs=[Processed],
            subject=[1],
            trial=[1],
        )

        p2 = Processed.load(subject=1, trial=1, factor=2.0, introspect=True)
        p3 = Processed.load(subject=1, trial=1, factor=3.0, introspect=True)
        # branch_params keys are namespaced as "{fn_name}.{param_name}"
        assert p2.branch_params.get("scale.factor") == 2.0
        assert p3.branch_params.get("scale.factor") == 3.0


# ---------------------------------------------------------------------------
# load(as_df=True, introspect=True)
# ---------------------------------------------------------------------------


class TestLoadIntrospectAsDf:
    def test_returns_dataframe(self, db, ScalarVar):
        import pandas as pd

        ScalarVar.save(42, subject=1, trial=1)
        df = ScalarVar.load(as_df=True, subject=1, trial=1, introspect=True)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_introspect_columns_present(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        df = ScalarVar.load(as_df=True, subject=1, trial=1, introspect=True)
        for col in (
            "record_id",
            "branch_params",
            "content_hash",
            "where",
            "version_mode",
        ):
            assert col in df.columns, f"missing column: {col}"

    def test_record_id_is_non_empty_string(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        df = ScalarVar.load(as_df=True, subject=1, trial=1, introspect=True)
        assert isinstance(df["record_id"].iloc[0], str)
        assert len(df["record_id"].iloc[0]) > 0

    def test_branch_params_is_dict(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        df = ScalarVar.load(as_df=True, subject=1, trial=1, introspect=True)
        assert isinstance(df["branch_params"].iloc[0], dict)

    def test_where_none_when_no_filter(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        df = ScalarVar.load(as_df=True, subject=1, trial=1, introspect=True)
        assert df["where"].iloc[0] is None

    def test_where_repr_when_filter_passed(self, db, ScalarVar):
        # BaseVariable.to_db() stores under column "value"
        ScalarVar.save(42, subject=1, trial=1)
        filt = ScalarVar["value"] > 5
        df = ScalarVar.load(as_df=True, subject=1, trial=1, introspect=True, where=filt)
        assert df["where"].iloc[0] == repr(filt)

    def test_version_mode_repeated(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        ScalarVar.save(20, subject=2, trial=1)
        df = ScalarVar.load(as_df=True, trial=1, introspect=True)
        assert (df["version_mode"] == "latest").all()

    def test_column_order_schema_data_introspect(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        df = ScalarVar.load(as_df=True, subject=1, trial=1, introspect=True)
        cols = list(df.columns)
        data_idx = cols.index("data")
        record_id_idx = cols.index("record_id")
        assert data_idx < record_id_idx

    def test_no_introspect_columns_without_flag(self, db, ScalarVar):
        ScalarVar.save(42, subject=1, trial=1)
        df = ScalarVar.load(as_df=True, subject=1, trial=1)
        for col in (
            "record_id",
            "branch_params",
            "content_hash",
            "where",
            "version_mode",
        ):
            assert col not in df.columns


# ---------------------------------------------------------------------------
# for_each(introspect=True)
# ---------------------------------------------------------------------------


def _identity(x):
    return x


class TestForEachIntrospect:
    def _setup(self, db, ScalarVar):
        """Save records and register an output class. Returns the Out class."""

        class Out(BaseVariable):
            schema_version = 1

        db.register(Out)
        return Out

    def test_record_id_column_present(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        ScalarVar.save(20, subject=2, trial=1)
        Out = self._setup(db, ScalarVar)

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1, 2],
            trial=[1],
            introspect=True,
        )
        assert "_record_id_x" in result.columns

    def test_record_id_values_are_strings(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        Out = self._setup(db, ScalarVar)

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1],
            trial=[1],
            introspect=True,
        )
        assert isinstance(result["_record_id_x"].iloc[0], str)
        assert len(result["_record_id_x"].iloc[0]) > 0

    def test_branch_params_column_present_and_is_dict(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        Out = self._setup(db, ScalarVar)

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1],
            trial=[1],
            introspect=True,
        )
        assert "_branch_params_x" in result.columns
        assert isinstance(result["_branch_params_x"].iloc[0], dict)

    def test_call_id_column_is_16char_hex(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        Out = self._setup(db, ScalarVar)

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1],
            trial=[1],
            introspect=True,
        )
        assert "_call_id" in result.columns
        call_id = result["_call_id"].iloc[0]
        assert isinstance(call_id, str) and len(call_id) == 16
        int(call_id, 16)  # valid hex

    def test_call_id_same_on_every_row(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        ScalarVar.save(20, subject=2, trial=1)
        Out = self._setup(db, ScalarVar)

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1, 2],
            trial=[1],
            introspect=True,
        )
        assert result["_call_id"].nunique() == 1

    def test_config_keys_is_valid_json_with_fn(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        Out = self._setup(db, ScalarVar)

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1],
            trial=[1],
            introspect=True,
        )
        assert "_config_keys" in result.columns
        ck = json.loads(result["_config_keys"].iloc[0])
        assert "__fn" in ck
        assert "__fn_hash" in ck

    def test_where_column_none_when_no_filter(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        Out = self._setup(db, ScalarVar)

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1],
            trial=[1],
            introspect=True,
        )
        assert "_where" in result.columns
        assert result["_where"].iloc[0] is None

    def test_where_column_set_when_filter_passed(self, db, ScalarVar):
        # BaseVariable.to_db() stores under column "value"
        ScalarVar.save(10, subject=1, trial=1)
        Out = self._setup(db, ScalarVar)
        filt = ScalarVar["value"] > 5

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1],
            trial=[1],
            where=filt,
            introspect=True,
        )
        assert "_where" in result.columns
        assert result["_where"].iloc[0] == repr(filt)

    def test_fixed_input_record_id_same_on_every_row(self, db, ScalarVar):
        # Fixed input: one record used for all combos → same record_id every row.
        baseline_rid = ScalarVar.save(100, subject=1, trial=1)

        class Dummy(BaseVariable):
            schema_version = 1

        class Out(BaseVariable):
            schema_version = 1

        db.register(Dummy)
        db.register(Out)

        Dummy.save(1, subject=1, trial=1)
        Dummy.save(2, subject=2, trial=1)

        # Return x unchanged; fixed is only present to verify its record_id tracking.
        def f(x, fixed):
            return x

        result = for_each(
            f,
            inputs={"x": Dummy, "fixed": Fixed(ScalarVar, subject=1, trial=1)},
            outputs=[Out],
            subject=[1, 2],
            trial=[1],
            introspect=True,
        )
        assert "_record_id_fixed" in result.columns
        assert result["_record_id_fixed"].nunique() == 1
        assert result["_record_id_fixed"].iloc[0] == baseline_rid

    def test_column_order_schema_outputs_introspect(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        Out = self._setup(db, ScalarVar)

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1],
            trial=[1],
            introspect=True,
        )
        cols = list(result.columns)
        out_idx = cols.index("Out")
        record_id_idx = cols.index("_record_id_x")
        call_id_idx = cols.index("_call_id")
        assert out_idx < record_id_idx < call_id_idx

    def test_no_introspect_columns_by_default(self, db, ScalarVar):
        ScalarVar.save(10, subject=1, trial=1)
        Out = self._setup(db, ScalarVar)

        result = for_each(
            _identity,
            inputs={"x": ScalarVar},
            outputs=[Out],
            subject=[1],
            trial=[1],
        )
        for col in result.columns:
            assert not col.startswith("_record_id_")
            assert not col.startswith("_branch_params_")
        assert "_call_id" not in result.columns
        assert "_config_keys" not in result.columns
        assert "_where" not in result.columns
