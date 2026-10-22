"""
Declared schema-key types are STORED IN THE DATABASE (scidb.schema_types).

User decision 2026-10-21: a key's type decides how its values are stored, so
it belongs with the dataset, not with the script that happens to open it. The
GUI opened databases without ``schema_key_types=`` and so never saw a
declaration a script had made. Now:

* ``configure_database(schema_key_types=...)`` writes to the database: a key
  with no stored type gets it, the same type is a no-op, a different type
  raises SchemaKeyTypeConflictError;
* omitting the argument uses what is stored;
* ``db.set_schema_key_type`` changes or clears it, refusing a change that
  would re-spell stored values unless confirmed.
"""

from __future__ import annotations

import pytest
import scifor as _scifor

from scidb import (
    BaseVariable,
    SchemaKeyTypeChangeError,
    SchemaKeyTypeConflictError,
    SchemaKeyTypeError,
    configure_database,
)
from scidb import schema_types
from scidb.database import DatabaseManager

SCHEMA = ["subject", "trial"]


class StoredTypeValue(BaseVariable):
    pass


@pytest.fixture
def db_path(tmp_path):
    _scifor.set_schema([])
    yield tmp_path / "stored_types.duckdb"
    _scifor.set_schema([])


def _open(path, key_types=None):
    return configure_database(path, SCHEMA, schema_key_types=key_types)


def _stored_trials(db):
    """Raw VARCHAR spellings in _schema, no load-time restore."""
    return sorted(db._duck.distinct_schema_values("trial"))


class TestDeclarationPersists:
    def test_kwarg_is_stored_and_read_back_without_it(self, db_path):
        db = _open(db_path, {"trial": "numeric"})
        db.close()

        db = _open(db_path)  # no kwarg: the GUI's route
        try:
            assert db.dataset_schema_key_types == {"trial": "numeric"}
            StoredTypeValue.save(1.0, subject="s1", trial="001")
            assert _stored_trials(db) == ["1"]
        finally:
            db.close()

    def test_same_kwarg_again_writes_nothing(self, db_path):
        _open(db_path, {"trial": "numeric"}).close()
        db = _open(db_path, {"trial": "numeric"})
        try:
            assert len(schema_types.history(db._duck, "trial")) == 1
        finally:
            db.close()

    def test_conflicting_kwarg_raises_naming_both_types(self, db_path):
        _open(db_path, {"trial": "numeric"}).close()
        with pytest.raises(SchemaKeyTypeConflictError) as exc:
            _open(db_path, {"trial": "string"})
        msg = str(exc.value)
        assert "'numeric'" in msg and "'string'" in msg and "trial" in msg

    def test_kwarg_validation_happens_before_the_file_opens(self, tmp_path):
        with pytest.raises(ValueError, match="not schema keys"):
            DatabaseManager(tmp_path / "x.duckdb", SCHEMA, dataset_schema_key_types={"nope": "numeric"})
        with pytest.raises(ValueError, match="must be one of"):
            DatabaseManager(tmp_path / "y.duckdb", SCHEMA, dataset_schema_key_types={"trial": "int"})


class TestSetSchemaKeyType:
    def test_set_and_clear_append_history(self, db_path):
        db = _open(db_path)
        try:
            assert db.dataset_schema_key_types == {}
            db.set_schema_key_type("trial", "numeric", source="test")
            assert db.dataset_schema_key_types == {"trial": "numeric"}
            db.set_schema_key_type("trial", None, source="test")
            assert db.dataset_schema_key_types == {}
            rows = schema_types.history(db._duck, "trial")
            assert [r["key_type"] for r in rows] == ["numeric", None]
            assert {r["source"] for r in rows} == {"test"}
        finally:
            db.close()

    def test_setting_the_stored_type_is_a_no_op(self, db_path):
        db = _open(db_path, {"trial": "string"})
        try:
            db.set_schema_key_type("trial", "string")
            assert len(schema_types.history(db._duck, "trial")) == 1
        finally:
            db.close()

    def test_unknown_key_rejected(self, db_path):
        db = _open(db_path)
        try:
            with pytest.raises(ValueError, match="schema key"):
                db.set_schema_key_type("speed", "numeric")
        finally:
            db.close()

    def test_load_type_follows_a_change_immediately(self, db_path):
        """The per-key numeric/string cache must not outlive a type change."""
        db = _open(db_path)
        try:
            for trial in ["1", "2"]:
                StoredTypeValue.save(1.0, subject="s1", trial=trial)
            assert {type(v) for v in StoredTypeValue.load(as_df=True)["trial"]} == {int}
            db.set_schema_key_type("trial", "string")
            assert {type(v) for v in StoredTypeValue.load(as_df=True)["trial"]} == {str}
        finally:
            db.close()


class TestRespellingImpact:
    def test_impact_lists_respelled_and_merged(self, db_path):
        db = _open(db_path)
        try:
            for trial in ["001", "1", "2"]:
                StoredTypeValue.save(1.0, subject="s1", trial=trial)
            imp = db.schema_key_type_impact("trial", "numeric")
            assert imp["from"] is None and imp["to"] == "numeric"
            assert imp["n_values"] == 3
            assert imp["respelled"] == [["001", "1"]]
            assert imp["merged"] == {"1": ["001", "1"]}
            assert imp["invalid"] == []
            # Nothing written by asking.
            assert db.dataset_schema_key_types == {}
        finally:
            db.close()

    def test_string_and_clear_never_respell(self, db_path):
        db = _open(db_path, {"trial": "numeric"})
        try:
            StoredTypeValue.save(1.0, subject="s1", trial="001")
            for new in ("string", None):
                imp = db.schema_key_type_impact("trial", new)
                assert imp["respelled"] == [] and imp["merged"] == {}
        finally:
            db.close()

    def test_unconfirmed_respelling_refused_with_impact(self, db_path):
        db = _open(db_path)
        try:
            StoredTypeValue.save(1.0, subject="s1", trial="001")
            with pytest.raises(SchemaKeyTypeChangeError) as exc:
                db.set_schema_key_type("trial", "numeric")
            assert exc.value.impact["respelled"] == [["001", "1"]]
            assert "re-run" in str(exc.value)
            assert db.dataset_schema_key_types == {}

            db.set_schema_key_type("trial", "numeric", confirm_respelling=True)
            assert db.dataset_schema_key_types == {"trial": "numeric"}
            # Old rows keep their spelling; new saves are canonical.
            StoredTypeValue.save(2.0, subject="s1", trial="002")
            assert _stored_trials(db) == ["001", "2"]
        finally:
            db.close()

    def test_non_numeric_values_block_numeric(self, db_path):
        db = _open(db_path)
        try:
            StoredTypeValue.save(1.0, subject="s1", trial="r")
            with pytest.raises(SchemaKeyTypeError, match="not numbers"):
                db.set_schema_key_type("trial", "numeric", confirm_respelling=True)
            assert db.dataset_schema_key_types == {}
        finally:
            db.close()

    def test_first_kwarg_declaration_on_padded_data_is_refused(self, db_path):
        db = _open(db_path)
        StoredTypeValue.save(1.0, subject="s1", trial="001")
        db.close()
        with pytest.raises(SchemaKeyTypeChangeError, match="confirm_respelling=True"):
            _open(db_path, {"trial": "numeric"})


class TestOtherProcesses:
    def test_reopen_sees_a_change_made_elsewhere(self, db_path):
        db = _open(db_path)
        db.close()

        other = DatabaseManager(db_path, SCHEMA)
        other.set_schema_key_type("trial", "numeric", source="other")
        other.close()

        db.reopen()
        try:
            assert db.dataset_schema_key_types == {"trial": "numeric"}
        finally:
            db.close()

    def test_read_only_database_without_the_table_reads_empty(self, db_path):
        db = _open(db_path)
        db._duck._execute(f"DROP TABLE {schema_types.TABLE}")
        db.close()

        ro = DatabaseManager(db_path, SCHEMA, read_only=True)
        try:
            assert ro.dataset_schema_key_types == {}
            with pytest.raises(ValueError, match="read-only"):
                ro.set_schema_key_type("trial", "numeric")
        finally:
            ro.close()


def _double(x):
    return x * 2


class TestForEachArgument:
    """for_each(schema_key_types=...) exists for parity with scifor.for_each;
    in scidb it records the types in the database, which owns them."""

    def test_records_in_the_database(self, db_path):
        from scidb import for_each

        db = _open(db_path)
        try:
            for_each(
                _double, {"x": 1.0}, [StoredTypeValue],
                subject=["s1"], trial=["001"],
                schema_key_types={"trial": "numeric"},
            )
            assert db.dataset_schema_key_types == {"trial": "numeric"}
            assert schema_types.history(db._duck, "trial")[0]["source"] == "for_each"
            assert _stored_trials(db) == ["1"]
        finally:
            db.close()

    def test_dry_run_writes_nothing(self, db_path):
        from scidb import for_each

        db = _open(db_path)
        try:
            for_each(
                _double, {"x": 1.0}, [StoredTypeValue],
                subject=["s1"], trial=[1], dry_run=True,
                schema_key_types={"trial": "numeric"},
            )
            assert db.dataset_schema_key_types == {}
        finally:
            db.close()

    def test_conflict_with_stored_type_raises(self, db_path):
        from scidb import for_each

        db = _open(db_path, {"trial": "string"})
        try:
            with pytest.raises(SchemaKeyTypeConflictError):
                for_each(
                    _double, {"x": 1.0}, [StoredTypeValue],
                    subject=["s1"], trial=[1],
                    schema_key_types={"trial": "numeric"},
                )
        finally:
            db.close()


class TestCli:
    def test_set_and_list(self, db_path, capsys):
        from scidb.inspect.cli import main

        db = _open(db_path)
        StoredTypeValue.save(1.0, subject="s1", trial="001")
        db.close()

        assert main(["--db", str(db_path), "key-types"]) == 0
        assert "undeclared" in capsys.readouterr().out

        # Re-spelling without --confirm is refused with the impact.
        assert main(["--db", str(db_path), "set-key-type", "trial", "numeric"]) == 1
        assert "re-run" in capsys.readouterr().err

        assert main(["--db", str(db_path), "set-key-type", "trial", "numeric", "--confirm"]) == 0
        assert main(["--db", str(db_path), "key-types", "--history"]) == 0
        out = capsys.readouterr().out
        assert "numeric" in out and "cli" in out
