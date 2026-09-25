"""Schema key type declarations (numeric/string) and spelling canonicalization.

The hybrid contract:
- No declaration needed while every path match is exact (zero syntax burden).
- A PathInput numeric-fallback resolution (trial=1 matching "6MWT-001.mat")
  on an UNDECLARED schema key raises SchemaKeyTypeError asking for a one-time
  declaration.
- Declared "numeric" keys canonicalize unconditionally — every spelling of
  the same number ("001", 1, 1.0) collapses to one stored identity — from
  every source (explicit iterables, discovery combos, direct save/load).
- Declared "string" keys are verbatim: spelling IS identity, and PathInput
  never bridges spellings for them (exact matches only).
"""

import numpy as np
import pytest
from scidb.database import DatabaseManager
from scidb.schema_values import canonical_numeric_value
from scidb.exceptions import SchemaKeyTypeError

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "trial"]


class PathName(BaseVariable):
    pass


def read_name(filepath):
    """Stand-in processing fn: reads the resolved file's numeric content.

    Reading (not just naming) the file makes unresolved literal paths fail
    the combo, and the per-file content proves WHICH file was resolved.
    """
    from pathlib import Path

    return float(Path(str(filepath)).read_text().strip())


@pytest.fixture
def padded_tree(tmp_path):
    """tmp_path/data/1/6MWT-001.mat (content 1.5), .../6MWT-002.mat (2.5)"""
    d = tmp_path / "data" / "1"
    d.mkdir(parents=True)
    (d / "6MWT-001.mat").write_text("1.5")
    (d / "6MWT-002.mat").write_text("2.5")
    return tmp_path / "data"


def _make_db(tmp_path, key_types=None):
    _scifor.set_schema([])
    return configure_database(
        tmp_path / "test_key_types.duckdb", SCHEMA, schema_key_types=key_types
    )


@pytest.fixture
def db_numeric(tmp_path):
    db = _make_db(tmp_path, {"trial": "numeric"})
    yield db
    _scifor.set_schema([])
    db.close()


@pytest.fixture
def db_string(tmp_path):
    db = _make_db(tmp_path, {"trial": "string"})
    yield db
    _scifor.set_schema([])
    db.close()


@pytest.fixture
def db_undeclared(tmp_path):
    db = _make_db(tmp_path)
    yield db
    _scifor.set_schema([])
    db.close()


class TestDeclarationValidation:
    def test_unknown_key_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="not schema keys"):
            DatabaseManager(
                tmp_path / "x.duckdb",
                SCHEMA,
                dataset_schema_key_types={"nope": "numeric"},
            )

    def test_bad_type_value_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="must be one of"):
            DatabaseManager(
                tmp_path / "x.duckdb",
                SCHEMA,
                dataset_schema_key_types={"trial": "int"},
            )


class TestCanonicalNumericValue:
    def test_spellings_collapse(self):
        assert canonical_numeric_value("trial", "001") == 1
        assert canonical_numeric_value("trial", 1) == 1
        assert canonical_numeric_value("trial", 1.0) == 1
        assert canonical_numeric_value("trial", "1.50") == 1.5

    def test_non_numeric_raises(self):
        with pytest.raises(SchemaKeyTypeError, match="non-numeric"):
            canonical_numeric_value("trial", "abc")

    def test_bool_raises(self):
        with pytest.raises(SchemaKeyTypeError, match="bool"):
            canonical_numeric_value("trial", True)


class TestDirectSaveLoadIdentity:
    def test_padded_save_unpadded_load(self, db_numeric):
        PathName.save(np.array([1.0, 2.0]), subject=1, trial="001")
        var = PathName.load(subject=1, trial=1)
        assert not isinstance(var, list)  # exactly one record: one identity

    def test_string_key_spellings_stay_distinct(self, db_string):
        PathName.save(np.array([1.0]), subject=1, trial="001")
        PathName.save(np.array([2.0]), subject=1, trial="1")
        assert sorted(db_string.distinct_schema_values("trial")) == ["001", "1"]


class TestForEachNumericDeclared:
    def test_explicit_ints_resolve_and_store_canonical(self, db_numeric, padded_tree):
        for_each(
            read_name,
            {
                "filepath": _scifor.PathInput(
                    "{subject}/6MWT-{trial}.mat", root_folder=str(padded_tree), name="{subject}/6MWT-{trial}.mat"
                )
            },
            [PathName],
            subject=[1],
            trial=[1, 2],
        )
        # Files resolved despite padding; stored identity is canonical "1"/"2".
        assert sorted(db_numeric.distinct_schema_values("trial")) == ["1", "2"]
        # Per-file contents prove the padded files were the ones resolved.
        values = sorted(float(v.data) for v in PathName.load(subject=1))
        assert values == [1.5, 2.5]

    def test_discovery_driven_combos_canonicalized(self, db_numeric, padded_tree):
        for_each(
            read_name,
            {
                "filepath": _scifor.PathInput(
                    "{subject}/6MWT-{trial}.mat", root_folder=str(padded_tree), name="{subject}/6MWT-{trial}.mat"
                )
            },
            [PathName],
            subject=[],
            trial=[],
        )
        # Discovery captured "001"/"002" but the declaration canonicalizes:
        # same stored identity as an explicit trial=[1, 2] run.
        assert sorted(db_numeric.distinct_schema_values("trial")) == ["1", "2"]

    def test_discovery_and_explicit_runs_share_identity(self, db_numeric, padded_tree):
        pi = _scifor.PathInput(
            "{subject}/6MWT-{trial}.mat", root_folder=str(padded_tree), name="{subject}/6MWT-{trial}.mat"
        )
        for_each(read_name, {"filepath": pi}, [PathName], subject=[], trial=[])
        n_after_first = len(
            db_numeric._duck._fetchall(
                "SELECT record_id FROM _record WHERE type='PathName'"
            )
        )
        for_each(read_name, {"filepath": pi}, [PathName], subject=[1], trial=[1, 2])
        n_after_second = len(
            db_numeric._duck._fetchall(
                "SELECT record_id FROM _record WHERE type='PathName'"
            )
        )
        # The explicit run re-saves the same identities — no new records.
        assert n_after_second == n_after_first


class TestForEachUndeclared:
    def test_resolution_raises_declare_error(self, db_undeclared, padded_tree):
        with pytest.raises(SchemaKeyTypeError, match="schema_key_types"):
            for_each(
                read_name,
                {
                    "filepath": _scifor.PathInput(
                        "{subject}/6MWT-{trial}.mat", root_folder=str(padded_tree), name="{subject}/6MWT-{trial}.mat"
                    )
                },
                [PathName],
                subject=[1],
                trial=[1, 2],
            )

    def test_exact_matches_need_no_declaration(self, db_undeclared, padded_tree):
        # Discovery-driven: combos carry "001"/"002", every path literal-hits,
        # the fallback never fires, no declaration needed (hybrid contract).
        for_each(
            read_name,
            {
                "filepath": _scifor.PathInput(
                    "{subject}/6MWT-{trial}.mat", root_folder=str(padded_tree), name="{subject}/6MWT-{trial}.mat"
                )
            },
            [PathName],
            subject=[],
            trial=[],
        )
        assert sorted(db_undeclared.distinct_schema_values("trial")) == ["001", "002"]


class TestForEachStringDeclared:
    def test_padded_strings_resolve_exactly(self, db_string, padded_tree):
        for_each(
            read_name,
            {
                "filepath": _scifor.PathInput(
                    "{subject}/6MWT-{trial}.mat", root_folder=str(padded_tree), name="{subject}/6MWT-{trial}.mat"
                )
            },
            [PathName],
            subject=[1],
            trial=["001", "002"],
        )
        assert sorted(db_string.distinct_schema_values("trial")) == ["001", "002"]

    def test_numeric_values_never_bridge(self, db_string, padded_tree):
        # trial=1 renders "6MWT-1.mat"; string declaration forbids the
        # numeric bridge to "001", so the combo fails (skip), and no
        # declare-error is raised (the key IS declared).
        for_each(
            read_name,
            {
                "filepath": _scifor.PathInput(
                    "{subject}/6MWT-{trial}.mat", root_folder=str(padded_tree), name="{subject}/6MWT-{trial}.mat"
                )
            },
            [PathName],
            subject=[1],
            trial=[1],
        )
        rows = db_string._duck._fetchall(
            "SELECT COUNT(*) FROM _record WHERE type='PathName'"
        )
        assert rows[0][0] == 0
