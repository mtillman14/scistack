"""Fixed/Merge/ColumnSelection/ColName/EachOf are now the same classes in
scifor and scidb (scidb re-exports scifor's Fixed/ColName/EachOf directly,
plus thin subclasses of ColumnSelection and Merge for DB-only methods --
comparison operators/.load() on ColumnSelection, a schema-id-keyed .to_csv()
on Merge). Coverage:

- isinstance(scidb.X, scifor.X) for every unified class.
- A bare scifor.ColumnSelection(plain_df, ...) / Fixed(plain_df, ...) now
  works under scidb.for_each -- previously impossible to even construct
  meaningfully, since scidb's old classes only ever wrapped variable types.
  This exercises the DataFrame-passthrough fast paths added to
  scidb.foreach._load_input / _resolve_per_combo_loader.
- Version-key (call_id) stability: to_key() output for Fixed/Merge/
  ColumnSelection must not silently regress into repr()-based identity
  (which would be non-deterministic across processes and fork every
  existing saved call site).
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root / "src"))

import scifor
from scifor import ColName as SciforColName
from scifor import ColumnSelection as SciforColumnSelection
from scifor import EachOf as SciforEachOf
from scifor import Fixed as SciforFixed
from scifor import Merge as SciforMerge

from scidb import BaseVariable, ColName, ColumnSelection, EachOf, Fixed, Merge
from scidb import configure_database, for_each
from scidb.foreach_config import ForEachConfig


class Measurement(BaseVariable):
    schema_version = 1


@pytest.fixture
def db(tmp_path):
    d = configure_database(tmp_path / "test.duckdb", ["subject"])
    yield d
    d.close()


class TestIsinstanceUnification:
    def test_fixed_is_scifor_fixed(self):
        assert Fixed is SciforFixed
        assert isinstance(Fixed(Measurement, subject=1), scifor.Fixed)

    def test_merge_is_a_scifor_merge_subclass(self):
        # Merge is NOT re-exported bare -- scidb's version adds a schema-id-
        # keyed .to_csv() (scifor's own .to_csv() only joins already-loaded
        # DataFrames, it can't load variable types from a database), so
        # it's a subclass, not the identical object.
        assert issubclass(Merge, SciforMerge)
        assert isinstance(Merge(Measurement, Measurement), SciforMerge)

    def test_colname_is_scifor_colname(self):
        assert ColName is SciforColName

    def test_each_of_is_scifor_each_of(self):
        assert EachOf is SciforEachOf

    def test_column_selection_is_a_scifor_column_selection_subclass(self):
        # ColumnSelection is NOT re-exported bare -- scidb's version adds
        # comparison operators / .load() / .to_csv(), so it's a subclass,
        # not the identical object.
        assert issubclass(ColumnSelection, SciforColumnSelection)
        cs = ColumnSelection(Measurement, ["value"])
        assert isinstance(cs, SciforColumnSelection)


class TestBareDataFrameInputsUnderScidb:
    """Landmine 2: scidb's loader must not crash when a Fixed/ColumnSelection
    wraps a plain DataFrame instead of a variable type -- previously
    unreachable, now reachable since the classes are unified."""

    def test_column_selection_over_plain_dataframe(self, db):
        # String subject values throughout (matching schema-key string
        # convention) sidesteps int/string coercion questions -- this test
        # is only about "does it crash", not type coercion.
        df = pd.DataFrame({"subject": ["1", "2"], "a": [10.0, 20.0], "b": [1.0, 2.0]})

        results = for_each(
            # ColumnSelection always returns a numpy array for a single
            # selected column, even for a 1-row match -- it never collapses
            # to a scalar (pre-existing scifor semantics, unrelated to this
            # refactor: see ColumnSelection's own docstring).
            lambda v: float(v[0]),
            {"v": SciforColumnSelection(df, ["a"])},
            [],
            save=False,
            subject=["1", "2"],
        )

        # outputs=[] -> scidb defaults the output column name to "result"
        # (its own convention), not scifor's "output".
        assert sorted(results["result"]) == [10.0, 20.0]

    def test_fixed_over_plain_dataframe(self, db):
        df = pd.DataFrame({"subject": ["1", "2"], "a": [10.0, 20.0]})

        results = for_each(
            lambda v: v,
            {"v": SciforFixed(df, subject="1")},
            [],
            save=False,
            subject=["1", "2"],
        )

        # Fixed pins subject="1" regardless of the current iteration's subject.
        assert len(results) == 2
        assert all(results["result"] == 10.0)

    def test_fixed_wrapping_column_selection_over_plain_dataframe(self, db):
        df = pd.DataFrame({"subject": ["1", "2"], "a": [10.0, 20.0]})

        results = for_each(
            # Same ColumnSelection-always-returns-an-array semantics as above.
            lambda v: float(v[0]),
            {"v": SciforFixed(SciforColumnSelection(df, ["a"]), subject="1")},
            [],
            save=False,
            subject=["1", "2"],
        )

        assert len(results) == 2
        assert all(results["result"] == 10.0)


class TestVersionKeyStability:
    """to_key() must stay a real, deterministic identity string -- not
    fall back to repr() (which embeds a memory address and would fork
    call_id on every process run)."""

    def test_fixed_to_key_is_deterministic_not_repr(self):
        config = ForEachConfig(lambda x: x, {"x": Fixed(Measurement, subject=1)})
        keys = config.to_version_keys()
        assert keys["__inputs"]["x"] == "Fixed(Measurement, subject=1)"
        assert "0x" not in keys["__inputs"]["x"]  # not a memory-address repr()

    def test_different_fixed_metadata_is_one_call_site(self):
        """A Fixed pin narrows WHICH record of the type feeds the call — that
        is the edge (invocation identity), not the call site. The canvas draws
        one node for both, the backward reconstruction unwraps to the type,
        and since 2026-09-20 the forward id agrees (test_identity_parity).
        The VERSION key still forks (test_fixed_to_key_is_deterministic_not_repr),
        so the two pins never share a record."""
        config_a = ForEachConfig(lambda x: x, {"x": Fixed(Measurement, subject=1)})
        config_b = ForEachConfig(lambda x: x, {"x": Fixed(Measurement, subject=2)})
        assert config_a.to_call_id() == config_b.to_call_id()
        assert config_a.to_version_keys() != config_b.to_version_keys()

    def test_same_fixed_metadata_shares_call_id_across_instances(self):
        config_a = ForEachConfig(lambda x: x, {"x": Fixed(Measurement, subject=1)})
        config_b = ForEachConfig(lambda x: x, {"x": Fixed(Measurement, subject=1)})
        assert config_a.to_call_id() == config_b.to_call_id()

    def test_column_selection_to_key_reaches_inputs_not_constants(self):
        cs = ColumnSelection(Measurement, ["value"])
        config = ForEachConfig(lambda x: x, {"x": cs})
        keys = config.to_version_keys()
        assert keys["__inputs"]["x"] == cs.to_key()
        assert keys["__constants"] == {}
