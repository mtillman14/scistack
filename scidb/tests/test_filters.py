"""Unit tests for the Filter class hierarchy (no database required)."""

# Import from the package
import sys
from pathlib import Path

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root / "src"))

from scidb.model.filters import (
    ColumnFilter,
    CompoundFilter,
    InFilter,
    NotFilter,
    RawFilter,
    SchemaKey,
    SchemaKeyCompareFilter,
    SchemaKeyInFilter,
    VariableFilter,
    raw_sql,
    schema_key,
)

from scidb import BaseVariable

# --- Test Variable Classes ---
# These are used for filter expression tests only (no DB interaction needed)


class Side(BaseVariable):
    """Test variable: gait side (L/R)."""

    schema_version = 1


class Speed(BaseVariable):
    """Test variable: walking speed (float)."""

    schema_version = 1


class MyVar(BaseVariable):
    """Test variable with tabular data."""

    schema_version = 1


# ===========================================================================
# Filter object construction tests
# ===========================================================================


class TestVariableFilterConstruction:
    """Tests that metaclass comparison operators produce VariableFilter objects."""

    def test_eq_produces_variable_filter(self):
        f = Side == "L"
        assert isinstance(f, VariableFilter)
        assert f.variable_class is Side
        assert f.op == "=="
        assert f.value == "L"

    def test_ne_produces_variable_filter(self):
        f = Side != "L"
        assert isinstance(f, VariableFilter)
        assert f.op == "!="
        assert f.value == "L"

    def test_lt_produces_variable_filter(self):
        f = Speed < 1.5
        assert isinstance(f, VariableFilter)
        assert f.op == "<"
        assert f.value == 1.5

    def test_le_produces_variable_filter(self):
        f = Speed <= 1.5
        assert isinstance(f, VariableFilter)
        assert f.op == "<="

    def test_gt_produces_variable_filter(self):
        f = Speed > 1.0
        assert isinstance(f, VariableFilter)
        assert f.op == ">"
        assert f.value == 1.0

    def test_ge_produces_variable_filter(self):
        f = Speed >= 1.0
        assert isinstance(f, VariableFilter)
        assert f.op == ">="

    def test_repr(self):
        f = Side == "L"
        assert "Side" in repr(f)
        assert "==" in repr(f)
        assert "L" in repr(f)


class TestMetaclassPreservation:
    """Tests that metaclass doesn't break normal class behavior."""

    def test_class_to_class_equality_preserved(self):
        """Side == Side should return True (class identity)."""
        assert Side == Side
        assert not (Side == Speed)

    def test_class_is_hashable(self):
        """Variable classes must be hashable (can be dict keys)."""
        d = {Side: "left", Speed: "fast"}
        assert d[Side] == "left"
        assert d[Speed] == "fast"

    def test_class_in_set(self):
        """Variable classes can be used in sets."""
        s = {Side, Speed, Side}
        assert len(s) == 2

    def test_class_instance_creation_unaffected(self):
        """Creating instances of variable classes still works."""
        import numpy as np

        v = Side(np.array([1.0, 2.0]))
        assert v.data is not None


# ===========================================================================
# Compound filter tests
# ===========================================================================


class TestCompoundFilter:
    """Tests for AND/OR compound filters."""

    def test_and_produces_compound_filter(self):
        f = (Side == "L") & (Speed > 1.0)
        assert isinstance(f, CompoundFilter)
        assert f.op == "AND"
        assert isinstance(f.left, VariableFilter)
        assert isinstance(f.right, VariableFilter)

    def test_or_produces_compound_filter(self):
        f = (Side == "L") | (Side == "R")
        assert isinstance(f, CompoundFilter)
        assert f.op == "OR"

    def test_not_produces_not_filter(self):
        f = ~(Side == "L")
        assert isinstance(f, NotFilter)
        assert isinstance(f.inner, VariableFilter)

    def test_nested_compound(self):
        f = (Side == "L") & (Speed > 1.0) & (Speed < 2.0)
        assert isinstance(f, CompoundFilter)
        assert f.op == "AND"

    def test_repr_compound(self):
        f = (Side == "L") & (Speed > 1.0)
        r = repr(f)
        assert "CompoundFilter" in r
        assert "AND" in r

    def test_repr_not(self):
        f = ~(Side == "L")
        assert "NotFilter" in repr(f)


# ===========================================================================
# ColumnSelection filter tests
# ===========================================================================


class TestColumnSelectionFilter:
    """Tests that MyVar["col"] == value produces ColumnFilter."""

    def test_column_eq_produces_column_filter(self):
        f = MyVar["Side"] == "L"
        assert isinstance(f, ColumnFilter)
        assert f.variable_class is MyVar
        assert f.column == "Side"
        assert f.op == "=="
        assert f.value == "L"

    def test_column_ne_produces_column_filter(self):
        f = MyVar["Side"] != "R"
        assert isinstance(f, ColumnFilter)
        assert f.op == "!="

    def test_column_lt_produces_column_filter(self):
        f = MyVar["Speed"] < 1.5
        assert isinstance(f, ColumnFilter)
        assert f.op == "<"

    def test_column_le_produces_column_filter(self):
        f = MyVar["Speed"] <= 1.5
        assert isinstance(f, ColumnFilter)
        assert f.op == "<="

    def test_column_gt_produces_column_filter(self):
        f = MyVar["Speed"] > 1.0
        assert isinstance(f, ColumnFilter)
        assert f.op == ">"

    def test_column_ge_produces_column_filter(self):
        f = MyVar["Speed"] >= 1.0
        assert isinstance(f, ColumnFilter)
        assert f.op == ">="

    def test_column_isin_produces_in_filter(self):
        f = MyVar["Side"].isin(["L", "R"])
        assert isinstance(f, InFilter)
        assert f.variable_class is MyVar
        assert f.column == "Side"
        assert set(f.values) == {"L", "R"}

    def test_column_filter_in_compound(self):
        f = (MyVar["Side"] == "L") & (MyVar["Speed"] > 1.0)
        assert isinstance(f, CompoundFilter)
        assert f.op == "AND"

    def test_column_repr(self):
        f = MyVar["Side"] == "L"
        r = repr(f)
        assert "MyVar" in r
        assert "Side" in r
        assert "L" in r


# ===========================================================================
# InFilter tests
# ===========================================================================


class TestInFilter:
    """Tests for InFilter construction."""

    def test_in_filter_repr(self):
        f = InFilter(Side, "value", ["L", "R"])
        r = repr(f)
        assert "InFilter" in r
        assert "Side" in r

    def test_empty_in_filter(self):
        f = InFilter(Side, "value", [])
        assert f.values == []


# ===========================================================================
# RawFilter / raw_sql tests
# ===========================================================================


class TestRawFilter:
    """Tests for raw SQL escape hatch."""

    def test_raw_sql_returns_raw_filter(self):
        f = raw_sql("\"Side\" = 'L'")
        assert isinstance(f, RawFilter)
        assert f.sql == "\"Side\" = 'L'"

    def test_raw_filter_repr(self):
        f = raw_sql("x > 1")
        assert "RawFilter" in repr(f)

    def test_raw_filter_in_compound_with_variable_filter(self):
        f = (Side == "L") & raw_sql("speed > 1.0")
        assert isinstance(f, CompoundFilter)
        assert isinstance(f.left, VariableFilter)
        assert isinstance(f.right, RawFilter)


# ===========================================================================
# SchemaKey filter construction tests (no DB required)
# ===========================================================================


class TestSchemaKeyConstruction:
    """Tests that SchemaKey builder operators produce the correct Filter objects."""

    def test_isin_returns_in_filter(self):
        f = schema_key("session").isin(["BL", "POST"])
        assert isinstance(f, SchemaKeyInFilter)
        assert f.key == "session"
        assert f.values == ["BL", "POST"]

    def test_isin_numeric(self):
        f = schema_key("subject").isin([1, 2, 3])
        assert isinstance(f, SchemaKeyInFilter)
        assert f.values == [1, 2, 3]

    def test_eq_returns_compare_filter(self):
        f = schema_key("session") == "BL"
        assert isinstance(f, SchemaKeyCompareFilter)
        assert f.key == "session"
        assert f.op == "=="
        assert f.value == "BL"

    def test_ne_returns_compare_filter(self):
        f = schema_key("session") != "BL"
        assert isinstance(f, SchemaKeyCompareFilter)
        assert f.op == "!="

    def test_lt_returns_compare_filter(self):
        f = schema_key("subject") < 3
        assert isinstance(f, SchemaKeyCompareFilter)
        assert f.op == "<"
        assert f.value == 3

    def test_le_returns_compare_filter(self):
        f = schema_key("subject") <= 3
        assert isinstance(f, SchemaKeyCompareFilter)
        assert f.op == "<="

    def test_gt_returns_compare_filter(self):
        f = schema_key("subject") > 1
        assert isinstance(f, SchemaKeyCompareFilter)
        assert f.op == ">"

    def test_ge_returns_compare_filter(self):
        f = schema_key("subject") >= 1
        assert isinstance(f, SchemaKeyCompareFilter)
        assert f.op == ">="

    def test_compare_filter_combinable_with_and(self):
        f = (schema_key("subject") > 1) & (schema_key("subject") < 4)
        assert isinstance(f, CompoundFilter)
        assert f.op == "AND"

    def test_compare_filter_combinable_with_or(self):
        f = (schema_key("session") == "BL") | (schema_key("session") == "POST")
        assert isinstance(f, CompoundFilter)
        assert f.op == "OR"

    def test_in_filter_combinable_with_not(self):
        f = ~schema_key("session").isin(["BL"])
        assert isinstance(f, NotFilter)

    def test_schema_key_factory_returns_schema_key(self):
        sk = schema_key("session")
        assert isinstance(sk, SchemaKey)
        assert sk.key == "session"

    def test_repr_schema_key(self):
        assert "session" in repr(schema_key("session"))

    def test_repr_compare_filter(self):
        f = schema_key("subject") > 2
        assert "SchemaKeyCompareFilter" in repr(f)
        assert "subject" in repr(f)

    def test_repr_in_filter(self):
        f = schema_key("session").isin(["BL", "POST"])
        assert "SchemaKeyInFilter" in repr(f)
        assert "session" in repr(f)

    def test_to_key_compare_filter(self):
        f = schema_key("session") == "BL"
        assert f.to_key() == "schema:session == 'BL'"

    def test_to_key_in_filter_sorted(self):
        f = schema_key("session").isin(["POST", "BL"])
        # to_key uses sorted values for stability
        assert "BL" in f.to_key()
        assert "POST" in f.to_key()
        assert f.to_key().index("BL") < f.to_key().index("POST")
