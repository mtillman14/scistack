"""Tests for Fixed class."""

from scidb import Fixed


class TestFixed:
    """Tests for the Fixed metadata wrapper."""

    def test_init_with_type_only(self):
        """Should initialize with just a type."""

        class MockType:
            pass

        fixed = Fixed(MockType)
        assert fixed.data is MockType
        assert fixed.fixed_metadata == {}

    def test_init_with_metadata(self):
        """Should initialize with type and metadata."""

        class MockType:
            pass

        fixed = Fixed(MockType, subject=1, session="baseline")
        assert fixed.data is MockType
        assert fixed.fixed_metadata == {"subject": 1, "session": "baseline"}

    def test_stores_various_metadata_types(self):
        """Should store various types of metadata values."""

        class MockType:
            pass

        fixed = Fixed(
            MockType,
            int_val=42,
            float_val=3.14,
            str_val="hello",
            bool_val=True,
            none_val=None,
            list_val=[1, 2, 3],
        )

        assert fixed.fixed_metadata["int_val"] == 42
        assert fixed.fixed_metadata["float_val"] == 3.14
        assert fixed.fixed_metadata["str_val"] == "hello"
        assert fixed.fixed_metadata["bool_val"] is True
        assert fixed.fixed_metadata["none_val"] is None
        assert fixed.fixed_metadata["list_val"] == [1, 2, 3]

    def test_fixed_metadata_is_accessible(self):
        """Fixed metadata should be directly accessible."""

        class MockType:
            pass

        fixed = Fixed(MockType, key="value")
        assert fixed.fixed_metadata["key"] == "value"
