"""Lightweight filter classes for column-based DataFrame filtering in scifor.

These operate directly on pandas DataFrames (no database required).
For database-backed filtering, see scidb.filters.
"""

from typing import Any


class ColFilter:
    """A filter comparing a DataFrame column to a value.

    Created via ``Col("column_name") == value`` etc.
    """

    def __init__(self, column: str, op: str, value: Any):
        self.column = column
        self.op = op
        self.value = value

    def apply(self, df: "pd.DataFrame") -> "pd.Series":
        """Return a boolean mask for rows matching this filter."""
        col = df[self.column]
        if self.op == "==":
            return col == self.value
        elif self.op == "!=":
            return col != self.value
        elif self.op == "<":
            return col < self.value
        elif self.op == "<=":
            return col <= self.value
        elif self.op == ">":
            return col > self.value
        elif self.op == ">=":
            return col >= self.value
        else:
            raise ValueError(f"Unknown operator: {self.op!r}")

    def to_key(self) -> str:
        return f"Col({self.column!r}) {self.op} {self.value!r}"

    def __and__(self, other: "ColFilter | CompoundFilter") -> "CompoundFilter":
        return CompoundFilter("&", self, other)

    def __or__(self, other: "ColFilter | CompoundFilter") -> "CompoundFilter":
        return CompoundFilter("|", self, other)

    def __invert__(self) -> "NotFilter":
        return NotFilter(self)

    def __repr__(self) -> str:
        return f"ColFilter({self.column!r} {self.op} {self.value!r})"


class CompoundFilter:
    """Combines two filters with & or |."""

    def __init__(self, op: str, left: Any, right: Any):
        self.op = op
        self.left = left
        self.right = right

    def apply(self, df: "pd.DataFrame") -> "pd.Series":
        left_mask = self.left.apply(df)
        right_mask = self.right.apply(df)
        if self.op == "&":
            return left_mask & right_mask
        elif self.op == "|":
            return left_mask | right_mask
        else:
            raise ValueError(f"Unknown operator: {self.op!r}")

    def to_key(self) -> str:
        return f"({self.left.to_key()} {self.op} {self.right.to_key()})"

    def __and__(self, other) -> "CompoundFilter":
        return CompoundFilter("&", self, other)

    def __or__(self, other) -> "CompoundFilter":
        return CompoundFilter("|", self, other)

    def __invert__(self) -> "NotFilter":
        return NotFilter(self)

    def __repr__(self) -> str:
        return f"CompoundFilter({self.op!r}, {self.left!r}, {self.right!r})"


class NotFilter:
    """Negates a filter."""

    def __init__(self, inner: Any):
        self.inner = inner

    def apply(self, df: "pd.DataFrame") -> "pd.Series":
        return ~self.inner.apply(df)

    def to_key(self) -> str:
        return f"~({self.inner.to_key()})"

    def __and__(self, other) -> "CompoundFilter":
        return CompoundFilter("&", self, other)

    def __or__(self, other) -> "CompoundFilter":
        return CompoundFilter("|", self, other)

    def __invert__(self):
        return self.inner

    def __repr__(self) -> str:
        return f"NotFilter({self.inner!r})"


class Col:
    """Entry point for building DataFrame column filters.

    Usage:
        Col("side") == "R"                          # ColFilter
        (Col("side") == "R") & (Col("speed") > 1.2) # CompoundFilter
        ~(Col("side") == "R")                       # NotFilter
    """

    def __init__(self, column: str):
        self.column = column

    def __eq__(self, other) -> ColFilter:
        return ColFilter(self.column, "==", other)

    def __ne__(self, other) -> ColFilter:
        return ColFilter(self.column, "!=", other)

    def __lt__(self, other) -> ColFilter:
        return ColFilter(self.column, "<", other)

    def __le__(self, other) -> ColFilter:
        return ColFilter(self.column, "<=", other)

    def __gt__(self, other) -> ColFilter:
        return ColFilter(self.column, ">", other)

    def __ge__(self, other) -> ColFilter:
        return ColFilter(self.column, ">=", other)

    def __repr__(self) -> str:
        return f"Col({self.column!r})"
