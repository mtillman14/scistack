"""Filter classes for the where= parameter in load().

Filters allow users to restrict which records are loaded based on the values
of another variable. Example:

    StepLength.load(where=Side == "L", subject=1)
    StepLength.load(where=(Side == "L") & (Speed > 1.2))
    StepLength.load(where=MyVar["Side"] == "L")
    StepLength.load(where=raw_sql('"Side" = \\'L\\''))

Filters are resolved at query time by inspecting the filter variable's data
table to determine which schema_ids match the condition. The resulting set of
schema_ids is then used to restrict which records of the target variable are
returned.
"""

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from scidb.store.schema_values import schema_str

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager


def _add_schema_column_casts(sql: str, schema_keys: list[str]) -> str:
    """Add TRY_CAST to schema columns in SQL where clauses.

    Schema keys are stored as VARCHAR in the database, so comparisons with
    numeric literals need explicit casting. This function automatically wraps
    schema column references with TRY_CAST(column AS INTEGER) when they appear
    in comparisons.

    Args:
        sql: The raw SQL WHERE condition
        schema_keys: List of schema column names (e.g., ['subject', 'trial'])

    Returns:
        SQL with casts added to schema columns

    Example:
        _add_schema_column_casts("subject <= 2", ["subject", "trial"])
        # Returns: "TRY_CAST(subject AS INTEGER) <= 2"
    """
    if not schema_keys:
        return sql

    # Build regex pattern to match schema columns followed by comparison operators
    # Match word boundaries to avoid partial matches
    pattern = r"\b(" + "|".join(re.escape(key) for key in schema_keys) + r")\b"

    def replace_with_cast(match):
        col_name = match.group(1)
        return f"TRY_CAST({col_name} AS INTEGER)"

    return re.sub(pattern, replace_with_cast, sql)


class Filter(ABC):
    """Abstract base class for all filters.

    Subclasses implement resolve() to return the set of schema_ids that
    satisfy the filter condition.
    """

    def __and__(self, other: Filter) -> CompoundFilter:
        return CompoundFilter(self, other, "AND")

    def __or__(self, other: Filter) -> CompoundFilter:
        return CompoundFilter(self, other, "OR")

    def __invert__(self) -> NotFilter:
        return NotFilter(self)

    @abstractmethod
    def to_key(self) -> str:
        """Return a canonical string representation of this filter.

        for_each records this string on the producing run (``_run.where_clause``)
        for **visual inspection only** — no matching logic reads it. Variant
        selection is semantic (by the consumed input schema_id set); see
        ``DatabaseManager._load_with_where`` (§10 where= redesign).
        """
        ...

    @abstractmethod
    def resolve(
        self,
        db: DatabaseManager,
        target_variable_class,
        target_table_name: str,
        validate_coverage: bool = True,
    ) -> set[int]:
        """Return the set of schema_ids that satisfy this filter.

        Args:
            db: The DatabaseManager instance (provides DuckDB connection and
                schema key info).
            target_variable_class: The variable class being loaded (used for
                schema-level validation and coverage checks).
            target_table_name: The table name of the target variable.
            validate_coverage: If False, skip the coverage check (used when
                loading Merge constituents, where coverage is validated once
                against the Merge result rather than per-constituent).

        Returns:
            Set of integer schema_ids that pass the filter.
        """
        ...

    def resolve_native(self, db: DatabaseManager) -> set[int]:
        """Schema_ids where this filter holds at its OWN variable granularity.

        Unlike :meth:`resolve`, this takes no target: it neither expands a
        coarse filter down to a finer target nor short-circuits a finer filter
        to "all target ids". It is used by the semantic ``where=`` variant match
        (``DatabaseManager._load_with_where``) to compare against a record's
        **consumed input** schema_ids, which live at the input variable's level
        — not the (possibly coarser) target's. For cross-level ``where=`` (output
        coarser than the filter variable) the target-resolved set is at the wrong
        level for that subset test; the native set is at the right one.

        Filters with no intrinsic variable level (e.g. raw SQL) raise
        ``NotImplementedError``; callers fall back to the target-resolved set.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support native-level resolution"
        )


def _op_to_sql(op: str) -> str:
    """Map Python comparison operator string to SQL operator."""
    mapping = {
        "==": "=",
        "=": "=",
        "!=": "!=",
        "<>": "<>",
        "<": "<",
        "<=": "<=",
        ">": ">",
        ">=": ">=",
    }
    if op not in mapping:
        raise ValueError(f"Unsupported filter operator: {op!r}")
    return mapping[op]


def _resolve_variable_schema_ids(
    db: DatabaseManager,
    filter_table_name: str,
    condition_sql: str,
    condition_params: list,
) -> set[int]:
    """Query a filter variable's data table and return matching schema_ids.

    Uses "latest version per parameter set" semantics — same as
    load(version="latest").

    Args:
        db: The DatabaseManager instance.
        filter_table_name: Table name of the filter variable.
        condition_sql: SQL fragment for the WHERE condition (uses ? placeholders).
        condition_params: Parameters for the SQL condition.

    Returns:
        Set of schema_ids where the condition is satisfied.
    """
    # Build a "latest version" query over the filter variable's data table.
    # schema_id/type come from the _record entity; recency from the save-event log.
    sql = f"""
        WITH ranked AS (
            SELECT r.schema_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY r.type, r.schema_id
                       ORDER BY rs.timestamp DESC
                   ) AS rn,
                   t.*
            FROM _record_save rs
            JOIN _record r ON r.record_id = rs.record_id
            JOIN "{filter_table_name}" t
                ON t.record_id = rs.record_id
            WHERE r.type = ?
        )
        SELECT DISTINCT schema_id
        FROM ranked
        WHERE rn = 1
          AND ({condition_sql})
    """
    params = [filter_table_name.removesuffix("_data")] + list(condition_params)
    try:
        rows = db._duck.fetchall(sql, params)
    except Exception as e:
        raise ValueError(f"Invalid where= filter condition: {e}") from e

    return {int(row[0]) for row in rows}


def _get_all_schema_ids_for_variable(
    db: DatabaseManager,
    table_name: str,
) -> set[int]:
    """Return all schema_ids that have at least one record for this variable.

    Uses latest-version-per-parameter-set semantics.
    """
    # Distinct schema locations that have at least one record of this type — read
    # straight from the content-addressed _record entity (one row per record_id).
    sql = "SELECT DISTINCT schema_id FROM _record WHERE type = ? AND schema_id IS NOT NULL"
    rows = db._duck.fetchall(sql, [table_name.removesuffix("_data")])
    return {int(row[0]) for row in rows}


def _validate_filter_schema_level(
    db: DatabaseManager,
    filter_class,
    target_class,
    filter_table_name: str,
    target_table_name: str,
) -> None:
    """Validate that the filter variable's key set fits inside the target's.

    - filter keys equal or a subset of the target's (coarser) -> OK
    - filter keys a superset (finer) -> error
    - neither (the keys sit on different branches and no record of the target
      names the filter's) -> error: there is no row to line them up on

    A variable's key set is the keys its locations name, with their ancestors
    (``schema_id_key_closure``); on a linear schema that is the prefix up to
    the deepest populated key, exactly the old "level".

    Raises:
        ValueError: If validation fails.
    """
    filter_schema_ids = _get_all_schema_ids_for_variable(db, filter_table_name)
    target_schema_ids = _get_all_schema_ids_for_variable(db, target_table_name)

    if not filter_schema_ids:
        raise ValueError(
            f"Filter variable '{filter_class.__name__}' is not registered. "
            "Save data to it first."
        )
    if not target_schema_ids:
        # Target has no data — nothing to filter
        return

    filter_keys = schema_id_key_closure(db, filter_schema_ids)
    target_keys = schema_id_key_closure(db, target_schema_ids)
    if filter_keys <= target_keys:
        return

    tree = db.schema_tree
    filter_level = ", ".join(tree.leaves(filter_keys)) or "unknown"
    target_level = ", ".join(tree.leaves(target_keys)) or "unknown"
    if filter_keys > target_keys:
        raise ValueError(
            f"Filter variable '{filter_class.__name__}' is stored at schema level "
            f"'{filter_level}' which is finer than target '{target_class.__name__}' "
            f"at level '{target_level}'. Filters must be at the same or coarser level "
            f"than the target."
        )
    raise ValueError(
        f"Filter variable '{filter_class.__name__}' (keys: {filter_level}) and target "
        f"'{target_class.__name__}' (keys: {target_level}) sit on different branches "
        f"of the schema, and no record of the target names "
        f"{sorted(filter_keys - target_keys)}, so there is no row to line the filter up "
        f"on. A variable saved at both levels is what would link them."
    )


def _expand_coarse_to_fine_schema_ids(
    db: DatabaseManager,
    coarse_schema_ids: set[int],
    target_table_name: str,
) -> set[int]:
    """Expand coarse-level schema_ids to fine-level schema_ids for the target.

    When the filter variable is at a coarser level (e.g., subject level) and
    the target is at a finer level (e.g., trial level), we need to expand each
    coarse schema_id to all fine schema_ids that share the same coarse key values.

    This is done by:
    1. Looking up the coarse schema rows (their key values)
    2. Finding all fine schema rows that have the same coarse key values
    3. Intersecting with schema_ids that the target variable has data for
    """
    if not coarse_schema_ids:
        return set()

    schema_keys = db.dataset_schema_keys

    # Get the coarse schema rows
    placeholders = ", ".join(["?"] * len(coarse_schema_ids))
    coarse_rows = db._duck.fetchdf(
        f"SELECT * FROM _schema WHERE schema_id IN ({placeholders})",
        list(coarse_schema_ids),
    )

    if len(coarse_rows) == 0:
        return set()

    # The coarse keys: those the filter's rows name, with their ancestors.
    coarse_keys = schema_id_key_closure(db, coarse_schema_ids)
    if not coarse_keys:
        return set()
    coarse_key_names = [k for k in schema_keys if k in coarse_keys]

    # Get all target schema_ids
    target_all_ids = _get_all_schema_ids_for_variable(db, target_table_name)
    if not target_all_ids:
        return set()

    # Get schema rows for all target schema_ids
    placeholders2 = ", ".join(["?"] * len(target_all_ids))
    target_rows = db._duck.fetchdf(
        f"SELECT * FROM _schema WHERE schema_id IN ({placeholders2})",
        list(target_all_ids),
    )

    if len(target_rows) == 0:
        return set()

    # Build a set of matching coarse key tuples from the filter
    coarse_tuples = set()
    for _, row in coarse_rows.iterrows():
        t = tuple(str(row[k]) if row[k] is not None else None for k in coarse_key_names)
        coarse_tuples.add(t)

    # Find all target schema_ids whose coarse keys match
    expanded = set()
    for _, row in target_rows.iterrows():
        t = tuple(
            str(row[k]) if k in row.index and row[k] is not None else None
            for k in coarse_key_names
        )
        if t in coarse_tuples:
            expanded.add(int(row["schema_id"]))

    return expanded


class VariableFilter(Filter):
    """Filter based on the data value of an entire variable record.

    Created by comparing a BaseVariable class to a value, e.g.:
        Side == "L"       (if Side is a scalar/single-value variable)

    The comparison checks the variable's primary data column against the value.

    Args:
        variable_class: The BaseVariable subclass whose data is tested.
        op: Comparison operator string ("==", "!=", "<", "<=", ">", ">=").
        value: The value to compare against.
    """

    def __init__(self, variable_class, op: str, value: Any):
        self.variable_class = variable_class
        self.op = op
        self.value = value

    def __repr__(self) -> str:
        return (
            f"VariableFilter({self.variable_class.__name__} {self.op} {self.value!r})"
        )

    def to_key(self) -> str:
        return f"{self.variable_class.__name__} {self.op} {self.value!r}"

    def resolve(
        self,
        db: DatabaseManager,
        target_variable_class,
        target_table_name: str,
        validate_coverage: bool = True,
    ) -> set[int]:
        filter_table_name = self.variable_class.table_name()
        sql_op = _op_to_sql(self.op)

        filter_keys, target_keys = _get_level_keys(
            db, filter_table_name, target_table_name
        )

        # A filter variable finer than the target is not applicable — skip it.
        if filter_keys > target_keys:
            return _get_all_schema_ids_for_variable(db, target_table_name)

        # Validate schema level compatibility
        _validate_filter_schema_level(
            db,
            self.variable_class,
            target_variable_class,
            filter_table_name,
            target_table_name,
        )

        # Ensure the filter variable is registered
        filter_schema_ids_all = _get_all_schema_ids_for_variable(db, filter_table_name)
        if not filter_schema_ids_all:
            raise ValueError(
                f"Filter variable '{self.variable_class.__name__}' is not registered. "
                "Save data to it first."
            )

        condition_sql = f'"value" {sql_op} ?'
        condition_params = [self.value]

        matching_ids = _resolve_variable_schema_ids(
            db, filter_table_name, condition_sql, condition_params
        )

        if filter_keys < target_keys:
            matching_ids = _expand_coarse_to_fine_schema_ids(
                db, matching_ids, target_table_name
            )

        if validate_coverage:
            _validate_filter_coverage(
                db,
                self.variable_class,
                target_variable_class,
                filter_table_name,
                target_table_name,
                filter_keys,
                target_keys,
            )

        return matching_ids

    def resolve_native(self, db: DatabaseManager) -> set[int]:
        sql_op = _op_to_sql(self.op)
        return _resolve_variable_schema_ids(
            db,
            self.variable_class.table_name(),
            f'"value" {sql_op} ?',
            [self.value],
        )


class ColumnFilter(Filter):
    """Filter based on a specific column in a tabular variable.

    Created by comparing a ColumnSelection to a value, e.g.:
        MyVar["Side"] == "L"

    Args:
        variable_class: The BaseVariable subclass whose table is queried.
        column: The column name to filter on.
        op: Comparison operator string.
        value: The value to compare against.
    """

    def __init__(self, variable_class, column: str, op: str, value: Any):
        self.variable_class = variable_class
        self.column = column
        self.op = op
        self.value = value

    def __repr__(self) -> str:
        return (
            f"ColumnFilter({self.variable_class.__name__}[{self.column!r}] "
            f"{self.op} {self.value!r})"
        )

    def to_key(self) -> str:
        return (
            f"{self.variable_class.__name__}['{self.column}'] {self.op} {self.value!r}"
        )

    def isin(self, values) -> InFilter:
        """Create an InFilter for set membership testing."""
        return InFilter(self.variable_class, self.column, list(values))

    def resolve(
        self,
        db: DatabaseManager,
        target_variable_class,
        target_table_name: str,
        validate_coverage: bool = True,
    ) -> set[int]:
        filter_table_name = self.variable_class.table_name()
        sql_op = _op_to_sql(self.op)

        filter_keys, target_keys = _get_level_keys(
            db, filter_table_name, target_table_name
        )

        # A filter variable finer than the target is not applicable — skip it.
        if filter_keys > target_keys:
            return _get_all_schema_ids_for_variable(db, target_table_name)

        # Validate schema level compatibility
        _validate_filter_schema_level(
            db,
            self.variable_class,
            target_variable_class,
            filter_table_name,
            target_table_name,
        )

        filter_schema_ids_all = _get_all_schema_ids_for_variable(db, filter_table_name)
        if not filter_schema_ids_all:
            raise ValueError(
                f"Filter variable '{self.variable_class.__name__}' is not registered. "
                "Save data to it first."
            )

        condition_sql = f'"{self.column}" {sql_op} ?'
        condition_params = [self.value]

        matching_ids = _resolve_variable_schema_ids(
            db, filter_table_name, condition_sql, condition_params
        )

        if filter_keys < target_keys:
            matching_ids = _expand_coarse_to_fine_schema_ids(
                db, matching_ids, target_table_name
            )

        if validate_coverage:
            _validate_filter_coverage(
                db,
                self.variable_class,
                target_variable_class,
                filter_table_name,
                target_table_name,
                filter_keys,
                target_keys,
            )

        return matching_ids

    def resolve_native(self, db: DatabaseManager) -> set[int]:
        sql_op = _op_to_sql(self.op)
        return _resolve_variable_schema_ids(
            db,
            self.variable_class.table_name(),
            f'"{self.column}" {sql_op} ?',
            [self.value],
        )


class InFilter(Filter):
    """Filter for set membership (WHERE column IN (...)).

    Args:
        variable_class: The BaseVariable subclass to query.
        column: The column name (or None for the primary value column).
        values: The set of accepted values.
    """

    def __init__(self, variable_class, column: str | None, values: list):
        self.variable_class = variable_class
        self.column = column
        self.values = values

    def __repr__(self) -> str:
        col = self.column or "value"
        return f"InFilter({self.variable_class.__name__}[{col!r}] IN {self.values!r})"

    def to_key(self) -> str:
        col = self.column or "value"
        return f"{self.variable_class.__name__}['{col}'] IN {sorted(self.values)}"

    def resolve(
        self,
        db: DatabaseManager,
        target_variable_class,
        target_table_name: str,
        validate_coverage: bool = True,
    ) -> set[int]:
        filter_table_name = self.variable_class.table_name()

        filter_keys, target_keys = _get_level_keys(
            db, filter_table_name, target_table_name
        )

        # A filter variable finer than the target is not applicable — skip it.
        if filter_keys > target_keys:
            return _get_all_schema_ids_for_variable(db, target_table_name)

        _validate_filter_schema_level(
            db,
            self.variable_class,
            target_variable_class,
            filter_table_name,
            target_table_name,
        )

        filter_schema_ids_all = _get_all_schema_ids_for_variable(db, filter_table_name)
        if not filter_schema_ids_all:
            raise ValueError(
                f"Filter variable '{self.variable_class.__name__}' is not registered. "
                "Save data to it first."
            )

        col = self.column or "value"
        if not self.values:
            return set()

        placeholders = ", ".join(["?"] * len(self.values))
        condition_sql = f'"{col}" IN ({placeholders})'
        condition_params = list(self.values)

        matching_ids = _resolve_variable_schema_ids(
            db, filter_table_name, condition_sql, condition_params
        )

        if filter_keys < target_keys:
            matching_ids = _expand_coarse_to_fine_schema_ids(
                db, matching_ids, target_table_name
            )

        if validate_coverage:
            _validate_filter_coverage(
                db,
                self.variable_class,
                target_variable_class,
                filter_table_name,
                target_table_name,
                filter_keys,
                target_keys,
            )

        return matching_ids

    def resolve_native(self, db: DatabaseManager) -> set[int]:
        col = self.column or "value"
        if not self.values:
            return set()
        placeholders = ", ".join(["?"] * len(self.values))
        return _resolve_variable_schema_ids(
            db,
            self.variable_class.table_name(),
            f'"{col}" IN ({placeholders})',
            list(self.values),
        )


class CompoundFilter(Filter):
    """AND/OR combination of two filters.

    Args:
        left: The left-hand filter.
        right: The right-hand filter.
        op: "AND" or "OR".
    """

    def __init__(self, left: Filter, right: Filter, op: str):
        self.left = left
        self.right = right
        self.op = op

    def __repr__(self) -> str:
        return f"CompoundFilter({self.left!r} {self.op} {self.right!r})"

    def to_key(self) -> str:
        return f"({self.left.to_key()}) {self.op} ({self.right.to_key()})"

    def resolve(
        self,
        db: DatabaseManager,
        target_variable_class,
        target_table_name: str,
        validate_coverage: bool = True,
    ) -> set[int]:
        left_ids = self.left.resolve(
            db, target_variable_class, target_table_name, validate_coverage
        )
        right_ids = self.right.resolve(
            db, target_variable_class, target_table_name, validate_coverage
        )

        if self.op == "AND":
            return left_ids & right_ids
        elif self.op == "OR":
            return left_ids | right_ids
        else:
            raise ValueError(f"Unknown compound operator: {self.op!r}")

    def resolve_native(self, db: DatabaseManager) -> set[int]:
        left_ids = self.left.resolve_native(db)
        right_ids = self.right.resolve_native(db)
        if self.op == "AND":
            return left_ids & right_ids
        elif self.op == "OR":
            return left_ids | right_ids
        else:
            raise ValueError(f"Unknown compound operator: {self.op!r}")


class NotFilter(Filter):
    """Complement of a filter's schema_id set.

    Args:
        inner: The filter whose result is inverted.
    """

    def __init__(self, inner: Filter):
        self.inner = inner

    def __repr__(self) -> str:
        return f"NotFilter({self.inner!r})"

    def to_key(self) -> str:
        return f"NOT ({self.inner.to_key()})"

    def resolve(
        self,
        db: DatabaseManager,
        target_variable_class,
        target_table_name: str,
        validate_coverage: bool = True,
    ) -> set[int]:
        inner_ids = self.inner.resolve(
            db, target_variable_class, target_table_name, validate_coverage
        )
        all_ids = _get_all_schema_ids_for_variable(db, target_table_name)
        return all_ids - inner_ids

    def resolve_native(self, db: DatabaseManager) -> set[int]:
        # Complement at the inner filter's OWN variable level. Requires the inner
        # to expose a single variable_class (the common leaf/NOT case); compound
        # inners have no single level → fall back via NotImplementedError.
        inner_var = getattr(self.inner, "variable_class", None)
        if inner_var is None:
            raise NotImplementedError(
                "NotFilter over a multi-variable filter has no native level"
            )
        inner_ids = self.inner.resolve_native(db)
        all_ids = _get_all_schema_ids_for_variable(db, inner_var.table_name())
        return all_ids - inner_ids


class RawFilter(Filter):
    """Raw SQL escape hatch.

    The SQL fragment is appended to the WHERE clause of the query against the
    target variable's data table (joined with _record_save / _record and _schema).

    Example:
        raw_sql('"Side" = \\'L\\'')

    Args:
        sql: A SQL fragment (no leading WHERE keyword needed).
    """

    def __init__(self, sql: str):
        self.sql = sql

    def __repr__(self) -> str:
        return f"RawFilter({self.sql!r})"

    def to_key(self) -> str:
        return f"RAW: {self.sql}"

    def resolve(
        self,
        db: DatabaseManager,
        target_variable_class,
        target_table_name: str,
        validate_coverage: bool = True,
    ) -> set[int]:
        # Run the raw SQL against the target table joined with schema
        schema_keys = db.dataset_schema_keys

        # Add casts for schema columns (stored as VARCHAR but often compared to integers)
        sql_with_casts = _add_schema_column_casts(self.sql, schema_keys)

        try:
            # Latest-version query over target table with the raw condition
            # Join with _schema to make schema keys available in the WHERE clause
            query = f"""
                WITH ranked AS (
                    SELECT r.schema_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY r.type, r.schema_id
                               ORDER BY rs.timestamp DESC
                           ) AS rn,
                           t.*,
                           s.*
                    FROM _record_save rs
                    JOIN _record r ON r.record_id = rs.record_id
                    JOIN "{target_table_name}" t
                        ON t.record_id = rs.record_id
                    LEFT JOIN _schema s
                        ON s.schema_id = r.schema_id
                    WHERE r.type = ?
                )
                SELECT DISTINCT schema_id
                FROM ranked
                WHERE rn = 1
                  AND ({sql_with_casts})
            """
            rows = db._duck.fetchall(query, [target_table_name.removesuffix("_data")])
        except Exception as e:
            raise ValueError(f"Invalid where= SQL: {e}") from e

        return {int(row[0]) for row in rows}


# ---------------------------------------------------------------------------
# Helper: raw_sql() factory
# ---------------------------------------------------------------------------


def raw_sql(sql: str) -> RawFilter:
    """Create a raw SQL filter for use in where= parameter.

    The SQL fragment is applied to the target variable's data table.

    Args:
        sql: A SQL WHERE condition fragment (no WHERE keyword).

    Example:
        StepLength.load(where=raw_sql('"Side" = \\'L\\''))
    """
    return RawFilter(sql)


# ---------------------------------------------------------------------------
# Helper: schema_key() builder and SchemaKey filters
# ---------------------------------------------------------------------------


class SchemaKey:
    """Builder for schema-key-based filters.

    Create via schema_key():
        schema_key("session").isin(["BL", "POST"])
        schema_key("subject") > 2
        schema_key("session") == "BL"

    Supports ==, !=, <, <=, >, >= and .isin().
    The resulting filter can be combined with & / | / ~ like any other Filter.
    """

    def __init__(self, key: str):
        self.key = key

    # Defining __eq__ suppresses __hash__; mark explicitly unhashable since
    # SchemaKey is a builder, not a value type.
    __hash__ = None  # type: ignore[assignment]

    def isin(self, values) -> SchemaKeyInFilter:
        """Return a filter matching records where key is in values."""
        return SchemaKeyInFilter(self.key, list(values))

    def __eq__(self, other) -> SchemaKeyCompareFilter:  # type: ignore[override]
        return SchemaKeyCompareFilter(self.key, "==", other)

    def __ne__(self, other) -> SchemaKeyCompareFilter:  # type: ignore[override]
        return SchemaKeyCompareFilter(self.key, "!=", other)

    def __lt__(self, other) -> SchemaKeyCompareFilter:
        return SchemaKeyCompareFilter(self.key, "<", other)

    def __le__(self, other) -> SchemaKeyCompareFilter:
        return SchemaKeyCompareFilter(self.key, "<=", other)

    def __gt__(self, other) -> SchemaKeyCompareFilter:
        return SchemaKeyCompareFilter(self.key, ">", other)

    def __ge__(self, other) -> SchemaKeyCompareFilter:
        return SchemaKeyCompareFilter(self.key, ">=", other)

    def __repr__(self) -> str:
        return f"SchemaKey({self.key!r})"


class SchemaKeyCompareFilter(Filter):
    """Filter on a schema key using a comparison operator.

    For ordering operators (<, <=, >, >=) the stored VARCHAR value is cast to
    DOUBLE via TRY_CAST so that numeric ordering is correct (e.g. subject 10
    sorts after subject 2).  For equality/inequality the value is compared as
    the VARCHAR string it was stored as.

    Args:
        key:   Schema key name (must be in dataset_schema_keys).
        op:    One of "==", "!=", "<", "<=", ">", ">=".
        value: The comparison value (string or numeric).
    """

    def __init__(self, key: str, op: str, value: Any):
        self.key = key
        self.op = op
        self.value = value

    def __repr__(self) -> str:
        return f"SchemaKeyCompareFilter(schema:{self.key} {self.op} {self.value!r})"

    def to_key(self) -> str:
        return f"schema:{self.key} {self.op} {self.value!r}"

    def resolve(
        self,
        db: DatabaseManager,
        target_variable_class,
        target_table_name: str,
        validate_coverage: bool = True,
    ) -> set[int]:
        schema_keys = db.dataset_schema_keys
        if self.key not in schema_keys:
            raise ValueError(
                f"Unknown schema key: {self.key!r}. Valid keys: {schema_keys}"
            )

        target_ids = _get_all_schema_ids_for_variable(db, target_table_name)
        if not target_ids:
            return set()

        # If the target variable is stored at a coarser level than the filter
        # key (e.g. subject-level variable with a session filter), the filter
        # is not applicable — return all target ids unfiltered.
        target_keys = _get_variable_key_closure(db, target_table_name)
        if target_keys and self.key not in target_keys:
            return target_ids

        sql_op = _op_to_sql(self.op)

        if self.op in ("<", "<=", ">", ">="):
            # Numeric ordering: keys are VARCHAR so cast to DOUBLE for correct ordering.
            col_expr = f'TRY_CAST("{self.key}" AS DOUBLE)'
            param: Any = self.value
        else:
            # Equality/inequality: compare as the stored VARCHAR representation.
            col_expr = f'"{self.key}"'
            param = schema_str(self.value)

        try:
            rows = db._duck.fetchall(
                f"SELECT schema_id FROM _schema WHERE {col_expr} {sql_op} ?",
                [param],
            )
        except Exception as e:
            raise ValueError(f"Invalid schema key filter: {e}") from e

        return {int(row[0]) for row in rows} & target_ids


class SchemaKeyInFilter(Filter):
    """Filter on a schema key using set membership (IN).

    Values are converted to their stored VARCHAR representation via
    schema_str so that e.g. isin([1, 2]) matches subject 1 and 2 even
    though subjects are stored as "1" and "2".

    Args:
        key:    Schema key name (must be in dataset_schema_keys).
        values: Iterable of accepted values (strings or numerics).
    """

    def __init__(self, key: str, values: list):
        self.key = key
        self.values = values

    def __repr__(self) -> str:
        return f"SchemaKeyInFilter(schema:{self.key} IN {self.values!r})"

    def to_key(self) -> str:
        return f"schema:{self.key} IN {sorted(schema_str(v) for v in self.values)}"

    def resolve(
        self,
        db: DatabaseManager,
        target_variable_class,
        target_table_name: str,
        validate_coverage: bool = True,
    ) -> set[int]:
        schema_keys = db.dataset_schema_keys
        if self.key not in schema_keys:
            raise ValueError(
                f"Unknown schema key: {self.key!r}. Valid keys: {schema_keys}"
            )

        target_ids = _get_all_schema_ids_for_variable(db, target_table_name)
        if not target_ids or not self.values:
            return set()

        # If the target variable is stored at a coarser level than the filter
        # key (e.g. subject-level variable with a session filter), the filter
        # is not applicable — return all target ids unfiltered.
        target_keys = _get_variable_key_closure(db, target_table_name)
        if target_keys and self.key not in target_keys:
            return target_ids

        str_values = [schema_str(v) for v in self.values]
        placeholders = ", ".join(["?"] * len(str_values))
        try:
            rows = db._duck.fetchall(
                f'SELECT schema_id FROM _schema WHERE "{self.key}" IN ({placeholders})',
                str_values,
            )
        except Exception as e:
            raise ValueError(f"Invalid schema key filter: {e}") from e

        return {int(row[0]) for row in rows} & target_ids


def schema_key(key: str) -> SchemaKey:
    """Create a schema-key filter builder.

    Usage:
        schema_key("session").isin(["BL", "POST"])
        schema_key("subject") > 2
        schema_key("session") == "BL"

    The returned SchemaKey supports ==, !=, <, <=, >, >= and .isin().
    Results are Filter objects combinable with & / | / ~.
    """
    return SchemaKey(key)


def _combine_and(a: Filter | None, b: Filter | None) -> Filter | None:
    """Combine two nullable filters with AND; returns None when both are None."""
    if a is not None and b is not None:
        return CompoundFilter(a, b, "AND")
    return a if a is not None else b


def split_schema_key_filters(
    filter_obj: Filter,
) -> tuple[Filter | None, Filter | None]:
    """Split a filter tree into (schema_key_part, variable_part).

    Recursively extracts SchemaKeyCompareFilter / SchemaKeyInFilter nodes
    from AND-connected compounds.  OR / NOT compounds and all other leaf
    types are left whole in the variable part.

    Returns:
        (schema_key_filter, variable_filter) — either element may be None.
    """
    if isinstance(filter_obj, (SchemaKeyCompareFilter, SchemaKeyInFilter)):
        return filter_obj, None
    if isinstance(filter_obj, CompoundFilter) and filter_obj.op == "AND":
        left_sk, left_var = split_schema_key_filters(filter_obj.left)
        right_sk, right_var = split_schema_key_filters(filter_obj.right)
        return _combine_and(left_sk, right_sk), _combine_and(left_var, right_var)
    return None, filter_obj


# ---------------------------------------------------------------------------
# Internal helpers used by multiple filter types
# ---------------------------------------------------------------------------


def schema_id_key_closure(db: DatabaseManager, schema_ids) -> frozenset:
    """The keys a set of schema locations name, closed upward.

    The keys with a non-NULL value in any of the locations, plus every ancestor
    of the deepest of them. On a linear schema this is the prefix of the key
    list up to the deepest populated key (the old "level index"); on a
    branched one it is the chain of each populated leaf, so a person-level
    variable is {brand, location, person} and never mentions category or item.
    The one owner of "what key set does this variable live at"; a variable
    that names two branches (a Purchase) keeps both. Empty when nothing is
    populated.
    """
    ids = set(schema_ids)
    if not ids:
        return frozenset()
    placeholders = ", ".join(["?"] * len(ids))
    rows = db._duck.fetchdf(
        f"SELECT * FROM _schema WHERE schema_id IN ({placeholders})",
        list(ids),
    )
    if len(rows) == 0:
        return frozenset()
    populated = {k for k in db.dataset_schema_keys if k in rows.columns and rows[k].notna().any()}
    tree = db.schema_tree
    closure: set[str] = set()
    for leaf in tree.leaves(populated):
        closure.update(tree.chain(leaf))
    return frozenset(closure)


def _get_variable_key_closure(db: DatabaseManager, table_name: str) -> frozenset:
    """:func:`schema_id_key_closure` of one variable's locations."""
    return schema_id_key_closure(db, _get_all_schema_ids_for_variable(db, table_name))


def _get_level_keys(
    db: DatabaseManager,
    filter_table_name: str,
    target_table_name: str,
) -> tuple[frozenset, frozenset]:
    """Return (filter_keys, target_keys): each variable's key set.

    Compare them as sets: ``filter_keys > target_keys`` means the filter is
    finer than the target (not applicable), ``<`` means coarser (expand to the
    target's rows), equal means the same level. Empty means no keys are
    populated (no records or no schema columns).
    """
    return (
        _get_variable_key_closure(db, filter_table_name),
        _get_variable_key_closure(db, target_table_name),
    )


def _validate_filter_coverage(
    db: DatabaseManager,
    filter_class,
    target_class,
    filter_table_name: str,
    target_table_name: str,
    filter_keys: frozenset,
    target_keys: frozenset,
    target_schema_ids_override: set[int] | None = None,
) -> None:
    """Validate that the filter covers all schema locations the target has data for.

    When filter is at same level: every target schema_id must be present in filter.
    When filter is at coarser level: every coarse key value that the target has
    must be present in the filter.

    Args:
        target_schema_ids_override: When provided, use this set as the coverage
            target instead of fetching schema_ids from target_table_name. Used by
            the Merge path to validate against Merge-result schema_ids.

    Raises:
        ValueError: If coverage is incomplete.
    """
    if target_schema_ids_override is not None:
        target_ids = target_schema_ids_override
    else:
        target_ids = _get_all_schema_ids_for_variable(db, target_table_name)
    if not target_ids:
        return  # Nothing to validate

    filter_ids = _get_all_schema_ids_for_variable(db, filter_table_name)

    target_name = (
        target_class.__name__ if target_class is not None else "the Merge result"
    )

    if filter_keys == target_keys:
        # Same level — every target schema_id must be in filter
        missing = target_ids - filter_ids
        if missing:
            raise ValueError(
                f"Filter variable '{filter_class.__name__}' is missing data at "
                f"{len(missing)} schema locations that {target_name} has "
                "data for. Ensure the filter variable covers all target locations."
            )
    elif filter_keys < target_keys:
        # Coarser level — every coarse key combination in the target must exist in filter
        schema_keys = db.dataset_schema_keys
        coarse_key_names = [k for k in schema_keys if k in filter_keys]

        # Get coarse key tuples from filter
        if filter_ids:
            placeholders = ", ".join(["?"] * len(filter_ids))
            filter_rows = db._duck.fetchdf(
                f"SELECT * FROM _schema WHERE schema_id IN ({placeholders})",
                list(filter_ids),
            )
            filter_coarse_tuples = set()
            for _, row in filter_rows.iterrows():
                t = tuple(
                    str(row[k]) if k in row.index and row[k] is not None else None
                    for k in coarse_key_names
                )
                filter_coarse_tuples.add(t)
        else:
            filter_coarse_tuples = set()

        # Get coarse key tuples from target
        placeholders2 = ", ".join(["?"] * len(target_ids))
        target_rows = db._duck.fetchdf(
            f"SELECT * FROM _schema WHERE schema_id IN ({placeholders2})",
            list(target_ids),
        )
        target_coarse_tuples = set()
        for _, row in target_rows.iterrows():
            t = tuple(
                str(row[k]) if k in row.index and row[k] is not None else None
                for k in coarse_key_names
            )
            target_coarse_tuples.add(t)

        missing_coarse = target_coarse_tuples - filter_coarse_tuples
        if missing_coarse:
            raise ValueError(
                f"Filter variable '{filter_class.__name__}' is missing data at "
                f"{len(missing_coarse)} schema locations that {target_name} "
                "has data for. Ensure the filter variable covers all target locations."
            )


# ---------------------------------------------------------------------------
# Filters as data (docs/claude/where-in-gui.md)
# ---------------------------------------------------------------------------
#
# A comparison filter written as a dict, ``{"variable": "Age", "column": None,
# "op": ">", "value": "80"}``. This is how a tool (the GUI's node config, a
# plot's filter check) holds a filter it must store as JSON. The ONE place
# that turns such a dict into a Filter and back; every run path, export and
# figure goes through here, so they cannot read the same dict differently.

#: The comparisons a filter dict may name.
FILTER_OPS: tuple[str, ...] = ("==", "!=", "<", "<=", ">", ">=")


def all_of(filters) -> "Filter | None":
    """The filters ANDed into one, in the order given. None for none.

    ``where=[a, b]`` means ``a & b``: one run that every filter must pass.
    (Separate runs, one per filter, are ``where=EachOf(a, b)``.)
    """
    combined = None
    for item in filters or ():
        if item is None:
            continue
        if isinstance(item, (list, tuple)):
            item = all_of(item)
            if item is None:
                continue
        if not isinstance(item, Filter):
            raise TypeError(
                f"where= list entries must be filters (e.g. Age > 80), got "
                f"{type(item).__name__}: {item!r}"
            )
        combined = item if combined is None else (combined & item)
    return combined


def as_filter(where: Any) -> Any:
    """``where=`` as the caller wrote it, with a list read as AND.

    Anything that is not a list or tuple is returned unchanged (a Filter, a
    raw string, an EachOf, None). An empty list is no filter.
    """
    if isinstance(where, (list, tuple)):
        return all_of(where)
    return where


def filter_value(raw: Any) -> Any:
    """The value a filter dict compares against.

    Text is read as a Python literal when it is one (``"80"`` is the number
    80, ``"True"`` the boolean, ``"'M'"`` the text M) and kept as text
    otherwise (``M``). A non-string is already a value.
    """
    if not isinstance(raw, str):
        return raw
    import ast

    try:
        return ast.literal_eval(raw.strip())
    except (ValueError, SyntaxError):
        return raw


def filter_parts(spec: dict) -> "tuple[str, str | None, str, Any]":
    """``(variable, column, op, value)`` of a filter dict, checked.

    The value is already read (:func:`filter_value`). Raises ValueError
    naming what is wrong: a filter that silently became "no filter" would run
    on rows the user meant to leave out. Needs no variable class, so a caller
    that only writes the filter as text (the MATLAB command) can use it.
    """
    spec = spec or {}
    name = str(spec.get("variable") or "").strip()
    op = str(spec.get("op") or "").strip()
    column = str(spec.get("column") or "").strip() or None
    if not name:
        raise ValueError(f"A where= filter names no variable: {spec!r}")
    if op not in FILTER_OPS:
        raise ValueError(
            f"where= filter on '{name}' uses {op!r}; it must be one of "
            f"{', '.join(FILTER_OPS)}."
        )
    raw_value = spec.get("value")
    if raw_value is None or (isinstance(raw_value, str) and raw_value.strip() == ""):
        raise ValueError(f"where= filter '{name} {op}' has no value to compare against.")
    return name, column, op, filter_value(raw_value)


def is_blank_filter_dict(spec: Any) -> bool:
    """A row the user added and has not filled in: no variable and no value.
    Such a row is not a filter yet and is skipped; a half-filled one is an
    error (:func:`filter_parts`)."""
    if not isinstance(spec, dict):
        return True
    value = spec.get("value")
    return not str(spec.get("variable") or "").strip() and (
        value is None or str(value).strip() == ""
    )


def filter_from_dict(spec: dict, resolve_class) -> "Filter":
    """One comparison filter from its dict form.

    ``resolve_class(name)`` returns the variable class called *name*, or None.
    """
    name, column, op, value = filter_parts(spec)
    variable_class = resolve_class(name)
    if variable_class is None:
        raise ValueError(
            f"where= filter names the variable '{name}', which is not a known variable."
        )
    if column:
        return ColumnFilter(variable_class, column, op, value)
    return VariableFilter(variable_class, op, value)


def filters_from_dicts(specs, resolve_class) -> "list[Filter]":
    """Each dict's filter, in order (:func:`filter_from_dict`); blank rows
    (:func:`is_blank_filter_dict`) are skipped."""
    return [
        filter_from_dict(spec, resolve_class)
        for spec in (specs or [])
        if not is_blank_filter_dict(spec)
    ]


def filter_to_dict(filter_obj: "Filter") -> dict:
    """The dict form of a comparison filter; the inverse of
    :func:`filter_from_dict`. Only a plain comparison has one."""
    if isinstance(filter_obj, ColumnFilter):
        column = filter_obj.column
    elif isinstance(filter_obj, VariableFilter):
        column = None
    else:
        raise ValueError(
            f"{type(filter_obj).__name__} has no dict form; only a comparison "
            "of a variable (or one of its columns) to a value does."
        )
    return {
        "variable": filter_obj.variable_class.__name__,
        "column": column,
        "op": filter_obj.op,
        "value": repr(filter_obj.value),
    }


def and_terms(where: Any) -> "list[Filter]":
    """The filters a ``where=`` ANDs together, in order: a list's entries,
    and the two sides of every ``a & b``, flattened. One filter is a list
    of one; None is empty."""
    where = as_filter(where)
    if where is None:
        return []
    if isinstance(where, CompoundFilter) and where.op == "AND":
        return and_terms(where.left) + and_terms(where.right)
    return [where]


def _filter_source(filter_obj: Any) -> str:
    """One filter as a Python expression."""
    if isinstance(filter_obj, (VariableFilter, ColumnFilter)):
        return filter_obj.to_key()  # a comparison's key IS its Python spelling
    if isinstance(filter_obj, CompoundFilter):
        symbol = "&" if filter_obj.op == "AND" else "|"
        return f"({_filter_source(filter_obj.left)}) {symbol} ({_filter_source(filter_obj.right)})"
    if isinstance(filter_obj, NotFilter):
        return f"~({_filter_source(filter_obj.inner)})"
    raise ValueError(
        f"A {type(filter_obj).__name__} filter cannot be written as Python source."
    )


def where_source(where: Any) -> str:
    """The Python text of a ``where=`` argument.

    One filter is written bare (``Age > 80``); filters ANDed together are
    written as the list they mean (``[Age > 80, Sex == 'M']``). Empty text
    for no filter. The one writer of filter source: the exported script and
    the text a node shows both come from here.
    """
    terms = [_filter_source(term) for term in and_terms(where)]
    if not terms:
        return ""
    if len(terms) == 1:
        return terms[0]
    return "[" + ", ".join(terms) + "]"


# ---------------------------------------------------------------------------
# The where= check: which locations pass which filter, and why not
# ---------------------------------------------------------------------------

#: The three verdicts of :func:`filter_report`.
VERDICT_PASS = "pass"
VERDICT_FAIL = "fail"
VERDICT_NO_VALUE = "no value"

#: Prefix of the verdict columns of :func:`filter_report` (then the filter's index).
VERDICT_PREFIX = "__where_f"

_OPPOSITE_OP = {"==": "!=", "!=": "==", "<": ">=", "<=": ">", ">": "<=", ">=": "<"}


def filter_report(db, target_class, filter_dicts, resolve_class):
    """Which locations of *target_class* pass each filter. One owner of the
    pass / fail / no-value verdict; the where= check figure only draws it.

    Returns ``(frame, keys)``: ``frame`` has one row per location the target
    has a record at, the schema keys of that location as text, and one column
    ``__where_f0``, ``__where_f1``... per filter (in the order given, blank rows skipped)
    holding ``"pass"``, ``"fail"`` or ``"no value"``; ``keys`` is each
    filter's ``to_key()`` in the same order.

    "No value" is a location where the filter's variable has nothing the
    comparison can answer (no record, or NULL/NaN). A run would raise a
    coverage error there; the check shows the gap instead of refusing to
    draw. A filter whose variable is finer than the target is not
    applicable (it passes everywhere), exactly as ``resolve`` treats it.
    """
    import pandas as pd

    table = target_class.table_name()
    ids = sorted(_get_all_schema_ids_for_variable(db, table))
    rows = [d for d in (filter_dicts or []) if not is_blank_filter_dict(d)]
    keys: list[str] = []
    verdicts: list[dict[int, str]] = []
    for spec in rows:
        passing = filter_from_dict(spec, resolve_class)
        _, _, op, _ = filter_parts(spec)
        failing = filter_from_dict({**spec, "op": _OPPOSITE_OP[op]}, resolve_class)
        pass_ids = passing.resolve(db, target_class, table, validate_coverage=False)
        fail_ids = failing.resolve(db, target_class, table, validate_coverage=False)
        verdicts.append(
            {
                i: VERDICT_PASS
                if i in pass_ids
                else VERDICT_FAIL
                if i in fail_ids
                else VERDICT_NO_VALUE
                for i in ids
            }
        )
        keys.append(passing.to_key())

    if ids:
        placeholders = ", ".join(["?"] * len(ids))
        schema = db._duck.fetchdf(
            f"SELECT * FROM _schema WHERE schema_id IN ({placeholders})", ids
        )
    else:
        schema = pd.DataFrame({"schema_id": []})
    key_columns = [k for k in db.dataset_schema_keys if k in schema.columns]
    frame = pd.DataFrame(
        {
            **{
                k: schema[k].map(lambda v: None if v is None or v != v else schema_str(v))
                for k in key_columns
            },
        }
    )
    for n, verdict in enumerate(verdicts):
        frame[f"{VERDICT_PREFIX}{n}"] = [verdict[int(i)] for i in schema["schema_id"]]

    counts = {
        k: {
            v: int((frame[f"{VERDICT_PREFIX}{n}"] == v).sum())
            for v in (VERDICT_PASS, VERDICT_FAIL, VERDICT_NO_VALUE)
        }
        for n, k in enumerate(keys)
    }
    logger.info(
        "[where_check] target=%s locations=%d filters=%s", table, len(frame), counts
    )
    return frame, keys


class _NamedVariable:
    """Stands in for a variable class where only its name is needed."""

    def __init__(self, name: str):
        self.__name__ = name


def filter_label(spec: dict) -> str:
    """The text a person reads for a filter dict (``Age > 80``,
    ``Gait['Side'] == 'L'``): the filter's own ``to_key``, so a chip, a
    legend title and the run record spell it one way. Needs no variable
    class. Raises ValueError for a half-filled row (:func:`filter_parts`)."""
    return filter_from_dict(spec, _NamedVariable).to_key()
