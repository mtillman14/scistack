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

import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from .schema_values import schema_str

if TYPE_CHECKING:
    from .database import DatabaseManager


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
        rows = db._duck._fetchall(sql, params)
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
    rows = db._duck._fetchall(sql, [table_name.removesuffix("_data")])
    return {int(row[0]) for row in rows}


def _validate_filter_schema_level(
    db: DatabaseManager,
    filter_class,
    target_class,
    filter_table_name: str,
    target_table_name: str,
) -> None:
    """Validate that the filter variable's schema level is same or coarser than target.

    The plan specifies:
    - filter at same or coarser level → OK (coarser = fewer schema keys)
    - filter at finer level → error
    - filter schema keys must be subset of target schema keys

    We infer schema level by looking at which schema columns are non-NULL in
    the filter variable's _schema rows.

    Raises:
        ValueError: If validation fails.
    """
    schema_keys = db.dataset_schema_keys

    # Get the schema_ids used by filter and target, then look at which
    # schema columns are populated. The "level" is the deepest key that
    # has a non-null value.
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

    # Get schema rows for filter
    if filter_schema_ids:
        placeholders = ", ".join(["?"] * len(filter_schema_ids))
        filter_schema_rows = db._duck._fetchdf(
            f"SELECT * FROM _schema WHERE schema_id IN ({placeholders})",
            list(filter_schema_ids),
        )

        if len(filter_schema_rows) > 0:
            # Find the deepest non-null key in the filter rows → filter level
            filter_level_idx = -1
            for i, key in enumerate(schema_keys):
                if key in filter_schema_rows.columns:
                    non_null = filter_schema_rows[key].notna()
                    if non_null.any():
                        filter_level_idx = i

            # Get target schema rows
            placeholders2 = ", ".join(["?"] * len(target_schema_ids))
            target_schema_rows = db._duck._fetchdf(
                f"SELECT * FROM _schema WHERE schema_id IN ({placeholders2})",
                list(target_schema_ids),
            )

            target_level_idx = -1
            for i, key in enumerate(schema_keys):
                if key in target_schema_rows.columns:
                    non_null = target_schema_rows[key].notna()
                    if non_null.any():
                        target_level_idx = i

            # Filter must be at same or coarser (earlier in hierarchy) level
            if filter_level_idx > target_level_idx:
                filter_level = (
                    schema_keys[filter_level_idx]
                    if filter_level_idx >= 0
                    else "unknown"
                )
                target_level = (
                    schema_keys[target_level_idx]
                    if target_level_idx >= 0
                    else "unknown"
                )
                raise ValueError(
                    f"Filter variable '{filter_class.__name__}' is stored at schema level "
                    f"'{filter_level}' which is finer than target '{target_class.__name__}' "
                    f"at level '{target_level}'. Filters must be at the same or coarser level "
                    f"than the target."
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
    coarse_rows = db._duck._fetchdf(
        f"SELECT * FROM _schema WHERE schema_id IN ({placeholders})",
        list(coarse_schema_ids),
    )

    if len(coarse_rows) == 0:
        return set()

    # Determine the coarse level (deepest non-null key in filter rows)
    coarse_level_idx = -1
    for i, key in enumerate(schema_keys):
        if key in coarse_rows.columns:
            non_null = coarse_rows[key].notna()
            if non_null.any():
                coarse_level_idx = i

    if coarse_level_idx < 0:
        return set()

    # The coarse keys are those up to and including coarse_level_idx
    coarse_key_names = schema_keys[: coarse_level_idx + 1]

    # Get all target schema_ids
    target_all_ids = _get_all_schema_ids_for_variable(db, target_table_name)
    if not target_all_ids:
        return set()

    # Get schema rows for all target schema_ids
    placeholders2 = ", ".join(["?"] * len(target_all_ids))
    target_rows = db._duck._fetchdf(
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

        filter_level_idx, target_level_idx = _get_level_indices(
            db, filter_table_name, target_table_name
        )

        # A filter variable finer than the target is not applicable — skip it.
        if filter_level_idx > target_level_idx:
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

        if filter_level_idx < target_level_idx:
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
                filter_level_idx,
                target_level_idx,
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

        filter_level_idx, target_level_idx = _get_level_indices(
            db, filter_table_name, target_table_name
        )

        # A filter variable finer than the target is not applicable — skip it.
        if filter_level_idx > target_level_idx:
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

        if filter_level_idx < target_level_idx:
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
                filter_level_idx,
                target_level_idx,
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

        filter_level_idx, target_level_idx = _get_level_indices(
            db, filter_table_name, target_table_name
        )

        # A filter variable finer than the target is not applicable — skip it.
        if filter_level_idx > target_level_idx:
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

        if filter_level_idx < target_level_idx:
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
                filter_level_idx,
                target_level_idx,
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
            rows = db._duck._fetchall(query, [target_table_name.removesuffix("_data")])
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
        filter_key_idx = schema_keys.index(self.key)
        target_level_idx = _get_variable_level_idx(db, target_table_name)
        if 0 <= target_level_idx < filter_key_idx:
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
            rows = db._duck._fetchall(
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
        filter_key_idx = schema_keys.index(self.key)
        target_level_idx = _get_variable_level_idx(db, target_table_name)
        if 0 <= target_level_idx < filter_key_idx:
            return target_ids

        str_values = [schema_str(v) for v in self.values]
        placeholders = ", ".join(["?"] * len(str_values))
        try:
            rows = db._duck._fetchall(
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


def _get_variable_level_idx(db: DatabaseManager, table_name: str) -> int:
    """Return the index of the deepest non-null schema key for this variable.

    Returns -1 when the variable has no records or no schema columns are populated.
    """
    schema_keys = db.dataset_schema_keys
    ids = _get_all_schema_ids_for_variable(db, table_name)
    if not ids:
        return -1
    placeholders = ", ".join(["?"] * len(ids))
    rows = db._duck._fetchdf(
        f"SELECT * FROM _schema WHERE schema_id IN ({placeholders})",
        list(ids),
    )
    if len(rows) == 0:
        return -1
    level_idx = -1
    for i, key in enumerate(schema_keys):
        if key in rows.columns and rows[key].notna().any():
            level_idx = i
    return level_idx


def _get_level_indices(
    db: DatabaseManager,
    filter_table_name: str,
    target_table_name: str,
) -> tuple[int, int]:
    """Return (filter_level_idx, target_level_idx) in dataset_schema_keys.

    -1 means no keys are populated (no records or no schema columns).
    """
    return (
        _get_variable_level_idx(db, filter_table_name),
        _get_variable_level_idx(db, target_table_name),
    )


def _validate_filter_coverage(
    db: DatabaseManager,
    filter_class,
    target_class,
    filter_table_name: str,
    target_table_name: str,
    filter_level_idx: int,
    target_level_idx: int,
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

    if filter_level_idx == target_level_idx:
        # Same level — every target schema_id must be in filter
        missing = target_ids - filter_ids
        if missing:
            raise ValueError(
                f"Filter variable '{filter_class.__name__}' is missing data at "
                f"{len(missing)} schema locations that {target_name} has "
                "data for. Ensure the filter variable covers all target locations."
            )
    elif filter_level_idx < target_level_idx:
        # Coarser level — every coarse key combination in the target must exist in filter
        schema_keys = db.dataset_schema_keys
        coarse_key_names = schema_keys[: filter_level_idx + 1]

        # Get coarse key tuples from filter
        if filter_ids:
            placeholders = ", ".join(["?"] * len(filter_ids))
            filter_rows = db._duck._fetchdf(
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
        target_rows = db._duck._fetchdf(
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
