"""Filter classes for the where= parameter in load() and load_all().

Filters allow users to restrict which records are loaded based on the values
of another variable. Example:

    StepLength.load(where=Side == "L", subject=1)
    StepLength.load_all(where=(Side == "L") & (Speed > 1.2))
    StepLength.load_all(where=MyVar["Side"] == "L")
    StepLength.load_all(where=raw_sql('"Side" = \\'L\\''))

Filters are resolved at query time by inspecting the filter variable's data
table to determine which schema_ids match the condition. The resulting set of
schema_ids is then used to restrict which records of the target variable are
returned.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

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
    pattern = r'\b(' + '|'.join(re.escape(key) for key in schema_keys) + r')\b'

    def replace_with_cast(match):
        col_name = match.group(1)
        return f'TRY_CAST({col_name} AS INTEGER)'

    return re.sub(pattern, replace_with_cast, sql)


class Filter(ABC):
    """Abstract base class for all filters.

    Subclasses implement resolve() to return the set of schema_ids that
    satisfy the filter condition.
    """

    def __and__(self, other: "Filter") -> "CompoundFilter":
        return CompoundFilter(self, other, "AND")

    def __or__(self, other: "Filter") -> "CompoundFilter":
        return CompoundFilter(self, other, "OR")

    def __invert__(self) -> "NotFilter":
        return NotFilter(self)

    @abstractmethod
    def to_key(self) -> str:
        """Return a canonical string representation for use as a version key.

        Used to serialize filter configs into _record_metadata.version_keys
        so that different where= configurations produce distinct version groups.
        """
        ...

    @abstractmethod
    def resolve(
        self,
        db: "DatabaseManager",
        target_variable_class,
        target_table_name: str,
    ) -> set[int]:
        """Return the set of schema_ids that satisfy this filter.

        Args:
            db: The DatabaseManager instance (provides DuckDB connection and
                schema key info).
            target_variable_class: The variable class being loaded (used for
                schema-level validation and coverage checks).
            target_table_name: The table name of the target variable.

        Returns:
            Set of integer schema_ids that pass the filter.
        """
        ...


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
    db: "DatabaseManager",
    filter_table_name: str,
    condition_sql: str,
    condition_params: list,
) -> set[int]:
    """Query a filter variable's data table and return matching schema_ids.

    Uses "latest version per parameter set" semantics — same as
    load_all(version_id="latest").

    Args:
        db: The DatabaseManager instance.
        filter_table_name: Table name of the filter variable.
        condition_sql: SQL fragment for the WHERE condition (uses ? placeholders).
        condition_params: Parameters for the SQL condition.

    Returns:
        Set of schema_ids where the condition is satisfied.
    """
    # Build a "latest version" query over the filter variable's data table,
    # joined with _record_metadata to get schema_id.
    sql = f"""
        WITH ranked AS (
            SELECT rm.schema_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY rm.variable_name, rm.schema_id, rm.version_keys
                       ORDER BY rm.timestamp DESC
                   ) AS rn,
                   t.*
            FROM _record_metadata rm
            JOIN "{filter_table_name}" t
                ON t.record_id = rm.record_id
            WHERE rm.variable_name = ?
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
    db: "DatabaseManager",
    table_name: str,
) -> set[int]:
    """Return all schema_ids that have at least one record for this variable.

    Uses latest-version-per-parameter-set semantics.
    """
    sql = """
        WITH ranked AS (
            SELECT rm.schema_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY rm.variable_name, rm.schema_id, rm.version_keys
                       ORDER BY rm.timestamp DESC
                   ) AS rn
            FROM _record_metadata rm
            WHERE rm.variable_name = ?
        )
        SELECT DISTINCT schema_id FROM ranked WHERE rn = 1
    """
    rows = db._duck._fetchall(sql, [table_name.removesuffix("_data")])
    return {int(row[0]) for row in rows}


def _validate_filter_schema_level(
    db: "DatabaseManager",
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
                filter_level = schema_keys[filter_level_idx] if filter_level_idx >= 0 else "unknown"
                target_level = schema_keys[target_level_idx] if target_level_idx >= 0 else "unknown"
                raise ValueError(
                    f"Filter variable '{filter_class.__name__}' is stored at schema level "
                    f"'{filter_level}' which is finer than target '{target_class.__name__}' "
                    f"at level '{target_level}'. Filters must be at the same or coarser level "
                    f"than the target."
                )


def _expand_coarse_to_fine_schema_ids(
    db: "DatabaseManager",
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
        t = tuple(str(row[k]) if k in row.index and row[k] is not None else None for k in coarse_key_names)
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
        return f"VariableFilter({self.variable_class.__name__} {self.op} {self.value!r})"

    def to_key(self) -> str:
        return f"{self.variable_class.__name__} {self.op} {self.value!r}"

    def resolve(
        self,
        db: "DatabaseManager",
        target_variable_class,
        target_table_name: str,
    ) -> set[int]:
        filter_table_name = self.variable_class.table_name()
        sql_op = _op_to_sql(self.op)

        # Validate schema level compatibility
        _validate_filter_schema_level(
            db, self.variable_class, target_variable_class,
            filter_table_name, target_table_name,
        )

        # Ensure the filter variable is registered
        filter_schema_ids_all = _get_all_schema_ids_for_variable(db, filter_table_name)
        if not filter_schema_ids_all:
            raise ValueError(
                f"Filter variable '{self.variable_class.__name__}' is not registered. "
                "Save data to it first."
            )

        # Find the data column (the "value" column in native single-column mode)
        # We'll use the "value" column as the filter column for simple variables
        condition_sql = f'"value" {sql_op} ?'
        condition_params = [self.value]

        matching_ids = _resolve_variable_schema_ids(
            db, filter_table_name, condition_sql, condition_params
        )

        # Check if we need to expand (coarser filter level)
        filter_level_idx, target_level_idx = _get_level_indices(
            db, filter_table_name, target_table_name
        )
        if filter_level_idx < target_level_idx:
            # Filter is at coarser level — expand to fine-level schema_ids
            matching_ids = _expand_coarse_to_fine_schema_ids(
                db, matching_ids, target_table_name
            )

        # Validate coverage: every target schema_id must have a filter value
        _validate_filter_coverage(
            db, self.variable_class, target_variable_class,
            filter_table_name, target_table_name, filter_level_idx, target_level_idx
        )

        return matching_ids


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
        return f"{self.variable_class.__name__}['{self.column}'] {self.op} {self.value!r}"

    def isin(self, values) -> "InFilter":
        """Create an InFilter for set membership testing."""
        return InFilter(self.variable_class, self.column, list(values))

    def resolve(
        self,
        db: "DatabaseManager",
        target_variable_class,
        target_table_name: str,
    ) -> set[int]:
        filter_table_name = self.variable_class.table_name()
        sql_op = _op_to_sql(self.op)

        # Validate schema level compatibility
        _validate_filter_schema_level(
            db, self.variable_class, target_variable_class,
            filter_table_name, target_table_name,
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

        filter_level_idx, target_level_idx = _get_level_indices(
            db, filter_table_name, target_table_name
        )
        if filter_level_idx < target_level_idx:
            matching_ids = _expand_coarse_to_fine_schema_ids(
                db, matching_ids, target_table_name
            )

        _validate_filter_coverage(
            db, self.variable_class, target_variable_class,
            filter_table_name, target_table_name, filter_level_idx, target_level_idx
        )

        return matching_ids


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
        db: "DatabaseManager",
        target_variable_class,
        target_table_name: str,
    ) -> set[int]:
        filter_table_name = self.variable_class.table_name()

        _validate_filter_schema_level(
            db, self.variable_class, target_variable_class,
            filter_table_name, target_table_name,
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

        filter_level_idx, target_level_idx = _get_level_indices(
            db, filter_table_name, target_table_name
        )
        if filter_level_idx < target_level_idx:
            matching_ids = _expand_coarse_to_fine_schema_ids(
                db, matching_ids, target_table_name
            )

        _validate_filter_coverage(
            db, self.variable_class, target_variable_class,
            filter_table_name, target_table_name, filter_level_idx, target_level_idx
        )

        return matching_ids


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
        db: "DatabaseManager",
        target_variable_class,
        target_table_name: str,
    ) -> set[int]:
        left_ids = self.left.resolve(db, target_variable_class, target_table_name)
        right_ids = self.right.resolve(db, target_variable_class, target_table_name)

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
        db: "DatabaseManager",
        target_variable_class,
        target_table_name: str,
    ) -> set[int]:
        inner_ids = self.inner.resolve(db, target_variable_class, target_table_name)
        all_ids = _get_all_schema_ids_for_variable(db, target_table_name)
        return all_ids - inner_ids


class RawFilter(Filter):
    """Raw SQL escape hatch.

    The SQL fragment is appended to the WHERE clause of the query against the
    target variable's data table (joined with _record_metadata and _schema).

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
        db: "DatabaseManager",
        target_variable_class,
        target_table_name: str,
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
                    SELECT rm.schema_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY rm.variable_name, rm.schema_id, rm.version_keys
                               ORDER BY rm.timestamp DESC
                           ) AS rn,
                           t.*,
                           s.*
                    FROM _record_metadata rm
                    JOIN "{target_table_name}" t
                        ON t.record_id = rm.record_id
                    LEFT JOIN _schema s
                        ON s.schema_id = rm.schema_id
                    WHERE rm.variable_name = ?
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
        StepLength.load_all(where=raw_sql('"Side" = \\'L\\''))
    """
    return RawFilter(sql)


# ---------------------------------------------------------------------------
# Internal helpers used by multiple filter types
# ---------------------------------------------------------------------------

def _get_level_indices(
    db: "DatabaseManager",
    filter_table_name: str,
    target_table_name: str,
) -> tuple[int, int]:
    """Return (filter_level_idx, target_level_idx) in dataset_schema_keys.

    -1 means no keys are populated (no records or no schema columns).
    """
    schema_keys = db.dataset_schema_keys

    filter_ids = _get_all_schema_ids_for_variable(db, filter_table_name)
    target_ids = _get_all_schema_ids_for_variable(db, target_table_name)

    def get_level(schema_ids):
        if not schema_ids:
            return -1
        placeholders = ", ".join(["?"] * len(schema_ids))
        rows = db._duck._fetchdf(
            f"SELECT * FROM _schema WHERE schema_id IN ({placeholders})",
            list(schema_ids),
        )
        if len(rows) == 0:
            return -1
        level_idx = -1
        for i, key in enumerate(schema_keys):
            if key in rows.columns:
                if rows[key].notna().any():
                    level_idx = i
        return level_idx

    return get_level(filter_ids), get_level(target_ids)


def _validate_filter_coverage(
    db: "DatabaseManager",
    filter_class,
    target_class,
    filter_table_name: str,
    target_table_name: str,
    filter_level_idx: int,
    target_level_idx: int,
) -> None:
    """Validate that the filter covers all schema locations the target has data for.

    When filter is at same level: every target schema_id must be present in filter.
    When filter is at coarser level: every coarse key value that the target has
    must be present in the filter.

    Raises:
        ValueError: If coverage is incomplete.
    """
    target_ids = _get_all_schema_ids_for_variable(db, target_table_name)
    if not target_ids:
        return  # Nothing to validate

    filter_ids = _get_all_schema_ids_for_variable(db, filter_table_name)

    if filter_level_idx == target_level_idx:
        # Same level — every target schema_id must be in filter
        missing = target_ids - filter_ids
        if missing:
            raise ValueError(
                f"Filter variable '{filter_class.__name__}' is missing data at "
                f"{len(missing)} schema locations that '{target_class.__name__}' has "
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
                f"{len(missing_coarse)} schema locations that '{target_class.__name__}' "
                "has data for. Ensure the filter variable covers all target locations."
            )
