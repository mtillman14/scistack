"""SciDuck: a ``DuckConnection`` bound to one dataset schema.

Maps schema locations to integer ``schema_id``s (``_schema``) and keeps the
variable list and groups (``_variables``, ``_variable_groups``). Moved here
from sciduckdb (D4): sciduckdb is the codec and the connection and creates
no tables; every fixed table of a project database, these three included,
is declared in ``scidb.store.tables`` and created on a writable open.
"""

from pathlib import Path

import duckdb
import pandas as pd

from sciduckdb import DuckConnection

from scidb.store import tables
from scidb.store.schema_values import schema_str


def schema_keys_from_db(db_path: str | Path) -> list[str]:
    """Read the dataset schema keys stored in an existing database.

    Opens ``db_path`` read-only, reads the ``_schema`` table's key columns
    (everything except ``schema_id``/``schema_level``, in ordinal order), and
    closes the connection. This lets tools open a database without knowing
    its schema keys in advance (e.g. the ``scidb`` CLI).

    Raises ValueError if the file has no ``_schema`` table (not a
    scidb/sciduckdb database) and whatever duckdb raises if the file does not
    exist or is locked.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = '_schema' "
            "AND column_name NOT IN ('schema_id', 'schema_level') "
            "ORDER BY ordinal_position"
        ).fetchall()
        has_table = (
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = '_schema'"
            ).fetchall()[0][0]
            > 0
        )
    finally:
        con.close()
    if not has_table:
        raise ValueError(
            f"{db_path} has no _schema table — not a scidb/sciduckdb database"
        )
    return [r[0] for r in rows]


class SciDuck(DuckConnection):
    """A ``DuckConnection`` bound to one dataset schema.

    Parameters
    ----------
    db_path : str or Path
        Path to the DuckDB database file.  Use ":memory:" for in-memory.
    dataset_schema : list of str
        Ordered hierarchy, e.g. ["subject", "session", "trial"].
    read_only : bool
        Open read-only. No DDL is executed (the database must already exist).

    Opening writable runs ``connect -> scidb.store.tables.ensure_core -> validate``:
    the tables exist before the schema columns are checked against
    *dataset_schema*.
    """

    def __init__(
        self, db_path: str | Path, dataset_schema: list[str], read_only: bool = False
    ):
        self.dataset_schema = list(dataset_schema)
        super().__init__(db_path, read_only=read_only)
        if self.read_only:
            self._validate_schema_columns()
            return
        tables.ensure_core(self, self.dataset_schema)
        # An existing _schema with rows was written for some schema; an empty
        # one (just created) has nothing to disagree with yet.
        if self.fetchall("SELECT COUNT(*) FROM _schema")[0][0] > 0:
            self._validate_schema_columns()

    # ------------------------------------------------------------------
    # Schema validation
    # ------------------------------------------------------------------

    def _validate_schema_columns(self):
        """Check that the existing _schema columns match dataset_schema.

        Runs no DDL, so it is safe on a read-only connection (where the
        tables must already exist).
        """
        if not self.table_exists("_schema"):
            if self.read_only:
                raise ValueError(
                    f"{self.db_path} has no _schema table — not a scidb/sciduckdb "
                    f"database (read-only open cannot create it)"
                )
            return
        existing_cols = [
            row[0]
            for row in self.fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = '_schema' "
                "AND column_name NOT IN ('schema_id', 'schema_level') "
                "ORDER BY ordinal_position"
            )
        ]
        if existing_cols != self.dataset_schema:
            raise ValueError(
                f"Database schema mismatch. "
                f"Existing: {existing_cols}, Provided: {self.dataset_schema}"
            )

    # ------------------------------------------------------------------
    # Schema entry management
    # ------------------------------------------------------------------

    def get_or_create_schema_id(self, schema_level: str, key_values: dict) -> int:
        """Look up or insert a row in _schema.  Return the schema_id."""
        key_cols = [k for k in self.dataset_schema if k in key_values]

        # Build WHERE clause
        conditions = []
        params = [schema_level]
        for col in key_cols:
            conditions.append(f'"{col}" = ?')
            params.append(schema_str(key_values[col]))
        # Columns above the level that should be NULL are implicit —
        # but to be safe, also require NULLs for levels below.
        for col in self.dataset_schema:
            if col not in key_cols:
                conditions.append(f'"{col}" IS NULL')

        where = " AND ".join(conditions)
        rows = self.fetchall(
            f"SELECT schema_id FROM _schema WHERE schema_level = ? AND {where}",
            params,
        )
        if rows:
            return rows[0][0]

        # Insert new entry — use MAX+1 for consistency with batch path
        new_id = self.fetchall("SELECT COALESCE(MAX(schema_id), 0) + 1 FROM _schema")[
            0
        ][0]
        col_names = ["schema_id", "schema_level"] + key_cols
        placeholders = ", ".join(["?"] * len(col_names))
        col_str = ", ".join(f'"{c}"' for c in col_names)
        values = [new_id, schema_level] + [schema_str(key_values[c]) for c in key_cols]
        self.execute(
            f"INSERT INTO _schema ({col_str}) VALUES ({placeholders})", values
        )
        return new_id

    def batch_get_or_create_schema_ids(
        self,
        combos: dict,  # {(schema_level, key_tuple): key_values_dict}
    ) -> dict:
        """
        Batch-resolve schema IDs for multiple (schema_level, key_values) combos.

        Instead of N individual SELECT+INSERT round-trips, does:
        1. One SELECT to fetch all existing schema entries
        2. Batch INSERT for missing entries
        3. One SELECT to get IDs for newly inserted entries

        Args:
            combos: dict mapping (schema_level, key_tuple) -> key_values dict

        Returns:
            dict mapping (schema_level, key_tuple) -> schema_id
        """
        if not combos:
            return {}

        result = {}

        # Group combos by (schema_level, key set) for efficient querying
        by_level_and_keys = {}
        for (schema_level, key_tuple), key_values in combos.items():
            group_key = (schema_level, frozenset(key_values.keys()))
            by_level_and_keys.setdefault(group_key, []).append(
                ((schema_level, key_tuple), key_values)
            )

        for (schema_level, key_set), entries in by_level_and_keys.items():
            key_cols = [k for k in self.dataset_schema if k in key_set]
            null_cols = [c for c in self.dataset_schema if c not in key_cols]

            # Build a single query to find all existing matches at this level
            # We fetch all rows for this schema_level and match in Python
            null_conditions = " AND ".join(f'"{col}" IS NULL' for col in null_cols)
            where_clause = "schema_level = ?"
            if null_conditions:
                where_clause += f" AND {null_conditions}"

            col_select = ", ".join(f'"{c}"' for c in key_cols)
            rows = self.fetchall(
                f"SELECT schema_id, {col_select} FROM _schema WHERE {where_clause}",
                [schema_level],
            )

            # Build lookup: tuple of col values -> schema_id
            existing_lookup = {}
            for row in rows:
                sid = row[0]
                row_key = tuple(
                    schema_str(v) if v is not None else "" for v in row[1:]
                )
                existing_lookup[row_key] = sid

            # Match entries against existing rows
            missing = []  # [(combo_key, key_values), ...]
            for combo_key, key_values in entries:
                match_key = tuple(schema_str(key_values.get(c, "")) for c in key_cols)
                if match_key in existing_lookup:
                    result[combo_key] = existing_lookup[match_key]
                else:
                    missing.append((combo_key, key_values, match_key))

            # Batch insert missing entries
            if missing:
                # Allocate a block of IDs from current max instead of N nextval() calls
                max_row = self.fetchall(
                    "SELECT COALESCE(MAX(schema_id), 0) FROM _schema"
                )
                first_id = max_row[0][0] + 1

                col_names = ["schema_id", "schema_level"] + key_cols

                insert_rows = []
                for idx, (combo_key, key_values, _) in enumerate(missing):
                    new_id = first_id + idx
                    row = [new_id, schema_level] + [
                        schema_str(key_values[c]) for c in key_cols
                    ]
                    insert_rows.append(row)
                    result[combo_key] = new_id

                self.bulk_insert("_schema", col_names, insert_rows)

        return result

    # ------------------------------------------------------------------
    # List / inspect
    # ------------------------------------------------------------------

    _LIST_VARIABLES_SQL = """
            SELECT variable_name, schema_level, created_at, description
            FROM _variables
            ORDER BY variable_name
        """

    def list_variables(self) -> pd.DataFrame:
        """
        List all variables with their schema level and creation time.
        """
        return self.fetchdf(self._LIST_VARIABLES_SQL)

    def list_variables_if_open(self) -> pd.DataFrame | None:
        """``list_variables``, or None when the connection is closed.

        For best-effort readers that can do without the database. The
        open check and the query are one step under ``_lock``, so a
        concurrent ``close()`` cannot slip between them.
        """
        return self.fetchdf_if_open(self._LIST_VARIABLES_SQL)

    # ------------------------------------------------------------------
    # Groups
    # ------------------------------------------------------------------

    def add_to_group(self, group_name: str, variable_names: str | list[str]):
        """Add one or more variables to a group."""
        if isinstance(variable_names, str):
            variable_names = [variable_names]
        for vn in variable_names:
            self.execute(
                "INSERT INTO _variable_groups (group_name, variable_name) "
                "VALUES (?, ?) ON CONFLICT DO NOTHING",
                [group_name, vn],
            )

    def remove_from_group(self, group_name: str, variable_names: str | list[str]):
        """Remove one or more variables from a group."""
        if isinstance(variable_names, str):
            variable_names = [variable_names]
        for vn in variable_names:
            self.execute(
                "DELETE FROM _variable_groups "
                "WHERE group_name = ? AND variable_name = ?",
                [group_name, vn],
            )

    def list_groups(self) -> list[str]:
        """List all group names."""
        rows = self.fetchall(
            "SELECT DISTINCT group_name FROM _variable_groups ORDER BY group_name"
        )
        return [r[0] for r in rows]

    def get_group(self, group_name: str) -> list[str]:
        """Get all variable names in a group."""
        rows = self.fetchall(
            "SELECT variable_name FROM _variable_groups "
            "WHERE group_name = ? ORDER BY variable_name",
            [group_name],
        )
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def distinct_schema_values(self, key: str) -> list:
        """Return all distinct non-null values for a schema column, sorted."""
        if key not in self.dataset_schema:
            raise ValueError(
                f"'{key}' is not a schema column. Available: {self.dataset_schema}"
            )
        rows = self.fetchall(
            f'SELECT DISTINCT "{key}" FROM _schema '
            f'WHERE "{key}" IS NOT NULL '
            f'ORDER BY "{key}"'
        )
        return [r[0] for r in rows]

    def distinct_schema_combinations(self, keys: list[str]) -> list[tuple]:
        """Return all distinct non-null combinations for multiple schema columns.

        Args:
            keys: List of schema column names to query.

        Returns:
            List of tuples, each tuple being one existing combination of values
            (as strings, since _schema stores VARCHAR columns). Sorted by the
            column order given.
        """
        for k in keys:
            if k not in self.dataset_schema:
                raise ValueError(
                    f"'{k}' is not a schema column. Available: {self.dataset_schema}"
                )
        col_list = ", ".join(f'"{k}"' for k in keys)
        where_clause = " AND ".join(f'"{k}" IS NOT NULL' for k in keys)
        order_clause = ", ".join(f'"{k}"' for k in keys)
        rows = self.fetchall(
            f"SELECT DISTINCT {col_list} FROM _schema "
            f"WHERE {where_clause} "
            f"ORDER BY {order_clause}"
        )
        return [tuple(r) for r in rows]

    def __repr__(self):
        try:
            n_vars = self.fetchall(
                "SELECT COUNT(DISTINCT variable_name) FROM _variables"
            )[0][0]
        except Exception:
            n_vars = "?"
        return (
            f"SciDuck(path='{self.db_path}', "
            f"schema={self.dataset_schema}, variables={n_vars})"
        )
