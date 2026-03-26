"""DuckDB query interface for analytical queries on SciStack data."""

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .database import DatabaseManager


class QueryInterface:
    """
    DuckDB-powered query interface for SciStack.

    Since SciStack now uses DuckDB as its storage backend, queries run directly
    against the database tables.

    Example:
        db = configure_database("experiment.duckdb", ["subject", "session"], "pipeline.db")
        qi = QueryInterface(db)

        # Query a single variable type
        df = qi.query("SELECT * FROM StepLength WHERE value > 0.5")

        # Get available tables
        print(qi.tables())
    """

    def __init__(self, db: "DatabaseManager"):
        """
        Initialize query interface.

        Args:
            db: The DatabaseManager instance to query
        """
        self.db = db

    def tables(self) -> list[str]:
        """
        List all queryable variable types.

        Returns:
            List of table names (variable types that have been registered)
        """
        rows = self.db._duck._fetchall(
            "SELECT table_name FROM _registered_types ORDER BY table_name"
        )
        return [row[0] for row in rows]

    def schema(self, table_name: str) -> pd.DataFrame:
        """
        Get the schema (columns and types) for a variable type.

        Args:
            table_name: The variable type table name

        Returns:
            DataFrame with column names and types
        """
        rows = self.db._duck._fetchall(
            f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
            """,
            [table_name],
        )

        return pd.DataFrame({
            "column": [row[0] for row in rows],
            "dtype": [row[1] for row in rows]
        })

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute an analytical SQL query.

        Queries run directly against DuckDB tables. Table names match
        the variable class names (e.g., "StepLength", "EMGData").

        Args:
            sql: SQL query string

        Returns:
            Query results as a pandas DataFrame

        Example:
            # Simple query
            df = qi.query("SELECT * FROM StepLength WHERE value > 0.5")

            # Aggregation
            df = qi.query("SELECT subject, AVG(value) FROM StepLength GROUP BY subject")

            # Join across types
            df = qi.query('''
                SELECT s.subject, s.value as step_length, w.value as step_width
                FROM StepLength s
                JOIN StepWidth w ON s.subject = w.subject AND s.session = w.session
            ''')
        """
        return self.db._duck._fetchdf(sql)

    def close(self):
        """Close the query interface (no-op, db manages connection)."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def query(db: "DatabaseManager", sql: str) -> pd.DataFrame:
    """
    Execute a one-off analytical query.

    Args:
        db: The DatabaseManager instance
        sql: SQL query string

    Returns:
        Query results as DataFrame

    Example:
        from scidb.query import query

        db = configure_database("experiment.duckdb", ["subject", "session"], "pipeline.db")
        df = query(db, "SELECT * FROM StepLength WHERE value > 0.5")
    """
    with QueryInterface(db) as qi:
        return qi.query(sql)
