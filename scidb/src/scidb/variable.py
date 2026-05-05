"""Base class for database-storable variables."""

import itertools
from typing import Any, Self

import pandas as pd


class VariableMeta(type):
    """Metaclass for BaseVariable that enables class-level comparison operators.

    This allows expressions like ``Side == "L"`` (at the class level, not
    instance level) to produce ``VariableFilter`` objects for use in the
    ``where=`` parameter of ``load()`` and ``load_all()``.

    Class-to-class equality (e.g. ``Side == Side``) is preserved so that
    class identity checks still work. Hashing is also preserved so that
    variable classes can be used as dict keys.
    """

    def __eq__(cls, other):
        if isinstance(other, type):
            return type.__eq__(cls, other)
        from .filters import VariableFilter
        return VariableFilter(cls, "==", other)

    def __ne__(cls, other):
        if isinstance(other, type):
            return type.__ne__(cls, other)
        from .filters import VariableFilter
        return VariableFilter(cls, "!=", other)

    def __lt__(cls, other):
        from .filters import VariableFilter
        return VariableFilter(cls, "<", other)

    def __le__(cls, other):
        from .filters import VariableFilter
        return VariableFilter(cls, "<=", other)

    def __gt__(cls, other):
        from .filters import VariableFilter
        return VariableFilter(cls, ">", other)

    def __ge__(cls, other):
        from .filters import VariableFilter
        return VariableFilter(cls, ">=", other)

    def __hash__(cls):
        return type.__hash__(cls)


class BaseVariable(metaclass=VariableMeta):
    """
    Base class for all database-storable variable types.

    For most data types (scalars, numpy arrays, lists, dicts), no methods
    need to be overridden — SciDuck handles serialization automatically.

    Override to_db() and from_db() only for types that need custom
    serialization (e.g., pandas DataFrames, domain-specific objects).

    Example (minimal — native SciDuck storage):
        class StepLength(BaseVariable):
            schema_version = 1

        StepLength.save(np.array([0.65, 0.72, 0.68]), subject=1, session="A")
        loaded = StepLength.load(subject=1, session="A")

    Example (custom serialization):
        class RotationMatrix(BaseVariable):
            schema_version = 1

            def to_db(self) -> pd.DataFrame:
                return pd.DataFrame({
                    'row': [0, 0, 0, 1, 1, 1, 2, 2, 2],
                    'col': [0, 1, 2, 0, 1, 2, 0, 1, 2],
                    'value': self.data.flatten().tolist()
                })

            @classmethod
            def from_db(cls, df: pd.DataFrame) -> np.ndarray:
                values = df.sort_values(['row', 'col'])['value'].values
                return values.reshape(3, 3)
    """

    schema_version: int = 1 # Default schema version. Change whenever the structure of a variable changes.

    # Reserved metadata keys that users cannot use
    _reserved_keys = frozenset(
        {"record_id", "id", "created_at", "schema_version", "index", "loc", "iloc"}
    )

    # Global registry of all subclasses (for auto-registration with database)
    _all_subclasses: dict[str, type["BaseVariable"]] = {}

    def __init_subclass__(cls, **kwargs):
        """Register subclass in global registry when defined."""
        super().__init_subclass__(**kwargs)
        cls._all_subclasses[cls.__name__] = cls

    def __class_getitem__(cls, key):
        """
        Select specific columns from a table variable.

        Returns a ColumnSelection wrapper that, when used in for_each() inputs,
        extracts only the specified columns after loading.

        Args:
            key: Column name (str) or list of column names

        Returns:
            ColumnSelection wrapping this class and the requested columns

        Example:
            # Single column — function receives numpy array
            for_each(fn, inputs={"x": MyVar["col_a"]}, ...)

            # Multiple columns — function receives DataFrame subset
            for_each(fn, inputs={"x": MyVar[["col_a", "col_b"]]}, ...)
        """
        from scidb.column_selection import ColumnSelection

        if isinstance(key, str):
            return ColumnSelection(cls, [key])
        elif isinstance(key, (list, tuple)):
            return ColumnSelection(cls, list(key))
        raise TypeError(
            f"Column selection key must be str or list of str, got {type(key).__name__}"
        )

    @classmethod
    def get_subclass_by_name(cls, name: str) -> type["BaseVariable"] | None:
        """Look up a subclass by its name from the global registry."""
        return cls._all_subclasses.get(name)

    def __init__(self, data: Any):
        """
        Initialize with native data.

        Args:
            data: The native Python object (numpy array, etc.)
        """
        # Convert memoryviews to numpy arrays
        # DuckDB/pandas sometimes returns numpy arrays as memoryviews for efficiency
        if isinstance(data, memoryview):
            import numpy as np
            self.data = np.asarray(data)
        else:
            self.data = data
        self.record_id: str | None = None
        self.metadata: dict | None = None
        self.content_hash: str | None = None
        self.lineage_hash: str | None = None
        self.branch_params: dict = {}

    def to_db(self) -> pd.DataFrame:
        """
        Convert native data to a DataFrame for storage.

        You do NOT need to override this for common types (scalars, numpy
        arrays, lists, dicts). SciDuck handles those natively with proper
        DuckDB types (DOUBLE[], JSON, etc.). Only override when you need
        custom multi-column serialization (e.g., pandas DataFrames).

        If you override to_db(), you must also override from_db().

        Returns:
            pd.DataFrame: Tabular representation of the data
        """
        return pd.DataFrame({"value": [self.data]})

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> Any:
        """
        Convert a DataFrame back to the native data type.

        Only override this if you also override to_db(). For common types,
        SciDuck's native type restoration is used automatically.

        Args:
            df: The DataFrame retrieved from storage

        Returns:
            The native Python object
        """
        if len(df) == 1:
            return df["value"].iloc[0]
        return df["value"].tolist()

    @classmethod
    def table_name(cls) -> str:
        """
        Get the table name for this variable type.

        Returns the exact class name (e.g., "StepLength", "EMGData").

        Returns:
            str: Table name matching the class name
        """
        return cls.__name__ + "_data"

    @classmethod
    def view_name(cls) -> str:
        """
        Get the view name for this variable type.

        Returns the class name (e.g., "StepLength", "EMGData").
        The view joins the data table with _schema and _variables.

        Returns:
            str: View name matching the class name
        """
        return cls.__name__

    @classmethod
    def save(
        cls,
        data: Any,
        index: Any | None = None,
        db=None,
        **metadata,
    ) -> str:
        """
        Save data to the database as this variable type.

        Accepts an existing BaseVariable instance or raw data. For saving
        ThunkOutput (lineage-tracked results), use scihist.for_each or
        call scihist's save helpers.

        Args:
            data: The data to save. Can be:
                - BaseVariable: an existing variable instance
                - Any other type: raw data (numpy array, etc.)
            index: Optional index for the DataFrame. Sets df.index after to_db()
                is called. Useful for storing lists/arrays with semantic indexing.
                Must match the length of the DataFrame rows.
            db: Optional DatabaseManager instance to use instead of the global
                database. Allows one-shot operations against a specific database
                without changing the global default.
            **metadata: Addressing metadata (e.g., subject=1, trial=1)

        Returns:
            str: The record_id of the saved data

        Raises:
            ReservedMetadataKeyError: If metadata contains reserved keys
            NotRegisteredError: If this variable type is not registered
            DatabaseNotConfiguredError: If no database is available
            ValueError: If index length doesn't match DataFrame row count

        Example:
            # Save raw data
            record_id = CleanData.save(np.array([1, 2, 3]), subject=1, trial=1)

            # Save with index for later indexed access
            record_id = StepLength.save(step_lengths, index=range(10), subject=1)

            # Re-save an existing variable with new metadata
            var = CleanData.load(subject=1, trial=1)
            record_id = CleanData.save(var, subject=1, trial=2)

            # Save to a specific database
            record_id = CleanData.save(data, db=aim2_db, subject=1, trial=1)
        """
        from .exceptions import ReservedMetadataKeyError

        # Validate metadata keys
        reserved_used = set(metadata.keys()) & cls._reserved_keys
        if reserved_used:
            raise ReservedMetadataKeyError(
                f"Cannot use reserved metadata keys: {reserved_used}"
            )

        from .database import get_database
        _db = db or get_database()
        return _db.save_variable(cls, data, index=index, **metadata)

    @classmethod
    def load(
        cls,
        version: str = "latest",
        loc: Any | None = None,
        iloc: Any | None = None,
        as_table: bool = False,
        where=None,
        db=None,
        **metadata,
    ) -> "Self | list[Self] | pd.DataFrame":
        """
        Load variable(s) from the database.

        Returns a single variable when exactly one record matches, or a list
        of variables when multiple records match. This allows partial schema
        key queries to naturally return all matching rows.

        When as_table=True and multiple results match, returns a pandas
        DataFrame with schema key columns, version key columns,
        and a data column named after the variable's class name.

        Args:
            version: "latest" for most recent, or specific record_id
            loc: Optional label-based index selection (like pandas df.loc[]).
                Supports single values, lists, ranges, or slices.
            iloc: Optional integer position-based index selection (like pandas df.iloc[]).
                Supports single values, lists, ranges, or slices.
            as_table: If True, return a pd.DataFrame when multiple results
                match instead of a list of variables. Single results still
                return a single variable instance. Default is False.
            db: Optional DatabaseManager instance to use instead of the global
                database. Allows one-shot operations against a specific database
                without changing the global default.
            **metadata: Addressing metadata to match

        Returns:
            A single variable if one record matches, or
            a list of variables (or DataFrame if as_table=True) if multiple
            records match.

        Raises:
            NotFoundError: If no matching data found
            NotRegisteredError: If this variable type is not registered
            DatabaseNotConfiguredError: If no database is available
            ValueError: If both loc and iloc are provided

        Example:
            # Load single record (all schema keys provided)
            var = StepLength.load(subject=1, device="left")

            # Load multiple records (partial schema keys)
            vars = StepLength.load(subject=1)

            # Load as DataFrame
            df = StepLength.load(as_table=True, subject=1)

            # Load with indexing
            var = StepLength.load(subject=1, device="left", loc=5)

            # Load from a specific database
            var = StepLength.load(db=aim2_db, subject=1, device="left")
        """
        from .database import get_database

        if loc is not None and iloc is not None:
            raise ValueError("Cannot specify both 'loc' and 'iloc'. Use one or the other.")

        _db = db or get_database()

        # Loading by specific version/record_id → always single variable
        if version != "latest" and version is not None:
            return _db.load(cls, metadata, version=version, loc=loc, iloc=iloc)

        # Separate schema metadata from branch_params_filter
        schema_keys_set = set(_db.dataset_schema_keys)
        schema_metadata = {k: v for k, v in metadata.items() if k in schema_keys_set}
        branch_params_filter = {k: v for k, v in metadata.items()
                                 if k not in schema_keys_set} or None

        # Query all matching records (latest version per parameter set)
        from .exceptions import AmbiguousVersionError, NotFoundError

        results = list(_db.load_all(
            cls, schema_metadata, version_id="latest", where=where,
            branch_params_filter=branch_params_filter,
        ))

        if not results:
            raise NotFoundError(
                f"No {cls.__name__} found matching metadata: {metadata}"
            )
        elif len(results) == 1:
            # Single match → return variable directly (with loc/iloc support)
            if loc is not None or iloc is not None:
                return _db.load(cls, {}, version=results[0].record_id,
                                loc=loc, iloc=iloc)
            return results[0]
        else:
            # Multiple results: check if at the same schema location
            first_schema = {k: v for k, v in (results[0].metadata or {}).items()
                            if k in schema_keys_set}
            same_schema = all(
                {k: v for k, v in (r.metadata or {}).items() if k in schema_keys_set}
                == first_schema
                for r in results[1:]
            )
            if same_schema:
                loc_str = ", ".join(f"{k}={v!r}" for k, v in first_schema.items())
                lines = [
                    f"{len(results)} variants exist for {cls.__name__} at {loc_str}.",
                    "Specify branch parameters to select one:",
                ]
                for r in results:
                    bp = getattr(r, 'branch_params', {}) or {}
                    bp_str = ", ".join(f"{k}={v!r}" for k, v in bp.items())
                    lines.append(f"  {bp_str or '(no branch params)'}  "
                                 f"(record_id: {r.record_id!r})")
                raise AmbiguousVersionError("\n".join(lines))
            else:
                # Different schema locations → return list (existing behavior)
                if as_table:
                    return cls._results_to_dataframe(results)
                return results

    @classmethod
    def _results_to_dataframe(cls, results: list["BaseVariable"]) -> pd.DataFrame:
        """Convert a list of loaded variables to a DataFrame.

        Columns: schema key columns + version key columns +
        data column (named after cls.view_name()).
        """
        rows = []
        for var in results:
            row = dict(var.metadata) if var.metadata else {}
            row[cls.view_name()] = var.data
            rows.append(row)
        return pd.DataFrame(rows)

    @classmethod
    def load_all(
        cls,
        as_df: bool = False,
        include_record_id: bool = False,
        version_id: str = "all",
        where=None,
        db=None,
        **metadata,
    ):
        """
        Load all matching variables from the database.

        By default returns a generator for memory-efficient iteration.
        Use as_df=True to load all records into a pandas DataFrame.

        Args:
            as_df: If True, return a DataFrame instead of a generator.
                   The DataFrame has columns for each metadata key plus 'data'.
            include_record_id: If True and as_df=True, include record_id column.
            version_id: Which versions to return:
                - "all" (default): return every version
                - "latest": return only the latest version per (schema, version_keys)
            db: Optional DatabaseManager instance to use instead of the global
                database. Allows one-shot operations against a specific database
                without changing the global default.
            **metadata: Addressing metadata to match (partial matching supported).
                List values are interpreted as "match any" (OR semantics).

        Returns:
            Generator of variable instances (default), or
            pandas DataFrame if as_df=True

        Raises:
            NotRegisteredError: If this variable type is not registered
            DatabaseNotConfiguredError: If no database is available
            NotFoundError: If as_df=True and no matching data found

        Example:
            # Iterate over records (memory-efficient)
            for signal in ProcessedSignal.load_all(subject=1):
                print(signal.metadata, signal.data.shape)

            # Load all as DataFrame for analysis
            df = ProcessedSignal.load_all(subject=1, as_df=True)

            # Batch load: match any of several subjects
            for var in ProcessedSignal.load_all(subject=[1, 2, 3]):
                print(var.metadata)

            # Control version selection
            for var in ProcessedSignal.load_all(subject=1, version_id="latest"):
                print(var.data)

            # Load from a specific database
            df = ProcessedSignal.load_all(db=aim2_db, subject=1, as_df=True)
        """
        import pandas as pd
        from .database import get_database
        from .exceptions import NotFoundError

        _db = db or get_database()

        if not as_df:
            # Return generator via helper to avoid making this function a generator
            return cls._load_all_generator(_db, metadata, version_id=version_id, where=where)
        else:
            # Collect into DataFrame
            results = list(_db.load_all(cls, metadata, version_id=version_id, where=where))

            if not results:
                raise NotFoundError(
                    f"No {cls.__name__} found matching metadata: {metadata}"
                )

            rows = []
            for var in results:
                row = dict(var.metadata) if var.metadata else {}
                row["data"] = var.data
                if include_record_id:
                    row["record_id"] = var.record_id
                rows.append(row)

            return pd.DataFrame(rows)

    @classmethod
    def _load_all_generator(cls, db, metadata: dict, version_id: str = "all", where=None):
        """Helper generator for load_all() to avoid making load_all a generator."""
        yield from db.load_all(cls, metadata, version_id=version_id, where=where)

    @classmethod
    def list_versions(
        cls,
        db=None,
        **metadata,
    ) -> list[dict]:
        """
        List all versions at a schema location.

        Shows all saved versions (including those with empty version {})
        at a given schema location. This is useful for seeing what
        computational variants exist for a given dataset location.

        Args:
            db: Optional DatabaseManager instance to use instead of the global
                database. Allows one-shot operations against a specific database
                without changing the global default.
            **metadata: Schema metadata to match

        Returns:
            List of dicts with record_id, schema, version, created_at

        Raises:
            NotRegisteredError: If this variable type is not registered
            DatabaseNotConfiguredError: If no database is available

        Example:
            versions = ProcessedSignal.list_versions(subject=1, visit=2)
            for v in versions:
                print(f"record_id: {v['record_id']}, version: {v['version']}")
        """
        from .database import get_database

        _db = db or get_database()
        return _db.list_versions(cls, **metadata)

    @classmethod
    def save_from_dataframe(
        cls,
        df: pd.DataFrame,
        data_column: str,
        metadata_columns: list[str],
        db=None,
        **common_metadata,
    ) -> list[str]:
        """
        Save each row of a DataFrame as a separate database record.

        Use this when a DataFrame contains multiple independent data items,
        each with its own metadata (e.g., different subjects/trials per row).

        Args:
            df: DataFrame where each row is a separate data item
            data_column: Column name containing the data to store
            metadata_columns: Column names to use as metadata for each row
            db: Optional DatabaseManager instance to use instead of the global
                database. Allows one-shot operations against a specific database
                without changing the global default.
            **common_metadata: Additional metadata applied to all rows

        Returns:
            List of record_ides for each saved record

        Example:
            # DataFrame with 10 rows (2 subjects x 5 trials)
            #   Subject  Trial  MyVar
            #   1        1      0.5
            #   1        2      0.6
            #   ...

            record_ides = ScalarValue.save_from_dataframe(
                df=results_df,
                data_column="MyVar",
                metadata_columns=["Subject", "Trial"],
                experiment="exp1"  # Applied to all rows
            )
        """
        from .exceptions import ReservedMetadataKeyError

        # Validate metadata keys
        all_keys = set(metadata_columns) | set(common_metadata.keys())
        reserved_used = all_keys & cls._reserved_keys
        if reserved_used:
            raise ReservedMetadataKeyError(
                f"Cannot use reserved metadata keys: {reserved_used}"
            )

        from .database import get_database
        _db = db or get_database()

        # Build data_items list: [(data_value, flat_metadata_dict), ...]
        # Extract columns as Python lists for fast iteration (avoids iterrows overhead)
        meta_lists = {}
        for col in metadata_columns:
            series = df[col]
            # Convert numpy scalars to Python natives in bulk
            if hasattr(series.dtype, 'kind') and series.dtype.kind in ('i', 'u', 'f', 'b'):
                meta_lists[col] = series.tolist()
            else:
                meta_lists[col] = list(series)

        data_series = df[data_column]
        if hasattr(data_series.dtype, 'kind') and data_series.dtype.kind in ('i', 'u', 'f', 'b'):
            data_list = data_series.tolist()
        else:
            data_list = list(data_series)

        n = len(df)
        data_items = []
        for i in range(n):
            row_metadata = {col: meta_lists[col][i] for col in metadata_columns}
            full_metadata = {**common_metadata, **row_metadata}
            data_items.append((data_list[i], full_metadata))

        return _db.save_batch(cls, data_items)

    @classmethod
    def head(cls, n: int = 1, db=None, **metadata) -> pd.DataFrame:
        """
        Peek at the first N records of this variable (latest version).

        Convenience method for quickly inspecting stored data without
        loading everything.

        Args:
            n: Number of records to return. Default is 1.
            db: Optional DatabaseManager instance to use instead of the
                global database.
            **metadata: Optional metadata filters (e.g., subject=1).

        Returns:
            pd.DataFrame with schema key columns and a 'data' column.
            Returns an empty DataFrame if no records exist.

        Example:
            StepLength.head()       # first record
            StepLength.head(5)      # first 5 records
            StepLength.head(3, subject=1)  # first 3 for subject 1
        """
        from .database import get_database

        _db = db or get_database()
        results = list(itertools.islice(
            _db.load_all(cls, metadata, version_id="latest"), n
        ))
        if not results:
            return pd.DataFrame()
        rows = []
        for var in results:
            row = dict(var.metadata) if var.metadata else {}
            row["data"] = var.data
            rows.append(row)
        return pd.DataFrame(rows)

    def to_csv(self, path: str) -> None:
        """
        Export this variable's data to a CSV file.

        Exports the DataFrame representation (from to_db()) to CSV format
        for viewing in external tools.

        Args:
            path: Output file path (will be overwritten if exists)

        Example:
            var = TimeSeries.load(subject=1)
            var.to_csv("subject1_data.csv")
        """
        df = self.to_db()
        df.to_csv(path, index=False)

    def __repr__(self) -> str:
        """Return a string representation of this variable."""
        type_name = type(self).__name__
        if self.record_id:
            return f"{type_name}(record_id={self.record_id[:12]}...)"
        return f"{type_name}(data={type(self.data).__name__})"
