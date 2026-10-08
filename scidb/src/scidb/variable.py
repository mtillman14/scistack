"""Base class for database-storable variables."""

import itertools
from typing import TYPE_CHECKING, Any

import pandas as pd

from .log import Log
from .names import library_of

if TYPE_CHECKING:
    from .column_selection import ColumnSelection


class VariableMeta(type):
    """Metaclass for BaseVariable that enables class-level comparison operators.

    This allows expressions like ``Side == "L"`` (at the class level, not
    instance level) to produce ``VariableFilter`` objects for use in the
    ``where=`` parameter of ``load()``.

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

    schema_version: int = 1  # Default schema version. Change whenever the structure of a variable changes.

    # Reserved metadata keys that users cannot use
    _reserved_keys = frozenset(
        {"record_id", "id", "created_at", "schema_version", "index", "loc", "iloc"}
    )

    # Global registry of all subclasses (for auto-registration with database)
    _all_subclasses: dict[str, type["BaseVariable"]] = {}

    #: Same-name declarations from two modules that are NOT the same
    #: definition: ``{name: (kept, rejected)}``. See :func:`same_definition`.
    _definition_conflicts: dict[str, tuple[type, type]] = {}

    def __init_subclass__(cls, **kwargs):
        """Register subclass in global registry when defined.

        A Variable's name is its table, so it is not namespaced: the same name
        from two places is ONE type when the definitions agree
        (:func:`same_definition`; decided 2026-10-08, ``docs/claude/
        portability.md`` "Reusing code"). When they do not, the conflict is
        recorded with both classes (the GUI reports it as a load error naming
        both sources) and the project's definition is kept over a library's,
        whichever was defined first. A redefinition in the SAME module is
        an edit, and simply replaces.
        """
        super().__init_subclass__(**kwargs)
        name = cls.__name__
        existing = BaseVariable._all_subclasses.get(name)
        if (
            existing is not None
            and existing is not cls
            and getattr(cls, "_declared_only", False)
            and not getattr(existing, "_declared_only", False)
        ):
            # A name-only declaration (an entities TOML) of a type that is
            # already DEFINED: the definition stays (same type, more specific).
            return
        if (
            existing is None
            or existing is cls
            or getattr(existing, "__module__", None) == getattr(cls, "__module__", None)
            or same_definition(existing, cls)
        ):
            BaseVariable._all_subclasses[name] = cls
            return
        keep, rejected = _preferred_definition(existing, cls)
        BaseVariable._definition_conflicts[name] = (keep, rejected)
        BaseVariable._all_subclasses[name] = keep
        Log.warn(
            f"[variable] Variable {name!r} is defined differently in "
            f"{rejected.__module__} and {keep.__module__}; keeping "
            f"{keep.__module__}'s. A Variable's name is its table: remove one "
            f"definition or rename it."
        )

    @classmethod
    def definition_conflicts(cls) -> dict[str, tuple[type, type]]:
        """``{name: (kept, rejected)}`` for every unresolved conflict."""
        return dict(BaseVariable._definition_conflicts)

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
    def for_columns(cls, columns: "list[str] | None" = None) -> "ColumnSelection":
        """
        Iterate a for_each() function over each column of this table variable.

        Unlike ``MyVar[[...]]`` (which passes the columns to the function as one
        argument), ``MyVar.for_columns([...])`` runs the function once per
        column and reassembles the per-column results into a single output
        variable whose data, per schema combo, is a one-row table with the same
        column names as the source.

        When multiple inputs use ``for_columns`` (e.g. a ``Fixed`` baseline and
        a value), they are zipped by column name and must share the same column
        set.

        Args:
            columns: Column names to iterate over. An empty list ``[]`` (the
                default, via ``MyVar.for_columns()``) iterates over all data
                columns, resolved at for_each time — mirroring how an empty
                iteration list (``subject=[]``) means "all values from the
                data". ``None`` is accepted as an alias for ``[]``.

        Returns:
            A ``ColumnSelection`` in iterate mode.

        Example:
            for_each(
                mean_change_from_reference,
                inputs={
                    "baseline": Fixed(GaitData.for_columns(), session="BL"),
                    "value":    GaitData.for_columns(),
                },
                outputs=[DeltaGaitData],
                subject=[], session=[],
            )
        """
        from scidb.column_selection import ColumnSelection

        if columns is None:
            columns = []
        if isinstance(columns, str):
            columns = [columns]
        # ColumnSelection normalizes the empty/None all-columns sentinel.
        return ColumnSelection(cls, columns, iterate=True)

    @classmethod
    def get_subclass_by_name(cls, name: str) -> type["BaseVariable"] | None:
        """Look up a subclass by its name from the global registry."""
        return cls._all_subclasses.get(name)

    @classmethod
    def unregister(cls, name: str) -> bool:
        """Drop *name* from the global registry. Returns whether it was there.

        The counterpart ``__init_subclass__`` never had. Without it the
        registry only ever grew: every other registry a discovery scan
        maintains is cleared and rebuilt, but variables accumulated for the
        life of the process, so a class whose declaration had been deleted
        (or that belonged to a previously-opened project) stayed registered
        forever. That made the GUI list variables nothing on disk declared,
        and made creating a variable fail as "already exists" against a name
        with no source file behind it.

        Callers are the discovery layers, which track *where* each name came
        from and so know which of their own registrations to withdraw --
        this deliberately takes a name rather than clearing wholesale, so
        entries registered by something else (scidb's own types, a test
        fixture, a still-live import) are never collateral.

        Unregistering does not invalidate the class object itself; anything
        already holding a reference keeps working. It only stops *lookup by
        name* from finding it.
        """
        BaseVariable._definition_conflicts.pop(name, None)
        return cls._all_subclasses.pop(name, None) is not None

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

        Accepts an existing BaseVariable instance or raw data. For
        provenance-tracked pipeline outputs, use ``for_each``.

        Args:
            data: The data to save. Can be:
                - BaseVariable: an existing variable instance
                - pd.DataFrame: if columns include any dataset_schema_keys, each
                  row is saved as a separate record automatically (schema-key
                  columns become per-row metadata, remaining columns become the
                  data). DataFrames with no schema-key columns are saved as a
                  single record. Returns a list[str] of record_ids in this case.
                - Any other type: raw data (numpy array, etc.)
            index: Optional index for the DataFrame. Sets df.index after to_db()
                is called. Useful for storing lists/arrays with semantic indexing.
                Must match the length of the DataFrame rows.
            db: Optional DatabaseManager instance to use instead of the global
                database. Allows one-shot operations against a specific database
                without changing the global default.
            **metadata: Addressing metadata (e.g., subject=1, trial=1). When
                auto-distributing a DataFrame, these are applied as common
                metadata to every row.

        Returns:
            str: The record_id of the saved data, or list[str] when
                auto-distributing a DataFrame with schema-key columns.

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

        # Auto-detect DataFrame-distribute: if data is a DataFrame with columns
        # matching the configured schema keys, dispatch to save_from_dataframe.
        if isinstance(data, pd.DataFrame):
            schema_keys = _db.dataset_schema_keys
            df_cols = list(data.columns)
            meta_cols = [c for c in schema_keys if c in df_cols]
            if meta_cols:
                data_cols = [c for c in df_cols if c not in schema_keys]
                if not data_cols:
                    raise ValueError(
                        f"DataFrame has no data columns (all columns are schema keys: {meta_cols})."
                    )
                if len(data_cols) == 1:
                    return cls.save_from_dataframe(
                        data, data_cols[0], meta_cols, db=_db, **metadata
                    )
                else:
                    # Multiple data columns: save each row's sub-DataFrame as data
                    sub_df = data[data_cols].reset_index(drop=True)
                    aug = data[meta_cols].copy().reset_index(drop=True)
                    aug["__scidb_row_data__"] = [
                        sub_df.iloc[[i]].reset_index(drop=True)
                        for i in range(len(data))
                    ]
                    return cls.save_from_dataframe(
                        aug, "__scidb_row_data__", meta_cols, db=_db, **metadata
                    )
            # No schema-key columns in DataFrame → fall through and save as one record.

        return _db.save_variable(cls, data, index=index, **metadata)

    @classmethod
    def load(
        cls,
        as_df: bool = False,
        version: str = "latest",
        where=None,
        db=None,
        introspect: bool = False,
        **metadata,
    ) -> "BaseVariable | list[BaseVariable] | pd.DataFrame":
        """
        Load variable(s) from the database.

        Returns a single ``BaseVariable`` when exactly one record matches, a
        ``list`` when multiple records match, or a ``pd.DataFrame`` when
        ``as_df=True``. Raises ``NotFoundError`` when nothing matches.

        Args:
            as_df: If True, return a DataFrame instead of BaseVariable/list.
                   The DataFrame has columns for each metadata key plus 'data'.
            version: Which version(s) to return:
                - "latest" (default): latest version per schema/version-key combination
                - "all": every stored version
                - any other string: specific record_id (single-record fast path)
            where: Optional VariableFilter for additional filtering.
            db: Optional DatabaseManager instance to use instead of the global
                database. Allows one-shot operations against a specific database
                without changing the global default.
            introspect: If True, attach call-context attributes to returned
                BaseVariable instances (.where, .version_mode), or append
                introspection columns to the right of a DataFrame result
                (record_id, branch_params, content_hash, where, version_mode).
            **metadata: Addressing metadata to match (partial matching supported).
                List values are interpreted as "match any" (OR semantics).

        Returns:
            Single BaseVariable when exactly one record matches,
            list of BaseVariable when multiple records match, or
            pandas DataFrame if as_df=True.

        Raises:
            NotFoundError: If no matching data found
            AmbiguousVersionError: If version="latest" and multiple branch-param
                variants exist at the same schema location
            NotRegisteredError: If this variable type is not registered
            DatabaseNotConfiguredError: If no database is available

        Example:
            # Single result — returned directly
            var = StepLength.load(subject=1, session="A")
            print(var.data)

            # Multiple results — returned as list
            vars = StepLength.load(subject=1)
            for v in vars:
                print(v.data)

            # Load as DataFrame
            df = StepLength.load(as_df=True, subject=1)

            # Load every stored version
            versions = StepLength.load(version="all", subject=1, session="A")

            # Inspect internal metadata for a loaded variable
            var = StepLength.load(subject=1, session="A", introspect=True)
            print(var.record_id, var.branch_params, var.where)
        """
        from .database import get_database
        from .exceptions import AmbiguousVersionError, NotFoundError

        _db = db or get_database()

        # Specific record_id fast path
        if version not in ("latest", "all"):
            var = next(_db.load(cls, metadata, version_id=version))
            if introspect:
                var.where = where
                var.version_mode = version
            if as_df:
                return (
                    _build_introspect_df([var], where, version)
                    if introspect
                    else _build_packed_df([var])
                )
            return var

        if as_df:
            if introspect:
                instances = _load_instances(cls, metadata, version, where, _db)
                if not instances:
                    raise NotFoundError(
                        f"No {cls.__name__} found matching metadata: {metadata}"
                    )
                return _build_introspect_df(instances, where, version)
            # No variant kwargs → the pinned default, if one reaches this type
            # (scidb.variant_pins; one owner for what "the default" is).
            from .variant_pins import default_load_args

            schema_keys_set = set(_db.dataset_schema_keys)
            named = {k: v for k, v in metadata.items() if k not in schema_keys_set}
            default_bp, df_version = (
                (None, version)
                if named
                else default_load_args(_db, cls, None, version)
            )
            df = _db.load_all_as_df(
                cls,
                metadata,
                layout="packed",
                include_rid=False,
                version_id=df_version,
                where=where,
                **({"branch_params_filter": default_bp} if default_bp else {}),
            )
            if df.empty:
                raise NotFoundError(
                    f"No {cls.__name__} found matching metadata: {metadata}"
                )
            return df

        # Generator path: "latest" or "all"
        if version == "latest":
            schema_keys_set = set(_db.dataset_schema_keys)
            schema_metadata = {
                k: v for k, v in metadata.items() if k in schema_keys_set
            }
            branch_params_filter = {
                k: v for k, v in metadata.items() if k not in schema_keys_set
            } or None
            # No variant kwargs → the pinned default (scidb.variant_pins).
            from .variant_pins import default_load_args

            branch_params_filter, load_version = default_load_args(
                _db, cls, branch_params_filter, "latest"
            )
            results = list(
                _db.load(
                    cls,
                    schema_metadata,
                    version_id=load_version,
                    where=where,
                    branch_params_filter=branch_params_filter,
                )
            )
        else:  # version == "all"
            results = list(_db.load(cls, metadata, version_id="all", where=where))

        if not results:
            raise NotFoundError(
                f"No {cls.__name__} found matching metadata: {metadata}"
            )

        if version == "latest" and len(results) > 1:
            schema_keys_set = set(_db.dataset_schema_keys)
            first_schema = {
                k: v
                for k, v in (results[0].metadata or {}).items()
                if k in schema_keys_set
            }
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
                    bp = getattr(r, "branch_params", {}) or {}
                    bp_str = ", ".join(f"{k}={v!r}" for k, v in bp.items())
                    lines.append(
                        f"  {bp_str or '(no branch params)'}  "
                        f"(record_id: {r.record_id!r})"
                    )
                raise AmbiguousVersionError("\n".join(lines))

        if introspect:
            for r in results:
                r.where = where
                r.version_mode = version

        return results[0] if len(results) == 1 else results

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
            if hasattr(series.dtype, "kind") and series.dtype.kind in (
                "i",
                "u",
                "f",
                "b",
            ):
                meta_lists[col] = series.tolist()
            else:
                meta_lists[col] = list(series)

        data_series = df[data_column]
        if hasattr(data_series.dtype, "kind") and data_series.dtype.kind in (
            "i",
            "u",
            "f",
            "b",
        ):
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
    def head(cls, n: int = 5, db=None, **metadata) -> pd.DataFrame:
        """
        Peek at the first N records of this variable (latest version).

        Convenience method for quickly inspecting stored data without
        loading everything.

        Args:
            n: Number of records to return. Default is 5.
            db: Optional DatabaseManager instance to use instead of the
                global database.
            **metadata: Optional metadata filters (e.g., subject=1).

        Returns:
            pd.DataFrame with schema key columns and a 'data' column.
            Returns an empty DataFrame if no records exist.

        Example:
            StepLength.head()       # first 5 records
            StepLength.head(10)     # first 10 records
            StepLength.head(3, subject=1)  # first 3 for subject 1
        """
        from .database import get_database

        _db = db or get_database()
        results = list(
            itertools.islice(_db.load(cls, metadata, version_id="latest"), n)
        )
        if not results:
            return pd.DataFrame()
        rows = []
        for var in results:
            row = dict(var.metadata) if var.metadata else {}
            row["data"] = var.data
            rows.append(row)
        return pd.DataFrame(rows)

    @classmethod
    def to_csv(cls, filename: str, *args, **kwargs) -> None:
        """
        Export this variable to a CSV file in flat table format.

        Loads every record matching ``kwargs`` and writes one row per unique
        schema_id: one column per schema key (e.g. ``subject``, ``trial``)
        plus a single value column named after this variable class. A
        trial-level variable produces one row per subject/trial combination;
        a subject-level variable produces one row per subject (no ``trial``
        column).

        The constraint is **one row per schema_id**: a record's data may have
        multiple *columns* (a single-row table writes one column per table
        column), but not multiple *rows*. A record holding a multi-row table,
        or a bare vector (``DOUBLE[]`` / ``DOUBLE[][]``), raises ``ValueError``.

        ``ColumnSelection`` (``MyVar["col"].to_csv(...)``) and ``Merge``
        (``Merge(A, B).to_csv(...)``) are also exportable — see their own
        ``to_csv`` methods.

        Args:
            filename: Required output path. Must end with ``.csv`` or a
                ``ValueError`` is raised.
            **kwargs: Forwarded verbatim to ``load()``, so every ``load()``
                option is supported:
                - ``where=`` for ``scidb`` Filter objects (``ColumnFilter`` /
                  ``VariableFilter``, composed with ``&`` / ``|`` / ``~``),
                - branch-param keyword args for ``Variant`` selection (non-schema
                  keys are routed to the loader's ``branch_params_filter``),
                - ``version=``, ``db=``, and schema-key metadata filters.
                ``as_df`` and ``introspect`` are ignored — ``to_csv`` controls
                its own output shape.

        Raises:
            ValueError: If ``filename`` does not end with ``.csv``, or if any
                matched record holds non-scalar data.
            NotFoundError: If no records match (propagated from ``load()``).

        Example:
            # All trials for subject 1 -> subject,trial,StepLength columns
            StepLength.to_csv("step_lengths.csv", subject=1)

            # With a value filter and a pinned variant
            StepLength.to_csv("clean.csv", where=StepLength != 0, low_hz=20)
        """
        from .csv_export import export_csv

        export_csv(cls, filename, *args, **kwargs)

    def __repr__(self) -> str:
        """Return a string representation of this variable."""
        type_name = type(self).__name__
        if self.record_id:
            return f"{type_name}(record_id={self.record_id[:12]}...)"
        return f"{type_name}(data={type(self.data).__name__})"


# ---------------------------------------------------------------------------
# load() helpers (module-level so they stay out of the class namespace)
# ---------------------------------------------------------------------------


def _load_instances(cls, metadata, version, where, _db):
    """Return a list of BaseVariable instances for the given version/metadata."""
    if version == "latest":
        schema_keys_set = set(_db.dataset_schema_keys)
        schema_metadata = {k: v for k, v in metadata.items() if k in schema_keys_set}
        branch_params_filter = {
            k: v for k, v in metadata.items() if k not in schema_keys_set
        } or None
        from .variant_pins import default_load_args

        branch_params_filter, load_version = default_load_args(
            _db, cls, branch_params_filter, "latest"
        )
        return list(
            _db.load(
                cls,
                schema_metadata,
                version_id=load_version,
                where=where,
                branch_params_filter=branch_params_filter,
            )
        )
    return list(_db.load(cls, metadata, version_id="all", where=where))


def _build_packed_df(instances):
    """Build the standard packed-layout DataFrame from a list of instances."""
    rows = []
    for var in instances:
        row = dict(var.metadata) if var.metadata else {}
        row["data"] = var.data
        rows.append(row)
    return pd.DataFrame(rows)


def _build_introspect_df(instances, where, version):
    """Build a packed DataFrame with introspection columns appended to the right.

    Column order: metadata keys → data → record_id → branch_params →
    content_hash → where → version_mode
    """
    rows = []
    for var in instances:
        row = dict(var.metadata) if var.metadata else {}
        row["data"] = var.data
        rows.append(row)

    df = pd.DataFrame(rows)
    df["record_id"] = [var.record_id for var in instances]
    df["branch_params"] = [var.branch_params or {} for var in instances]
    df["content_hash"] = [var.content_hash for var in instances]
    df["where"] = repr(where) if where is not None else None
    df["version_mode"] = version
    return df


# ---------------------------------------------------------------------------
# Same name, same definition? (portability: identical declarations merge)
# ---------------------------------------------------------------------------

#: What makes two Variable definitions different: how the data is stored.
_CODEC_METHODS = ("to_db", "from_db")


def _own_codec(cls: type, method: str):
    """The function *cls* uses for *method* if a class below BaseVariable
    overrides it, else ``None`` (the default codec)."""
    for klass in cls.__mro__:
        if klass is BaseVariable:
            return None
        if method in vars(klass):
            attr = vars(klass)[method]
            return getattr(attr, "__func__", attr)
    return None


def _code_key(fn) -> tuple:
    """A comparison key for a function's logic, independent of which file
    it was written in: bytecode, names, and constants other than nested
    code objects (whose repr carries a file name)."""
    code = getattr(fn, "__code__", None)
    if code is None:
        return (repr(fn),)
    consts = tuple(c for c in code.co_consts if not hasattr(c, "co_code"))
    return (code.co_code, code.co_names, repr(consts))


def same_definition(a: type, b: type) -> bool:
    """Whether two same-named Variable classes are the same TYPE.

    Same ``schema_version`` and the same codec (``to_db``/``from_db``: both
    the default, or the same code). A class built from an entities TOML
    declaration (``_declared_only``) only says the type EXISTS, so it agrees
    with any definition of that name.
    """
    if getattr(a, "_declared_only", False) or getattr(b, "_declared_only", False):
        return True
    if getattr(a, "schema_version", 1) != getattr(b, "schema_version", 1):
        return False
    for method in _CODEC_METHODS:
        fa, fb = _own_codec(a, method), _own_codec(b, method)
        if fa is None and fb is None:
            continue
        if fa is None or fb is None or _code_key(fa) != _code_key(fb):
            return False
    return True


def _preferred_definition(existing: type, new: type) -> "tuple[type, type]":
    """``(kept, rejected)`` for two conflicting definitions: the project's
    own over a library's (``scidb.names.library_of``), else the newer."""
    if library_of(new) and not library_of(existing):
        return existing, new
    return new, existing
