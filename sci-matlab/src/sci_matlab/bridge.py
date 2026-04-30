"""Python bridge for MATLAB-SciStack integration.

Provides proxy classes that satisfy the duck-typing contracts of
scilineage's LineageFcn, LineageFcnInvocation, and LineageFcnResult classes.
This allows MATLAB functions to participate fully in the lineage / caching
system without any changes to existing Python packages.

The key insight is that every Python function that touches these objects
uses duck-typing (attribute access), not isinstance checks on LineageFcn or
LineageFcnInvocation.  LineageFcnResult *is* instantiated directly from
scilineage, so isinstance checks in save_variable() pass naturally.

Duck-typing contracts satisfied
-------------------------------
MatlabLineageFcn provides:
    .hash            str   (64-char hex, same algorithm as LineageFcn.__init__)
    .fcn.__name__    str   (used by extract_lineage)
    .unpack_output   bool
    .unwrap          bool
    .invocations     tuple

MatlabLineageFcnInvocation provides:
    .fcn             MatlabLineageFcn
    .inputs          dict[str, Any]
    .outputs         tuple
    .unwrap          bool
    .hash            property -> compute_lineage_hash()
    .compute_lineage_hash()  str  (reuses classify_inputs from scilineage)
"""

from hashlib import sha256

from scilineage.inputs import classify_inputs

STRING_REPR_DELIMITER = "-"


def _describe_value(val):
    """Return a short type/shape string for logging."""
    import numpy as np
    import pandas as pd

    t = type(val).__name__
    if isinstance(val, pd.DataFrame):
        return f"DataFrame {val.shape[0]}x{val.shape[1]} cols={list(val.columns)}"
    if isinstance(val, np.ndarray):
        return f"ndarray shape={val.shape} dtype={val.dtype}"
    if isinstance(val, (list, tuple)):
        return f"{t} len={len(val)}"
    if isinstance(val, (int, float, str, bool)):
        return f"{t}"
    return f"{t}"


# ---------------------------------------------------------------------------
# Proxy classes
# ---------------------------------------------------------------------------


class _FunctionProxy:
    """Minimal proxy so that ``inv.fcn.fcn.__name__`` works in extract_lineage."""

    def __init__(self, name: str):
        self.__name__ = name


class MatlabLineageFcn:
    """Proxy for a MATLAB function in the scilineage system.

    Satisfies the same duck-typing contract as ``scilineage.core.LineageFcn``
    for every consumer that reads ``.hash``, ``.fcn.__name__``, etc.

    Parameters
    ----------
    source_hash : str
        SHA-256 hex digest of the MATLAB function source code.
    function_name : str
        Human-readable function name (used in lineage records).
    unpack_output : bool
        Whether the function returns multiple outputs.
    """

    def __init__(
        self,
        source_hash: str,
        function_name: str,
        unpack_output: bool = False,
    ):
        self.fcn = _FunctionProxy(function_name)
        self.unpack_output = unpack_output
        self.unwrap = True
        self.invocations: tuple = ()

        # Same algorithm as LineageFcn.__init__
        string_repr = f"{source_hash}{STRING_REPR_DELIMITER}{unpack_output}"
        self.hash: str = sha256(string_repr.encode()).hexdigest()
        self.generates_file = False


class MatlabLineageFcnInvocation:
    """Proxy for a specific MATLAB function invocation.

    Satisfies the same duck-typing contract as
    ``scilineage.core.LineageFcnInvocation``. Reuses ``classify_inputs``
    and the lineage-hash algorithm from scilineage so that cache lookups
    and lineage extraction work unchanged.

    Parameters
    ----------
    matlab_lineage_fcn : MatlabLineageFcn
        The parent lineage function (function identity).
    inputs : dict
        Mapping of argument names (``"arg_0"``, ``"arg_1"``, ...) to
        Python-side values (BaseVariable instances, LineageFcnResults, or
        plain scalars/arrays).
    """

    def __init__(self, matlab_lineage_fcn: MatlabLineageFcn, inputs: dict):
        self.fcn = matlab_lineage_fcn
        self.inputs: dict = dict(inputs)
        self.outputs: tuple = ()
        self.unwrap = True

    def compute_lineage_hash(self) -> str:
        """Compute lineage hash — identical algorithm to LineageFcnInvocation."""
        classified = classify_inputs(self.inputs)
        input_tuples = [c.to_cache_tuple() for c in classified]
        hash_input = f"{self.fcn.hash}{STRING_REPR_DELIMITER}{input_tuples}"
        return sha256(hash_input.encode()).hexdigest()

    @property
    def hash(self) -> str:
        return self.compute_lineage_hash()


# ---------------------------------------------------------------------------
# Helper functions called from MATLAB
# ---------------------------------------------------------------------------


def split_flat_to_lists(flat_array, lengths):
    """Split a flat numpy array into a list of Python lists by lengths.

    Used by MATLAB's to_python cell-column fast path.  Instead of
    N separate MATLAB→Python bridge crossings (one per cell element),
    MATLAB concatenates all cell elements into one flat array, records
    their lengths, and sends both in a single crossing.  This function
    splits the flat array back into per-element Python lists.

    Parameters
    ----------
    flat_array : numpy.ndarray
        1-D array of concatenated values.
    lengths : numpy.ndarray
        1-D integer array where ``lengths[i]`` is the number of elements
        belonging to sub-list *i*.

    Returns
    -------
    list of list
        One Python list per entry in *lengths*, each containing native
        Python scalars (float, int, bool).
    """
    result = []
    pos = 0
    for length in lengths.tolist():
        result.append(flat_array[pos:pos + length].tolist())
        pos += length
    return result


def check_cache(invocation: MatlabLineageFcnInvocation):
    """Check if a computation is already cached.

    Returns
    -------
    list or None
        List of cached output values (raw data), or None on miss.
    """
    from scilineage.backend import _get_backend

    _backend = _get_backend()
    if _backend is not None:
        try:
            return _backend.find_by_lineage(invocation)
        except Exception:
            pass
    return None


def make_lineage_fcn_result(invocation: MatlabLineageFcnInvocation, output_num: int, data):
    """Create a real LineageFcnResult backed by a MatlabLineageFcnInvocation.

    The returned object is a genuine ``scilineage.core.LineageFcnResult``
    instance, so ``isinstance`` checks in ``save_variable`` pass.
    """
    from scilineage.core import LineageFcnResult

    return LineageFcnResult(invocation, output_num, True, data)


def register_matlab_variable(type_name: str, schema_version: int = 1):
    """Create a Python surrogate BaseVariable subclass for a MATLAB type.

    The surrogate is auto-registered in ``BaseVariable._all_subclasses``
    via ``__init_subclass__`` and, if a database is configured, registered
    with the ``DatabaseManager`` as well.

    Returns the surrogate class.
    """
    from scidb.variable import BaseVariable

    existing = BaseVariable.get_subclass_by_name(type_name)
    if existing is not None:
        return existing

    surrogate = type(type_name, (BaseVariable,), {"schema_version": schema_version})

    try:
        from scidb.database import get_database
        get_database().register(surrogate)
    except Exception:
        pass  # Database not yet configured; will register on configure_database

    return surrogate


def for_each_batch_save(type_name, data_list, metadata_list, db=None):
    """Batch save for for_each parallel results.

    Each element in data_list is a single data value (already converted via
    to_python on the MATLAB side).  Each element in metadata_list is a Python
    dict of metadata key-value pairs.

    Parameters
    ----------
    type_name : str
        Variable class name (e.g. "ProcessedSignal").
    data_list : list
        One data value per result row.
    metadata_list : list of dict
        One metadata dict per result row.
    db : DatabaseManager or None
        Optional database; uses global default when None.

    Returns
    -------
    str
        Newline-joined record IDs.
    """
    import time as _time
    from scidb.log import Log
    from scidb.variable import BaseVariable
    from scidb.database import get_database

    cls = BaseVariable.get_subclass_by_name(type_name)
    if cls is None:
        raise ValueError(
            f"Variable type '{type_name}' is not registered. "
            f"Call scidb.register_variable('{type_name}') first."
        )

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()

    t_start = _time.perf_counter()

    data_items = []
    for i in range(len(data_list)):
        data_items.append((data_list[i], dict(metadata_list[i])))

    t_convert = _time.perf_counter()
    n_items = len(data_items)
    Log.debug(f"for_each_batch_save({type_name}): {n_items} items, "
              f"data_items construction {t_convert - t_start:.3f}s")

    if n_items > 0:
        Log.info(f"for_each_batch_save({type_name}): {n_items} items, "
                 f"Python data: {_describe_value(data_items[0][0])}")

    result = "\n".join(_db.save_batch(cls, data_items))

    Log.info(f"for_each_batch_save({type_name}): {n_items} items, "
             f"total {_time.perf_counter() - t_start:.3f}s")
    return result


def for_each_batch_save_dataframe(type_name, dataframe, row_counts, meta_keys, meta_columns, common_metadata=None, db=None):
    """Batch save for for_each when outputs are DataFrames.

    Accepts a single concatenated DataFrame (all output tables vertcat'd),
    a row_counts array indicating how many rows belong to each item, columnar
    metadata (one list/array per metadata key), and common metadata applied to
    every item.  Splits the DataFrame, assembles (sub_df, metadata_dict) tuples,
    and calls save_batch once.

    This avoids per-row MATLAB-to-Python bridge crossings for to_python() and
    metadata_to_pydict(), replacing ~N*K crossings with a small constant number.

    Parameters
    ----------
    type_name : str
        Variable class name (e.g. "ProcessedSignal").
    dataframe : pandas.DataFrame
        Single concatenated DataFrame with all rows.
    row_counts : list or numpy array
        Number of rows per item (len = number of items to save).
    meta_keys : list of str
        Metadata column names, same order as meta_columns.
    meta_columns : list of (list or numpy array or str)
        One inner list/array per metadata key, each with one value per item.
        Strings are record-separator (\\x1e) delimited.
    common_metadata : dict or None
        Extra metadata applied to every item (e.g. config_nv + constant_nv).
    db : DatabaseManager or None
        Optional database; uses global default when None.

    Returns
    -------
    str
        Newline-joined record IDs.
    """
    import time as _time
    from scidb.log import Log
    from scidb.variable import BaseVariable
    from scidb.database import get_database

    cls = BaseVariable.get_subclass_by_name(type_name)
    if cls is None:
        raise ValueError(
            f"Variable type '{type_name}' is not registered. "
            f"Call scidb.register_variable('{type_name}') first."
        )

    t_start = _time.perf_counter()

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()
    common = dict(common_metadata) if common_metadata else {}
    keys = list(meta_keys)

    # Convert row_counts to a Python list
    if hasattr(row_counts, 'tolist'):
        rc = row_counts.tolist()
    else:
        rc = [int(x) for x in row_counts]

    n = len(rc)

    if n == 0:
        Log.info(f"for_each_batch_save_dataframe({type_name}): 0 items, nothing to save")
        return ""

    # Convert columnar metadata (same pattern as save_batch_bridge)
    meta_lists = []
    for j in range(len(keys)):
        col = meta_columns[j]
        if isinstance(col, str):
            # Joined string from MATLAB (record-separator delimited)
            meta_lists.append(col.split('\x1e'))
        elif hasattr(col, 'tolist'):
            meta_lists.append(col.tolist())
        else:
            meta_lists.append([v.item() if hasattr(v, 'item') else v for v in col])

    # Split the concatenated DataFrame into per-item DataFrames
    t_split = _time.perf_counter()
    offsets = []
    pos = 0
    for count in rc:
        offsets.append(pos)
        pos += count

    data_items = []
    for i in range(n):
        start = offsets[i]
        end = start + rc[i]
        sub_df = dataframe.iloc[start:end].reset_index(drop=True)

        meta = dict(common)
        for j, key in enumerate(keys):
            meta[key] = meta_lists[j][i]

        data_items.append((sub_df, meta))

    t_convert = _time.perf_counter()
    Log.debug(f"for_each_batch_save_dataframe({type_name}): {n} items, "
              f"split+meta {t_convert - t_split:.3f}s, "
              f"total prep {t_convert - t_start:.3f}s")

    if n > 0:
        Log.info(f"for_each_batch_save_dataframe({type_name}): {n} items, "
                 f"DataFrame {dataframe.shape[0]}x{dataframe.shape[1]}")

    result = "\n".join(_db.save_batch(cls, data_items))

    Log.info(f"for_each_batch_save_dataframe({type_name}): {n} items, "
             f"total {_time.perf_counter() - t_start:.3f}s")
    return result


def save_batch_bridge(type_name, data_values, metadata_keys, metadata_columns, common_metadata=None, db=None):
    """Bridge function for MATLAB save_from_table.

    Accepts columnar data (one list per column) from MATLAB and assembles
    the (data_value, metadata_dict) tuples that DatabaseManager.save_batch()
    expects.  This avoids per-row MATLAB↔Python round-trips.

    Parameters
    ----------
    type_name : str
        Variable class name (e.g. "StepLength").
    data_values : list or numpy array
        One data value per row.
    metadata_keys : list of str
        Metadata column names, same order as metadata_columns.
    metadata_columns : list of (list or numpy array)
        One inner list/array per metadata key, each with one value per row.
    common_metadata : dict or None
        Extra metadata applied to every row.
    db : DatabaseManager or None
        Optional database; uses global default when None.

    Returns
    -------
    list of str
        Record IDs for each saved row.
    """
    import time as _time
    from scidb.log import Log
    from scidb.variable import BaseVariable
    from scidb.database import get_database

    cls = BaseVariable.get_subclass_by_name(type_name)
    if cls is None:
        raise ValueError(
            f"Variable type '{type_name}' is not registered. "
            f"Call scidb.register_variable('{type_name}') first."
        )

    t_start = _time.perf_counter()

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()
    common = dict(common_metadata) if common_metadata else {}
    keys = list(metadata_keys)

    # Bulk-convert numpy arrays to native Python lists (one call instead of
    # N per-element .item() calls).  Plain Python lists pass through unchanged.
    if hasattr(data_values, 'tolist'):
        data_list = data_values.tolist()
    else:
        data_list = [v.item() if hasattr(v, 'item') else v for v in data_values]

    meta_lists = []
    for j in range(len(keys)):
        col = metadata_columns[j]
        if isinstance(col, str):
            # Joined string from MATLAB (record-separator delimited)
            meta_lists.append(col.split('\x1e'))
        elif hasattr(col, 'tolist'):
            meta_lists.append(col.tolist())
        else:
            meta_lists.append([v.item() if hasattr(v, 'item') else v for v in col])

    n = len(data_list)
    data_items = []
    for i in range(n):
        meta = dict(common)
        for j, key in enumerate(keys):
            meta[key] = meta_lists[j][i]
        data_items.append((data_list[i], meta))

    t_convert = _time.perf_counter()
    Log.debug(f"save_batch_bridge({type_name}): {n} items, "
              f"data_items construction {t_convert - t_start:.3f}s")

    result = "\n".join(_db.save_batch(cls, data_items))

    Log.info(f"save_batch_bridge({type_name}): {n} items, "
             f"total {_time.perf_counter() - t_start:.3f}s")
    return result


# ---------------------------------------------------------------------------
# Batch cache — keeps data/py_vars in Python so they never cross to MATLAB's
# proxy layer.  MATLAB accesses individual items via get_batch_item().
# ---------------------------------------------------------------------------

_batch_cache = {}
_batch_id_counter = 0


def _cache_batch(data_list, py_vars_list):
    """Store data and py_vars lists server-side, return an integer handle."""
    global _batch_id_counter
    bid = _batch_id_counter
    _batch_id_counter += 1
    _batch_cache[bid] = (data_list, py_vars_list)
    return bid


def get_batch_item(batch_id, index):
    """Return (data, py_var) for one element from a cached batch."""
    data_list, py_vars_list = _batch_cache[int(batch_id)]
    i = int(index)
    return data_list[i], py_vars_list[i]


def get_batch_data_item(batch_id, index):
    """Return just the data for one element from a cached batch."""
    data_list, _ = _batch_cache[int(batch_id)]
    return data_list[int(index)]


def free_batch(batch_id):
    """Release a cached batch."""
    _batch_cache.pop(int(batch_id), None)


def wrap_batch_bridge(py_vars_list):
    """Extract all fields from a list of BaseVariables into bulk format.

    Scalar fields are packed into newline-joined strings and metadata into
    a single JSON string.  The ``py_vars`` list is returned directly so
    MATLAB can convert it to a cell array in one call.  When all data
    values are scalars (int/float), they are packed into a numpy array
    (``scalar_data``) for single-crossing transfer; otherwise data is
    stored in a Python-side cache for per-item access.

    Parameters
    ----------
    py_vars_list : list of BaseVariable
        Python BaseVariable instances to extract.

    Returns
    -------
    dict with keys:
        n              : int
        py_vars        : list  — BaseVariable objects for MATLAB cell() conversion
        batch_id       : int   — handle for get_batch_data_item (non-scalar only)
        record_ids     : str   — newline-joined
        content_hashes : str   — newline-joined
        lineage_hashes : str   — newline-joined ('' for None)
        json_meta      : str   — JSON array of metadata dicts
        scalar_data    : numpy.ndarray (optional) — present when all data are scalars
    """
    import json
    import numpy as np

    py_vars = list(py_vars_list) if not isinstance(py_vars_list, list) else py_vars_list
    n = len(py_vars)

    record_ids = []
    content_hashes = []
    lineage_hashes = []
    meta_dicts = []
    data = []

    for v in py_vars:
        record_ids.append(v.record_id or '')
        content_hashes.append(v.content_hash or '')
        lh = v.lineage_hash
        lineage_hashes.append(lh if lh is not None else '')
        meta = v.metadata
        meta_dicts.append(dict(meta) if meta is not None else {})
        data.append(v.data)

    # Cache data for non-scalar fallback access
    batch_id = _cache_batch(data, py_vars)

    result = {
        'n': n,
        'py_vars': py_vars,
        'batch_id': batch_id,
        'record_ids': '\n'.join(record_ids),
        'content_hashes': '\n'.join(content_hashes),
        'lineage_hashes': '\n'.join(lineage_hashes),
        'json_meta': json.dumps(meta_dicts),
    }

    # Scalar fast path: pack all data into a single numpy array
    if n > 0 and all(isinstance(d, (int, float)) for d in data):
        result['scalar_data'] = np.array(data, dtype=float)

    # DataFrame fast path: concatenate same-schema DataFrames into one
    # so MATLAB converts a single large table instead of N small ones
    if n > 0 and 'scalar_data' not in result:
        import pandas as pd
        if all(isinstance(d, pd.DataFrame) for d in data):
            first_cols = list(data[0].columns)
            if all(list(d.columns) == first_cols for d in data):
                row_counts = [len(d) for d in data]
                concat_df = pd.concat(data, ignore_index=True)
                result['concat_df'] = concat_df
                result['concat_df_row_counts'] = np.array(row_counts, dtype=np.int64)

    return result


def load_and_extract(py_class, metadata_dict, version_id='latest', db=None, where=None):
    """Load all matching variables and extract fields in bulk.

    Combines load_all -> list -> wrap_batch_bridge in one Python call.
    The intermediate BaseVariable list and data arrays stay in Python
    (accessed later via get_batch_item).  Only lightweight strings/JSON
    cross back to MATLAB.

    Parameters
    ----------
    py_class : type
        BaseVariable subclass to load.
    metadata_dict : dict
        Metadata filter (values can be lists for "match any").
    version_id : str or int
        Version filter ('latest', 'all', or an integer).
    db : DatabaseManager or None
        Optional database; uses global default when None.
    where : Filter or None
        Optional where= filter (scidb.filters.Filter instance).

    Returns
    -------
    dict
        Same format as wrap_batch_bridge (with batch_id, no data/py_vars).
    """
    from scidb.database import get_database

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()

    gen = _db.load_all(py_class, dict(metadata_dict), version_id=version_id, where=where)
    py_vars = list(gen)  # materializes entirely in Python
    return wrap_batch_bridge(py_vars)


def get_surrogate_class(type_name: str):
    """Retrieve the Python surrogate class for a MATLAB variable type.

    Raises ValueError if not registered.
    """
    from scidb.variable import BaseVariable

    cls = BaseVariable.get_subclass_by_name(type_name)
    if cls is None:
        raise ValueError(
            f"MATLAB variable type '{type_name}' is not registered. "
            f"Call scidb.register_variable('{type_name}') first."
        )
    return cls


def get_data_column_name(py_class, db=None):
    """Resolve the single data column name for a variable type.

    Used by MATLAB's scidb.ColName to resolve column names via the
    Python bridge.

    Parameters
    ----------
    py_class : type
        BaseVariable subclass to query.
    db : DatabaseManager or None
        Optional database; uses global default when None.

    Returns
    -------
    str
        The single data column name.

    Raises
    ------
    ValueError
        If the variable has 0 or 2+ data columns.
    """
    import json
    from scidb.database import get_database

    _db = db if db is not None and not isinstance(db, type(None)) else get_database()
    var_name = py_class.__name__
    schema_keys = list(_db.dataset_schema_keys)

    row = _db._execute(
        "SELECT dtype FROM _variables WHERE variable_name = ?",
        [var_name],
    ).fetchone()

    if row is None:
        # Variable not yet saved — fall back to view_name
        if hasattr(py_class, 'view_name'):
            return py_class.view_name()
        return var_name

    dtype_meta = json.loads(row[0])
    mode = dtype_meta.get("mode", "single_column")

    if mode == "single_column":
        col_names = list(dtype_meta.get("columns", {}).keys())
        if col_names:
            return col_names[0]
        if hasattr(py_class, 'view_name'):
            return py_class.view_name()
        return var_name

    if mode == "dataframe":
        df_columns = dtype_meta.get("df_columns", list(dtype_meta.get("columns", {}).keys()))
        data_cols = [c for c in df_columns if c not in schema_keys]
        if len(data_cols) == 1:
            return data_cols[0]
        elif len(data_cols) == 0:
            raise ValueError(
                f"ColName({var_name}): variable has no data columns "
                f"(all columns are schema keys). "
                f"Columns: {df_columns}, schema keys: {schema_keys}"
            )
        else:
            raise ValueError(
                f"ColName({var_name}): variable has {len(data_cols)} "
                f"data columns ({data_cols}), expected exactly 1. "
                f"Schema keys: {schema_keys}"
            )

    if mode == "multi_column":
        raise ValueError(
            f"ColName({var_name}): not supported for dict-type (multi_column) variables."
        )

    if hasattr(py_class, 'view_name'):
        return py_class.view_name()
    return var_name
