"""Conversion of values between MATLAB and Python.

Numpy buffers MATLAB can read in one copy (``ndarray_to_buffer``), flat
arrays split back into lists or frames, datetimes with their zone, nested
dicts and sequences in the shapes ``from_python.m`` / ``to_python.m`` expect,
and the small type probes MATLAB asks before converting (``is_dataframe``,
``is_datetime_value``, ``numpy_scalar_to_python``). No scidb concepts.
"""

import sys

from .crossing import matlab_entry


def split_flat_to_lists(flat_array, lengths):
    """Split a flat numpy array into a list of Python lists by lengths.

    Used by MATLAB's to_python cell-column fast path.  Instead of
    N separate MATLAB→Python bridge crossings (one per cell element),
    MATLAB concatenates all cell elements into one flat array, records
    their lengths, and sends both in a single crossing.  This function
    splits the flat array back into per-element Python lists.

    MATLAB's Python bridge preserves the 2-D shape of MATLAB row
    vectors for some dtypes (notably logical) when calling
    ``py.numpy.array(...)``, while flattening for others (double).
    We ravel both inputs defensively so this routine is robust to a
    1-D or 2-D incoming shape and a future bridge change can't break
    the cell-column round trip silently.

    Parameters
    ----------
    flat_array : numpy.ndarray
        Concatenated values (any shape; raveled here).
    lengths : numpy.ndarray
        Integer array where ``lengths[i]`` is the number of elements
        belonging to sub-list *i* (any shape; raveled here).

    Returns
    -------
    list of list
        One Python list per entry in *lengths*, each containing native
        Python scalars (float, int, bool).
    """
    import numpy as np

    flat = np.asarray(flat_array).ravel()
    lens = np.asarray(lengths).ravel().tolist()
    result = []
    pos = 0
    for length in lens:
        result.append(flat[pos : pos + length].tolist())
        pos += length
    return result


def split_df_to_dataframes(df, row_counts):
    """Split a concatenated DataFrame into a list of sub-DataFrames by row counts.

    Used by MATLAB's to_python Strategy 1 (homogeneous table concat) to
    efficiently convert a cell column whose elements are multi-row tables.
    MATLAB concatenates all K tables into one DataFrame (KM rows total),
    sends it across the bridge once, then calls this function to slice it
    back into K DataFrames each with the original number of rows.

    This avoids the per-element bridge cost of Strategy 3 (K separate
    to_python calls) while correctly handling tables with any row count —
    unlike the old to_dict('records') approach which always produced
    single-row DataFrames.

    Parameters
    ----------
    df : pandas.DataFrame
        Concatenated DataFrame (KM rows).
    row_counts : numpy.ndarray or list of int
        Integer array where ``row_counts[i]`` is the row count of the
        i-th original sub-DataFrame.

    Returns
    -------
    list of pandas.DataFrame
        One DataFrame per entry in *row_counts*, sliced from *df* in order.
    """
    import numpy as np

    counts = np.asarray(row_counts).ravel().tolist()
    result = []
    pos = 0
    for n in counts:
        n = int(n)
        result.append(df.iloc[pos : pos + n].reset_index(drop=True))
        pos += n
    return result


# ---------------------------------------------------------------------------
# Bulk array transfer
# ---------------------------------------------------------------------------

# numpy (dtype.kind, itemsize) -> MATLAB class accepted by typecast().
# Deliberately partial: every dtype NOT listed here has no fixed-width raw
# buffer MATLAB can reinterpret (object, str, bytes, datetime, timedelta,
# complex) or no typecast target (float16, float128), and must keep the
# element-by-element path in +scidb/+internal/from_python.m.
_BUFFER_DTYPES = {
    ("f", 4): "single",
    ("f", 8): "double",
    ("i", 1): "int8",
    ("i", 2): "int16",
    ("i", 4): "int32",
    ("i", 8): "int64",
    ("u", 1): "uint8",
    ("u", 2): "uint16",
    ("u", 4): "uint32",
    ("u", 8): "uint64",
    ("b", 1): "logical",
}


def ndarray_to_buffer(arr):
    """Describe a numpy array as ONE raw byte buffer for MATLAB.

    MATLAB's ``from_python`` used to convert every numpy array via
    ``ndarray.tolist()`` + ``cell(py_list)``, which crosses the Python/MATLAB
    boundary once *per element*. At scale that is the whole cost: a 419-record
    EMG variable spread over 10 array columns is 1.74e8 samples, i.e. 1.74e8
    crossings, which reads as a hang rather than as slowness.

    A raw buffer crosses once. MATLAB converts ``py.bytes`` to ``uint8`` in a
    single memcpy and ``typecast``s it back to the original class, so the
    element count stops mattering.

    The buffer is written in **Fortran (column-major) order** so MATLAB can
    ``reshape(vec, shape)`` directly — MATLAB is column-major, numpy is not.
    For the 1-D case (the common one here) the two orders coincide.

    Parameters
    ----------
    arr : numpy.ndarray
        Array to describe. Anything ``numpy.asarray`` accepts also works.

    Returns
    -------
    dict
        ``ok`` (bool), and when ok:
        ``buffer`` (bytes, Fortran order), ``matlab_class`` (str, a typecast
        target or ``"logical"``), ``count`` (int, element count),
        ``itemsize`` (int, bytes per element), ``reason`` (str, empty).
        When not ok, ``reason`` says why and the caller falls back to the
        element-by-element path. Declining is never an error — every dtype
        this refuses still converts correctly, just slowly.

        ``matlab_class`` names the dtype's *typecast* target, not the class
        the user finally sees: MATLAB casts every numeric result to double
        afterwards, because that is what the element-by-element path
        returned and what TestDataRoundTrip pins. Only ``logical`` survives
        as itself.

    Notes
    -----
    ``tobytes`` copies, so this transiently doubles the array's memory in
    Python. That is the price of a single crossing and is still far cheaper
    than the MATLAB-side cell array the old path built (one ~112-byte mxArray
    per element).
    """
    import numpy as np

    empty = {
        "ok": False,
        "buffer": b"",
        "matlab_class": "",
        "count": 0,
        "itemsize": 0,
        "reason": "",
    }

    try:
        a = np.asarray(arr)
    except Exception as exc:  # not array-like at all
        return {**empty, "reason": f"not array-like: {type(exc).__name__}"}

    key = (a.dtype.kind, int(a.dtype.itemsize))
    matlab_class = _BUFFER_DTYPES.get(key)
    if matlab_class is None:
        return {**empty, "reason": f"dtype {a.dtype!s} has no raw-buffer form"}

    try:
        buf = a.tobytes(order="F")
    except Exception as exc:  # e.g. MemoryError on a very large array
        return {**empty, "reason": f"tobytes failed: {type(exc).__name__}"}

    return {
        "ok": True,
        "buffer": buf,
        "matlab_class": matlab_class,
        "count": int(a.size),
        "itemsize": int(a.dtype.itemsize),
        "reason": "",
    }


# ---------------------------------------------------------------------------
# Datetimes (docs/claude/datetime-roundtrip.md, "MATLAB")
# ---------------------------------------------------------------------------
#
# A MATLAB datetime array crosses as int64 nanoseconds from 1970-01-01
# (`convertTo(..., 'epochtime', 'TicksPerSecond', 1e9)`: exact, no double
# rounding) plus a NaT mask and its ONE TimeZone text. Unzoned ('') = naive:
# the ticks are the wall clock. Zoned: the ticks are the UTC instant. The
# zone label is scicanonicalhash's, so a MATLAB value and its Python twin
# hash alike and a MATLAB re-save of a loaded value is "0 new rows".

_MATLAB_OFFSET_RE = None


def matlab_zone_label(tz) -> "str | None":
    """A MATLAB ``datetime.TimeZone`` text as a scistack zone label.

    ``''`` -> ``None`` (unzoned = naive); ``'UTC'``/``'Z'``/
    ``'UTCLeapSeconds'`` -> ``"UTC"`` (MATLAB has already converted a leap-
    second value by then); ``'+0200'``/``'+02'``/``'+02:00'`` -> ``"+02:00"``;
    an IANA name stays itself. Raises ``ValueError`` naming the text when no
    time-zone database knows it.
    """
    import re

    from scicanonicalhash import zone_from_label

    global _MATLAB_OFFSET_RE
    if _MATLAB_OFFSET_RE is None:
        _MATLAB_OFFSET_RE = re.compile(r"^([+-])(\d{2}):?(\d{2})?$")
    text = str(tz or "").strip()
    if not text:
        return None
    if text in ("UTC", "Z", "UTCLeapSeconds"):
        return "UTC"
    m = _MATLAB_OFFSET_RE.match(text)
    if m:
        sign, hh, mm = m.groups()
        label = f"{sign}{hh}:{mm or '00'}"
        return "UTC" if label in ("+00:00", "-00:00") else label
    zone_from_label(text)  # validates; raises ValueError naming it
    return text


@matlab_entry
def datetimes_from_matlab(ns, nat, tz, kind="scalar"):
    """MATLAB datetimes -> pandas values.

    ``ns``: int64 ticks (ns from 1970-01-01; wall clock when ``tz`` is '',
    else the UTC instant), ``nat``: NaT mask, ``tz``: the array's TimeZone.
    ``kind``: ``"scalar"`` -> one ``pd.Timestamp`` / ``pd.NaT`` (MATLAB receives
    it boxed, :mod:`.crossing`);
    ``"column"`` -> a ``datetime64[ns(, zone)]`` Series (a table column);
    ``"list"`` -> a list of Timestamps (one record per element,
    ``save_from_table``'s data column).
    """
    import numpy as np
    import pandas as pd
    from scicanonicalhash import zone_from_label

    label = matlab_zone_label(tz)
    ticks = np.asarray(ns, dtype=np.int64).ravel()
    mask = np.asarray(nat, dtype=bool).ravel()
    values = ticks.view("datetime64[ns]").copy()
    if mask.size:
        values[mask] = np.datetime64("NaT")
    series = pd.Series(values)
    if label is not None:
        series = series.dt.tz_localize("UTC").dt.tz_convert(zone_from_label(label))
    from scidb.model.log import Log

    Log.debug(
        f"[bridge] datetimes_from_matlab: {len(series)} value(s), "
        f"zone={label or 'naive'} (MATLAB TimeZone {str(tz or '')!r}), "
        f"{int(mask.sum())} NaT, kind={kind}"
    )
    if kind == "scalar":
        if len(series) != 1:
            raise ValueError(f"datetimes_from_matlab: kind='scalar' got {len(series)} values")
        return series.iloc[0]
    if kind == "list":
        return series.tolist()
    return series


@matlab_entry
def datetimes_to_matlab(obj) -> dict:
    """pandas / Python datetimes -> what MATLAB rebuilds a datetime array from.

    ``obj``: a datetime scalar (``datetime``, ``pd.Timestamp``, NaT,
    ``np.datetime64``), a datetime64 Series (naive or tz-aware), or a
    sequence of datetime scalars. Returns ``{"ok", "ns" (int64 ndarray),
    "ns_bytes" (the same ticks as little-endian int64 bytes), "nat" (bool
    ndarray), "tz" (MATLAB TimeZone text, '' = unzoned), "scalar" (bool),
    "reason"}``. MATLAB reads ``ns_bytes`` (``typecast``): a numeric array
    through ``from_python`` becomes double, which cannot hold ~1.8e18 ns
    exactly. ``ok`` is False (with ``reason``) when the values hold more than
    one zone, or naive and aware mixed: a MATLAB datetime array has one
    TimeZone, so the caller converts those one at a time.
    """
    import numpy as np
    import pandas as pd
    from scicanonicalhash import is_datetime_value, is_missing_datetime, utc_ns, zone_label

    def result(ns, nat, label, scalar, reason=""):
        ns = np.asarray(ns, dtype=np.int64)
        return {"ok": not reason, "ns": ns, "ns_bytes": ns.astype("<i8").tobytes(),
                "nat": np.asarray(nat, dtype=bool), "tz": label or "",
                "scalar": bool(scalar), "reason": reason}

    scalar = is_datetime_value(obj)
    if isinstance(obj, pd.Series) and pd.api.types.is_datetime64_any_dtype(obj.dtype):
        tz = obj.dt.tz
        nat = obj.isna().to_numpy()
        inst = obj.dt.tz_convert("UTC").dt.tz_localize(None) if tz is not None else obj
        ns = inst.astype("datetime64[ns]").to_numpy().view(np.int64).copy()
        ns[nat] = 0
        label = zone_label(pd.Timestamp(0, tz=tz)) if tz is not None else None
        return result(ns, nat, label, False)

    values = [obj] if scalar else list(obj)
    labels, ns, nat = set(), [], []
    for v in values:
        if isinstance(v, np.datetime64):
            v = pd.Timestamp(v)
        if v is None or is_missing_datetime(v) or (isinstance(v, float) and v != v):
            ns.append(0)
            nat.append(True)
            continue
        if not is_datetime_value(v):
            return result([], [], None, False, f"not a datetime: {type(v).__name__}")
        labels.add(zone_label(v))
        ns.append(utc_ns(v))
        nat.append(False)
    if len(labels) > 1:
        return result([], [], None, False,
                      f"values hold {len(labels)} zones ({sorted(map(str, labels))}); "
                      f"a MATLAB datetime array has one")
    return result(ns, nat, next(iter(labels), None), scalar)


@matlab_entry
def durations_to_matlab(obj) -> dict:
    """One timedelta scalar (``timedelta``, ``pd.Timedelta``,
    ``np.timedelta64``, NaT) -> ``{"seconds": float, "nat": bool}`` for
    ``+scidb/+internal/duration_from_python.m``. MATLAB's ``duration`` is a
    double, so seconds as a float lose nothing it could hold. NaT ->
    ``seconds`` NaN, ``nat`` True. MATLAB hands over the box
    (``DurationCrossing``); :func:`matlab_entry` unboxes it."""
    import datetime as _dt
    import math

    np = sys.modules.get("numpy")
    value = obj
    if np is not None and isinstance(value, np.timedelta64):
        if np.isnat(value):
            return {"seconds": math.nan, "nat": True}
        return {"seconds": float(value / np.timedelta64(1, "ns")) / 1e9, "nat": False}
    if value != value:  # pd.NaT (also a timedelta-NaT)
        return {"seconds": math.nan, "nat": True}
    if not isinstance(value, _dt.timedelta):
        raise TypeError(f"durations_to_matlab: not a timedelta: {type(obj).__name__}")
    pd = sys.modules.get("pandas")
    if pd is not None and isinstance(value, pd.Timedelta):
        seconds = pd.Timedelta(value).value / 1e9  # ns-exact
    else:
        seconds = value.total_seconds()
    return {"seconds": float(seconds), "nat": False}


def flatten_sequences(py_list):
    """Flatten a list of numeric/boolean sequences into a single array with lengths.

    Used by MATLAB's from_python to handle object-dtype columns containing
    lists of varying-length arrays (numeric or boolean). Flattens all sequences
    into one numpy array and records their lengths, enabling a SINGLE Python→MATLAB
    bridge crossing instead of N crossings.

    Parameters
    ----------
    py_list : list
        List of sequences (lists, tuples, or numpy arrays) containing
        homogeneous typed values (numeric or boolean).  Sequences may have
        different lengths.

    Returns
    -------
    tuple of (numpy.ndarray, numpy.ndarray) or (None, None)
        - flat_array: Concatenated array (all sequences flattened)
        - lengths: int64 array where lengths[i] = length of sequence i
        Returns (None, None) if conversion failed (mixed types, non-numeric/bool, etc.)

    Examples
    --------
    >>> flatten_sequences([[1, 2, 3], [4, 5], [6, 7, 8, 9]])
    (array([1, 2, 3, 4, 5, 6, 7, 8, 9]), array([3, 2, 4]))

    >>> flatten_sequences([[True, False], [True], [False, False, True]])
    (array([True, False, True, False, False, True]), array([2, 1, 3]))
    """
    import numpy as np

    if not isinstance(py_list, list) or len(py_list) == 0:
        return None, None

    try:
        # Check if all elements are sequences and convert to arrays
        arrays = []
        for elem in py_list:
            if not isinstance(elem, (list, tuple, np.ndarray)):
                return None, None
            arr = np.asarray(elem)
            # Reject if not typed (object dtype = heterogeneous or non-numeric/bool)
            if arr.dtype.kind == "O":
                return None, None
            # Reject anything that is not a flat sequence. The contract here is
            # "list of variable-length numeric SEQUENCES", and the caller splits
            # the concatenated result back apart with `lengths` — which only
            # describes a 1-D element. For a 2-D element `len(arr)` is its ROW
            # count, so a single 4x3 matrix reports length 4 and MATLAB slices
            # the first 4 elements of a 12-element buffer: a 4x1 column where a
            # 4x3 matrix belongs, silently. Decline and let the element-by-
            # element path convert each array with its own shape intact.
            if arr.ndim != 1:
                return None, None
            arrays.append(arr)

        # Record lengths and concatenate
        lengths = np.array([len(arr) for arr in arrays], dtype="int64")
        flat = np.concatenate(arrays)

        return flat, lengths

    except Exception:
        return None, None


def convert_nested_dicts_arrays_to_lists(py_list):
    """Convert all numpy arrays in nested dicts to Python lists.

    Used by MATLAB's from_python to handle lists of nested dicts containing
    numpy arrays (e.g., structs stored as JSON in DuckDB). Recursively walks
    the structure and converts all numpy arrays to Python lists, avoiding
    expensive numpy→MATLAB bridge crossings for each array.

    Parameters
    ----------
    py_list : list
        List of dicts with potentially nested structure. Leaf values should
        be numpy arrays. Works with arbitrary nesting depth.

    Returns
    -------
    list or None
        Modified list where all numpy arrays have been converted to Python
        lists. Structure and nesting are preserved. Returns None if input
        is not a list of dicts.

    Examples
    --------
    >>> data = [{'a': np.array([1, 2]), 'b': {'c': np.array([3, 4])}}]
    >>> convert_nested_dicts_arrays_to_lists(data)
    [{'a': [1, 2], 'b': {'c': [3, 4]}}]
    """
    import numpy as np

    if not isinstance(py_list, list) or len(py_list) == 0:
        return None

    try:

        def convert_value(val):
            """Recursively convert a value, handling arbitrary nesting."""
            if isinstance(val, np.ndarray):
                # Convert numpy array to Python list
                return val.tolist()
            elif isinstance(val, dict):
                # Recursively convert dict values
                return {k: convert_value(v) for k, v in val.items()}
            elif isinstance(val, (list, tuple)):
                # Recursively convert list/tuple elements
                return [convert_value(item) for item in val]
            else:
                # Native Python types (int, float, str, bool, None) pass through
                return val

        # Check all elements are dicts and convert them
        result = []
        for item in py_list:
            if not isinstance(item, dict):
                # Not all elements are dicts - can't use this fast path
                return None
            result.append(convert_value(item))

        return result

    except Exception:
        return None


def convert_nested_dicts_to_json(py_list):
    """Convert list of nested dicts with numpy arrays to JSON string.

    Combines array-to-list conversion with JSON serialization for efficient
    transfer to MATLAB. Enables a SINGLE bridge crossing (the JSON string)
    instead of N dict conversions.

    Parameters
    ----------
    py_list : list
        List of dicts with potentially nested structure containing numpy arrays.

    Returns
    -------
    str or None
        JSON string representation of the list, or None if conversion failed.

    Examples
    --------
    >>> data = [{'a': np.array([1, 2]), 'b': {'c': np.array([3])}}]
    >>> convert_nested_dicts_to_json(data)
    '[{"a": [1, 2], "b": {"c": [3]}}]'
    """
    import json

    # First convert all numpy arrays to Python lists
    converted = convert_nested_dicts_arrays_to_lists(py_list)
    if converted is None:
        return None

    try:
        # Serialize to JSON string (single string for entire list)
        return json.dumps(converted)
    except Exception:
        return None



def numpy_scalar_to_python(obj):
    """``[True, obj.item()]`` when *obj* is a numpy scalar, else ``[False, None]``.

    For ``+scidb/+internal/from_python.m``: an integer record loads back as
    ``np.int64``, which MATLAB does not convert. The check has to live here
    because MATLAB cannot name a Python class as a value: ``py.numpy.generic``
    in an argument list CALLS the class (and numpy.generic cannot be
    instantiated), so an ``isinstance`` written in MATLAB always fails.
    A list, not a tuple, so MATLAB can ``cell()`` it.
    """
    import numpy as np

    # Never a datetime64: .item() of a ns value is a bare int (MATLAB would
    # make it a double). from_python.m asks is_datetime_value first.
    if isinstance(obj, np.generic) and not isinstance(obj, np.datetime64):
        return [True, obj.item()]
    return [False, None]


def is_datetime_value(obj) -> bool:
    """Whether *obj* is a datetime scalar (``datetime``, ``pd.Timestamp``,
    ``pd.NaT``, ``np.datetime64``), for ``from_python.m``'s fallback when
    MATLAB's ``isa`` misses a subclass (pandas' Timestamp / NaTType are
    Cython subclasses of ``datetime.datetime``). Owner:
    ``scicanonicalhash.is_datetime_value``."""
    from scicanonicalhash import is_datetime_value as _is_datetime_value

    return bool(_is_datetime_value(obj))



def is_dataframe(obj) -> bool:
    """Whether *obj* is a pandas DataFrame, for ``from_python.m``'s fallback
    when MATLAB's ``isa`` misses one (class proxy resolution varies by MATLAB
    version). Here for the same reason as :func:`numpy_scalar_to_python`:
    ``py.pandas.DataFrame`` in a MATLAB argument list CALLS the class (an
    empty frame), so a MATLAB-side ``isinstance`` raises instead of answering.
    """
    import pandas as pd

    return isinstance(obj, pd.DataFrame)



@matlab_entry
def boxed_tolist(obj) -> list:
    """``obj.tolist()`` (an ndarray or Series) with its datetime/timedelta
    elements boxed, for ``from_python.m``'s element-by-element paths:
    MATLAB reads each element out of the list, and R2024a converts a bare
    datetime as it does (:mod:`.crossing`)."""
    return obj.tolist()
