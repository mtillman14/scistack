"""Datetime/timedelta scalars cross the MATLAB bridge only boxed.

MATLAB R2024a converts every Python datetime/timedelta it is handed (a call's
return value, or an element read out of a py.list / py.dict) to a MATLAB
datetime/duration. ``ZoneInfo('Europe/Berlin')`` came back as ``+02:00`` and
``pd.NaT`` crashed MATLAB with "NULL pointer returned from Python"
(``TestDatetimeRoundTrip``, R2024a Update 5, 2026-11-02).
``scimatlab.bridge.crossing`` owns the rule; these tests pin it without MATLAB.
"""

import datetime
import re
import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402
import scimatlab.bridge as bridge  # noqa: E402
from scimatlab.bridge import (  # noqa: E402
    DatetimeCrossing,
    DurationCrossing,
    box_datetimes,
    durations_to_matlab,
    get_batch_data_item,
    matlab_entry,
    unbox_datetimes,
    variable_data,
)
from scimatlab.bridge.entry import _cache_batch  # noqa: E402

_MATLAB = _root / "scimatlab" / "src" / "scimatlab" / "matlab"

BERLIN = pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin")
SCALARS = [
    BERLIN,
    pd.Timestamp("2026-01-01 09:00", tz="+02:00"),
    pd.Timestamp("2026-01-01 10:30:15.25"),
    pd.NaT,
    np.datetime64("NaT"),
    np.datetime64("2026-01-01T00:00:00.000000007"),
    datetime.datetime(2026, 1, 1, 12),
]
DURATIONS = [
    datetime.timedelta(seconds=1.5),
    pd.Timedelta("1 days 00:00:00.000000007"),
    np.timedelta64(3, "ms"),
]


@pytest.mark.parametrize("value", SCALARS, ids=repr)
def test_datetime_scalar_boxes_and_unboxes_to_itself(value):
    boxed, n = box_datetimes(value)
    assert isinstance(boxed, DatetimeCrossing) and n == 1
    back, m = unbox_datetimes(boxed)
    assert back is value and m == 1


@pytest.mark.parametrize("value", DURATIONS, ids=repr)
def test_timedelta_scalar_boxes_as_a_duration(value):
    boxed, _ = box_datetimes(value)
    assert isinstance(boxed, DurationCrossing)
    assert unbox_datetimes(boxed)[0] is value


def test_named_zone_survives_the_box():
    boxed, _ = box_datetimes(BERLIN)
    back, _ = unbox_datetimes(boxed)
    assert str(back.tzinfo) == "Europe/Berlin"


def test_nested_containers_box_every_scalar():
    obj = {"a": [BERLIN, 1, "x"], "b": (pd.NaT, {"c": datetime.timedelta(1)}), "d": 2.0}
    boxed, n = box_datetimes(obj)
    assert n == 3
    assert isinstance(boxed["a"][0], DatetimeCrossing) and boxed["a"][1:] == [1, "x"]
    assert isinstance(boxed["b"], tuple) and isinstance(boxed["b"][0], DatetimeCrossing)
    assert isinstance(boxed["b"][1]["c"], DurationCrossing)
    back, m = unbox_datetimes(boxed)
    assert m == 3 and back["a"][0] is BERLIN and back["b"][0] is pd.NaT


def test_unchanged_container_is_not_copied():
    obj = {"a": [1, 2, {"b": "x"}]}
    assert box_datetimes(obj) == (obj, 0)
    assert box_datetimes(obj)[0] is obj
    assert unbox_datetimes(obj)[0] is obj


def test_frames_and_arrays_are_left_alone():
    """Their datetime columns cross as one buffer (datetimes_to_matlab)."""
    s = pd.Series(pd.to_datetime(["2026-01-01"]))
    df = pd.DataFrame({"t": s})
    arr = np.array(["2026-01-01"], dtype="datetime64[ns]")
    for obj in (s, df, arr):
        assert box_datetimes(obj)[0] is obj


def test_matlab_entry_unboxes_arguments_and_boxes_the_result():
    seen = []

    @matlab_entry
    def echo(x, *, y=None):
        seen.append((x, y))
        return {"x": x, "y": y}

    out = echo(DatetimeCrossing(BERLIN), y=[DurationCrossing(datetime.timedelta(2))])
    assert seen[0][0] is BERLIN and seen[0][1] == [datetime.timedelta(2)]
    assert isinstance(out["x"], DatetimeCrossing) and out["x"].value is BERLIN
    assert isinstance(out["y"][0], DurationCrossing)


def test_matlab_entry_logs_the_counts(caplog):
    @matlab_entry
    def ident(x):
        return x

    with caplog.at_level("DEBUG"):
        ident(DatetimeCrossing(BERLIN))
    assert "[crossing] ident: unboxed 1 argument value(s), boxed 1 result value(s)" in caplog.text


def test_batch_data_item_crosses_boxed():
    batch_id = _cache_batch([BERLIN, pd.NaT, 3.0], [])
    first = get_batch_data_item(batch_id, 0)
    assert isinstance(first, DatetimeCrossing) and first.value is BERLIN
    assert isinstance(get_batch_data_item(batch_id, 1), DatetimeCrossing)
    assert get_batch_data_item(batch_id, 2) == 3.0
    bridge.free_batch(batch_id)


def test_variable_data_crosses_boxed():
    class Var:
        data = pd.NaT

    assert isinstance(variable_data(Var()), DatetimeCrossing)


@pytest.mark.parametrize(
    "value, seconds",
    [
        (datetime.timedelta(seconds=1.5), 1.5),
        (pd.Timedelta("1 days 00:00:00.000000007"), 86400.000000007),
        (np.timedelta64(3, "ms"), 0.003),
    ],
)
def test_durations_to_matlab_seconds(value, seconds):
    desc = durations_to_matlab(DurationCrossing(value))
    assert desc["nat"] is False and desc["seconds"] == pytest.approx(seconds, abs=1e-12)


@pytest.mark.parametrize("value", [np.timedelta64("NaT"), pd.NaT])
def test_durations_to_matlab_nat(value):
    desc = durations_to_matlab(DurationCrossing(value))
    assert desc["nat"] is True and desc["seconds"] != desc["seconds"]


# --- static guards over the MATLAB sources -----------------------------------


def _matlab_code(rel: str) -> str:
    text = (_MATLAB / rel).read_text(encoding="utf-8")
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("%"))


def test_from_python_handles_both_boxes_first():
    code = _matlab_code("+scidb/+internal/from_python.m")
    dt = code.index("'py.scimatlab.bridge.crossing.DatetimeCrossing'")
    du = code.index("'py.scimatlab.bridge.crossing.DurationCrossing'")
    nd = code.index("'py.numpy.ndarray'")
    assert dt < nd and du < nd


def test_box_class_paths_in_matlab_name_the_defining_module():
    """MATLAB isa() needs the class's defining module (crossing), not the
    package re-export."""
    assert DatetimeCrossing.__module__ == "scimatlab.bridge.crossing"
    assert DurationCrossing.__module__ == "scimatlab.bridge.crossing"


def test_no_matlab_reads_py_var_data_directly():
    """A ``py_var.data`` read in MATLAB converts a datetime unboxed; MATLAB
    goes through ``bridge.variable_data``."""
    offenders = []
    for path in sorted(_MATLAB.rglob("*.m")):
        code = _matlab_code(str(path.relative_to(_MATLAB)))
        if re.search(r"\bpy_var\.data\b", code):
            offenders.append(path.name)
    assert offenders == []


def test_matlab_never_calls_scidb_with_a_converted_value_directly():
    """Saves and filters built from to_python values go through the bridge
    (which unboxes): never ``.save_variable(`` or the filter classes."""
    pattern = re.compile(
        r"\.save_variable\(|filters\.(ColumnFilter|VariableFilter|SchemaKeyCompareFilter)\("
    )
    offenders = [
        path.name for path in sorted(_MATLAB.rglob("*.m"))
        if pattern.search(_matlab_code(str(path.relative_to(_MATLAB))))
    ]
    assert offenders == []


@pytest.mark.parametrize(
    "name",
    [
        "datetimes_from_matlab", "datetimes_to_matlab", "durations_to_matlab", "boxed_tolist",
        "for_each_prepare", "for_each_describe_loaded_input", "for_each_save",
        "normalize_stat_result", "save_batch_bridge", "save_variable_bridge",
        "get_batch_item", "get_batch_data_item", "wrap_batch_bridge",
        "load_and_extract", "to_csv_bridge", "variable_filter",
        "schema_key_filter", "variable_data",
        "pipeline_create", "pipeline_bind", "pipeline_use", "pipeline_plan",
        "pipeline_endpoints", "pipeline_execution_order",
        "pipeline_register_step", "pipeline_run_python_step",
    ],
)
def test_data_carrying_entry_points_are_matlab_entries(name):
    assert getattr(getattr(bridge, name), "__matlab_entry__", False), name


def test_boxed_tolist_boxes_an_object_columns_datetimes():
    """convert_dataframe's object branch reads elements one by one: a
    mixed-zone column must reach MATLAB boxed, NaT included."""
    col = pd.Series([BERLIN, pd.Timestamp("2026-01-01", tz="UTC"), pd.NaT, 3], dtype=object)
    out = bridge.boxed_tolist(col)
    assert [type(v) for v in out[:3]] == [DatetimeCrossing] * 3 and out[3] == 3
    assert out[0].value is BERLIN
