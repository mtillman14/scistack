"""Every optimized serialization path produces the FROZEN bytes (F30).

``canonical_hash`` feeds ``record_id``: one changed byte makes every stored
record look new. The speed work (.claude/plan-f30-fast-canonical-hash.md)
adds a fast scalar dispatch and a fast DataFrame column path; this corpus
pins both to ``tests/reference_serializer.py``, a verbatim copy of the
serializer from before that work.
"""

import enum
import logging

import pytest

from scicanonicalhash import canonical_hash, hashing
from .reference_serializer import reference_serialize

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")


class Color(enum.IntEnum):
    RED = 1


class MyFloat(float):
    pass


SCALARS = [
    None, True, False, 0, 1, -1, 2**70, -(2**70), 0.0, -0.0, 1.5, -2.25,
    0.1, 1e300, -1e-320, 5e-324, float("nan"), float("inf"), float("-inf"),
    "", "a", "héllo", 'quo"te', "back\\slash", "new\nline", "☃", "\x00",
    Color.RED, MyFloat(2.5), np.float64(1.5), np.float32(0.1), np.int64(3),
    np.bool_(True),
]


def _scalar_ids():
    return [f"{type(v).__name__}:{v!r}"[:40] for v in SCALARS]


def _serializable(value) -> bool:
    try:
        reference_serialize(value)
        return True
    except ValueError:
        return False


@pytest.fixture(autouse=True)
def _fresh_caches():
    hashing._reset_frame_caches()
    yield
    hashing._reset_frame_caches()


def _same(obj):
    ref = reference_serialize(obj)
    assert hashing._serialize_for_hash(obj) == ref
    return ref


@pytest.mark.parametrize("value", SCALARS, ids=_scalar_ids())
def test_scalars(value):
    if not _serializable(value):
        with pytest.raises(ValueError):
            hashing._serialize_for_hash(value)
        return
    _same(value)


def test_containers():
    _same([1, 2.5, "x", None, [True, {"b": 1, "a": [0.1, float("nan")]}]])
    _same((1, (2, 3.0), []))
    _same({"b": 1, "a": 2, 3: "int key", "nested": {"z": [1e-7], "y": ()}})
    _same({})
    _same([])


def test_arrays():
    for arr in [
        np.arange(6, dtype=np.int64).reshape(2, 3),
        np.array([0.1, np.nan, -np.inf], dtype=np.float64),
        np.array([1.5], dtype=np.float32),
        np.array([True, False]),
        np.array(["a", "bc"]),
        np.array([1, "a", None, 2.5, [1, 2]], dtype=object),
        np.array([np.arange(3), np.arange(2)], dtype=object),
        np.array([], dtype=np.float64),
        np.asfortranarray(np.arange(6.0).reshape(2, 3)),
        np.arange(10.0)[::2],
        np.array(["2024-01-01"], dtype="datetime64[ns]"),
    ]:
        _same(arr)


def _frames():
    rng = np.random.default_rng(0)
    wide = {f"dbl_{j:02d}": [float(rng.standard_normal())] for j in range(5)}
    wide.update({f"arr_{j:02d}": [rng.standard_normal(4)] for j in range(3)})
    wide["json_00"] = [{"mean": 0.5, "idx": 1}]
    yield "wide-one-row", pd.DataFrame(wide)
    yield "multi-row", pd.DataFrame(
        {
            "t": np.arange(4, dtype=float),
            "label": [f"s{k}" for k in range(4)],
            "curve": [np.arange(3.0) + k for k in range(4)],
        }
    )
    yield "column-order", pd.DataFrame({"b": [1], "a": [2.0], "c": ["x"]})
    yield "non-default-index", pd.DataFrame({"a": [1, 2]}, index=[10, 20])
    yield "int-column-names", pd.DataFrame({2: [1.0], 1: [2.0], "1": [3.0]})
    yield "mixed-object", pd.DataFrame({"m": [1, "a", None, 2.5]})
    yield "nullable-int", pd.DataFrame({"n": pd.array([1, None, 3], dtype="Int64")})
    yield "string-dtype", pd.DataFrame({"s": pd.array(["a", None], dtype="string")})
    yield "category", pd.DataFrame({"c": pd.Categorical(["x", "y", "x"])})
    yield "datetime", pd.DataFrame({"d": pd.to_datetime(["2024-01-01", "2024-06-01"])})
    yield "datetime-tz", pd.DataFrame(
        {"d": pd.to_datetime(["2024-01-01"]).tz_localize("UTC")}
    )
    yield "bool-int32-float32", pd.DataFrame(
        {
            "b": [True, False],
            "i": np.array([1, 2], dtype=np.int32),
            "f": np.array([0.5, 1.5], dtype=np.float32),
        }
    )
    yield "empty", pd.DataFrame({"a": pd.Series([], dtype=float)})
    yield "no-columns", pd.DataFrame()
    yield "duplicate-columns", pd.DataFrame([[1, 2]], columns=["a", "a"])
    yield "multiindex-columns", pd.DataFrame(
        [[1, 2]], columns=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
    )
    yield "sliced-view", pd.DataFrame({"a": np.arange(10.0), "b": np.arange(10)}).iloc[
        2:5
    ]
    yield "series", pd.Series([1.0, 2.0], name="s")


@pytest.mark.parametrize("name,frame", list(_frames()), ids=lambda x: x if isinstance(x, str) else "")
def test_frames_match_twice(name, frame):
    """Twice: the first call verifies the fast path for this signature, the
    second uses the cached verdict — both must give the frozen bytes.

    Identical behaviour includes FAILING identically: a frame the frozen
    serializer cannot hash (pd.NA in a string column, tz-aware Timestamps)
    must still raise, not start producing a hash nothing before could."""
    if not _serializable(frame):
        for _ in range(2):
            with pytest.raises(ValueError):
                hashing._serialize_for_hash(frame)
        return
    ref = _same(frame)
    assert hashing._serialize_for_hash(frame) == ref


def test_the_fast_path_is_actually_taken_for_plain_frames():
    frame = pd.DataFrame({"a": [1.0], "b": [np.arange(3.0)], "c": ["x"]})
    before = hashing.frame_path_counts()
    for _ in range(5):
        _same(frame)
    after = hashing.frame_path_counts()
    assert after["fast"] - before["fast"] == 5
    assert after["reference"] == before["reference"]


def test_frames_the_fast_path_cannot_address_use_the_reference():
    dup = pd.DataFrame([[1, 2]], columns=["a", "a"])
    before = hashing.frame_path_counts()
    _same(dup)
    assert hashing.frame_path_counts()["reference"] == before["reference"] + 1


def test_a_disagreeing_fast_path_falls_back_and_warns(monkeypatch, caplog):
    """The runtime cross-check: if the fast column values ever differ from
    the public API's, that signature is served by the reference path — a bug
    costs speed, never identity."""
    real = hashing._column_as_numpy

    def corrupt(values):
        arr = real(values)
        return arr[::-1].copy() if len(arr) > 1 else arr

    monkeypatch.setattr(hashing, "_column_as_numpy", corrupt)
    frame = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
    with caplog.at_level(logging.WARNING, logger="scicanonicalhash.hashing"):
        _same(frame)
        _same(frame)
    assert any("fast column path disagrees" in r.message for r in caplog.records)
    assert hashing.frame_path_counts()["reference"] >= 2


def test_many_records_of_one_shape_hash_like_one_at_a_time():
    """save_batch's shape: thousands of one-row frames sharing columns."""
    rng = np.random.default_rng(1)
    frames = [
        pd.DataFrame(
            {
                "x": [float(rng.standard_normal())],
                "v": [rng.standard_normal(4)],
                "j": [{"i": i}],
            }
        )
        for i in range(50)
    ]
    import hashlib

    for f in frames:
        expected = hashlib.sha256(reference_serialize(f)).hexdigest()[:16]
        assert canonical_hash(f) == expected
