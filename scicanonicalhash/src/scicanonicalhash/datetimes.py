"""Datetime identity — the one owner of how a datetime is spelled.

Two questions live here because their answers must agree:

- **the zone label** of a value (``zone_label``): ``None`` for a naive value,
  else an IANA name (``"Europe/Berlin"``), ``"UTC"``, or a fixed offset
  (``"+02:00"``). sciduckdb stores this label beside the UTC instant
  (the ``<col>__tz`` companion column) and rebuilds the zone from it on load
  (``zone_from_label``);
- **the hash bytes** of a value (``datetime_hash_bytes``), which include that
  same label. Because both read one function, a value hashes the same before
  a save and after its load, so re-saving a loaded value is "0 new rows".

``datetime.datetime``, ``pd.Timestamp`` and ``np.datetime64`` holding the same
value hash identically (user decision 2026-10-22), and every missing marker
(``pd.NaT``, ``np.datetime64("NaT")``) has one spelling. No pandas import is
needed; numpy is consulted only when it is already loaded.
"""

from __future__ import annotations

import datetime
import re
import sys
from typing import Any

_EPOCH = datetime.datetime(1970, 1, 1)
_US = datetime.timedelta(microseconds=1)
_OFFSET_RE = re.compile(r"^([+-])(\d{2}):(\d{2})(?::(\d{2}))?$")

# np.datetime64 units in attoseconds (Y and M are calendar units: converted
# to days first).
_UNIT_AS = {
    "W": 7 * 86400 * 10**18,
    "D": 86400 * 10**18,
    "h": 3600 * 10**18,
    "m": 60 * 10**18,
    "s": 10**18,
    "ms": 10**15,
    "us": 10**12,
    "ns": 10**9,
    "ps": 10**6,
    "fs": 10**3,
    "as": 1,
}

NAT_BYTES = b"datetime:NaT"


def _offset_label(offset: datetime.timedelta) -> str:
    total = int(offset.total_seconds())
    sign = "-" if total < 0 else "+"
    total = abs(total)
    hours, rest = divmod(total, 3600)
    minutes, seconds = divmod(rest, 60)
    label = f"{sign}{hours:02d}:{minutes:02d}"
    if seconds:
        label += f":{seconds:02d}"
    return label


def is_datetime_value(value: Any) -> bool:
    """True for a datetime scalar: ``datetime.datetime`` (``pd.Timestamp``
    and ``pd.NaT`` included) or ``np.datetime64``."""
    if isinstance(value, datetime.datetime):
        return True
    np = sys.modules.get("numpy")
    return np is not None and isinstance(value, np.datetime64)


def is_missing_datetime(value: Any) -> bool:
    """True for ``pd.NaT`` and ``np.datetime64("NaT")``."""
    if isinstance(value, datetime.datetime):
        return value != value  # only NaT is unequal to itself
    np = sys.modules.get("numpy")
    return np is not None and isinstance(value, np.datetime64) and bool(np.isnat(value))


def zone_label(value: Any) -> str | None:
    """The zone of one datetime, as it is stored and hashed.

    ``None`` = naive (``np.datetime64`` and NaT are always naive). A named zone
    (zoneinfo ``key`` / pytz ``zone``) keeps its name, since that name carries
    the daylight-saving rules. Any other zone (``datetime.timezone``, a pytz
    or dateutil fixed offset) is spelled by THIS value's offset; an unnamed
    zero offset is ``"UTC"`` (so the label survives zone_from_label).
    """
    if not isinstance(value, datetime.datetime) or is_missing_datetime(value):
        return None
    tz = value.tzinfo
    if tz is None:
        return None
    offset = value.utcoffset()
    if offset is None:
        return None
    for attr in ("key", "zone"):  # zoneinfo.ZoneInfo / pytz
        name = getattr(tz, attr, None)
        if isinstance(name, str) and name:
            return name
    if offset == datetime.timedelta(0):
        return "UTC"
    return _offset_label(offset)


def zone_from_label(label: str) -> datetime.tzinfo:
    """The tzinfo a stored ``zone_label`` names (the inverse, for loads).

    Raises ``ValueError`` naming the label when no time-zone database knows
    it (on Windows, zoneinfo needs the ``tzdata`` package; pandas installs it).
    """
    if label == "UTC":
        return datetime.timezone.utc
    m = _OFFSET_RE.match(label)
    if m:
        sign, hh, mm, ss = m.groups()
        delta = datetime.timedelta(hours=int(hh), minutes=int(mm), seconds=int(ss or 0))
        return datetime.timezone(-delta if sign == "-" else delta)
    try:
        import zoneinfo

        return zoneinfo.ZoneInfo(label)
    except Exception as zi_err:  # ZoneInfoNotFoundError, ValueError
        try:
            import pytz  # type: ignore[import-not-found]

            return pytz.timezone(label)
        except Exception:
            raise ValueError(
                f"time zone {label!r} is not known to this machine's time-zone "
                f"database ({type(zi_err).__name__}: {zi_err}); install the "
                f"'tzdata' package"
            ) from zi_err


def _wall_ns(value: datetime.datetime) -> int:
    """Nanoseconds from 1970-01-01 of a datetime's own fields (no zone)."""
    plain = datetime.datetime(
        value.year, value.month, value.day,
        value.hour, value.minute, value.second, value.microsecond,
    )
    return ((plain - _EPOCH) // _US) * 1000 + int(getattr(value, "nanosecond", 0))


def utc_ns(value: datetime.datetime) -> int:
    """Nanoseconds from the epoch: the UTC instant of an aware value, the
    wall clock of a naive one."""
    ns = _wall_ns(value)
    offset = value.utcoffset() if value.tzinfo is not None else None
    if offset is not None:
        ns -= (offset // _US) * 1000
    return ns


def _datetime64_ns_text(value: Any) -> str:
    """An ``np.datetime64`` as ns from the epoch, exactly (any unit)."""
    np = sys.modules["numpy"]
    unit, count = np.datetime_data(value.dtype)
    if unit in ("Y", "M"):
        value = value.astype("datetime64[D]")
        unit, count = "D", 1
    total_as = int(value.astype(np.int64)) * count * _UNIT_AS[unit]
    ns, rest = divmod(total_as, 10**9)
    return str(ns) if rest == 0 else f"{total_as}as"


def datetime_hash_bytes(value: Any) -> bytes | None:
    """The hash bytes of a datetime scalar, or ``None`` if it is not one.

    naive -> ``datetime:<ns>``; aware -> ``datetimetz:<utc ns>:<zone label>``;
    missing -> ``datetime:NaT``.
    """
    if isinstance(value, datetime.datetime):
        if is_missing_datetime(value):
            return NAT_BYTES
        label = zone_label(value)
        if label is None:
            return b"datetime:" + str(_wall_ns(value)).encode()
        return b"datetimetz:" + str(utc_ns(value)).encode() + b":" + label.encode()
    np = sys.modules.get("numpy")
    if np is not None and isinstance(value, np.datetime64):
        if np.isnat(value):
            return NAT_BYTES
        return b"datetime:" + _datetime64_ns_text(value).encode()
    return None
