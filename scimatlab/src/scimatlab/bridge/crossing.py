"""Datetime and timedelta scalars cross the MATLAB bridge only inside a box.

MATLAB R2024a converts every Python ``datetime`` / ``timedelta`` it is handed
into a MATLAB ``datetime`` / ``duration``. That covers a ``py.`` call's
return value and an element MATLAB reads out of a py.list or py.dict. The
conversion loses an IANA zone: ``ZoneInfo('Europe/Berlin')`` becomes the fixed
offset ``+02:00`` when the value crosses back. It also crashes on ``pd.NaT``
with "NULL pointer returned from Python". Found 2026-11-02 by
``TestDatetimeRoundTrip`` on R2024a Update 5. See
docs/claude/datetime-roundtrip.md, "MATLAB".

This module is the ONE owner of the rule:

* :class:`DatetimeCrossing` / :class:`DurationCrossing` hold the Python value.
  MATLAB does not convert them. MATLAB never reads ``.value``; it hands the
  box back to Python (``datetimes_to_matlab`` / ``durations_to_matlab``).
* :func:`matlab_entry` decorates every bridge function MATLAB calls with data.
  It unboxes the arguments and boxes the result, so no scalar crosses bare in
  either direction.

Only stdlib imports at module level: the bridge loads in MATLAB's Python
start-up, and numpy/pandas are looked up in ``sys.modules`` (a value of
their type means they are already imported).
"""

from __future__ import annotations

import datetime
import functools
import sys
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

from scistacklog import Log

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class DatetimeCrossing:
    """One datetime scalar (``datetime``, ``pd.Timestamp``, ``pd.NaT``,
    ``np.datetime64``) on its way across the MATLAB bridge."""

    value: Any


@dataclass(frozen=True)
class DurationCrossing:
    """One timedelta scalar (``timedelta``, ``pd.Timedelta``,
    ``np.timedelta64``) on its way across the MATLAB bridge."""

    value: Any


Crossing = (DatetimeCrossing, DurationCrossing)


def _is_datetime_scalar(value: Any) -> bool:
    if isinstance(value, datetime.datetime):  # pd.Timestamp, pd.NaT included
        return True
    np = sys.modules.get("numpy")
    return np is not None and isinstance(value, np.datetime64)


def _is_timedelta_scalar(value: Any) -> bool:
    if isinstance(value, datetime.timedelta):  # pd.Timedelta included
        return True
    np = sys.modules.get("numpy")
    return np is not None and isinstance(value, np.timedelta64)


def box_scalar(value: Any) -> Any:
    """``value`` boxed when it is a datetime/timedelta scalar, else itself."""
    if _is_datetime_scalar(value):
        return DatetimeCrossing(value)
    if _is_timedelta_scalar(value):
        return DurationCrossing(value)
    return value


def box_datetimes(obj: Any) -> tuple[Any, int]:
    """``obj`` with every datetime/timedelta scalar boxed, recursing into
    list / tuple / dict. Frames, Series and arrays are left alone: their
    datetime columns cross as one buffer (``datetimes_to_matlab``). Returns
    ``(obj, n_boxed)``; an unchanged container is returned as is (no copy)."""
    boxed = box_scalar(obj)
    if boxed is not obj:
        return boxed, 1
    if isinstance(obj, dict):
        items = [(k, *box_datetimes(v)) for k, v in obj.items()]
        n = sum(c for _, _, c in items)
        return ({k: v for k, v, _ in items} if n else obj), n
    if isinstance(obj, (list, tuple)):
        items = [box_datetimes(v) for v in obj]
        n = sum(c for _, c in items)
        if not n:
            return obj, 0
        values = [v for v, _ in items]
        return (values if isinstance(obj, list) else tuple(values)), n
    return obj, 0


def unbox_datetimes(obj: Any) -> tuple[Any, int]:
    """The inverse of :func:`box_datetimes`: every box replaced by its
    value, recursing into list / tuple / dict. Returns ``(obj, n_unboxed)``."""
    if isinstance(obj, Crossing):
        return obj.value, 1
    if isinstance(obj, dict):
        items = [(k, *unbox_datetimes(v)) for k, v in obj.items()]
        n = sum(c for _, _, c in items)
        return ({k: v for k, v, _ in items} if n else obj), n
    if isinstance(obj, (list, tuple)):
        items = [unbox_datetimes(v) for v in obj]
        n = sum(c for _, c in items)
        if not n:
            return obj, 0
        values = [v for v, _ in items]
        return (values if isinstance(obj, list) else tuple(values)), n
    return obj, 0


def matlab_entry(fn: F) -> F:
    """Decorate a bridge function MATLAB calls with data: arguments arrive
    unboxed, the result leaves boxed. Logs the counts at DEBUG when any box
    moved."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        unboxed_args = []
        n_in = 0
        for a in args:
            v, n = unbox_datetimes(a)
            unboxed_args.append(v)
            n_in += n
        unboxed_kwargs = {}
        for k, a in kwargs.items():
            v, n = unbox_datetimes(a)
            unboxed_kwargs[k] = v
            n_in += n
        result, n_out = box_datetimes(fn(*unboxed_args, **unboxed_kwargs))
        if n_in or n_out:
            Log.debug(
                "[crossing] %s: unboxed %d argument value(s), boxed %d result value(s)",
                fn.__name__, n_in, n_out, layer="matlab",
            )
        return result

    wrapper.__matlab_entry__ = True  # type: ignore[attr-defined]
    return wrapper  # type: ignore[return-value]


def datetime_keys_as_text(metadata: dict) -> dict:
    """*metadata* with each date or datetime value as its schema-key text
    (``scicanonicalhash.datetime_key_text``), for MATLAB to read.

    A date key reaches MATLAB as that exact text, not a MATLAB ``datetime``:
    the conversion would turn a named zone into a fixed offset, and the value
    would then re-save at a different address.
    """
    from scicanonicalhash import datetime_key_text

    out = {}
    for key, value in dict(metadata).items():
        text = datetime_key_text(value)
        out[key] = value if text is None else text
    return out


def datetime_key_text_of(value: Any) -> str:
    """The schema-key text of one MATLAB ``datetime`` metadata value, handed
    over as ``datetime_to_python(dt, 'scalar')`` crosses it (boxed)."""
    from scicanonicalhash import datetime_key_text

    if isinstance(value, DatetimeCrossing):
        value = value.value
    text = datetime_key_text(value)
    if text is None:
        raise ValueError(f"a datetime schema key must have a value, got {value!r}")
    return text
