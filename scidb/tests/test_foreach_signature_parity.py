"""``scidb.for_each`` and ``scifor.for_each`` are drop-in replacements by kwarg.

Until 2026-10-18 scifor named its output argument ``output_names`` (and put it
seventh) while scidb named it ``outputs`` (third). A call written for one layer
silently misbehaved on the other: ``output_names=`` landed in scidb's
``**metadata_iterables`` as a bogus schema key, and a positional third argument
became scifor's ``dry_run``.

These tests pin the public signatures so the two cannot drift again. A new
public kwarg on either side must be added to the other, or deliberately listed
in ``scifor.DB_ONLY_KWARGS`` (which scifor accepts and discards).
"""

import inspect

import scifor
import scidb


def _public_params(fn) -> dict[str, inspect.Parameter]:
    return {
        name: p
        for name, p in inspect.signature(fn).parameters.items()
        if not name.startswith("_") and p.kind is not inspect.Parameter.VAR_KEYWORD
    }


def test_every_scifor_kwarg_is_accepted_by_scidb():
    scifor_params = _public_params(scifor.for_each)
    scidb_params = _public_params(scidb.for_each)
    missing = set(scifor_params) - set(scidb_params)
    assert not missing, f"scidb.for_each lacks scifor kwargs: {sorted(missing)}"


def test_scidb_only_kwargs_are_exactly_the_db_only_set():
    scifor_params = _public_params(scifor.for_each)
    scidb_params = _public_params(scidb.for_each)
    assert set(scidb_params) - set(scifor_params) == scifor.DB_ONLY_KWARGS


def test_outputs_is_named_and_positioned_identically():
    scifor_names = list(inspect.signature(scifor.for_each).parameters)
    scidb_names = list(inspect.signature(scidb.for_each).parameters)
    assert "output_names" not in scifor_names
    assert scifor_names[:3] == ["fn", "inputs", "outputs"]
    assert scidb_names[:3] == ["fn", "inputs", "outputs"]


def test_shared_kwargs_have_the_same_defaults():
    # ``outputs`` is required in scidb (it names the variable types to save)
    # and optional in scifor (it only names result columns).
    scifor_params = _public_params(scifor.for_each)
    scidb_params = _public_params(scidb.for_each)
    shared = (set(scifor_params) & set(scidb_params)) - {"fn", "inputs", "outputs"}
    differing = {
        name: (scifor_params[name].default, scidb_params[name].default)
        for name in shared
        if scifor_params[name].default != scidb_params[name].default
    }
    assert not differing, f"(scifor, scidb) defaults differ: {differing}"
