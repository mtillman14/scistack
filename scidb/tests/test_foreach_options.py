"""One declaration of ``for_each``'s options (``scifor.options``), and the
bundles scidb groups them into (``scidb.run.foreach_call``).

Guards (P4 stage 2, design review 04b W3):

* ``scidb.for_each``'s keyword signature IS ``SCIDB_FOR_EACH_OPTIONS`` — the
  same names, order and defaults — so the declaration cannot drift from the
  function users call;
* ``scifor.for_each`` takes exactly the options that are not ``db_only``
  (plus ``hooks``), with the same defaults;
* ``scifor.DB_ONLY_KWARGS`` is derived from the declaration;
* every option lands on a field of its bundle, and ``ForEachCall`` turns
  back into the keywords it was built from (a deferred pipeline step replays
  through that), so no recursion or replay can drop an option.
"""

import dataclasses
import inspect

import pytest
import scidb
import scifor
from scidb.run.foreach_call import (
    SCIDB_FOR_EACH_OPTIONS,
    ForEachCall,
    RunContext,
    RunSelection,
)
from scidb.run.foreach_config import RunOptions
from scifor import DB_ONLY_KWARGS, FOR_EACH_OPTIONS, UNSET

_NOT_OPTIONS = {"fn", "inputs", "outputs", "hooks"}


def _keyword_params(fn) -> "list[inspect.Parameter]":
    return [
        p
        for name, p in inspect.signature(fn).parameters.items()
        if name not in _NOT_OPTIONS and p.kind is not inspect.Parameter.VAR_KEYWORD
    ]


def test_scidb_signature_is_the_declaration():
    params = _keyword_params(scidb.for_each)
    assert [p.name for p in params] == [o.name for o in SCIDB_FOR_EACH_OPTIONS]
    for p, spec in zip(params, SCIDB_FOR_EACH_OPTIONS, strict=True):
        assert p.default is spec.default or p.default == spec.default, spec.name


def test_scifor_signature_is_the_non_db_only_options():
    params = {p.name: p for p in _keyword_params(scifor.for_each)}
    shared = {o.name: o for o in FOR_EACH_OPTIONS if not o.db_only}
    assert set(params) == set(shared)
    for name, spec in shared.items():
        assert params[name].default == spec.default, name


def test_db_only_set_is_derived():
    assert DB_ONLY_KWARGS == {o.name for o in FOR_EACH_OPTIONS if o.db_only}
    assert "pipeline" in DB_ONLY_KWARGS and "where" not in DB_ONLY_KWARGS


def test_pipeline_default_is_the_shared_sentinel():
    assert inspect.signature(scidb.for_each).parameters["pipeline"].default is UNSET


_BUNDLE_TYPES = {"run": RunOptions, "selection": RunSelection, "context": RunContext}


@pytest.mark.parametrize("spec", SCIDB_FOR_EACH_OPTIONS, ids=lambda s: s.name)
def test_every_option_has_a_field(spec):
    holder = _BUNDLE_TYPES[spec.bundle] if spec.bundle is not None else ForEachCall
    assert spec.attribute in {f.name for f in dataclasses.fields(holder)}


def _all_options():
    values = {o.name: o.default for o in SCIDB_FOR_EACH_OPTIONS}
    values.update(dry_run=True, as_table=["x"], where="a > 1", step="s", _cancel_check=bool)
    return values


def test_call_round_trips_to_its_keywords():
    options = _all_options()
    call = ForEachCall.from_options(print, {"x": 1}, [], {"subject": [1]}, **options)
    assert call.run == RunOptions(as_table=["x"])
    assert call.selection.where == "a > 1"
    assert call.selection.metadata_iterables == {"subject": [1]}
    assert call.context.step == "s" and call.context.cancel_check is bool
    expected = {k: v for k, v in options.items() if k != "pipeline"}
    assert call.option_values() == expected


def test_a_missing_option_fails_loudly():
    options = _all_options()
    options.pop("glue")
    with pytest.raises(TypeError, match="glue"):
        ForEachCall.from_options(print, {}, [], {}, **options)
