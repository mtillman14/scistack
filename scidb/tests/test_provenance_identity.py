"""Tests for the bipartite provenance identity helpers and schema (Phase 2).

Covers ``scidb.provenance``:
- content-addressed ids are deterministic / idempotent (re-run → same id)
- distinct inputs (function, value, binding set, output slot) → distinct ids
- identity-neutral facts (where, binding order, as_table order) don't shift ids
- run_id is fresh per call (NOT content-addressed)
- the seven tables are created with the expected columns
"""

import pytest
import scifor as _scifor

import scidb.provenance as prov

from scidb import configure_database


# ---------------------------------------------------------------------------
# Constant record ids
# ---------------------------------------------------------------------------
def test_constant_id_deterministic():
    assert prov.compute_constant_record_id(20) == prov.compute_constant_record_id(20)


def test_constant_id_distinguishes_values():
    assert prov.compute_constant_record_id(20) != prov.compute_constant_record_id(21)


def test_constant_id_is_16_hex():
    rid = prov.compute_constant_record_id("hello")
    assert len(rid) == 16 and all(c in "0123456789abcdef" for c in rid)


def test_constant_value_rendering():
    assert prov.constant_value_repr(20) == "20"
    assert prov.constant_value_type(20) == "int"
    assert prov.constant_value_type("x") == "str"


# ---------------------------------------------------------------------------
# Constants identity key (variant grouping / de-duplication)
# ---------------------------------------------------------------------------
# A constant is NOT always a scalar: an inline table under [parameters] in
# scistack_entities.toml *is* the value, so dict- and list-valued constants
# reach every consumer that groups variants. Those consumers put the key in a
# set or use it as a dict key, so a non-hashable key is an immediate crash
# ("unhashable type: 'dict'" out of generate_matlab_command).
def test_constants_key_is_hashable_for_dict_values():
    key = prov.constants_identity_key({"delsys_config": {"fs": 2000, "chans": ["a"]}})
    assert hash(key) is not None
    assert {key}  # usable as a set member / dict key


def test_constants_key_is_hashable_for_list_values():
    key = prov.constants_identity_key({"bands": [20, 450]})
    assert hash(key) is not None


def test_constants_key_order_insensitive():
    a = prov.constants_identity_key({"x": 1, "y": {"b": 2}})
    b = prov.constants_identity_key({"y": {"b": 2}, "x": 1})
    assert a == b


def test_constants_key_distinguishes_dict_values():
    a = prov.constants_identity_key({"cfg": {"fs": 2000}})
    b = prov.constants_identity_key({"cfg": {"fs": 1000}})
    assert a != b


def test_constants_key_empty_and_non_dict():
    assert prov.constants_identity_key({}) == ()
    assert prov.constants_identity_key(None) == ()


def test_constants_key_matches_repr_recipe():
    """The key must stay byte-identical to the inline
    ``sorted((k, repr(v)))`` recipe it replaced in provenance_query, so keys
    built by the per-record and batched variant-key paths still compare equal.
    """
    constants = {"low_hz": 20, "name": "x"}
    assert prov.constants_identity_key(constants) == tuple(
        sorted((k, repr(v)) for k, v in constants.items())
    )


# ---------------------------------------------------------------------------
# Invocation ids
# ---------------------------------------------------------------------------
def test_invocation_id_deterministic():
    a = prov.compute_invocation_id(
        "fnhash", [], False, [("signal", "rid1"), ("low_hz", "ridc")]
    )
    b = prov.compute_invocation_id(
        "fnhash", [], False, [("signal", "rid1"), ("low_hz", "ridc")]
    )
    assert a == b


def test_invocation_id_binding_order_insensitive():
    a = prov.compute_invocation_id("fn", [], False, [("a", "r1"), ("b", "r2")])
    b = prov.compute_invocation_id("fn", [], False, [("b", "r2"), ("a", "r1")])
    assert a == b


def test_invocation_id_function_hash_matters():
    a = prov.compute_invocation_id("fnA", [], False, [("a", "r1")])
    b = prov.compute_invocation_id("fnB", [], False, [("a", "r1")])
    assert a != b


def test_invocation_id_binding_value_matters():
    a = prov.compute_invocation_id(
        "fn", [], False, [("low_hz", prov.compute_constant_record_id(20))]
    )
    b = prov.compute_invocation_id(
        "fn", [], False, [("low_hz", prov.compute_constant_record_id(21))]
    )
    assert a != b


def test_invocation_id_as_table_matters():
    a = prov.compute_invocation_id("fn", [], False, [("x", "r1")])
    b = prov.compute_invocation_id("fn", ["x"], False, [("x", "r1")])
    assert a != b


def test_invocation_id_as_table_order_insensitive():
    a = prov.compute_invocation_id("fn", ["x", "y"], False, [("x", "r1")])
    b = prov.compute_invocation_id("fn", ["y", "x"], False, [("x", "r1")])
    assert a == b


def test_invocation_id_distribute_matters():
    a = prov.compute_invocation_id("fn", [], False, [("x", "r1")])
    b = prov.compute_invocation_id("fn", [], True, [("x", "r1")])
    assert a != b


# ---------------------------------------------------------------------------
# One identity recipe: invocation_identity (what the save stamps into the
# record) and record_run's inline assembly must agree byte for byte — and
# record_run refuses to write a graph that disagrees with the record.
# ---------------------------------------------------------------------------
def _inline_invocation_id(meta, edges):
    """Reproduce the exact assembly record_run does inline for one record."""
    from scidb.bindings import Binding
    from scidb.provenance import (
        compute_invocation_id,
        constant_record_id_from_hash,
    )
    from scidb.provenance_save import (
        _constant_bindings,
        _normalize_as_table,
        _parse_json_dict,
    )

    from scicanonicalhash import canonical_hash

    var_b = [Binding.coerce(e) for e in edges]
    const_b = _constant_bindings(meta)
    loadable = list(_parse_json_dict(meta.get("__inputs")).keys()) or [
        b.param for b in var_b
    ]
    as_table = _normalize_as_table(meta, loadable)
    distribute = bool(meta.get("__distribute", False))
    bindings = list(var_b)
    for param, value in const_b.items():
        bindings.append(
            Binding(param, constant_record_id_from_hash(canonical_hash(value)), None)
        )
    return compute_invocation_id(
        meta.get("__fn_hash") or "", as_table, distribute, bindings
    )


def test_inline_invocation_id_matches_helper():
    import json

    from scidb.provenance_save import invocation_identity

    cases = [
        # plain variable + constant
        ({"__fn_hash": "h1", "__constants": json.dumps({"low_hz": 20})}, [("signal", "rid_sig")]),
        # multiple constants, no variables
        ({"__fn_hash": "h2", "__constants": json.dumps({"a": 1, "b": "x", "c": 3.5})}, []),
        # aggregation flag + inputs list
        (
            {"__fn_hash": "h3", "__inputs": json.dumps({"x": "k", "y": "k"}), "__as_table": True},
            [("x", "r1"), ("y", "r2")],
        ),
        # distribute flag
        ({"__fn_hash": "h4", "__distribute": True}, [("x", "r1")]),
        # several edges under ONE parameter (an aggregating call)
        ({"__fn_hash": "h5", "__as_table": ["x"]}, [("x", "r1", None), ("x", "r2", None)]),
    ]
    for meta, edges in cases:
        assert _inline_invocation_id(meta, edges) == invocation_identity(meta, edges)


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "identity.duckdb", ["subject"])
    yield database
    _scifor.set_schema([])
    database.close()


def test_record_run_refuses_a_graph_that_disagrees_with_the_record(db):
    """The self-check: a GraphRecord whose stamped invocation id does not
    match the edges it carries is a bug of exactly the class the typed spine
    exists to catch, and record_run must not paper over it."""
    from scidb.bindings import Binding
    from scidb.provenance_save import GraphRecord, record_run

    meta = {"__fn": "f", "__fn_hash": "h", "__constants": {}}
    bad = GraphRecord(
        "Out", 1, 0, "out_rid_1", meta, bindings=[Binding("x", "r1")], invocation_id="0" * 16
    )
    with pytest.raises(RuntimeError, match="identity drift"):
        record_run(db, [bad], function_name="f", where_clause=None, user_id="t")


# ---------------------------------------------------------------------------
# Run ids — fresh per call
# ---------------------------------------------------------------------------
def test_run_id_is_fresh():
    assert prov.generate_run_id() != prov.generate_run_id()


def test_run_id_is_16_hex():
    rid = prov.generate_run_id()
    assert len(rid) == 16 and all(c in "0123456789abcdef" for c in rid)


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------
def test_provenance_tables_created(tmp_path):
    db = configure_database(tmp_path / "prov.duckdb", ["subject", "trial"])
    try:
        names = {
            r[0]
            for r in db._duck._fetchall(
                "SELECT table_name FROM information_schema.tables"
            )
        }
        for t in (
            "_record",
            "_constant",
            "_invocation",
            "_invocation_input",
            "_invocation_output",
            "_run",
            "_run_invocation",
        ):
            assert t in names, f"missing table {t}"
    finally:
        db.close()


def test_record_table_columns(tmp_path):
    db = configure_database(tmp_path / "prov2.duckdb", ["subject"])
    try:
        cols = {
            r[0]
            for r in db._duck._fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = '_record'"
            )
        }
        assert cols == {
            "record_id",
            "created_at",
            "type",
            "schema_id",
            "content_hash",
            "schema_version",
            "excluded",
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# variable_input_params — tells "no input edges because the inputs were
# files/constants" (normal) from "no input edges although a stored record was
# consumed" (severed lineage). Drives the batch_save log level, so a wrong
# answer either hides a real lineage break or cries wolf on every PathInput run.
# ---------------------------------------------------------------------------
_PI_SPEC = '{"__type": "PathInput", "template": "{subject}/x.csv", "root_folder": "data"}'


def test_variable_input_params_excludes_path_inputs():
    from scidb.provenance_save import variable_input_params

    assert variable_input_params({"__inputs": {"filepath_or_buffer": _PI_SPEC}}) == []


def test_variable_input_params_finds_variable_inputs():
    from scidb.provenance_save import variable_input_params

    meta = {"__inputs": {"signal": "RawSignal", "filepath_or_buffer": _PI_SPEC}}
    assert variable_input_params(meta) == ["signal"]


def test_variable_input_params_handles_json_string_inputs():
    """``__inputs`` arrives as a dict in-memory but as a JSON string off a
    stored record."""
    import json

    from scidb.provenance_save import variable_input_params

    meta = {"__inputs": json.dumps({"signal": "RawSignal", "fp": _PI_SPEC})}
    assert variable_input_params(meta) == ["signal"]


def test_variable_input_params_empty_when_no_inputs():
    from scidb.provenance_save import variable_input_params

    assert variable_input_params({}) == []
    assert variable_input_params({"__inputs": {}, "__constants": {"hz": 30}}) == []
