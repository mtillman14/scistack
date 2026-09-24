"""The canvas reads invocation edges in a fixed number of queries
(cleanup-audit F15/F16).

``pipeline_variants`` and ``function_variant_configs`` each ran three or four
queries PER INVOCATION; on a real 843-invocation database that was ~8 s of an
11 s ``get_pipeline``. Both now read through ``invocation_edges_batch``, so
the query count must not grow with the number of invocations — and the batch
must say exactly what the per-invocation readers say.
"""

import numpy as np
import pytest

import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each
from scidb import provenance_query as pq


class EdgeRaw(BaseVariable):
    pass


class EdgeOut(BaseVariable):
    pass


def scale(signal, k):
    return signal * k


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "edges.duckdb", ["subject", "session"])
    yield database
    _scifor.set_schema([])
    database.close()


def _run(n_subjects: int, k: float = 3.0) -> None:
    subjects = [f"S{i:02d}" for i in range(n_subjects)]
    for s in subjects:
        EdgeRaw.save(np.arange(3.0), subject=s, session="A")
    for_each(
        scale,
        {"signal": EdgeRaw, "k": k},
        [EdgeOut],
        subject=subjects,
        session=["A"],
    )


def _count_queries(db, monkeypatch, fn) -> int:
    calls = []
    real_all, real_one = db._duck._fetchall, db._duck._fetchone

    def counting_all(sql, params=None):
        calls.append(sql)
        return real_all(sql, params)

    def counting_one(sql, params=None):
        calls.append(sql)
        return real_one(sql, params)

    monkeypatch.setattr(db._duck, "_fetchall", counting_all)
    monkeypatch.setattr(db._duck, "_fetchone", counting_one)
    try:
        fn()
    finally:
        monkeypatch.setattr(db._duck, "_fetchall", real_all)
        monkeypatch.setattr(db._duck, "_fetchone", real_one)
    return len(calls)


def _invocation_ids(db) -> list[str]:
    return [
        r[0]
        for r in db._duck._fetchall(
            "SELECT invocation_id FROM _invocation WHERE function_name = ?",
            ["scale"],
        )
    ]


def test_the_batch_says_what_the_single_readers_say(db):
    _run(3)
    inv_ids = _invocation_ids(db)
    assert len(inv_ids) == 3
    batch = pq.invocation_edges_batch(db._duck, inv_ids)
    for inv_id in inv_ids:
        var_inputs, constants = pq.invocation_inputs(db._duck, inv_id)
        assert batch[inv_id]["var_inputs"] == var_inputs
        assert batch[inv_id]["constants"] == constants == {"k": 3.0}
        assert batch[inv_id]["path_inputs"] == pq.invocation_path_inputs(
            db._duck, inv_id
        )
        assert [i["variable_type"] for i in var_inputs] == ["EdgeRaw"]


def test_an_unknown_invocation_reads_as_no_edges(db):
    assert pq.invocation_edges_batch(db._duck, ["no-such-invocation"]) == {}
    assert pq.invocation_inputs(db._duck, "no-such-invocation") == ([], {})
    assert pq.invocation_path_inputs(db._duck, "no-such-invocation") == {}


def test_pipeline_variants_query_count_does_not_grow_with_invocations(
    db, monkeypatch
):
    _run(2)
    few = _count_queries(db, monkeypatch, lambda: pq.pipeline_variants(db._duck))
    _run(8, k=5.0)  # 8 more invocations, a second variant
    many = _count_queries(db, monkeypatch, lambda: pq.pipeline_variants(db._duck))
    assert many == few, f"{few} queries for 2 invocations, {many} for 10"

    variants = pq.pipeline_variants(db._duck)
    by_k = {v["constants"]["k"]: v["record_count"] for v in variants}
    assert by_k == {3.0: 2, 5.0: 8}


def test_function_variant_configs_query_count_does_not_grow(db, monkeypatch):
    """Grows invocations under ONE config. The count may still grow per
    CONFIG — each config reads where it iterated in one query
    (`iterated_keys_for_invocations`) — and a function has a handful of
    configs against hundreds of invocations."""
    _run(2)
    few = _count_queries(
        db, monkeypatch, lambda: pq.function_variant_configs(db._duck, "scale")
    )
    _run(8)  # same k: 6 more invocations, still one config
    many = _count_queries(
        db, monkeypatch, lambda: pq.function_variant_configs(db._duck, "scale")
    )
    assert many == few, f"{few} queries for 2 invocations, {many} for 8"

    [config] = pq.function_variant_configs(db._duck, "scale")
    assert config["constants"] == {"k": 3.0}
    assert len(config["invocation_ids"]) == 8
