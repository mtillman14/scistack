"""`scidb.bindings` — the typed spine for record ids (Stage 2a).

Pure: no database. The load-bearing assertion is that a `Binding` hashes
into `compute_invocation_id` as the same bytes the tuple it replaces did;
`test_identity_parity.py` pins the same thing end to end.
"""

from __future__ import annotations

import math

import pytest

from scidb.bindings import (
    RID_PREFIX,
    Binding,
    InputBinding,
    InputKind,
    RunBindings,
    is_rid_column,
    param_of,
    rid_column,
    rid_columns,
)
from scidb.provenance import compute_invocation_id


class TestSpelling:
    def test_the_prefix_is_spelled_here_and_nowhere_else(self):
        assert rid_column("value") == f"{RID_PREFIX}value" == "__rid_value"

    def test_param_of_reads_either_spelling(self):
        assert param_of("__rid_value") == "value"
        assert param_of("value") == "value"
        assert param_of(None) == "None"  # str() of anything, never an error

    def test_rid_columns_picks_the_rid_columns_in_order(self):
        assert rid_columns(["subject", "__rid_b", "a", "__rid_a"]) == ["__rid_b", "__rid_a"]
        assert not is_rid_column("__vsig_value")


class TestBinding:
    def test_iterates_as_the_three_tuple(self):
        assert tuple(Binding("p", "r", None)) == ("p", "r", None)

    def test_coerce_accepts_both_tuple_arities_and_itself(self):
        b = Binding("p", "r", "sel")
        assert Binding.coerce(b) is b
        assert Binding.coerce(("p", "r")) == Binding("p", "r", None)
        assert Binding.coerce(("p", "r", "sel")) == b
        with pytest.raises(ValueError):
            Binding.coerce(("p",))

    def test_same_bytes_as_the_tuple_it_replaces(self):
        tuples = [("value", "r1", '{"columns": ["a"]}'), ("scale", "c1")]
        bindings = [Binding("value", "r1", '{"columns": ["a"]}'), Binding("scale", "c1")]
        assert compute_invocation_id("h", None, False, tuples) == compute_invocation_id(
            "h", None, False, bindings
        )


class TestRunBindings:
    def _run(self):
        return RunBindings(
            {
                "value": InputBinding("value", InputKind.ITERATE, "Wide", '{"columns": ["a"]}'),
                "ref": InputBinding("ref", InputKind.PINNED, "Ref", pinned_rid="fixed1"),
                "cols": InputBinding("cols", InputKind.LINEAGE_ONLY, "Table"),
            }
        )

    def test_full_iteration_reads_the_combo_and_appends_the_pin(self):
        edges = self._run().for_combo({"subject": "01", "__rid_value": "r1", "__rid_cols": "c9"})
        assert set(edges) == {
            Binding("value", "r1", '{"columns": ["a"]}'),
            Binding("cols", "c9", None),
            Binding("ref", "fixed1", None),
        }

    def test_a_pin_already_on_the_combo_is_not_doubled(self):
        edges = self._run().for_combo({"__rid_ref": "fixed1", "__rid_value": "r1"})
        assert sum(1 for e in edges if e.param == "ref") == 1

    def test_aggregation_reads_the_pooled_set_under_the_real_name(self):
        edges = self._run().for_combo(
            {"subject": "01"}, pooled={"__rid_value": ["r1", "r2"], "cols": ["c1"]}
        )
        assert [e for e in edges if e.param == "value"] == [
            Binding("value", "r1", '{"columns": ["a"]}'),
            Binding("value", "r2", '{"columns": ["a"]}'),
        ]
        assert Binding("cols", "c1", None) in edges
        assert Binding("ref", "fixed1", None) in edges

    def test_a_nan_or_none_rid_is_not_an_edge(self):
        edges = self._run().for_combo({"__rid_value": math.nan, "__rid_cols": None})
        assert {e.param for e in edges} == {"ref"}

    def test_kinds_are_queryable(self):
        run = self._run()
        assert [b.param for b in run.of_kind(InputKind.PINNED)] == ["ref"]
        assert run["value"].column == "__rid_value"
        assert "cols" in run and "nope" not in run
