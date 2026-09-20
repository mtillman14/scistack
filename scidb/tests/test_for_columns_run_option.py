"""`for_columns` reported as a run option.

`iterate` is an execution mode that lived only inside a column list, which is
how it stayed invisible while being silently dropped. It is now recorded on
`_invocation.for_columns`, spelled by `run_options_label`, and reported by
`pipeline_variants[].run_options` — see
`docs/claude/for-columns-iteration.md` §"as a run option".

It is deliberately NOT a new identity term: the selector already carries
`iterate` and `compute_invocation_id` folds that in.
"""

from __future__ import annotations

import pandas as pd
import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each
from scidb.provenance import compute_invocation_id
from scidb.provenance_query import run_options_label


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "runopts.duckdb", ["subject", "trial"])
    yield db
    _scifor.set_schema([])
    db.close()


class TestLabel:
    def test_the_common_case_is_unchanged(self):
        assert run_options_label(False, None) == "distribute=false"
        assert run_options_label(False, None, None) == "distribute=false"
        assert run_options_label(False, None, []) == "distribute=false"

    def test_per_column_runs_are_distinguishable(self):
        assert (
            run_options_label(False, None, ["value"])
            == "distribute=false, for_columns=[value]"
        )

    def test_it_sits_after_as_table(self):
        assert run_options_label(True, ["cycles"], ["value"]) == (
            "distribute=true, as_table=[cycles], for_columns=[value]"
        )


class TestIdentity:
    def test_iterate_is_not_counted_twice(self):
        """The selector already distinguishes a per-column call, so nothing
        about invocation identity changes when the run option is recorded."""
        whole = compute_invocation_id("h", None, False, [("value", "r1", None)])
        per_col = compute_invocation_id(
            "h", None, False, [("value", "r1", '{"columns": ["a"], "iterate": true}')]
        )
        assert whole != per_col


class TestRecorded:
    def test_a_for_columns_run_reports_the_option(self, db):
        class Wide(BaseVariable):
            pass

        class Doubled(BaseVariable):
            pass

        Wide.save(pd.DataFrame({"a": [1.0], "b": [2.0]}), subject="01", trial="1")

        def double(value):
            return float(pd.Series(value).iloc[0]) * 2

        for_each(double, {"value": Wide.for_columns()}, [Doubled], subject=[], trial=[])

        variants = [
            v for v in db.list_pipeline_variants() if v["function_name"] == "double"
        ]
        assert variants, "no variant recorded for double"
        assert "for_columns=[value]" in variants[0]["run_options"]

    def test_a_whole_table_run_says_nothing_about_columns(self, db):
        class Plain(BaseVariable):
            pass

        class Summed(BaseVariable):
            pass

        # Two data columns: a table with ONE data column reaches the function
        # as that column alone (the 1-D collapse), not as a frame. The body
        # uses len() so it is indifferent to which shape arrives — this test
        # is about what gets RECORDED, not about the function's input type.
        Plain.save(
            pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]}), subject="01", trial="1"
        )

        def total(value):
            return float(len(value))

        for_each(total, {"value": Plain}, [Summed], subject=[], trial=[])

        variants = [
            v for v in db.list_pipeline_variants() if v["function_name"] == "total"
        ]
        assert variants, (
            "no variant recorded for total; runs="
            f"{db._duck._fetchall('SELECT function_name, run_id FROM _run')} "
            f"records={db._duck._fetchall('SELECT type, count(*) FROM _record GROUP BY type')}"
        )
        assert "for_columns" not in variants[0]["run_options"]
