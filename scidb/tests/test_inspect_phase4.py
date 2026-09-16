"""Phase 4 tests: sql escape hatch, show --values previews, color/TTY."""

import json

import pandas as pd
import pytest
from scidb.inspect import Inspector, render
from scidb.inspect.cli import _want_color, main

from scidb import BaseVariable, configure_database

SCHEMA_KEYS = ["subject", "session"]


class P4Scalar(BaseVariable):
    schema_version = 1


class P4Frame(BaseVariable):
    schema_version = 1


def build_p4_db(db_path):
    db = configure_database(db_path, SCHEMA_KEYS)
    P4Scalar.save(42.5, subject="S01", session="1")
    P4Frame.save(
        pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}), subject="S01", session="1"
    )
    db.close()


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("p4") / "p4.duckdb"
    build_p4_db(path)
    return path


@pytest.fixture
def insp(db_path):
    with Inspector.open(db_path) as inspector:
        yield inspector


class TestSql:
    def test_select(self, insp):
        result = insp.sql(
            "SELECT COUNT(*) AS n FROM _record "
            "WHERE type NOT IN ('__constant__', '__pathinput__')"
        )
        assert result.columns == ["n"]
        assert result.rows == [[2]]
        assert result.row_count == 1

    def test_values_are_jsonable(self, insp):
        result = insp.sql("SELECT record_id, timestamp FROM _record_save")
        assert json.loads(
            json.dumps({"columns": result.columns, "rows": result.rows}, default=str)
        )
        # timestamps must arrive as ISO strings, not raw datetime objects
        assert all(isinstance(row[1], str) for row in result.rows)

    def test_null_is_none_not_nan(self, insp):
        """A NULL DOUBLE must report as NULL, not as the number nan.

        `sql` used to fetch through pandas, which has no NULL for a float
        column — so a NULL arrived as NaN and printed as `nan`, making a NULL
        indistinguishable from a stored NaN. That sent a live investigation
        after a save-path difference that did not exist (2026-09-15).
        """
        result = insp.sql("SELECT CAST(NULL AS DOUBLE) AS v, CAST(1.5 AS DOUBLE) AS w")
        assert result.rows == [[None, 1.5]]

    def test_null_renders_as_the_word_null(self, db_path, capsys):
        assert (
            main(
                [
                    "--db",
                    str(db_path),
                    "sql",
                    "SELECT CAST(NULL AS DOUBLE) AS v",
                ]
            )
            == 0
        )
        out = capsys.readouterr().out
        assert "NULL" in out
        assert "nan" not in out

    def test_write_rejected_read_only(self, insp):
        with pytest.raises(Exception, match="(?i)read.only"):
            insp.sql("CREATE TABLE _sneaky (i INTEGER)")

    def test_cli_table_and_rowcount(self, db_path, capsys):
        assert (
            main(
                [
                    "--db",
                    str(db_path),
                    "sql",
                    "SELECT variable_name FROM _variables ORDER BY 1",
                ]
            )
            == 0
        )
        out = capsys.readouterr().out
        assert "P4Frame" in out and "P4Scalar" in out
        assert "rows)" in out

    def test_cli_json(self, db_path, capsys):
        assert (
            main(
                [
                    "--db",
                    str(db_path),
                    "sql",
                    "SELECT COUNT(*) AS n FROM _variables",
                    "--json",
                ]
            )
            == 0
        )
        payload = json.loads(capsys.readouterr().out)
        assert payload["rows"] == [[2]]

    def test_cli_write_fails(self, db_path, capsys):
        assert main(["--db", str(db_path), "sql", "DELETE FROM _record_save"]) == 1
        assert "read-only" in capsys.readouterr().err.lower()


class TestValuePreviews:
    def test_scalar_preview(self, insp):
        (rec,) = insp.records("P4Scalar", include_values=True)
        assert rec.value_preview is not None
        assert "42.5" in rec.value_preview

    def test_dataframe_preview_summarizes_rows(self, insp):
        (rec,) = insp.records("P4Frame", include_values=True)
        assert "3 rows" in rec.value_preview
        assert "a" in rec.value_preview and "b" in rec.value_preview

    def test_preview_off_by_default(self, insp):
        (rec,) = insp.records("P4Scalar")
        assert rec.value_preview is None

    def test_preview_truncation(self, insp):
        (rec,) = insp.records("P4Scalar", include_values=True, preview_len=3)
        assert len(rec.value_preview) <= 4  # 3 chars + ellipsis

    def test_render_adds_value_column(self, insp):
        recs = insp.records("P4Scalar", include_values=True)
        text = render.render_records(recs, SCHEMA_KEYS)
        assert "value" in text and "42.5" in text

    def test_cli_show_values(self, db_path, capsys):
        assert (
            main(["--db", str(db_path), "show", "P4Scalar", "--values", "--json"]) == 0
        )
        payload = json.loads(capsys.readouterr().out)
        assert "42.5" in payload[0]["value_preview"]


class TestColor:
    def test_ansi_style_colors_state_tags(self, insp):
        from scidb.inspect.api import NodeStateSummary

        colored = render.with_ansi_colors(render.DEFAULT_STYLE)
        states = [
            NodeStateSummary(
                function_name="f",
                state="green",
                state_basis="stored_hash",
                up_to_date=1,
                missing=0,
            )
        ]
        text = render.render_node_states(states, style=colored)
        assert "\x1b[32m" in text and "\x1b[0m" in text

    def test_default_style_has_no_ansi(self, insp):
        from scidb.inspect.api import NodeStateSummary

        states = [
            NodeStateSummary(
                function_name="f",
                state="red",
                state_basis="stored_hash",
                up_to_date=0,
                missing=1,
            )
        ]
        text = render.render_node_states(states)
        assert "\x1b[" not in text

    def test_want_color_matrix(self):
        assert _want_color(no_color=False, isatty=True) is True
        assert _want_color(no_color=True, isatty=True) is False
        assert _want_color(no_color=False, isatty=False) is False

    def test_captured_cli_output_is_plain(self, db_path, capsys):
        # capsys stdout is not a TTY → no ANSI codes in piped output.
        assert main(["--db", str(db_path), "status"]) == 0
        assert "\x1b[" not in capsys.readouterr().out
