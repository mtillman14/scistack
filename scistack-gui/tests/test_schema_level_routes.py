"""Every run route iterates the level ONE owner decides.

docs/claude/cleanup-audit.md §4.2 / .claude/plan-schema-level-default.md: the
Python Run, the Python pipeline, the generated MATLAB command, the MATLAB
pipeline script and the settings panel all go through
``execution_service.default_schema_level`` -> ``scidb.schema_level``.
"""

import ast
from pathlib import Path

from scidb.schema_level import SchemaLevel

from scistack_gui.api.matlab_command import generate_matlab_command

KEYS = ["subject", "session", "speed", "trial", "cycle"]


def _cmd(**kw):
    return generate_matlab_command(
        function_name="loadDemographics",
        db_path="/data/experiment.duckdb",
        schema_keys=KEYS,
        **kw,
    )


class TestMatlabGeneratorRendersTheResolvedLevel:
    def test_one_call_emits_no_schema_kwargs(self):
        """loadDemographics: one placeholder-less PathInput. The owner says
        one call; the command must not iterate anything (it ran ×714)."""
        cmd = _cmd(schema_level=SchemaLevel.one_call())
        for key in KEYS:
            assert f"'{key}', []" not in cmd

    def test_keys_emit_exactly_those(self):
        cmd = _cmd(schema_level=SchemaLevel.of(["subject", "trial"], KEYS))
        assert "'subject', []" in cmd and "'trial', []" in cmd
        for key in ("session", "speed", "cycle"):
            assert f"'{key}', []" not in cmd

    def test_a_stored_empty_list_is_one_call_here_too(self):
        cmd = _cmd(schema_level=[])
        for key in KEYS:
            assert f"'{key}', []" not in cmd

    def test_an_unresolved_level_warns(self, caplog):
        """Only a direct caller with no database can get here; the service
        routes always resolve first."""
        import logging

        with caplog.at_level(logging.WARNING):
            _cmd(schema_level=None)
        assert "UNRESOLVED" in caplog.text


def test_as_table_plays_no_part_in_the_level(populated_db):
    """as_table is the format inputs arrive in, not how many calls there are.
    The Python Run used to read "as_table, no level" as one call; no other
    route did (cleanup-audit F32, dropped 2026-09-23)."""
    import inspect

    from scistack_gui.services.execution_service import default_schema_level

    assert "as_table" not in inspect.signature(default_schema_level).parameters
    level, rule = default_schema_level(populated_db, "never_ran", [])
    assert level.for_each_schema_keys() == ["subject", "session"]
    assert "as_table" not in rule


# ---------------------------------------------------------------------------
# Guard: nobody re-implements the precedence (plan Stage 6)
# ---------------------------------------------------------------------------

_INGREDIENTS = {
    "recorded_schema_keys",
    "finest_schema_keys",
    "variable_schema_keys",
    "resolve_schema_level",
    "input_levels",
}
_OWNER = ("services/execution_service.py", "default_schema_level")


def test_only_the_owner_calls_the_level_ingredients():
    """A second place that combines "recorded", "inputs" or "stated" into a
    level is how the MATLAB routes drifted (F22). Every call must sit in
    ``execution_service.default_schema_level``."""
    root = Path(__file__).resolve().parent.parent / "scistack_gui"
    offenders = []
    for path in root.rglob("*.py"):
        rel = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(fn):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
                if name in _INGREDIENTS and (rel, fn.name) != _OWNER:
                    offenders.append(f"{rel}:{node.lineno} {fn.name} calls {name}")
    assert not offenders, offenders
