"""The GUI's variant grouping keys go through ``constants_identity_key``.

A constant may be a ``dict`` (an inline table under ``[parameters]`` in
``scistack_entities.toml`` *is* the value), so ``tuple(sorted(constants.items()))``
is unhashable on real projects. scidb's sites moved to the one owner on
2026-09-14; six GUI sites kept their own copy, and
``_apply_manual_output_types`` crashed a node run with
``TypeError: unhashable type: 'dict'`` (2026-10-21).

See docs/claude/constants-are-not-scalars.md.
"""

from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

from scistack_gui.domain.variant_resolver import (
    deduplicate_variants,
    merge_pending_constants,
)
from scistack_gui.services.execution_service import _apply_manual_output_types

REPO = Path(__file__).resolve().parents[2]

CONFIG_A = {"fs": 2000, "channels": ["RHAM", "RTA"]}
CONFIG_B = {"fs": 1000, "channels": ["RHAM"]}


def _target(config, out="Recorded", hz=10):
    return {
        "input_types": {"signal": "Raw"},
        "output_type": out,
        "constants": {"delsys_config": config, "hz": hz},
    }


class TestApplyManualOutputTypes:
    def test_dict_constant_does_not_raise(self):
        out = _apply_manual_output_types([_target(CONFIG_A)], ["Drawn"], "test")
        assert [t["output_type"] for t in out] == ["Drawn"]

    def test_rows_differing_only_in_recorded_output_collapse(self):
        targets = [_target(dict(CONFIG_A), out="A"), _target(dict(CONFIG_A), out="B")]
        out = _apply_manual_output_types(targets, ["Drawn"], "test")
        assert len(out) == 1

    def test_different_dicts_stay_separate(self):
        targets = [_target(CONFIG_A), _target(CONFIG_B)]
        out = _apply_manual_output_types(targets, ["X", "Y"], "test")
        assert len(out) == 4


class TestVariantResolver:
    def test_deduplicate_with_dict_constant(self):
        targets = [_target(dict(CONFIG_A)), _target(dict(CONFIG_A)), _target(CONFIG_B)]
        assert len(deduplicate_variants(targets)) == 2

    def test_merge_pending_alongside_a_dict_constant(self):
        result = merge_pending_constants([_target(CONFIG_A)], {"hz": {"20", "10"}})
        assert sorted(t["constants"]["hz"] for t in result) == [10, 20]
        assert all(t["constants"]["delsys_config"] == CONFIG_A for t in result)


# ---------------------------------------------------------------------------
# Guard: no hand-built constants key anywhere in shipped source
# ---------------------------------------------------------------------------

#: The owner itself is the one place allowed to build the tuple.
_OWNER = "scidb/src/scidb/provenance.py"
_CONSTANTS = re.compile(r"\bconstants\b")


def _shipped_sources() -> list[str]:
    """Repo-relative paths of every package's shipped (non-test) source."""
    try:
        listed = subprocess.run(
            ["git", "ls-files", "*.py"], cwd=REPO, capture_output=True, text=True,
            check=True,
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):  # no git: scan everything
        listed = [p.relative_to(REPO).as_posix() for p in REPO.rglob("*.py")]
    return [
        p for p in listed
        if ("/src/" in p or p.startswith("scistack-gui/scistack_gui/"))
        and "/tests/" not in p
        and p != _OWNER
    ]


def _hand_built_keys(source: str) -> list[tuple[int, str]]:
    """``tuple(...)``/``frozenset(...)`` calls whose argument iterates a
    constants map's ``.items()``, as ``(lineno, call source)``."""
    hits = []
    for node in ast.walk(ast.parse(source)):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in ("tuple", "frozenset")
            and node.args
        ):
            continue
        arg = ast.unparse(node.args[0])
        if ".items()" in arg and _CONSTANTS.search(arg):
            hits.append((node.lineno, ast.unparse(node)))
    return hits


def test_no_hand_built_constants_key():
    hits = []
    for rel in _shipped_sources():
        try:
            source = (REPO / rel).read_text(encoding="utf-8")
            found = _hand_built_keys(source)
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        hits += [f"{rel}:{line}: {src}" for line, src in found]
    assert not hits, (
        "Group/dedup by constants through scidb.provenance.constants_identity_key "
        "(a constant may be a dict, so this tuple is unhashable):\n" + "\n".join(hits)
    )


def test_guard_catches_the_spellings_that_crashed():
    assert _hand_built_keys('k = tuple(sorted(v["constants"].items()))')
    assert _hand_built_keys('k = tuple(sorted((v.get("constants") or {}).items()))')
    assert _hand_built_keys("k = frozenset(constants.items())")
    # Not a constants map: record ids keyed by param are fine.
    assert not _hand_built_keys("k = tuple(sorted(constant_glue.items()))")
