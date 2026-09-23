"""Node ids and edge handles are spelled in ``scistack_gui.ids`` only.

``var__``/``fn__``/``param__``/``pathInput__`` node ids and ``in__``/``out__``/
``param__`` handles were string literals at ~75 sites until 2026-09-23, two of
which built function-node ids by hand next to ``ids.fn_node_id`` and one of
which (``plot_service``) kept its own copy of the Parameter handle prefix. A
spelling change in one place then silently stopped matching everywhere else.

The guard is on the AST, not on text, so docstrings, comments and log
messages that merely MENTION ``var__RawEMG`` are fine; a string literal (or
the literal head of an f-string) that BEGINS with one of the prefixes is not.
"""

import ast
import re
from pathlib import Path

from scistack_gui import ids

PACKAGE = Path(__file__).resolve().parent.parent / "scistack_gui"
OWNER = PACKAGE / "ids.py"
PREFIXES = (
    ids.VAR_ID_PREFIX,
    ids.FN_ID_PREFIX,
    ids.PARAM_ID_PREFIX,
    ids.PATH_INPUT_ID_PREFIX,
    ids.IN_HANDLE_PREFIX,
    ids.OUT_HANDLE_PREFIX,
)


def _docstring_nodes(tree: ast.AST) -> set[int]:
    """ids of the Constant nodes that are docstrings or bare string
    statements (comment-like), which may mention a prefix freely."""
    skip: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list):
            continue
        for stmt in body:
            if (
                isinstance(stmt, ast.Expr)
                and isinstance(stmt.value, ast.Constant)
                and isinstance(stmt.value.value, str)
            ):
                skip.add(id(stmt.value))
    return skip


def _offenders(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    skip = _docstring_nodes(tree)
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            head = node.values[0] if node.values else None
            if isinstance(head, ast.Constant) and str(head.value).startswith(PREFIXES):
                found.append(f"{path.name}:{node.lineno} f-string {head.value!r}...")
                skip.add(id(head))
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in skip
            and node.value.startswith(PREFIXES)
        ):
            found.append(f"{path.name}:{node.lineno} {node.value!r}")
    return found


def test_no_inline_id_or_handle_spelling_outside_ids():
    offenders = []
    for path in sorted(PACKAGE.rglob("*.py")):
        if path == OWNER:
            continue
        offenders += _offenders(path)
    assert offenders == [], (
        "spell node ids / handles through scistack_gui.ids "
        "(var_node_id, fn_node_id, in_handle, ...):\n" + "\n".join(offenders)
    )


def test_the_guard_catches_what_it_is_for(tmp_path):
    """The guard must actually fire: a literal id, an f-string id and a
    literal handle are caught; a docstring mentioning one is not."""
    sample = tmp_path / "sample.py"
    sample.write_text(
        'def f(t):\n'
        '    """Mentions var__RawEMG freely."""\n'
        '    a = "fn__"\n'
        '    b = f"var__{t}"\n'
        '    return a, b, "in__x"\n',
        encoding="utf-8",
    )
    assert len(_offenders(sample)) == 3


def test_frontend_drop_handler_mints_the_same_prefixes():
    """The canvas mints a manual node's id (``{prefix}__{label}__{rand}``)
    before the backend sees it, so its type->prefix choice has to match
    ``ids.NODE_TYPE_PREFIXES`` -- graduation matches on that prefix."""
    tsx = (
        PACKAGE.parent / "frontend" / "src" / "components" / "DAG" / "PipelineDAG.tsx"
    ).read_text(encoding="utf-8")
    line = next(l for l in tsx.splitlines() if "const prefix = nodeType ===" in l)
    pairs = dict(re.findall(r"nodeType === '(\w+)' \? '(\w+)'", line))
    default = re.search(r": '(\w+)'\s*$", line.strip()).group(1)
    minted = {**pairs, "variableNode": default}
    assert {t: f"{p}__" for t, p in minted.items()} == ids.NODE_TYPE_PREFIXES


def test_constructors_round_trip_through_the_parsers():
    fn_id = ids.fn_node_id("bandpass", "0123456789abcdef")
    assert ids.parse_fn_node_id(fn_id) == ("bandpass", "0123456789abcdef")
    assert fn_id.startswith(ids.fn_nodes_prefix("bandpass"))
    assert ids.handle_name(ids.in_handle("signal")) == "signal"
    assert ids.handle_name(ids.out_handle("Filtered"), ids.OUT_HANDLE_PREFIX) == "Filtered"
    assert ids.handle_name(ids.param_handle("low_hz"), ids.PARAM_HANDLE_PREFIX) == "low_hz"
    assert ids.handle_name("out__x") is None
    assert ids.var_node_id("RawEMG") == "var__RawEMG"
    assert ids.path_input_node_id("EMG") == "pathInput__EMG"
    assert ids.param_node_id("low_hz") == "param__low_hz"


def test_the_root_scope_is_spelled_root_scope():
    """``ids.ROOT_SCOPE`` was ``"main"`` as a default argument in 14 places
    across five modules until 2026-09-23."""
    offenders = []
    for path in sorted(PACKAGE.rglob("*.py")):
        if path == OWNER:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        skip = _docstring_nodes(tree)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and node.value == ids.ROOT_SCOPE
                and id(node) not in skip
            ):
                offenders.append(f"{path.name}:{node.lineno}")
    assert offenders == [], "use ids.ROOT_SCOPE:\n" + "\n".join(offenders)
