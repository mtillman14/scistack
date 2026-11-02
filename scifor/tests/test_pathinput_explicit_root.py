"""A PathInput resolves against an explicit root, with no global involved.

Before plan P3 stage 7 the only way to say "this project's files are HERE"
was ``set_project_root``, one process-wide pin, so two projects could not be
resolved side by side in one process (design review 02, R4). Now the root is
decided once at the entry point -- ``for_each(project_root=...)``, else the
pinned root -- and bound into each PathInput (``PathInput.bound_to``).
Guards that two roots stay independent, that identity never depends on the
root, and that nothing touches the global.
"""

import pytest

from scifor import PathInput, for_each, set_schema
from scifor.foreach import bind_path_inputs
from scifor.project.root import clear_project_root, get_project_root


@pytest.fixture(autouse=True)
def _no_pin():
    clear_project_root()
    yield
    clear_project_root()


@pytest.fixture
def two_projects(tmp_path):
    roots = []
    for name in ("alpha", "beta"):
        root = tmp_path / name
        (root / "S1").mkdir(parents=True)
        (root / "S1" / "raw.csv").write_text(name, encoding="utf-8")
        roots.append(root)
    return roots


def test_two_roots_resolve_independently(two_projects):
    alpha, beta = two_projects
    raw = PathInput("{subject}/raw.csv", name="Raw")

    in_alpha = raw.bound_to(alpha)
    in_beta = raw.bound_to(beta)

    assert in_alpha.load(subject="S1") == (alpha / "S1" / "raw.csv").resolve()
    assert in_beta.load(subject="S1") == (beta / "S1" / "raw.csv").resolve()
    assert get_project_root() is None


def test_binding_keeps_identity(two_projects):
    raw = PathInput("{subject}/raw.csv", name="Raw")
    bound = raw.bound_to(two_projects[0])
    assert bound.to_key() == raw.to_key()
    assert bound.to_spec() == raw.to_spec()
    assert raw.bound_to(None) is raw


def test_discovery_walks_the_bound_root(two_projects):
    alpha, _ = two_projects
    assert PathInput("{subject}/raw.csv", name="Raw").bound_to(alpha).discover() == [
        {"subject": "S1"}
    ]


def test_for_each_project_root_reaches_the_function(two_projects):
    set_schema(["subject"])
    seen = {}

    def read(path):
        seen[path.parent.parent.name] = path.read_text(encoding="utf-8")
        return 1

    for root in two_projects:
        for_each(
            read,
            inputs={"path": PathInput("{subject}/raw.csv", name="Raw")},
            subject=["S1"],
            project_root=root,
        )

    assert seen == {"alpha": "alpha", "beta": "beta"}
    assert get_project_root() is None


def test_bind_path_inputs_leaves_other_inputs_alone(two_projects):
    raw = PathInput("{subject}/raw.csv", name="Raw")
    bound = bind_path_inputs({"path": raw, "k": 3}, two_projects[0])
    assert bound["k"] == 3
    assert bound["path"] is not raw
    assert bound["path"].load(subject="S1").read_text(encoding="utf-8") == "alpha"
