"""scidb.environment: install what an imported project lacks -- all or
nothing, after a full check (portability Stage 10d, user decisions
2026-10-09). pip is never really run here: ``_pip`` is the one seam."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scidb import environment


class FakePip:
    """Stands in for ``environment._pip``: answers a dry run with *resolved*
    and records every call; *fail_install* makes the real install fail."""

    def __init__(self, resolved, *, fail_install=False, installs=None):
        self.resolved = resolved
        self.fail_install = fail_install
        self.calls: list[list[str]] = []
        self.installed: set[str] = set()
        self.installs = installs if installs is not None else []

    def __call__(self, args, timeout=None):
        self.calls.append(list(args))
        if "--dry-run" in args:
            report = Path(args[args.index("--report") + 1])
            report.write_text(json.dumps({"install": [
                {"metadata": {"name": n, "version": v}} for n, v in self.resolved
            ]}))
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "install":
            names = [a.split("==")[0] for a in args if "==" in a]
            if self.fail_install:
                self.installed |= set(names[:1])  # half went in, then pip failed
                return subprocess.CompletedProcess(args, 1, "", "build failed")
            self.installed |= set(names)
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "uninstall":
            self.installed -= set(args[2:])
            return subprocess.CompletedProcess(args, 0, "", "")
        return subprocess.CompletedProcess(args, 0, "", "")


@pytest.fixture
def venv(monkeypatch):
    monkeypatch.setattr(environment, "in_virtual_env", lambda: True)


def _install(monkeypatch, fake, installed_before=()):
    monkeypatch.setattr(environment, "_pip", fake)
    monkeypatch.setattr(
        environment, "_installed_version",
        lambda name: "0.1" if (name in installed_before or name in fake.installed) else None,
    )


def test_nothing_missing_runs_no_pip(monkeypatch, venv):
    fake = FakePip([])
    _install(monkeypatch, fake)
    assert environment.install_missing([]).status == "nothing"
    assert fake.calls == []


def test_a_system_python_is_never_installed_into(monkeypatch):
    fake = FakePip([("newpkg", "1.0")])
    _install(monkeypatch, fake)
    monkeypatch.setattr(environment, "in_virtual_env", lambda: False)
    r = environment.install_missing(["newpkg==1.0"])
    assert r.status == "skipped" and "virtual" in r.reason
    assert fake.calls == [] and "pip" in r.command


def test_any_change_to_an_installed_package_stops_everything(monkeypatch, venv):
    fake = FakePip([("newpkg", "1.0"), ("numpy", "1.26.0")])
    _install(monkeypatch, fake, installed_before={"numpy"})
    r = environment.install_missing(["newpkg==1.0"])
    assert r.status == "stopped"
    assert any("numpy" in c for c in r.conflicts)
    assert all("--dry-run" in c for c in fake.calls)  # nothing but the check ran
    assert fake.installed == set()


def test_scistacks_own_packages_are_never_touched(monkeypatch, venv):
    fake = FakePip([("scidb", "9.9")])
    _install(monkeypatch, fake)
    r = environment.install_missing(["something==1"])
    assert r.status == "stopped" and "SciStack" in r.conflicts[0]


def test_installs_exactly_the_checked_set_pinned_without_resolving_again(monkeypatch, venv):
    fake = FakePip([("newpkg", "1.0"), ("torchish", "2.4.1")])
    _install(monkeypatch, fake)
    r = environment.install_missing(["newpkg==1.0"])
    assert r.status == "installed"
    install = fake.calls[-1]
    assert install[0] == "install" and "--no-deps" in install
    assert {"newpkg==1.0", "torchish==2.4.1"} <= set(install)  # dependencies included
    assert fake.installed == {"newpkg", "torchish"}


def test_a_failed_install_is_rolled_back(monkeypatch, venv):
    fake = FakePip([("newpkg", "1.0"), ("other", "2.0")], fail_install=True)
    _install(monkeypatch, fake)
    r = environment.install_missing(["newpkg==1.0"])
    assert r.status == "failed" and "build failed" in r.reason
    assert fake.installed == set()  # what went in came out again
    assert any(c[0] == "uninstall" for c in fake.calls)


def test_the_wheelhouse_is_searched_first_but_the_index_still_serves(monkeypatch, venv, tmp_path):
    fake = FakePip([("newpkg", "1.0")])
    _install(monkeypatch, fake)
    environment.install_missing(["newpkg==1.0"], find_links=tmp_path)
    for call in fake.calls:
        assert "--find-links" in call and "--no-index" not in call


def test_an_imported_project_installs_only_whats_missing(monkeypatch, venv, tmp_path):
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "p"\nversion = "0.1"\ndependencies = ["pytest", "notinstalled_t10d"]\n'
    )
    state = tmp_path / ".scistack"
    state.mkdir()
    (state / "environment.json").write_text(json.dumps({
        "python": "3.11", "distributions": {"notinstalled-t10d": "2.0", "pytest": "0.0.1"},
        "libraries": {},
    }))
    seen = {}

    def fake_install(reqs, *, find_links=None):
        seen["reqs"], seen["find_links"] = reqs, find_links
        return environment.InstallReport(status="installed", requirements=reqs)

    monkeypatch.setattr(environment, "install_missing", fake_install)
    r = environment.install_project_requirements(tmp_path)
    assert r.status == "installed"
    # pytest is installed here (at another version: reported, never changed).
    assert seen["reqs"] == ["notinstalled-t10d==2.0"]
    assert seen["find_links"] is None


def test_a_project_without_a_recorded_environment_installs_nothing(tmp_path):
    assert environment.install_project_requirements(tmp_path).status == "nothing"


# --- wheels ------------------------------------------------------------------


def test_build_wheel_runs_pip_wheel_without_deps(monkeypatch, tmp_path):
    src = tmp_path / "lib"
    src.mkdir()
    (src / "pyproject.toml").write_text("[project]\nname='lib'\n")
    out = tmp_path / "dist"

    def fake(args, timeout=None):
        assert args[:2] == ["wheel", "--no-deps"]
        Path(args[args.index("--wheel-dir") + 1], "lib-0.1-py3-none-any.whl").write_bytes(b"w")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(environment, "_pip", fake)
    assert environment.build_wheel(src, out).name == "lib-0.1-py3-none-any.whl"
    with pytest.raises(RuntimeError, match="pyproject"):
        environment.build_wheel(tmp_path / "nowhere", out)


def test_a_locally_installed_library_is_built_from_its_source(tmp_path):
    class Dist:
        def read_text(self, name):
            assert name == "direct_url.json"
            return json.dumps({"url": tmp_path.as_uri(), "dir_info": {"editable": True}})

    assert environment._local_source(Dist()) == tmp_path

    class IndexDist:
        def read_text(self, name):
            return json.dumps({"url": "https://files.example/x.whl", "archive_info": {}})

    assert environment._local_source(IndexDist()) is None
