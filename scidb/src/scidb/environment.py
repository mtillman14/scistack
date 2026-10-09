"""
Installing what an imported project needs -- safely, or not at all
(portability Stage 10d; ``.claude/plan-portability.md``; user decisions
2026-10-09).

The rule: install only what is MISSING, only after the user trusted the
bundle (the caller's job: this module never asks), only into a virtual or
conda environment, and only when a full check shows nothing installed would
change. Otherwise install nothing and say why. The import never depends on
this: a stopped or failed install is a report, not an error.

1. **Check** -- ``pip install --dry-run --report`` resolves the whole tree of
   *requirements*. Any resolved package that is already installed (it would
   be upgraded, downgraded or replaced) or that is one of SciStack's own
   distributions stops everything.
2. **Install exactly that** -- the resolved list, pinned, with
   ``--no-deps``: pip cannot resolve differently between the check and the
   install. Dependencies (pytorch, say) are in the list, so they ARE
   installed.
3. **Roll back** -- pip is not transactional; since everything installed was
   new, a failure uninstalls exactly what went in.

Every pip invocation goes through :func:`_pip` (one owner: logging, the
interpreter, and the seam tests replace).
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

from scistacklog import Log

#: SciStack's own distributions: never installed, upgraded or replaced by an
#: import (the running SciStack is not the bundle's to change).
PROTECTED = frozenset({
    "scistack", "scistack-gui", "scistack-db", "scidb", "scifor", "scimatlab",
    "sciduckdb", "scilineage", "scihist", "scistacklog", "scistackplot",
    "scistackplotdb", "scicanonicalhash", "scidb-net",
})


def canonical(name: str) -> str:
    """PEP 503 normalised distribution name."""
    return re.sub(r"[-_.]+", "-", name).lower()


@dataclass
class InstallReport:
    #: "installed" | "nothing" | "stopped" | "failed" | "skipped"
    status: str
    requirements: list[str] = field(default_factory=list)
    installed: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)
    reason: str = ""
    #: What to run by hand when the install did not happen.
    command: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _pip(args: list[str], *, timeout: "float | None" = None) -> subprocess.CompletedProcess:
    """Run ``<this python> -m pip <args>``; the one place pip is invoked."""
    cmd = [sys.executable, "-m", "pip", *args]
    Log.info(f"[environment] pip: {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    tail = (proc.stdout + proc.stderr).strip().splitlines()[-15:]
    Log.info(f"[environment] pip exited {proc.returncode}" + ("\n  " + "\n  ".join(tail) if tail else ""))
    return proc


def in_virtual_env() -> bool:
    """A venv, virtualenv or conda environment (never a system Python)."""
    import os

    return sys.prefix != getattr(sys, "base_prefix", sys.prefix) or bool(os.environ.get("CONDA_PREFIX"))


def _installed_version(name: str) -> "str | None":
    import importlib.metadata

    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _source_args(find_links: "Path | None") -> list[str]:
    """The bundle's wheelhouse is searched FIRST, the package index still
    serves what it does not hold (a library's wheels are local; pytorch is
    not in a wheelhouse)."""
    return ["--find-links", str(find_links)] if find_links else []


def _manual_command(requirements: list[str], find_links: "Path | None") -> str:
    parts = [f'"{sys.executable}"', "-m", "pip", "install", *_source_args(find_links)]
    parts += [f'"{r}"' for r in requirements]
    return " ".join(parts)


def plan_install(requirements: list[str], *, find_links: "Path | None" = None) -> tuple[list[tuple[str, str]], list[str], str]:
    """Resolve *requirements* without installing: ``([(name, version) to
    install], [conflict, ...], error)``. A conflict is a resolved package
    that is already installed (at any version) or protected."""
    with tempfile.TemporaryDirectory() as tmp:
        report_path = Path(tmp) / "report.json"
        proc = _pip([
            "install", "--dry-run", "--quiet", "--report", str(report_path),
            *_source_args(find_links), *requirements,
        ])
        if proc.returncode != 0 or not report_path.is_file():
            tail = (proc.stderr or proc.stdout).strip().splitlines()[-5:]
            return [], [], "pip could not resolve the requirements: " + " | ".join(tail)
        report = json.loads(report_path.read_text(encoding="utf-8"))
    resolved: list[tuple[str, str]] = []
    conflicts: list[str] = []
    for item in report.get("install") or []:
        meta = item.get("metadata") or {}
        name, version = meta.get("name"), meta.get("version")
        if not name or not version:
            continue
        resolved.append((name, version))
        here = _installed_version(name)
        if canonical(name) in PROTECTED:
            conflicts.append(f"{name} {version}: one of SciStack's own packages (installed: {here or 'no'})")
        elif here is not None:
            conflicts.append(f"{name}: installed {here}, the project's requirements would install {version}")
    return resolved, conflicts, ""


def install_missing(requirements: list[str], *, find_links: "Path | None" = None) -> InstallReport:
    """Install *requirements* (only ever MISSING ones are passed) after the
    full check; all or nothing. See the module docstring."""
    requirements = sorted(set(requirements))
    report = InstallReport(status="nothing", requirements=requirements)
    report.command = _manual_command(requirements, find_links) if requirements else ""
    if not requirements:
        Log.info("[environment] install_missing: nothing is missing")
        return report
    if not in_virtual_env():
        report.status = "skipped"
        report.reason = (
            f"{sys.executable} is not in a virtual or conda environment; SciStack never "
            "installs into a system Python. Activate a project environment and run the command."
        )
        Log.warn(f"[environment] install skipped: {report.reason}")
        return report

    resolved, conflicts, error = plan_install(requirements, find_links=find_links)
    if error:
        report.status = "stopped"
        report.reason = error
        Log.warn(f"[environment] install stopped: {error}")
        return report
    if conflicts:
        report.status = "stopped"
        report.conflicts = conflicts
        report.reason = (
            "installing would change packages already in this environment; nothing was "
            "installed (resolve the conflicts, or install by hand)"
        )
        Log.warn(f"[environment] install stopped: {len(conflicts)} conflict(s): {conflicts}")
        return report
    if not resolved:
        Log.info("[environment] install_missing: everything already satisfied")
        return report

    pins = [f"{n}=={v}" for n, v in resolved]
    proc = _pip(["install", "--no-deps", *_source_args(find_links), *pins])
    if proc.returncode != 0:
        went_in = [n for n, _ in resolved if _installed_version(n) is not None]
        if went_in:
            _pip(["uninstall", "-y", *went_in])
        left = [n for n in went_in if _installed_version(n) is not None]
        report.status = "failed"
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-5:]
        report.reason = "pip failed while installing: " + " | ".join(tail)
        if left:
            report.reason += f"; could NOT roll back: {left} (uninstall them by hand)"
        Log.error(f"[environment] install failed and rolled back {went_in}; left {left}")
        return report
    report.status = "installed"
    report.installed = pins
    Log.info(f"[environment] installed {len(pins)} package(s): {pins}")
    return report


# ---------------------------------------------------------------------------
# An imported project's requirements
# ---------------------------------------------------------------------------

#: Where an import keeps the exporter's environment record and wheelhouse.
STATE_DIR = ".scistack"
ENVIRONMENT_FILE = "environment.json"
WHEELHOUSE_DIR = "wheelhouse"


def install_project_requirements(root: "Path | str") -> InstallReport:
    """Install what the project at *root* needs and this environment lacks:
    the missing declared dependencies and libraries (at the exporter's
    versions), from the project's wheelhouse first. Call only after the
    user trusted the project. Never raises for an install problem."""
    from .bundle import check_environment

    root = Path(root)
    env_path = root / STATE_DIR / ENVIRONMENT_FILE
    if not env_path.is_file():
        Log.info(f"[environment] {root}: no recorded environment; nothing to install")
        return InstallReport(status="nothing", reason="the project records no environment")
    env = json.loads(env_path.read_text(encoding="utf-8"))
    check = check_environment(root, env)
    wheelhouse = root / STATE_DIR / WHEELHOUSE_DIR
    find_links = wheelhouse if wheelhouse.is_dir() and any(wheelhouse.glob("*.whl")) else None
    report = install_missing(list(check.get("missing_requirements") or []), find_links=find_links)
    unknown = [
        lib for lib in check.get("missing_libraries") or []
        if not ((env.get("libraries") or {}).get(lib) or {}).get("distribution")
    ]
    if unknown:
        report.reason = (report.reason + "; " if report.reason else "") + (
            f"librar(ies) {unknown} were not installed as distributions on the exporter's "
            "machine; install them by hand"
        )
    Log.info(f"[environment] install_project_requirements {root}: {report.status} {report.reason}")
    return report


# ---------------------------------------------------------------------------
# Wheels (the bundle's wheelhouse; `scistack library build`)
# ---------------------------------------------------------------------------


def _new_wheel(out_dir: Path, before: set, what: str) -> Path:
    new = sorted(set(out_dir.glob("*.whl")) - before)
    if not new:
        raise RuntimeError(f"pip produced no wheel for {what}")
    return new[-1]


def build_wheel(source_dir: "Path | str", out_dir: "Path | str") -> Path:
    """Build *source_dir* (a package with a pyproject.toml) into a wheel in
    *out_dir*; returns its path. Dependencies are not built: a wheelhouse
    holds the libraries, the index serves the rest."""
    source_dir, out_dir = Path(source_dir).resolve(), Path(out_dir).resolve()
    if not (source_dir / "pyproject.toml").is_file():
        raise RuntimeError(f"{source_dir} has no pyproject.toml")
    out_dir.mkdir(parents=True, exist_ok=True)
    before = set(out_dir.glob("*.whl"))
    proc = _pip(["wheel", "--no-deps", "--wheel-dir", str(out_dir), str(source_dir)])
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-5:]
        raise RuntimeError(f"building {source_dir} failed: " + " | ".join(tail))
    return _new_wheel(out_dir, before, str(source_dir))


def _local_source(dist) -> "Path | None":
    """The source folder of a distribution installed from a local directory
    (``pip install -e .`` or ``pip install ./lib``), from PEP 610's
    ``direct_url.json``; ``None`` for an index install."""
    from urllib.parse import unquote, urlparse

    try:
        raw = dist.read_text("direct_url.json")
    except Exception:
        raw = None
    if not raw:
        return None
    info = json.loads(raw)
    if "dir_info" not in info:
        return None
    url = urlparse(info.get("url", ""))
    if url.scheme != "file":
        return None
    path = Path(unquote(url.path))
    if sys.platform == "win32" and str(path).startswith("\\"):  # pragma: no cover
        path = Path(str(path).lstrip("\\"))
    return path if path.is_dir() else None


def library_wheel(package: str, out_dir: "Path | str") -> Path:
    """A wheel of the installed library whose import name is *package*:
    built from its source folder when it was installed from one (the usual
    case for a lab's own library), else fetched from the index at the
    installed version. Raises with the reason when neither works."""
    import importlib.metadata

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        dists = importlib.metadata.packages_distributions().get(package) or []
    except Exception:  # pragma: no cover
        dists = []
    if not dists:
        raise RuntimeError(f"{package} is not installed as a distribution (only on sys.path)")
    dist = importlib.metadata.distribution(dists[0])
    source = _local_source(dist)
    if source is not None:
        Log.info(f"[environment] wheel for {package}: building from its source {source}")
        return build_wheel(source, out_dir)
    pin = f"{dist.metadata['Name']}=={dist.version}"
    Log.info(f"[environment] wheel for {package}: fetching {pin} from the index")
    before = set(out_dir.glob("*.whl"))
    proc = _pip(["download", "--no-deps", "--only-binary=:all:", "--dest", str(out_dir), pin])
    if proc.returncode != 0:
        proc = _pip(["wheel", "--no-deps", "--wheel-dir", str(out_dir), pin])
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-3:]
        raise RuntimeError(f"could not fetch {pin}: " + " | ".join(tail))
    return _new_wheel(out_dir, before, pin)
