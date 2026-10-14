"""Import every module of every installed SciStack package in a FRESH venv.

    <fresh-venv>/bin/python -I .github/scripts/smoke_install.py <dist-dir>

Run it after `pip install --find-links <dist-dir> scistack==V scihist==V` into
an empty venv, so only what the packages DECLARE is present. Development (one
venv, every package editable with --no-deps) can never see an undeclared
dependency; a user's `pip install` sees it at once. Example: scistack-db imported
scilineage without declaring it, and the GUI failed at startup for every user.

Fails when a module raises ModuleNotFoundError for another SciStack package
(an undeclared sibling). A missing third-party module, or any other import
error, is printed as a warning but does not fail the check: optional extras
(seaborn, the MATLAB engine) are legitimately absent here.

Stdlib only: the fresh venv has nothing else.
"""

from __future__ import annotations

import importlib
import importlib.metadata as md
import re
import sys
import traceback
import zipfile
from pathlib import Path


def canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def modules_of(dist: md.Distribution) -> list[str]:
    mods = []
    for f in dist.files or []:
        parts = f.parts
        if f.suffix != ".py" or ".." in parts or parts[0].endswith((".dist-info", ".data")):
            continue
        if "tests" in parts or parts[-1] == "__main__.py":
            continue
        mod = ".".join(parts)[: -len(".py")]
        mods.append(mod[: -len(".__init__")] if mod.endswith(".__init__") else mod)
    return sorted(set(mods))


def main(dist_dir: Path) -> int:
    wanted = sorted({canonical(w.name.split("-")[0]) for w in dist_dir.glob("*.whl")})
    if not wanted:
        print(f"::error::no wheels in {dist_dir}")
        return 1
    installed: dict[str, md.Distribution] = {}
    for name in wanted:
        try:
            installed[name] = md.distribution(name)
        except md.PackageNotFoundError:
            print(f"  {name}: not installed by this smoke install, skipped")
    # From the WHEELS, not the installed set: an undeclared sibling is exactly
    # the one that is not installed.
    internal_tops = set()
    for w in dist_dir.glob("*.whl"):
        with zipfile.ZipFile(w) as zf:
            internal_tops |= {
                n.split("/")[0].removesuffix(".py") for n in zf.namelist()
                if not n.split("/")[0].endswith((".dist-info", ".data", ".pth"))
            }
    print(f"internal import names: {', '.join(sorted(internal_tops))}")

    errors, warnings, imported = [], [], 0
    for name, dist in sorted(installed.items()):
        mods = modules_of(dist)
        print(f"  {name} {dist.version}: {len(mods)} modules")
        for mod in mods:
            try:
                importlib.import_module(mod)
                imported += 1
            except ModuleNotFoundError as e:
                missing = (e.name or "").split(".")[0]
                msg = f"{name}: import {mod} -> No module named {e.name!r}"
                (errors if missing in internal_tops else warnings).append(msg)
            except BaseException as e:  # noqa: BLE001 - report, keep going
                warnings.append(f"{name}: import {mod} -> {type(e).__name__}: {e}")
                traceback.print_exc(limit=3)

    for w in warnings:
        print(f"::warning::{w}")
    for e in errors:
        print(f"::error::{e} (undeclared SciStack dependency: add it to [project] "
              f"dependencies AND override-versions-of in that package's pyproject.toml)")
    print(f"{imported} modules imported, {len(warnings)} warning(s), {len(errors)} error(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(Path(sys.argv[1])))
