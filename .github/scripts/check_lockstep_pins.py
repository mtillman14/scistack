"""Fail unless every built SciStack wheel pins its siblings to its own version.

    python .github/scripts/check_lockstep_pins.py <dist-dir>

Releases are lockstep: one tag, one version for every package. Each package's
pyproject.toml lists its siblings unversioned and the hatch-dependency-coversion
hook rewrites them to "==<version>" at build time, but only for the names in
that file's override-versions-of list. A sibling missing from that list ships
unversioned; this check is what catches it.

The internal packages are exactly the wheels in <dist-dir>, so the build loop
in the workflow stays the single owner of that list.
"""

from __future__ import annotations

import sys
import zipfile
from email.parser import Parser
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def wheel_metadata(wheel: Path):
    with zipfile.ZipFile(wheel) as zf:
        name = next(n for n in zf.namelist() if n.endswith(".dist-info/METADATA"))
        return Parser().parsestr(zf.read(name).decode())


def main(dist: Path) -> int:
    wheels = sorted(dist.glob("*.whl"))
    if not wheels:
        print(f"::error::no wheels in {dist}")
        return 1
    metas = {canonicalize_name(m["Name"]): m for m in map(wheel_metadata, wheels)}
    versions = {m["Version"] for m in metas.values()}
    errors = []
    if len(versions) != 1:
        errors.append(f"wheels disagree on the version: {sorted(versions)}")

    for name, meta in sorted(metas.items()):
        version = meta["Version"]
        pins = []
        for line in meta.get_all("Requires-Dist") or []:
            req = Requirement(line)
            dep = canonicalize_name(req.name)
            if dep not in metas:
                continue
            pins.append(f"{req.name}{req.specifier}")
            if str(req.specifier) != f"=={version}":
                errors.append(
                    f"{name} {version} requires {line!r}; expected {req.name}=={version} "
                    f"(add {req.name!r} to override-versions-of in its pyproject.toml)"
                )
        print(f"  {name} {version}: {', '.join(pins) or '(no sibling dependencies)'}")

    for e in errors:
        print(f"::error::{e}")
    print(f"{len(metas)} wheels, {len(errors)} problem(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(Path(sys.argv[1])))
