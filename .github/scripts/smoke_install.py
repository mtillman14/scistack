"""Import every module of every installed SciStack package in a FRESH venv.

    <fresh-venv>/bin/python -I .github/scripts/smoke_install.py <dist-dir>

Run it after `pip install --find-links <dist-dir> scistack==V scihist==V` into
an empty venv, so only what the packages DECLARE is present. Development (one
venv, every package editable with --no-deps) can never see an undeclared
dependency; a user's `pip install` sees it at once. Example: scistack-db imported
scilineage without declaring it, and the GUI failed at startup for every user.

Fails on every ModuleNotFoundError except a module that is legitimately
absent: one a SciStack package declares only as an OPTIONAL extra (seaborn,
plotly, kaleido ...) or one listed in KNOWN_OPTIONAL (the MATLAB engine,
which ships with MATLAB, not PyPI). So it fails for an undeclared sibling,
an undeclared third-party dependency, and a declared dependency whose
installed wheel does not provide its module. Until 2026-10-18 only the
first failed: csv-stats 0.2.0 shipped an EMPTY wheel, every
`import csvstats` was a ModuleNotFoundError, and the check printed warnings
and passed. Other import errors are printed as warnings.

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


#: Import names that may be missing although no extra declares them.
KNOWN_OPTIONAL = {
    "matlab": "the MATLAB Engine for Python ships with MATLAB, not PyPI",
}

#: Distribution name -> import name, where they differ beyond -/_ spelling.
IMPORT_NAME_ALIASES = {"pyyaml": "yaml", "pillow": "PIL", "scikit-learn": "sklearn"}


def canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def import_name_of(dist_name: str) -> str:
    c = canonical(dist_name)
    return IMPORT_NAME_ALIASES.get(c, c.replace("-", "_"))


def optional_imports(dists) -> dict[str, str]:
    """{import name: "<package>[<extra>]"} for every requirement the given
    distributions declare ONLY under an extra."""
    required, optional = set(), {}
    for dist in dists:
        for req in dist.requires or []:
            name = re.split(r"[\s;\[<>=!~(]", req, maxsplit=1)[0]
            m = re.search(r"""extra\s*==\s*['"]([^'"]+)""", req)
            if m:
                optional.setdefault(import_name_of(name), f"{dist.metadata['Name']}[{m.group(1)}]")
            else:
                required.add(import_name_of(name))
    return {k: v for k, v in optional.items() if k not in required}


def provider_hint(missing: str) -> str:
    """Name an INSTALLED distribution that should provide *missing* (e.g.
    csv-stats for csvstats): then the wheel is broken, not undeclared."""
    flat = re.sub(r"[-_.]", "", missing).lower()
    for dist in md.distributions():
        if re.sub(r"[-_.]", "", dist.metadata["Name"] or "").lower() == flat:
            return (f"; {dist.metadata['Name']} {dist.version} IS installed but provides "
                    f"no {missing!r} module (an empty or broken wheel?)")
    return ("; no SciStack package declares it as an optional extra: declare it in "
            "[project] dependencies, or under an extra if it is optional")


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
    optional = optional_imports(installed.values())
    print(f"optional (extra-only) imports: {', '.join(sorted(optional)) or '(none)'}")

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
                if missing in internal_tops:
                    errors.append(
                        f"{msg} (undeclared SciStack dependency: add it to [project] "
                        f"dependencies AND override-versions-of in that package's "
                        f"pyproject.toml)"
                    )
                elif missing in optional:
                    warnings.append(f"{msg} (optional: {optional[missing]})")
                elif missing in KNOWN_OPTIONAL:
                    warnings.append(f"{msg} (optional: {KNOWN_OPTIONAL[missing]})")
                else:
                    errors.append(msg + provider_hint(missing))
            except BaseException as e:  # noqa: BLE001 - report, keep going
                warnings.append(f"{name}: import {mod} -> {type(e).__name__}: {e}")
                traceback.print_exc(limit=3)

    for w in warnings:
        print(f"::warning::{w}")
    for e in errors:
        print(f"::error::{e}")
    print(f"{imported} modules imported, {len(warnings)} warning(s), {len(errors)} error(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(Path(sys.argv[1])))
