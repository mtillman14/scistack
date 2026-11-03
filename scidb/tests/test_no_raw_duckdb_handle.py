"""No code outside sciduckdb reaches the raw ``duckdb`` handle (``.con``).

Every statement goes through ``DuckConnection`` (execute / fetch* /
bulk_insert / insert_frame / transaction), which holds the lock, logs
contention and recovers from a failed autocommit statement. Raw
``duck.con.execute`` / ``duck.con.register`` skipped all three (design
review F14, fixed 2026-11-02). This scans the source so a new raw site fails
here instead of in a contended save.
"""

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_PACKAGES = [
    _ROOT / "scidb" / "src",
    _ROOT / "scistack-gui" / "scistack_gui",
    _ROOT / "scistackplotdb" / "src",
    _ROOT / "scistackstatsdb" / "src",
    _ROOT / "scimatlab" / "src",
]
_RAW = re.compile(r"\b(?:_?duck|db\._duck|self\._duck|conn?)\.con\.(?:execute|register|unregister|sql|executemany)\b")


def test_no_raw_con_access_outside_sciduckdb():
    hits = []
    for pkg in _PACKAGES:
        if not pkg.exists():
            continue
        for path in pkg.rglob("*.py"):
            for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if _RAW.search(line.split("#", 1)[0]):
                    hits.append(f"{path.relative_to(_ROOT)}:{n}: {line.strip()}")
    assert not hits, "raw duckdb handle used outside sciduckdb:\n" + "\n".join(hits)
