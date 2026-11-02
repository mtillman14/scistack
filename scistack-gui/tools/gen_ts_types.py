"""Regenerate the frontend's request types from the backend's handler table.

Run from the repo root (the user runs this; it needs the GUI's Python env):

    python scistack-gui/tools/gen_ts_types.py

1. writes ``scistack-gui/frontend/src/generated/api-schema.json`` from
   ``scistack_gui.api.wire_schema.api_json_schema`` (every ``Handler.params``
   pydantic model);
2. runs ``npm run gen:types`` in ``scistack-gui/frontend``, which turns that
   schema into ``src/generated/api-types.ts`` (json-schema-to-typescript).

Commit both files. ``tests/test_api_handlers.py`` fails while the JSON is
stale; the frontend's ``generatedTypes.test.ts`` fails while the ``.ts`` is.
"""

import subprocess
import sys
from pathlib import Path

GUI = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(GUI))

from scistack_gui.api.wire_schema import SCHEMA_PATH, schema_text  # noqa: E402


def main() -> int:
    target = GUI / SCHEMA_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(schema_text(), encoding="utf-8")
    print(f"wrote {target}")
    npm = "npm.cmd" if sys.platform == "win32" else "npm"
    return subprocess.call([npm, "run", "gen:types"], cwd=GUI / "frontend")


if __name__ == "__main__":
    raise SystemExit(main())
