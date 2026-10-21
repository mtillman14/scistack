"""
Manual built-in/library function references.

Validates a reference to a function the user did NOT write themselves
(e.g. ``numpy.mean``, ``pandas.read_csv``, a Python stdlib call, or a
native MATLAB command like ``mean``) so it can be used as a normal
function node, alongside — never instead of — auto-discovered functions
from the user's own code. Shared by both the FastAPI route
(``api/builtin_functions.py``) and the JSON-RPC handler (``server.py``,
VS Code extension mode).

Python references are restricted to the standard library plus numpy/pandas
(exactly what this feature is scoped to, not a general import backdoor),
and conventional import aliases are accepted and canonicalized —
``pd.read_csv`` is stored as ``pandas.read_csv``. Both rules live in
:mod:`scistack_gui.library_functions`.

MATLAB references are validated by shelling out to a real MATLAB
installation (``matlab -batch "disp(exist(...))"``) since there is no
lightweight way to confirm a MATLAB builtin/toolbox function exists
without one.

**Python and MATLAB references are handled asymmetrically on purpose.**
Both are persisted (see ``pipeline_store.write_builtin_function``) because
neither is rediscovered by scanning disk. But a Python reference is never
put into ``registry._functions``: it is imported on demand at every use
site (:mod:`scistack_gui.library_functions`), so there is nothing to
replay and no refresh path that can evict it. A MATLAB reference has no
import equivalent — it must be re-registered into ``matlab_registry``,
which is what :func:`replay_persisted_builtins` still exists to do.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess

from scistack_gui import library_functions
from scistack_gui.matlab_parser import MatlabFunctionInfo

logger = logging.getLogger(__name__)

# MATLAB: a strict identifier check BEFORE the name ever touches a
# constructed shell command — never interpolate raw user input otherwise.
_MATLAB_IDENTIFIER_RE = re.compile(r"^[A-Za-z]\w*$")

_MATLAB_EXIST_TIMEOUT_S = 30


def create_builtin_function(language: str, reference: str) -> dict:
    language = (language or "").strip().lower()
    reference = (reference or "").strip()
    logger.info(
        "[builtin_function_service] create_builtin_function request: "
        "language=%r reference=%r",
        language,
        reference,
    )

    if not reference:
        return {"ok": False, "error": "Enter a function name or reference."}

    if language == "python":
        return _create_python_builtin(reference)
    if language == "matlab":
        return _create_matlab_builtin(reference)
    return {"ok": False, "error": f"Unknown language: {language!r}"}


# ---------------------------------------------------------------------------
# Python
# ---------------------------------------------------------------------------


def _create_python_builtin(reference: str) -> dict:
    """Validate and persist a Python library reference.

    Note what is NOT done here: the resolved callable is thrown away rather
    than registered. It is re-imported on demand by
    ``registry.lookup_function`` — see this module's docstring.
    """
    error, qualified_name, _fn = library_functions.validate(reference)
    if error is not None:
        logger.info(
            "[builtin_function_service] Rejected Python reference %r: %s",
            reference,
            error["error"],
        )
        return error

    _persist_builtin(qualified_name, "python")

    if qualified_name != reference.strip():
        logger.info(
            "[builtin_function_service] Recorded Python library function: %s "
            "(canonicalized from %r)",
            qualified_name,
            reference,
        )
    else:
        logger.info(
            "[builtin_function_service] Recorded Python library function: %s", qualified_name
        )
    return {"ok": True, "name": qualified_name}


# ---------------------------------------------------------------------------
# MATLAB
# ---------------------------------------------------------------------------


def _create_matlab_builtin(reference: str) -> dict:
    if not _MATLAB_IDENTIFIER_RE.match(reference):
        return {
            "ok": False,
            "error": "MATLAB function names must be a plain identifier "
            "(letters, digits, underscore, starting with a letter).",
        }

    matlab_exe = shutil.which("matlab")
    if matlab_exe is None:
        return {
            "ok": False,
            "error": "MATLAB was not found on PATH. MATLAB must be installed "
            "and on PATH to validate a built-in function reference.",
        }

    # `reference` is already validated against a strict identifier regex
    # above, so it's safe to interpolate into the MATLAB literal below —
    # never do this before that check has run.
    try:
        result = subprocess.run(
            [matlab_exe, "-batch", f"disp(exist('{reference}'))"],
            capture_output=True,
            text=True,
            timeout=_MATLAB_EXIST_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "Timed out waiting for MATLAB to respond."}
    except OSError as e:
        return {"ok": False, "error": f"Could not run MATLAB: {e}"}

    exists_code = _last_nonblank_line(result.stdout)
    if exists_code is None or exists_code == "0" or not exists_code.isdigit():
        logger.info(
            "[builtin_function_service] MATLAB exist('%s') == %r; rejecting",
            reference,
            exists_code,
        )
        return {
            "ok": False,
            "error": f"MATLAB does not recognize '{reference}' as a function, "
            "built-in, or class (exist() returned 0).",
        }

    _register_matlab_builtin_in_memory(reference)
    _persist_builtin(reference, "matlab")

    logger.info("[builtin_function_service] Registered MATLAB builtin: %s", reference)
    return {"ok": True, "name": reference}


def _last_nonblank_line(text: str | None) -> str | None:
    if not text:
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[-1] if lines else None


def _register_matlab_builtin_in_memory(reference: str) -> None:
    from scistack_gui import matlab_registry

    info = MatlabFunctionInfo(
        name=reference,
        file_path=None,
        params=[],
        source_hash="",
        n_outputs=0,
        output_names=[],
    )
    matlab_registry.register_builtin_function(info)


# ---------------------------------------------------------------------------
# Persistence / replay
# ---------------------------------------------------------------------------


def _persist_builtin(name: str, language: str) -> None:
    from scistack_gui import pipeline_store
    from scistack_gui.db import get_db

    pipeline_store.write_builtin_function(get_db(), name, language)


def get_python_library_function_names(db) -> list[str]:
    """Every persisted Python library reference.

    The DB table is now the ONLY record of which library functions the
    user has added — they are not in ``registry._functions`` — so anything
    listing available functions (``pipeline_service.get_registry``) has to
    read it here.
    """
    from scistack_gui import pipeline_store

    try:
        rows = pipeline_store.get_builtin_functions(db)
    except Exception as e:  # no DB open yet, or a fresh one without the table
        logger.debug("[builtin_function_service] Could not read library functions: %s", e)
        return []
    return [row["name"] for row in rows if row["language"] == "python"]


def replay_persisted_builtins(db) -> dict:
    """Re-register every persisted MATLAB builtin, and re-check Python ones.

    Called after startup DB init and after every registry refresh
    (``matlab_registry.load_from_config``/``registry_reload_service.refresh_registries()`` clear the
    in-memory MATLAB registry, and a MATLAB builtin has no file on disk to
    be rediscovered from, so it must be explicitly replayed).

    **Python references are no longer registered here** — they are
    imported on demand at every use site, so there is nothing to restore
    and no refresh path that can evict them (see the module docstring).
    They are still cheaply re-validated, because that is what surfaces
    "numpy was uninstalled since" as a reported failure now instead of a
    mystery at run time.

    MATLAB references are NOT re-validated by shelling out to MATLAB
    again — that's slow and would make every refresh fail hard if MATLAB
    happens to be momentarily unavailable; a MATLAB builtin already passed
    validation once, at creation time, and that's trusted on replay.
    """
    from scistack_gui import pipeline_store

    rows = pipeline_store.get_builtin_functions(db)
    counts = {"python": 0, "matlab": 0}
    failed = []
    for row in rows:
        name, language = row["name"], row["language"]
        if language == "python":
            error, _qualified_name, _fn = library_functions.validate(name)
            if error is not None:
                failed.append({"name": name, "error": error["error"]})
                continue
            counts["python"] += 1
        elif language == "matlab":
            _register_matlab_builtin_in_memory(name)
            counts["matlab"] += 1
        else:
            logger.warning(
                "[builtin_function_service] Unknown language %r for persisted builtin %r",
                language,
                name,
            )

    if failed:
        logger.warning(
            "[builtin_function_service] Failed to restore %d persisted builtin(s): %s",
            len(failed),
            failed,
        )
    logger.info("[builtin_function_service] Replayed persisted builtins: %s", counts)
    return {"counts": counts, "failed": failed}
