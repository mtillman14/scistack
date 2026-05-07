"""
Run service — single source of truth for pipeline execution.

Wraps the run thread logic from api/run.py. Called by both JSON-RPC
handlers (server.py) and FastAPI routes (api/run.py).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def cancel_run(run_id: str) -> dict:
    """Cooperatively cancel a running for_each."""
    logger.info("[run_service] Delegating cooperative cancel to api.run for run_id=%s", run_id)
    from scistack_gui.api.run import cancel_run as _cancel
    result = _cancel(run_id)
    logger.debug("[run_service] Cancel result: %s (run_id=%s)", result, run_id)
    return result


def force_cancel_run(run_id: str) -> dict:
    """Force-cancel a running for_each by injecting KeyboardInterrupt."""
    logger.info("[run_service] Delegating force cancel to api.run for run_id=%s", run_id)
    from scistack_gui.api.run import force_cancel_run as _force_cancel
    result = _force_cancel(run_id)
    logger.debug("[run_service] Force cancel result: %s (run_id=%s)", result, run_id)
    return result
