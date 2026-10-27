"""
Which frontend bundle a webview is running, written into the shared log.

On 2026-10-16 Plot Studio had no Copy PNG button under scistack-gui 0.1.37:
VS Code was running a hand-built 0.1.0 copy of the extension, whose
``dist/webview/`` predated the button. scistack.log said nothing about it,
because nothing ever reported which bundle a panel loaded.

Now every webview sends ``report_client_build`` once on load
(``frontend/src/bundleBuild.ts``). In VS Code the extension host adds the
extension that loaded the bundle and the version verdict before forwarding
(``extension/src/webviewBuild.ts``) — the verdict is decided THERE, by the
same rule as the extension-vs-server check, and only logged here. The
standalone browser build has no host, so its report arrives without one and
is logged as unchecked.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: Every field is display text from our own bundle or host; keep lines short.
MAX_FIELD = 500


def _field(params: dict[str, Any], key: str, default: str = "unknown") -> str:
    text = str(params.get(key) or "").strip() or default
    return text if len(text) <= MAX_FIELD else text[: MAX_FIELD - 1] + "…"


def report_client_build(params: dict[str, Any]) -> dict[str, Any]:
    """Log one webview's bundle build, and WARN when the host judged it stale.

    ``params``: the bundle's ``version``, ``commit``, ``built_at``, ``target``
    and ``view``; from the VS Code host also ``extension_version``,
    ``extension_path``, ``extension_mode``, ``verdict`` (``match`` /
    ``skipped`` / ``mismatch``) and ``verdict_detail``. Returns the server's
    version so the webview's console can show both halves.
    """
    from scistack_gui import __version__ as server_version

    view = _field(params, "view")
    verdict = _field(params, "verdict", default="unchecked (no extension host)")
    detail = _field(params, "verdict_detail", default="")
    host = (
        f"extension {_field(params, 'extension_version')} "
        f"({_field(params, 'extension_mode')}) at {_field(params, 'extension_path')}"
        if params.get("extension_path")
        else "no extension host"
    )
    logger.info(
        "[client] %s view runs bundle %s (commit %s, built %s, target %s) via %s; "
        "scistack-gui %s — %s%s",
        view,
        _field(params, "version"),
        _field(params, "commit"),
        _field(params, "built_at"),
        _field(params, "target"),
        host,
        server_version,
        verdict,
        f" ({detail})" if detail and verdict == "skipped" else "",
    )
    if verdict == "mismatch":
        logger.warning("[client] stale webview bundle in the %s view: %s", view, detail)
    return {"server_version": server_version, "verdict": verdict}
