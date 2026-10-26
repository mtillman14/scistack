"""MkDocs hook: the version label shown in the docs header.

The git tag is the one owner of the release version (hatch-vcs builds every
package from it), so the docs read it from git too and never hard-code it.
config.extra.doc_version is consumed by overrides/partials/source.html.

  - A tag build on Read the Docs (READTHEDOCS_VERSION_NAME == the tag), or any
    build sitting exactly on a tag:          label "v0.1.57"
  - A build ahead of the last tag (latest):  label "v0.1.57+3 (dev)"
  - No reachable tag (shallow clone):        label "dev (<sha>)", or "dev"

Read the Docs clones shallowly; .readthedocs.yaml fetches the tags so
`git describe` can see them.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess

log = logging.getLogger("mkdocs.hooks.doc_version")

# `git describe --tags --long` output: v0.1.57-3-gabc1234
_DESCRIBE = re.compile(r"^(?P<tag>.+)-(?P<ahead>\d+)-g(?P<sha>[0-9a-f]+)$")


def _git(*args: str) -> str | None:
    try:
        out = subprocess.run(
            ["git", *args], capture_output=True, text=True, check=True, timeout=10
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log.debug("git %s failed: %s", " ".join(args), exc)
        return None
    return out.stdout.strip() or None


def format_label(describe: str | None, sha: str | None) -> tuple[str, str]:
    """(label, tooltip) from `git describe --tags --long` output and HEAD sha."""
    m = _DESCRIBE.match(describe or "")
    if m is None:
        if sha:
            return f"dev ({sha})", f"Development docs, commit {sha}; no release tag found"
        return "dev", "Development docs; version unknown"
    tag, ahead = m["tag"], int(m["ahead"])
    if ahead == 0:
        return tag, f"Docs for release {tag}"
    return (
        f"{tag}+{ahead} (dev)",
        f"Development docs: {ahead} commit(s) after {tag}, commit {m['sha']}",
    )


def on_config(config):
    describe = _git("describe", "--tags", "--long", "--match", "v[0-9]*")
    sha = _git("rev-parse", "--short", "HEAD")
    label, tooltip = format_label(describe, sha)
    rtd_version = os.environ.get("READTHEDOCS_VERSION_NAME")
    config.extra["doc_version"] = {"label": label, "tooltip": tooltip}
    log.info(
        "Docs version label %r (git describe=%r, sha=%r, READTHEDOCS_VERSION_NAME=%r)",
        label, describe, sha, rtd_version,
    )
    return config
