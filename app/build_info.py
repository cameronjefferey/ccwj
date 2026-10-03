"""Deploy identity for live checks.

Render sets ``RENDER_GIT_COMMIT`` on the web service. The boot time is
this process, so a new deploy moves it even when the commit is unchanged.
"""
from datetime import datetime, timezone
import os

BOOTED_AT = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def deploy_commit():
    """Running git commit, or None when the env var is unset (local dev)."""
    value = (os.environ.get("RENDER_GIT_COMMIT") or "").strip()
    return value or None
