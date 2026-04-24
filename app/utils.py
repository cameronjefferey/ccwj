# app/utils.py
import os
from urllib.parse import urlparse

# Post-login ?next= must stay on this site (relative path + query only).
_MAX_INTERNAL_NEXT_LEN = 2048


def safe_internal_next(raw) -> str | None:
    """
    Validate a redirect target after login: same-origin path and query only.
    Rejects full URLs, scheme-relative //... open redirects, and overlong values.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s or len(s) > _MAX_INTERNAL_NEXT_LEN:
        return None
    if not s.startswith("/") or s.startswith("//"):
        return None
    if "\\" in s or "\x00" in s:
        return None
    p = urlparse(s)
    if p.netloc:
        return None
    return s


def read_sql_file(filename: str) -> str:
    sql_path = os.path.join("app", "queries", filename)
    with open(sql_path, "r") as f:
        return f.read()
