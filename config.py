import os
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()

_DEFAULT_SECRET = "you-will-never-guess"
_SECRET = os.environ.get("SECRET_KEY") or _DEFAULT_SECRET
if _SECRET == _DEFAULT_SECRET:
    raise RuntimeError(
        "SECRET_KEY must be set. Add to .env: SECRET_KEY=<random-string>\n"
        'Generate one: python -c "import secrets; print(secrets.token_hex(32))"'
    )


def _env_bool(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).lower() in ("1", "true", "yes", "on")


# Production-ish: HTTPS cookies. Local `flask run` is HTTP unless you use mkcert;
# default false so dev works out of the box. Set SESSION_COOKIE_SECURE=true on Render.
_is_prod = _env_bool("FLASK_PRODUCTION", "false") or _env_bool("RENDER", "false")


class Config:
    SECRET_KEY = _SECRET

    # CSRF (Flask-WTF). Tests set WTF_CSRF_ENABLED=false via app.config in conftest.
    WTF_CSRF_ENABLED = _env_bool("WTF_CSRF_ENABLED", "true")

    # Open registration. Set SIGNUP_ENABLED=false for invite-only.
    SIGNUP_ENABLED = _env_bool("SIGNUP_ENABLED", "true")

    # Session / remember-me cookies
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE = _env_bool("SESSION_COOKIE_SECURE", "true") if _is_prod else False

    REMEMBER_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_SAMESITE = "Lax"
    REMEMBER_COOKIE_SECURE = _env_bool("REMEMBER_COOKIE_SECURE", "true") if _is_prod else False

    # Set PERMANENT_SESSION_DAYS=7 in env to expire logged-in sessions sooner.
    _session_days = int(os.environ.get("PERMANENT_SESSION_DAYS", "14"))
    PERMANENT_SESSION_LIFETIME = timedelta(days=max(1, _session_days))

    # Log out after this many minutes without any request (0 = disabled, e.g. tests).
    _idle_min = int(os.environ.get("SESSION_IDLE_TIMEOUT_MINUTES", "10"))
    SESSION_IDLE_TIMEOUT_MINUTES = max(0, _idle_min)

    # /insights (Coach) UI: off by default; set INSIGHTS_ENABLED=1 in .env to show nav + /insights*.
    # Tests force-enable in conftest.
    INSIGHTS_ENABLED = _env_bool("INSIGHTS_ENABLED", "false")

    # CSV uploads (manual upload page). Prevents accidental huge POSTs.
    _max_mb = int(os.environ.get("MAX_UPLOAD_MB", "32"))
    MAX_CONTENT_LENGTH = _max_mb * 1024 * 1024
