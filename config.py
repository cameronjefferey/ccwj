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

    # Open registration. Set SIGNUP_ENABLED=false to hide /signup entirely.
    SIGNUP_ENABLED = _env_bool("SIGNUP_ENABLED", "true")

    # Soft gate: if SIGNUP_INVITE_CODE is set, /signup is reachable but the
    # form requires a matching code (compare with hmac.compare_digest). Empty
    # string = no gate (open signup, current behavior). Use this for closed
    # beta with strangers without flipping SIGNUP_ENABLED off entirely.
    SIGNUP_INVITE_CODE = (os.environ.get("SIGNUP_INVITE_CODE", "") or "").strip()

    # Session / remember-me cookies
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE = _env_bool("SESSION_COOKIE_SECURE", "true") if _is_prod else False

    REMEMBER_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_SAMESITE = "Lax"
    REMEMBER_COOKIE_SECURE = _env_bool("REMEMBER_COOKIE_SECURE", "true") if _is_prod else False

    # Logged-in session cookie lasts a week by default. Sessions are marked
    # permanent on login (see _touch_session_last_activity) so this lifetime
    # actually takes effect instead of dying when the browser closes.
    # Set PERMANENT_SESSION_DAYS in env to lengthen/shorten.
    _session_days = int(os.environ.get("PERMANENT_SESSION_DAYS", "7"))
    PERMANENT_SESSION_LIFETIME = timedelta(days=max(1, _session_days))

    # Log out after this many minutes without any request (0 = disabled, e.g. tests).
    # A week by default so checking the app once a day (or skipping a few days)
    # never forces a surprise re-login. Shorten via SESSION_IDLE_TIMEOUT_MINUTES
    # (e.g. 60) in env for a shared/kiosk machine; 0 disables idle expiry entirely.
    _idle_min = int(os.environ.get("SESSION_IDLE_TIMEOUT_MINUTES", "10080"))
    SESSION_IDLE_TIMEOUT_MINUTES = max(0, _idle_min)

    # /insights (Coach) UI: on by default; set INSIGHTS_ENABLED=0 in .env to hide nav + /insights*.
    # Tests force-enable in conftest.
    INSIGHTS_ENABLED = _env_bool("INSIGHTS_ENABLED", "true")

    # Behavior observations (BQML-ranked evidence) embedded in /insights coach.
    # Default on; set BEHAVIOR_INSIGHTS_ENABLED=0 to hide while the ml_models
    # dataset is still being backfilled/tuned.
    BEHAVIOR_INSIGHTS_ENABLED = _env_bool("BEHAVIOR_INSIGHTS_ENABLED", "true")

    # Community surface (followers, posts, public profiles, "Show" trade publish).
    # Default OFF: the trading-mirror identity is single-player and the community
    # surface still needs notifications, moderation, seeding, and on-strategy
    # redesign before it should ship to real users. Code, schema, and routes
    # stay in the repo so iteration can continue behind the flag — set
    # COMMUNITY_ENABLED=1 to turn it back on (e.g. local dev, internal preview).
    # When OFF: the /community, /u/<username>, /community/* routes 404, the
    # Community nav link + Profile tabs disappear, and the Weekly Review "Show"
    # column + publish modal are not rendered. Tests force-enable in conftest.
    COMMUNITY_ENABLED = _env_bool("COMMUNITY_ENABLED", "false")

    # EarningsFollower — tandem product (separate deploy) surfaced via deep-links.
    # The /earnings ("Earnings Watch") page and the EarningsFollower cross-links
    # in the Daily Review are gated on this flag (default ON). Set
    # EARNINGS_FOLLOWER_ENABLED=0 to hide the page + nav + cross-links.
    EARNINGS_FOLLOWER_ENABLED = _env_bool("EARNINGS_FOLLOWER_ENABLED", "true")

    # Base URL of the deployed EarningsFollower web app. Deep-links are built
    # against this (see app.utils.earnings_follower_url). Override per-env if the
    # product moves off its current Render hostname. rstrip so we never emit
    # a double slash when appending query strings.
    EARNINGS_FOLLOWER_URL = (
        os.environ.get("EARNINGS_FOLLOWER_URL", "https://earningsfollower-web.onrender.com")
        or ""
    ).rstrip("/")

    # CSV uploads (manual upload page). Prevents accidental huge POSTs.
    _max_mb = int(os.environ.get("MAX_UPLOAD_MB", "32"))
    MAX_CONTENT_LENGTH = _max_mb * 1024 * 1024
