import os
from dotenv import load_dotenv

load_dotenv()

_DEFAULT_SECRET = "you-will-never-guess"
_SECRET = os.environ.get("SECRET_KEY") or _DEFAULT_SECRET
if _SECRET == _DEFAULT_SECRET:
    raise RuntimeError(
        "SECRET_KEY must be set. Add to .env: SECRET_KEY=<random-string>\n"
        'Generate one: python -c "import secrets; print(secrets.token_hex(32))"'
    )


class Config:
    SECRET_KEY = _SECRET