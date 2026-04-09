import os

import redis


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=_env_int("REDIS_PORT", 6379),
    db=_env_int("REDIS_DB", 0),
    password=os.getenv("REDIS_PASSWORD") or None,
    decode_responses=os.getenv("REDIS_DECODE_RESPONSES", "true").strip().lower()
    in {"1", "true", "yes", "on"},
)
