import os
try:
    from dotenv import load_dotenv
except ImportError:
    from pathlib import Path

    def load_dotenv(*args, **kwargs):
        dotenv_path = kwargs.get("dotenv_path")
        if dotenv_path:
            path = Path(dotenv_path)
        else:
            path = Path.cwd() / ".env"

        if not path.exists():
            return False

        loaded = False
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
                loaded = True
        return loaded


load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_csv_list(name: str, default_values):
    raw = os.getenv(name, "").strip()
    if not raw:
        return list(default_values)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip()
    return value if value else default


def _resolve_cors_allowed_origins(default_origins):
    cors_origins_raw = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
    if not cors_origins_raw:
        return default_origins

    cors_origins = [
        item.strip() for item in cors_origins_raw.split(",") if item.strip()
    ]
    env_cors_origins = [origin for origin in cors_origins if origin != "*"]

    merged = list(env_cors_origins)
    for origin in default_origins:
        if origin not in merged:
            merged.append(origin)
    return merged


def _resolve_socketio_cors_allowed_origins(default_origins, cors_origins):
    raw = os.getenv("SOCKETIO_CORS_ALLOWED_ORIGINS", "").strip()
    if not raw:
        return list(default_origins)

    normalized = raw.lower()
    if normalized in {"none", "reverse_proxy", "[]"}:
        return []
    if normalized in {"inherit", "same_as_cors"}:
        return list(cors_origins)
    if raw == "*":
        return "*"

    return [item.strip() for item in raw.split(",") if item.strip()]


class Config:
    APP_ENV = _env_str(
        "APP_ENV",
        _env_str("FLASK_ENV", "development"),
    )

    DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
    SQLALCHEMY_DATABASE_URI = DATABASE_URL or "sqlite:///messenger.db"
    DATABASE_URL_WAS_EXPLICIT = bool(DATABASE_URL)
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-secret")

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "supersecret")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "media")
    MINIO_SECURE = _env_bool("MINIO_SECURE", False)
    MINIO_CONNECT_TIMEOUT = float(os.getenv("MINIO_CONNECT_TIMEOUT", "5"))
    MINIO_READ_TIMEOUT = float(os.getenv("MINIO_READ_TIMEOUT", "20"))
    MINIO_OPERATION_TIMEOUT = float(os.getenv("MINIO_OPERATION_TIMEOUT", "8"))
    MINIO_HTTP_POOL_MAXSIZE = int(os.getenv("MINIO_HTTP_POOL_MAXSIZE", "32"))
    MINIO_PUBLIC_BASE_URL = os.getenv(
        "MINIO_PUBLIC_BASE_URL",
        "http://127.0.0.1:9000"
    )
    MINIO_REGION = os.getenv("MINIO_REGION", "").strip() or None
    MINIO_USE_ACCELERATE_ENDPOINT = _env_bool("MINIO_USE_ACCELERATE_ENDPOINT", False)
    MINIO_USE_VIRTUAL_STYLE = _env_bool("MINIO_USE_VIRTUAL_STYLE", False)
    APP_PUBLIC_BASE_URL = os.getenv("APP_PUBLIC_BASE_URL", "").strip()
    MEDIA_LOCAL_FALLBACK_ENABLED = _env_bool(
        "MEDIA_LOCAL_FALLBACK_ENABLED",
        True,
    )
    MEDIA_CACHE_MAX_AGE_SECONDS = int(
        os.getenv("MEDIA_CACHE_MAX_AGE_SECONDS", str(7 * 24 * 60 * 60))
    )
    MEDIA_CACHE_IMMUTABLE = _env_bool("MEDIA_CACHE_IMMUTABLE", True)
    MEDIA_STREAM_CHUNK_SIZE = int(os.getenv("MEDIA_STREAM_CHUNK_SIZE", str(256 * 1024)))
    PROFILE_VIDEO_MAX_DURATION_SECONDS = int(
        os.getenv("PROFILE_VIDEO_MAX_DURATION_SECONDS", "5")
    )
    PROFILE_VIDEO_MAX_SIZE_BYTES = int(
        os.getenv("PROFILE_VIDEO_MAX_SIZE_BYTES", str(15 * 1024 * 1024))
    )

    MESSAGE_ATTACHMENT_MAX_SIZE_BYTES = int(
        os.getenv("MESSAGE_ATTACHMENT_MAX_SIZE_BYTES", str(25 * 1024 * 1024))
    )
    MESSAGE_ATTACHMENT_SPOOL_MAX_MEMORY_BYTES = int(
        os.getenv("MESSAGE_ATTACHMENT_SPOOL_MAX_MEMORY_BYTES", str(1024 * 1024))
    )

    # Credentialed CORS cannot use a wildcard origin.
    # Defaults include known dev ports + localhost any port.
    _default_cors_origins = [
        "http://localhost:5173",
        "http://localhost:5175",
        "http://localhost:5176",
        "https://dinosocial.ir",
        r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    ]
    CORS_ALLOWED_ORIGINS = _resolve_cors_allowed_origins(_default_cors_origins)
    CORS_ALLOWED_METHODS = _env_csv_list(
        "CORS_ALLOWED_METHODS",
        ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    )

    CORS_ALLOWED_HEADERS = _env_csv_list(
        "CORS_ALLOWED_HEADERS",
        ["Content-Type", "Authorization"],
    )

    # Keep production-safe defaults; local runs can override via .env.
    SESSION_COOKIE_SAMESITE = _env_str("SESSION_COOKIE_SAMESITE", "None")
    SESSION_COOKIE_SECURE = _env_bool("SESSION_COOKIE_SECURE", True)
    JWT_COOKIE_SAMESITE = _env_str("JWT_COOKIE_SAMESITE", "None")
    JWT_COOKIE_SECURE = _env_bool("JWT_COOKIE_SECURE", True)

    # Socket.IO configuration
    SOCKETIO_CORS_ALLOWED_ORIGINS = _resolve_socketio_cors_allowed_origins(
        default_origins=[],
        cors_origins=CORS_ALLOWED_ORIGINS,
    )
    SOCKETIO_MESSAGE_QUEUE = os.getenv("SOCKETIO_MESSAGE_QUEUE", "").strip() or None
    SOCKETIO_PING_TIMEOUT = _env_int("SOCKETIO_PING_TIMEOUT", 25)
    SOCKETIO_PING_INTERVAL = _env_int("SOCKETIO_PING_INTERVAL", 20)
    SOCKETIO_LOGGER = _env_bool("SOCKETIO_LOGGER", False)
    SOCKETIO_ENGINEIO_LOGGER = _env_bool("SOCKETIO_ENGINEIO_LOGGER", False)
    SOCKET_PENDING_PRIVATE_BATCH_SIZE = int(
        os.getenv("SOCKET_PENDING_PRIVATE_BATCH_SIZE", "100")
    )
    SOCKET_PENDING_GROUP_BATCH_SIZE = int(
        os.getenv("SOCKET_PENDING_GROUP_BATCH_SIZE", "100")
    )
    PRESENCE_CONNECTION_TTL_SECONDS = max(
        15,
        _env_int("PRESENCE_CONNECTION_TTL_SECONDS", 75),
    )
    PRESENCE_HEARTBEAT_INTERVAL_SECONDS = max(
        5,
        _env_int("PRESENCE_HEARTBEAT_INTERVAL_SECONDS", 20),
    )
    PRESENCE_CLEANUP_BATCH_SIZE = max(
        10,
        _env_int("PRESENCE_CLEANUP_BATCH_SIZE", 200),
    )

    # Optional async task worker queue.
    ASYNC_TASKS_ENABLED = _env_bool("ASYNC_TASKS_ENABLED", False)
    ASYNC_TASK_QUEUE_NAME = _env_str("ASYNC_TASK_QUEUE_NAME", "sec_message:async_tasks")
    ASYNC_TASK_RETRY_QUEUE_NAME = _env_str("ASYNC_TASK_RETRY_QUEUE_NAME", "")
    ASYNC_TASK_FAILED_QUEUE_NAME = _env_str("ASYNC_TASK_FAILED_QUEUE_NAME", "")
    ASYNC_TASK_METRICS_KEY = _env_str("ASYNC_TASK_METRICS_KEY", "")
    ASYNC_TASK_WORKER_REGISTRY_KEY = _env_str("ASYNC_TASK_WORKER_REGISTRY_KEY", "")
    ASYNC_TASK_WORKER_BLOCK_TIMEOUT_SECONDS = max(
        1,
        _env_int("ASYNC_TASK_WORKER_BLOCK_TIMEOUT_SECONDS", 5),
    )
    ASYNC_TASK_MAX_RETRIES = max(
        0,
        _env_int("ASYNC_TASK_MAX_RETRIES", 2),
    )
    ASYNC_TASK_RETRY_BACKOFF_BASE_SECONDS = max(
        0.0,
        _env_float("ASYNC_TASK_RETRY_BACKOFF_BASE_SECONDS", 1.0),
    )
    ASYNC_TASK_RETRY_BACKOFF_MAX_SECONDS = max(
        0.1,
        _env_float("ASYNC_TASK_RETRY_BACKOFF_MAX_SECONDS", 30.0),
    )
    ASYNC_TASK_INLINE_FALLBACK = _env_bool("ASYNC_TASK_INLINE_FALLBACK", True)
    ASYNC_TASK_MIN_WORKER_COUNT = max(
        1,
        _env_int("ASYNC_TASK_MIN_WORKER_COUNT", 1),
    )
    ASYNC_TASK_WORKER_HEARTBEAT_STALE_SECONDS = max(
        5,
        _env_int("ASYNC_TASK_WORKER_HEARTBEAT_STALE_SECONDS", 30),
    )
    ASYNC_TASK_WORKER_STARTUP_STRICT = _env_bool(
        "ASYNC_TASK_WORKER_STARTUP_STRICT",
        False,
    )
    ASYNC_TASK_SKIP_STARTUP_WORKER_CHECK = _env_bool(
        "ASYNC_TASK_SKIP_STARTUP_WORKER_CHECK",
        False,
    )
    ASYNC_TASK_ENQUEUE_SOCKET_TIMEOUT_SECONDS = max(
        0.1,
        _env_float("ASYNC_TASK_ENQUEUE_SOCKET_TIMEOUT_SECONDS", 0.75),
    )
    ASYNC_TASK_ENQUEUE_CONNECT_TIMEOUT_SECONDS = max(
        0.1,
        _env_float("ASYNC_TASK_ENQUEUE_CONNECT_TIMEOUT_SECONDS", 0.75),
    )

    # Activity engagement milestones (likes/comments on your post)
    ACTIVITY_MILESTONE_ENABLED = _env_bool("ACTIVITY_MILESTONE_ENABLED", True)
    ACTIVITY_MILESTONE_ACTIVE_USERS_WINDOW_DAYS = max(
        1,
        _env_int("ACTIVITY_MILESTONE_ACTIVE_USERS_WINDOW_DAYS", 7),
    )
    ACTIVITY_MILESTONE_ACTIVE_USERS_CACHE_TTL_SECONDS = max(
        0,
        _env_int("ACTIVITY_MILESTONE_ACTIVE_USERS_CACHE_TTL_SECONDS", 300),
    )
    ACTIVITY_MILESTONE_LIKE_PERCENT = min(
        100,
        max(1, _env_int("ACTIVITY_MILESTONE_LIKE_PERCENT", 10)),
    )
    ACTIVITY_MILESTONE_COMMENT_PERCENT = min(
        100,
        max(1, _env_int("ACTIVITY_MILESTONE_COMMENT_PERCENT", 5)),
    )
    ACTIVITY_MILESTONE_MIN_LIKES = max(
        1,
        _env_int("ACTIVITY_MILESTONE_MIN_LIKES", 5),
    )
    ACTIVITY_MILESTONE_MIN_COMMENTERS = max(
        1,
        _env_int("ACTIVITY_MILESTONE_MIN_COMMENTERS", 3),
    )

    # Moderation/reporting retention policy
    MODERATION_SOFT_DELETE_DAYS = int(
        os.getenv("MODERATION_SOFT_DELETE_DAYS", "7")
    )
    REPORT_DECISION_RETENTION_DAYS = int(
        os.getenv("REPORT_DECISION_RETENTION_DAYS", "7")
    )
    MODERATION_CLEANUP_INTERVAL_SECONDS = int(
        os.getenv("MODERATION_CLEANUP_INTERVAL_SECONDS", "300")
    )
    MODERATION_CLEANUP_BATCH_SIZE = max(
        1,
        int(os.getenv("MODERATION_CLEANUP_BATCH_SIZE", "100")),
    )
    MODERATION_CLEANUP_RUNNER = (
        os.getenv("MODERATION_CLEANUP_RUNNER", "inprocess").strip().lower()
    )
    MODERATION_CLEANUP_BACKGROUND_ENABLED = _env_bool(
        "MODERATION_CLEANUP_BACKGROUND_ENABLED",
        MODERATION_CLEANUP_RUNNER == "inprocess",
    )
