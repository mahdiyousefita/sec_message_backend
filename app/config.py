import os
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False


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


class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///messenger.db")
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

    # Cross-site cookies for HTTPS requests from frontend.
    SESSION_COOKIE_SAMESITE = "None"
    SESSION_COOKIE_SECURE = True
    JWT_COOKIE_SAMESITE = "None"
    JWT_COOKIE_SECURE = True

    # Socket.IO configuration
    SOCKETIO_CORS_ALLOWED_ORIGINS = CORS_ALLOWED_ORIGINS
    SOCKETIO_MESSAGE_QUEUE = os.getenv("SOCKETIO_MESSAGE_QUEUE", "").strip() or None
    SOCKETIO_PING_TIMEOUT = int(os.getenv("SOCKETIO_PING_TIMEOUT", "25"))
    SOCKETIO_PING_INTERVAL = int(os.getenv("SOCKETIO_PING_INTERVAL", "20"))
    SOCKETIO_LOGGER = _env_bool("SOCKETIO_LOGGER", False)
    SOCKETIO_ENGINEIO_LOGGER = _env_bool("SOCKETIO_ENGINEIO_LOGGER", False)
    SOCKET_PENDING_PRIVATE_BATCH_SIZE = int(
        os.getenv("SOCKET_PENDING_PRIVATE_BATCH_SIZE", "100")
    )
    SOCKET_PENDING_GROUP_BATCH_SIZE = int(
        os.getenv("SOCKET_PENDING_GROUP_BATCH_SIZE", "100")
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
