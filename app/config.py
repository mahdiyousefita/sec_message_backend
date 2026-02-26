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

    # Credentialed CORS cannot use a wildcard origin.
    # Defaults include known dev ports + localhost any port.
    _default_cors_origins = [
        "http://localhost:5175",
        "http://localhost:5176",
        r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    ]
    _cors_origins_raw = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
    if _cors_origins_raw:
        _cors_origins = [
            item.strip() for item in _cors_origins_raw.split(",") if item.strip()
        ]
        _env_cors_origins = [
            origin for origin in _cors_origins if origin != "*"
        ]
        CORS_ALLOWED_ORIGINS = _env_cors_origins + [
            origin for origin in _default_cors_origins
            if origin not in _env_cors_origins
        ]
    else:
        CORS_ALLOWED_ORIGINS = _default_cors_origins

    # Cross-site cookies for HTTPS requests from frontend.
    SESSION_COOKIE_SAMESITE = "None"
    SESSION_COOKIE_SECURE = True
    JWT_COOKIE_SAMESITE = "None"
    JWT_COOKIE_SECURE = True
