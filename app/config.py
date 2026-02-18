import os


class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "sqlite:///messenger.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-secret")

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "supersecret")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "media")
    MINIO_SECURE = False
    MINIO_PUBLIC_BASE_URL = os.getenv(
        "MINIO_PUBLIC_BASE_URL",
        "http://127.0.0.1:9000"
    )

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
