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

