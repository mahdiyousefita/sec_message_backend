import urllib3
from threading import Lock

from flask import current_app
from minio import Minio


_minio_client = None
_minio_signature = None
_minio_lock = Lock()


def _build_signature():
    return (
        current_app.config["MINIO_ENDPOINT"],
        current_app.config["MINIO_ACCESS_KEY"],
        current_app.config["MINIO_SECRET_KEY"],
        current_app.config["MINIO_SECURE"],
        current_app.config["MINIO_CONNECT_TIMEOUT"],
        current_app.config["MINIO_READ_TIMEOUT"],
        current_app.config.get("MINIO_HTTP_POOL_MAXSIZE", 32),
    )


def get_minio_client():
    global _minio_client, _minio_signature

    signature = _build_signature()
    with _minio_lock:
        if _minio_client is not None and _minio_signature == signature:
            return _minio_client

        timeout = urllib3.Timeout(
            connect=current_app.config["MINIO_CONNECT_TIMEOUT"],
            read=current_app.config["MINIO_READ_TIMEOUT"],
        )
        http_client = urllib3.PoolManager(
            timeout=timeout,
            retries=False,
            maxsize=current_app.config.get("MINIO_HTTP_POOL_MAXSIZE", 32),
        )

        _minio_client = Minio(
            current_app.config["MINIO_ENDPOINT"],
            access_key=current_app.config["MINIO_ACCESS_KEY"],
            secret_key=current_app.config["MINIO_SECRET_KEY"],
            secure=current_app.config["MINIO_SECURE"],
            http_client=http_client,
        )
        _minio_signature = signature
        return _minio_client
