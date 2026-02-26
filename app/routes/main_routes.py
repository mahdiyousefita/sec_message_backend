from datetime import timezone

from flask import (
    Blueprint,
    Response,
    jsonify,
    render_template,
    stream_with_context,
    current_app,
    request,
)
from minio.error import S3Error
from werkzeug.http import http_date, parse_date

from app.extensions.minio_client import get_minio_client

main_bp = Blueprint("main", __name__)


@main_bp.route("", methods=["GET"])
def main():
    return render_template('index.html')


def _is_media_not_found(error: S3Error) -> bool:
    return error.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}


def _media_error_response(error: S3Error):
    if _is_media_not_found(error):
        return jsonify({"error": "Media not found"}), 404
    return jsonify({"error": "Media unavailable"}), 503


def _build_etag(value: str | None) -> str | None:
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    if value.startswith('"') and value.endswith('"'):
        return value
    return f'"{value}"'


def _build_last_modified(value):
    if not value:
        return None
    if hasattr(value, "timestamp"):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return http_date(value.timestamp())
    return str(value)


def _matches_if_none_match(if_none_match: str | None, etag: str | None) -> bool:
    if not if_none_match or not etag:
        return False
    candidates = [part.strip() for part in if_none_match.split(",") if part.strip()]
    if "*" in candidates:
        return True
    etag_value = etag.strip('"')
    return any(candidate.strip('"') == etag_value for candidate in candidates)


def _matches_if_modified_since(if_modified_since: str | None, last_modified) -> bool:
    if not if_modified_since or not last_modified:
        return False

    since_dt = parse_date(if_modified_since)
    if since_dt is None:
        return False

    last_modified_dt = last_modified
    if hasattr(last_modified_dt, "tzinfo"):
        if last_modified_dt.tzinfo is None:
            last_modified_dt = last_modified_dt.replace(tzinfo=timezone.utc)
    else:
        last_modified_dt = parse_date(str(last_modified_dt))
        if last_modified_dt is None:
            return False

    if since_dt.tzinfo is None:
        since_dt = since_dt.replace(tzinfo=timezone.utc)

    return int(last_modified_dt.timestamp()) <= int(since_dt.timestamp())


def _build_media_headers(*, content_type: str, content_length, etag: str | None, last_modified):
    cache_max_age = max(
        int(current_app.config.get("MEDIA_CACHE_MAX_AGE_SECONDS", 7 * 24 * 60 * 60)),
        0,
    )
    cache_control = f"public, max-age={cache_max_age}"
    if current_app.config.get("MEDIA_CACHE_IMMUTABLE", True):
        cache_control = f"{cache_control}, immutable"

    headers = {
        "Cache-Control": cache_control,
        "Accept-Ranges": "bytes",
        "Content-Type": content_type or "application/octet-stream",
    }

    if content_length is not None:
        headers["Content-Length"] = str(content_length)

    etag_header = _build_etag(etag)
    if etag_header:
        headers["ETag"] = etag_header

    last_modified_header = _build_last_modified(last_modified)
    if last_modified_header:
        headers["Last-Modified"] = last_modified_header

    return headers


@main_bp.route("/media/<path:object_name>", methods=["GET", "HEAD"])
def get_media(object_name: str):
    bucket = current_app.config["MINIO_BUCKET"]
    minio = get_minio_client()

    try:
        stat = minio.stat_object(
            bucket_name=bucket,
            object_name=object_name,
        )
    except S3Error as e:
        return _media_error_response(e)
    except Exception:
        return jsonify({"error": "Media unavailable"}), 503

    headers = _build_media_headers(
        content_type=getattr(stat, "content_type", None) or "application/octet-stream",
        content_length=getattr(stat, "size", None),
        etag=getattr(stat, "etag", None),
        last_modified=getattr(stat, "last_modified", None),
    )

    if _matches_if_none_match(request.headers.get("If-None-Match"), headers.get("ETag")):
        return Response(status=304, headers=headers)
    if _matches_if_modified_since(
        request.headers.get("If-Modified-Since"),
        getattr(stat, "last_modified", None),
    ):
        return Response(status=304, headers=headers)

    if request.method == "HEAD":
        return Response(status=200, headers=headers)

    try:
        minio_response = minio.get_object(
            bucket_name=bucket,
            object_name=object_name,
        )
    except S3Error as e:
        if _is_media_not_found(e):
            return jsonify({"error": "Media not found"}), 404
        return jsonify({"error": "Media unavailable"}), 503
    except Exception:
        return jsonify({"error": "Media unavailable"}), 503

    chunk_size = max(
        int(current_app.config.get("MEDIA_STREAM_CHUNK_SIZE", 256 * 1024)),
        1024,
    )

    def _stream():
        try:
            for chunk in minio_response.stream(chunk_size):
                yield chunk
        finally:
            minio_response.close()
            minio_response.release_conn()

    return Response(
        stream_with_context(_stream()),
        status=200,
        headers=headers,
        direct_passthrough=True,
    )
