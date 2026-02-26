import os
import uuid
from flask import current_app, has_request_context, request
from sqlalchemy.orm import joinedload

from app.extensions.minio_client import get_minio_client
from app.models.profile_model import Profile
from app.models.post_model import Post
from app.models.user_model import User
from app.repositories.post_repository import create_post_by_username
from app.repositories.media_repository import add_media
from app.repositories import user_repository
from app.db import db




ALLOWED_IMAGE_MIME_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
}

ALLOWED_VIDEO_MIME_TYPES = {
    "video/mp4",
    "video/quicktime",
}

MAX_MEDIA_FILES = 8
MAX_VIDEO_DURATION_SECONDS = 30 * 60


class MediaStorageError(Exception):
    pass


def _build_media_url(object_name: str) -> str:
    base_url = current_app.config.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")

    if object_name.startswith("static/"):
        if base_url:
            return f"{base_url}/{object_name}"
        return f"/{object_name}"

    if base_url:
        return f"{base_url}/media/{object_name}"
    return f"/media/{object_name}"


def _build_author_maps(author_ids: set[int]):
    if not author_ids:
        return {}, {}

    users = User.query.filter(User.id.in_(author_ids)).all()
    profiles = Profile.query.filter(Profile.user_id.in_(author_ids)).all()

    user_by_id = {user.id: user for user in users}
    profile_by_user_id = {profile.user_id: profile for profile in profiles}
    return user_by_id, profile_by_user_id


def _serialize_author(author_id: int, user_by_id: dict, profile_by_user_id: dict):
    user = user_by_id.get(author_id)
    profile = profile_by_user_id.get(author_id)

    username = user.username if user else f"user-{author_id}"
    name = profile.name if profile else username

    profile_image_url = None
    if profile and profile.image_object_name:
        profile_image_url = _build_media_url(profile.image_object_name)

    return {
        "id": author_id,
        "username": username,
        "name": name,
        "profile_image_url": profile_image_url,
    }


def _serialize_post(post, user_by_id: dict, profile_by_user_id: dict):
    author_payload = _serialize_author(post.author_id, user_by_id, profile_by_user_id)

    return {
        "id": post.id,
        "text": post.text,
        "author": author_payload,
        "created_at": post.created_at.isoformat(),
        "media": [
            {
                "id": media.id,
                "url": _build_media_url(media.object_name),
                "mime_type": media.mime_type
            }
            for media in post.media
        ]
    }


def _extension_for_mimetype(mimetype: str) -> str:
    mapping = {
        "image/jpeg": "jpeg",
        "image/png": "png",
        "image/webp": "webp",
        "video/mp4": "mp4",
        "video/quicktime": "mov",
    }
    return mapping.get(mimetype, mimetype.split("/")[-1])


def _get_mp4_duration_seconds(file_storage):
    stream = getattr(file_storage, "stream", file_storage)

    try:
        original_pos = stream.tell()
    except Exception:
        original_pos = None

    try:
        try:
            stream.seek(0, 2)
            file_end = stream.tell()
            stream.seek(0)
        except Exception:
            return None

        def read_exact(num_bytes: int) -> bytes:
            data = stream.read(num_bytes)
            if len(data) != num_bytes:
                raise EOFError
            return data

        def walk_boxes(start: int, end: int) -> float | None:
            pos = start
            while pos + 8 <= end:
                stream.seek(pos)
                header = stream.read(8)
                if len(header) < 8:
                    return None

                size = int.from_bytes(header[0:4], "big")
                box_type = header[4:8]
                header_size = 8

                if size == 1:
                    try:
                        size = int.from_bytes(read_exact(8), "big")
                    except EOFError:
                        return None
                    header_size = 16
                elif size == 0:
                    size = end - pos

                if size < header_size:
                    return None

                box_end = pos + size
                if box_end > end:
                    return None

                payload_start = pos + header_size

                if box_type == b"moov":
                    duration = walk_boxes(payload_start, box_end)
                    if duration is not None:
                        return duration
                elif box_type == b"mvhd":
                    stream.seek(payload_start)
                    try:
                        version_flags = read_exact(4)
                    except EOFError:
                        return None

                    version = version_flags[0]
                    if version == 0:
                        try:
                            _ = read_exact(8)  # creation/modification
                            timescale = int.from_bytes(read_exact(4), "big")
                            duration = int.from_bytes(read_exact(4), "big")
                        except EOFError:
                            return None
                    elif version == 1:
                        try:
                            _ = read_exact(16)  # creation/modification
                            timescale = int.from_bytes(read_exact(4), "big")
                            duration = int.from_bytes(read_exact(8), "big")
                        except EOFError:
                            return None
                    else:
                        return None

                    if timescale <= 0:
                        return None
                    return duration / timescale

                pos = box_end
            return None

        return walk_boxes(0, file_end)
    finally:
        if original_pos is not None:
            try:
                stream.seek(original_pos)
            except Exception:
                pass


def _get_stream_and_length(file_storage):
    stream = getattr(file_storage, "stream", file_storage)
    try:
        stream.seek(0, 2)
        length = stream.tell()
        stream.seek(0)
        return stream, length
    except Exception:
        try:
            stream.seek(0)
        except Exception:
            pass
        return stream, -1


def _store_media_locally(file_storage, post_id: int, extension: str) -> str:
    filename = f"{uuid.uuid4()}.{extension}"
    relative_parts = ["uploads", "posts", str(post_id), filename]
    relative_path = os.path.join(*relative_parts)
    absolute_path = os.path.join(current_app.static_folder, relative_path)
    os.makedirs(os.path.dirname(absolute_path), exist_ok=True)

    try:
        file_storage.stream.seek(0)
    except Exception:
        pass

    file_storage.save(absolute_path)
    return "static/" + "/".join(relative_parts)


def create_post_with_media(username, text, files):
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Text is required")

    files = files or []
    if len(files) > MAX_MEDIA_FILES:
        raise ValueError("Maximum 8 media files allowed")

    validated_files = []
    for file in files:
        if not getattr(file, "filename", ""):
            raise ValueError("Media file is required")

        mimetype = getattr(file, "mimetype", None) or ""
        if mimetype in ALLOWED_VIDEO_MIME_TYPES:
            duration_seconds = _get_mp4_duration_seconds(file)
            if duration_seconds is None:
                raise ValueError("Could not determine video duration")
            if duration_seconds > MAX_VIDEO_DURATION_SECONDS:
                raise ValueError("Video must be 30 minutes or shorter")
        elif mimetype not in ALLOWED_IMAGE_MIME_TYPES:
            raise ValueError(f"Unsupported media type: {mimetype}")

        extension = _extension_for_mimetype(mimetype)
        validated_files.append((file, mimetype, extension))

    post = create_post_by_username(username, text.strip())

    if validated_files:
        minio = None
        bucket = current_app.config["MINIO_BUCKET"]
        local_fallback_enabled = bool(
            current_app.config.get("MEDIA_LOCAL_FALLBACK_ENABLED", True)
        )
        use_local_storage = False

        try:
            minio = get_minio_client()
            bucket_exists = minio.bucket_exists(bucket)
            if not bucket_exists:
                minio.make_bucket(bucket)
        except Exception as e:
            if not local_fallback_enabled:
                db.session.rollback()
                raise MediaStorageError("Media storage is unavailable") from e
            use_local_storage = True

        for file, mimetype, extension in validated_files:
            object_name = None

            if not use_local_storage:
                try:
                    object_name = f"posts/{post.id}/{uuid.uuid4()}.{extension}"
                    stream, length = _get_stream_and_length(file)
                    upload_kwargs = {
                        "bucket_name": bucket,
                        "object_name": object_name,
                        "data": stream,
                        "length": length,
                        "content_type": mimetype,
                    }
                    if length == -1:
                        upload_kwargs["part_size"] = 10 * 1024 * 1024

                    minio.put_object(**upload_kwargs)
                except Exception as e:
                    if not local_fallback_enabled:
                        db.session.rollback()
                        raise MediaStorageError("Media storage is unavailable") from e
                    use_local_storage = True

            if use_local_storage:
                try:
                    object_name = _store_media_locally(file, post.id, extension)
                except Exception as e:
                    db.session.rollback()
                    raise MediaStorageError("Media storage is unavailable") from e

            add_media(
                post_id=post.id,
                object_name=object_name,
                mime_type=mimetype,
            )

    db.session.commit()
    return {"post_id": post.id}


def get_posts(page: int, limit: int):
    if limit > 50:
        limit = 50

    query = (
        Post.query
        .options(joinedload(Post.media))
        .order_by(Post.created_at.desc())
    )

    total = query.count()
    posts = query.offset((page - 1) * limit).limit(limit).all()
    author_ids = {post.author_id for post in posts}
    user_by_id, profile_by_user_id = _build_author_maps(author_ids)

    result = [_serialize_post(post, user_by_id, profile_by_user_id) for post in posts]

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "posts": result
    }


def get_posts_by_username(username: str, page: int, limit: int):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    if limit > 50:
        limit = 50

    query = (
        Post.query
        .filter(Post.author_id == user.id)
        .options(joinedload(Post.media))
        .order_by(Post.created_at.desc())
    )

    total = query.count()
    posts = query.offset((page - 1) * limit).limit(limit).all()
    author_ids = {post.author_id for post in posts}
    user_by_id, profile_by_user_id = _build_author_maps(author_ids)

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "posts": [_serialize_post(post, user_by_id, profile_by_user_id) for post in posts]
    }
