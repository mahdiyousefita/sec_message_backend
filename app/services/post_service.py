import os
import uuid
import time
from flask import current_app, has_app_context, has_request_context, request
from minio.error import S3Error
from sqlalchemy import func, or_
from sqlalchemy.orm import selectinload

from app.extensions.minio_client import get_minio_client
from app.models.comment_model import Comment
from app.models.follow_model import Follow
from app.models.media_model import Media
from app.models.profile_model import Profile
from app.models.post_model import Post
from app.models.playlist_track_model import PlaylistTrack
from app.models.report_model import PostReport
from app.models.user_model import User
from app.models.vote_model import Vote
from app.repositories.post_repository import create_post_by_username
from app.repositories.media_repository import add_media
from app.repositories import user_repository
from app.services import block_service
from app.services import async_task_service
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

ALLOWED_AUDIO_MIME_TYPES = {
    "audio/mpeg",
    "audio/mp3",
    "audio/mp4",
    "audio/x-m4a",
    "audio/aac",
    "audio/wav",
    "audio/x-wav",
    "audio/ogg",
    "audio/flac",
    "audio/webm",
}

MAX_MEDIA_FILES = 8
MAX_VIDEO_DURATION_SECONDS = 30 * 60


class MediaStorageError(Exception):
    pass


def _is_media_not_found(error: S3Error) -> bool:
    return error.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}


def _delete_media_object(object_name: str | None):
    if not object_name:
        return

    if object_name.startswith("static/"):
        relative_path = object_name[len("static/"):]
        absolute_path = os.path.join(current_app.static_folder, relative_path)
        if os.path.isfile(absolute_path):
            os.remove(absolute_path)
        return

    bucket = current_app.config["MINIO_BUCKET"]
    minio = get_minio_client()
    try:
        minio.remove_object(
            bucket_name=bucket,
            object_name=object_name,
        )
    except S3Error as e:
        if _is_media_not_found(e):
            return
        raise


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

    user_rows = (
        db.session.query(User.id, User.username)
        .filter(User.id.in_(author_ids))
        .all()
    )
    profile_rows = (
        db.session.query(Profile.user_id, Profile.name, Profile.image_object_name)
        .filter(Profile.user_id.in_(author_ids))
        .all()
    )

    user_by_id = {
        user_id: {
            "id": user_id,
            "username": username,
        }
        for user_id, username in user_rows
    }
    profile_by_user_id = {
        user_id: {
            "user_id": user_id,
            "name": name,
            "image_object_name": image_object_name,
        }
        for user_id, name, image_object_name in profile_rows
    }
    return user_by_id, profile_by_user_id


def _serialize_author(author_id: int, user_by_id: dict, profile_by_user_id: dict):
    user = user_by_id.get(author_id)
    profile = profile_by_user_id.get(author_id)

    username = user["username"] if user else f"user-{author_id}"
    name = profile["name"] if profile else username

    profile_image_url = None
    if profile and profile.get("image_object_name"):
        profile_image_url = _build_media_url(profile["image_object_name"])

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
        "viewer_vote": 0,
        "media": [
            {
                "id": media.id,
                "url": _build_media_url(media.object_name),
                "mime_type": media.mime_type,
                "display_name": media.display_name,
                "title": media.title,
                "artist": media.artist,
            }
            for media in post.media
        ]
    }


def _viewer_user_id(viewer_username: str | None) -> int | None:
    if not viewer_username:
        return None

    viewer_user = user_repository.get_by_username(viewer_username)
    if not viewer_user or getattr(viewer_user, "is_suspended", False):
        return None

    return viewer_user.id


def _post_visibility_filter(viewer_user_id: int | None):
    if viewer_user_id is None:
        return Post.followers_only.is_(False)

    follower_relation_exists = (
        db.session.query(Follow.id)
        .filter(
            Follow.follower_id == viewer_user_id,
            Follow.following_id == Post.author_id,
        )
        .exists()
    )

    return or_(
        Post.followers_only.is_(False),
        Post.author_id == viewer_user_id,
        follower_relation_exists,
    )


def _build_vote_map(post_ids: set[int], viewer_user_id: int | None):
    if not post_ids or not viewer_user_id:
        return {}

    votes = (
        Vote.query
        .filter(
            Vote.user_id == viewer_user_id,
            Vote.target_type == "post",
            Vote.target_id.in_(post_ids),
        )
        .all()
    )
    return {vote.target_id: vote.value for vote in votes}


def _log_feed_timing(
    *,
    endpoint: str,
    page: int,
    limit: int,
    include_total: bool,
    rows: int,
    started_at: float,
):
    if not has_app_context():
        return

    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    threshold_ms = int(current_app.config.get("QUERY_TIMING_LOG_SLOW_MS", 150))
    level = "info" if elapsed_ms >= threshold_ms else "debug"
    log_fn = current_app.logger.info if level == "info" else current_app.logger.debug
    log_fn(
        "feed_query endpoint=%s page=%s limit=%s include_total=%s rows=%s duration_ms=%s",
        endpoint,
        page,
        limit,
        include_total,
        rows,
        elapsed_ms,
    )


def _extension_for_mimetype(mimetype: str) -> str:
    mapping = {
        "image/jpeg": "jpeg",
        "image/png": "png",
        "image/webp": "webp",
        "video/mp4": "mp4",
        "video/quicktime": "mov",
        "audio/mpeg": "mp3",
        "audio/mp3": "mp3",
        "audio/mp4": "m4a",
        "audio/x-m4a": "m4a",
        "audio/aac": "aac",
        "audio/wav": "wav",
        "audio/x-wav": "wav",
        "audio/ogg": "ogg",
        "audio/flac": "flac",
        "audio/webm": "webm",
    }
    return mapping.get(mimetype, mimetype.split("/")[-1])


def _is_audio_mimetype(mimetype: str) -> bool:
    normalized = (mimetype or "").strip().lower()
    return normalized in ALLOWED_AUDIO_MIME_TYPES or normalized.startswith("audio/")


def _build_audio_metadata(file_name: str | None):
    display_name = (file_name or "").strip()
    if not display_name:
        return None, None, None

    title = os.path.splitext(display_name)[0].strip() or display_name
    return display_name, title, None


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


def create_post_with_media(username, text, files, followers_only: bool = False):
    files = files or []
    if len(files) > MAX_MEDIA_FILES:
        raise ValueError("Maximum 8 media files allowed")

    normalized_files = []
    for file in files:
        if not getattr(file, "filename", ""):
            raise ValueError("Media file is required")
        mimetype = (getattr(file, "mimetype", "") or "").split(";")[0].strip().lower()
        normalized_files.append((file, file.filename, mimetype))

    has_audio_file = any(_is_audio_mimetype(mimetype) for _, _, mimetype in normalized_files)
    cleaned_text = text.strip() if isinstance(text, str) else ""

    if has_audio_file:
        if len(normalized_files) != 1:
            raise ValueError("Music posts can contain only one audio file")

        _, _, mimetype = normalized_files[0]
        if not _is_audio_mimetype(mimetype):
            raise ValueError(f"Unsupported media type: {mimetype}")
    elif cleaned_text == "":
        raise ValueError("Text is required")

    validated_files = []
    for file, file_name, mimetype in normalized_files:
        if has_audio_file:
            extension = _extension_for_mimetype(mimetype)
            display_name, title, artist = _build_audio_metadata(file_name)
            validated_files.append(
                (
                    file,
                    mimetype,
                    extension,
                    display_name,
                    title,
                    artist,
                )
            )
            continue

        if mimetype in ALLOWED_VIDEO_MIME_TYPES:
            duration_seconds = _get_mp4_duration_seconds(file)
            if duration_seconds is None:
                raise ValueError("Could not determine video duration")
            if duration_seconds > MAX_VIDEO_DURATION_SECONDS:
                raise ValueError("Video must be 30 minutes or shorter")
        elif mimetype not in ALLOWED_IMAGE_MIME_TYPES:
            raise ValueError(f"Unsupported media type: {mimetype}")

        extension = _extension_for_mimetype(mimetype)
        validated_files.append((file, mimetype, extension, None, None, None))

    post = create_post_by_username(
        username,
        cleaned_text,
        followers_only=followers_only,
    )

    media_post_process_items = []
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

        for file, mimetype, extension, display_name, title, artist in validated_files:
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
                display_name=display_name,
                title=title,
                artist=artist,
            )
            media_post_process_items.append(
                {
                    "object_name": object_name,
                    "mime_type": mimetype,
                }
            )

    db.session.commit()
    if media_post_process_items:
        async_task_service.enqueue_media_post_process_task(
            post_id=post.id,
            media_items=media_post_process_items,
            source="post_service.create_post_with_media",
        )
    return {"post_id": post.id}


def get_posts(
    page: int,
    limit: int,
    viewer_username: str | None = None,
    include_total: bool = True,
):
    page = max(1, int(page or 1))
    limit = max(1, min(int(limit or 10), 50))
    started_at = time.perf_counter()

    viewer_user_id = _viewer_user_id(viewer_username)
    hidden_user_ids = block_service.hidden_user_ids_for_viewer(viewer_username)

    filter_conditions = [
        Post.is_hidden.is_(False),
        User.is_suspended.is_(False),
        _post_visibility_filter(viewer_user_id),
    ]
    if hidden_user_ids:
        filter_conditions.append(~Post.author_id.in_(hidden_user_ids))

    query = (
        db.session.query(Post.id)
        .join(User, User.id == Post.author_id)
        .filter(*filter_conditions)
    )

    total = None
    if include_total:
        total = (
            query.with_entities(func.count(Post.id))
            .order_by(None)
            .scalar()
            or 0
        )

    offset = (page - 1) * limit
    paged_post_ids = [
        row[0]
        for row in query
        .order_by(Post.created_at.desc(), Post.id.desc())
        .offset(offset)
        .limit(limit)
        .all()
    ]
    if not paged_post_ids:
        _log_feed_timing(
            endpoint="posts_feed",
            page=page,
            limit=limit,
            include_total=include_total,
            rows=0,
            started_at=started_at,
        )
        return {
            "page": page,
            "limit": limit,
            "total": total,
            "posts": [],
        }

    posts = (
        Post.query
        .filter(Post.id.in_(paged_post_ids))
        .options(selectinload(Post.media))
        .all()
    )
    posts_by_id = {post.id: post for post in posts}
    ordered_posts = [
        posts_by_id[post_id]
        for post_id in paged_post_ids
        if post_id in posts_by_id
    ]

    author_ids = {post.author_id for post in posts}
    post_ids = set(paged_post_ids)
    user_by_id, profile_by_user_id = _build_author_maps(author_ids)
    vote_by_post_id = _build_vote_map(
        post_ids=post_ids,
        viewer_user_id=viewer_user_id,
    )

    result = []
    for post in ordered_posts:
        payload = _serialize_post(post, user_by_id, profile_by_user_id)
        payload["viewer_vote"] = int(vote_by_post_id.get(post.id, 0))
        result.append(payload)

    _log_feed_timing(
        endpoint="posts_feed",
        page=page,
        limit=limit,
        include_total=include_total,
        rows=len(result),
        started_at=started_at,
    )

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "posts": result
    }


def get_post(post_id: int, viewer_username: str | None = None):
    viewer_user_id = _viewer_user_id(viewer_username)
    hidden_user_ids = block_service.hidden_user_ids_for_viewer(viewer_username)

    query = (
        Post.query
        .join(User, User.id == Post.author_id)
        .filter(
            Post.id == post_id,
            Post.is_hidden.is_(False),
            User.is_suspended.is_(False),
            _post_visibility_filter(viewer_user_id),
        )
        .options(selectinload(Post.media))
    )
    if hidden_user_ids:
        query = query.filter(~Post.author_id.in_(hidden_user_ids))

    post = query.first()
    if not post:
        raise ValueError("Post not found")

    user_by_id, profile_by_user_id = _build_author_maps({post.author_id})
    vote_by_post_id = _build_vote_map(
        post_ids={post.id},
        viewer_user_id=viewer_user_id,
    )

    payload = _serialize_post(post, user_by_id, profile_by_user_id)
    payload["viewer_vote"] = int(vote_by_post_id.get(post.id, 0))
    return payload


def get_posts_by_username(
    username: str,
    page: int,
    limit: int,
    viewer_username: str | None = None,
):
    user = user_repository.get_by_username(username)
    if not user or getattr(user, "is_suspended", False):
        raise ValueError("User not found")
    if viewer_username and viewer_username != username:
        viewer = user_repository.get_by_username(viewer_username)
        if viewer and block_service.user_ids_have_block_relation(viewer.id, user.id):
            raise ValueError("User not found")

    if limit > 50:
        limit = 50

    viewer_user_id = _viewer_user_id(viewer_username)

    query = (
        Post.query
        .filter(Post.author_id == user.id)
        .filter(
            Post.is_hidden.is_(False),
            _post_visibility_filter(viewer_user_id),
        )
        .options(selectinload(Post.media))
        .order_by(Post.created_at.desc())
    )

    total = (
        query.with_entities(func.count(Post.id))
        .order_by(None)
        .scalar()
        or 0
    )
    posts = query.offset((page - 1) * limit).limit(limit).all()
    author_ids = {post.author_id for post in posts}
    post_ids = {post.id for post in posts}
    user_by_id, profile_by_user_id = _build_author_maps(author_ids)
    vote_by_post_id = _build_vote_map(
        post_ids=post_ids,
        viewer_user_id=viewer_user_id,
    )

    serialized_posts = []
    for post in posts:
        payload = _serialize_post(post, user_by_id, profile_by_user_id)
        payload["viewer_vote"] = int(vote_by_post_id.get(post.id, 0))
        serialized_posts.append(payload)

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "posts": serialized_posts
    }


def delete_post_by_username(post_id: int, username: str):
    user = user_repository.get_by_username(username)
    if not user or getattr(user, "is_suspended", False):
        raise ValueError("User not found")

    post = Post.query.get(post_id)
    if not post or post.is_hidden:
        raise ValueError("Post not found")

    if post.author_id != user.id:
        raise PermissionError("You can only delete your own posts")

    media_rows = (
        db.session.query(Media.id, Media.object_name)
        .filter(Media.post_id == post.id)
        .all()
    )
    media_ids = [row[0] for row in media_rows]
    media_object_names = [row[1] for row in media_rows]
    comment_ids = [
        row[0]
        for row in db.session.query(Comment.id).filter(Comment.post_id == post.id).all()
    ]

    if comment_ids:
        Vote.query.filter(
            Vote.target_type == "comment",
            Vote.target_id.in_(comment_ids),
        ).delete(synchronize_session=False)

    Comment.query.filter_by(post_id=post.id).delete(synchronize_session=False)
    Vote.query.filter(
        Vote.target_type == "post",
        Vote.target_id == post.id,
    ).delete(synchronize_session=False)
    if media_ids:
        PlaylistTrack.query.filter(
            PlaylistTrack.media_id.in_(media_ids)
        ).delete(synchronize_session=False)
    Media.query.filter_by(post_id=post.id).delete(synchronize_session=False)
    PostReport.query.filter_by(post_id=post.id).delete(synchronize_session=False)
    db.session.delete(post)
    db.session.commit()

    for object_name in media_object_names:
        try:
            _delete_media_object(object_name)
        except Exception:
            pass
