import json
import os
import time
from datetime import datetime, timedelta

from flask import current_app, has_request_context, request
from minio.error import S3Error
from sqlalchemy import case, func

from app.extensions.redis_client import redis_client
from app.extensions.minio_client import get_minio_client
from app.models.follow_model import Follow
from app.models.profile_model import Profile
from app.models.user_model import User
from app.repositories import message_repository, user_repository
from app.repositories.story_repository import (
    create_story,
    delete_story_with_views,
    delete_expired_stories,
    get_active_feed_grouped,
    get_active_story,
    get_active_stories_for_user,
    get_story_by_id,
    get_story_view_row,
    get_story_views_map,
    get_story_viewers_page,
    release_daily_story_slot,
    record_story_view,
    reserve_daily_story_slot,
    set_story_like,
)
from app.services import activity_notification_service
from app.services import message_service
from app.services.post_service import _get_mp4_duration_seconds
from app.services.media_security import normalize_mimetype
from app.db import db

DEFAULT_STORY_TTL_HOURS = 24
DEFAULT_FEED_CACHE_TTL_SECONDS = 45
DEFAULT_MAX_VIEWERS_LIMIT = 100
DEFAULT_STORY_CLEANUP_BATCH_SIZE = 200
DEFAULT_VIEW_QUEUE_BATCH_SIZE = 200

STORY_MEDIA_TYPES = {"image", "video"}
MAX_STORY_VIDEO_DURATION_SECONDS = 30
DEFAULT_STORY_DAILY_UPLOAD_LIMIT = 8
DEFAULT_STORY_MENTION_SUGGESTION_LIMIT = 6


class StoryProcessingError(Exception):
    pass


def _utc_now():
    return datetime.utcnow()


def _story_ttl_hours():
    return max(
        1,
        int(current_app.config.get("STORY_TTL_HOURS", DEFAULT_STORY_TTL_HOURS)),
    )


def _feed_cache_ttl_seconds():
    return max(
        1,
        int(
            current_app.config.get(
                "STORY_ACTIVE_FEED_CACHE_TTL_SECONDS",
                DEFAULT_FEED_CACHE_TTL_SECONDS,
            )
        ),
    )


def _view_queue_batch_size():
    return max(
        1,
        int(
            current_app.config.get(
                "STORY_VIEW_QUEUE_BATCH_SIZE",
                DEFAULT_VIEW_QUEUE_BATCH_SIZE,
            )
        ),
    )


def _max_viewers_limit():
    return max(
        1,
        int(current_app.config.get("STORY_VIEWERS_MAX_LIMIT", DEFAULT_MAX_VIEWERS_LIMIT)),
    )


def _cleanup_batch_size():
    return max(
        1,
        int(
            current_app.config.get(
                "STORY_CLEANUP_BATCH_SIZE",
                DEFAULT_STORY_CLEANUP_BATCH_SIZE,
            )
        ),
    )


def _view_recording_async_enabled():
    return bool(current_app.config.get("STORY_VIEW_ASYNC_ENABLED", False))


def _story_daily_upload_limit():
    return max(
        1,
        int(
            current_app.config.get(
                "STORY_DAILY_UPLOAD_LIMIT",
                DEFAULT_STORY_DAILY_UPLOAD_LIMIT,
            )
        ),
    )


def _story_mention_suggestion_limit(raw_limit: int | None) -> int:
    try:
        parsed = int(raw_limit or DEFAULT_STORY_MENTION_SUGGESTION_LIMIT)
    except (TypeError, ValueError):
        parsed = DEFAULT_STORY_MENTION_SUGGESTION_LIMIT
    return max(1, min(6, parsed))


def _story_daily_limit_tz_offset_minutes():
    return int(current_app.config.get("STORY_DAILY_LIMIT_TZ_OFFSET_MINUTES", 0) or 0)


def _daily_quota_bucket_start_utc(now: datetime):
    offset_minutes = _story_daily_limit_tz_offset_minutes()
    shifted = now + timedelta(minutes=offset_minutes)
    shifted_day_start = shifted.replace(hour=0, minute=0, second=0, microsecond=0)
    return shifted_day_start - timedelta(minutes=offset_minutes)


def _story_feed_cache_key(viewer_user_id: int) -> str:
    return f"story:feed:v1:{viewer_user_id}"


def _story_view_queue_key() -> str:
    return "story:view_queue:v1"


def _build_media_url_from_avatar(object_name: str | None):
    if not object_name:
        return None

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


def _build_story_link(story_id: int) -> str:
    base_url = current_app.config.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")
    if base_url:
        return f"{base_url}/api/story/{story_id}"
    return f"/api/story/{story_id}"


def _is_media_not_found(error: S3Error) -> bool:
    return error.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}


def _extract_object_name_from_media_url(media_url: str | None) -> str | None:
    if not media_url:
        return None
    marker = "/media/"
    if marker in media_url:
        return media_url.split(marker, 1)[1].strip("/")
    if media_url.startswith("/media/"):
        return media_url[len("/media/"):].strip("/")
    if "/static/" in media_url:
        static_part = media_url.split("/static/", 1)[1].strip("/")
        return f"static/{static_part}" if static_part else None
    if media_url.startswith("/static/"):
        static_part = media_url[len("/static/"):].strip("/")
        return f"static/{static_part}" if static_part else None
    return None


def _delete_story_media_object(media_url: str | None):
    object_name = _extract_object_name_from_media_url(media_url)
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


def _serialize_story_summary(item):
    return {
        "user_id": item["user_id"],
        "username": item["username"],
        "name": item.get("name") or item["username"],
        "badge": item.get("badge"),
        "avatar": _build_media_url_from_avatar(item.get("avatar_object_name")),
        "profile_image_shape": item.get("profile_image_shape", "circle"),
        "has_unseen_story": bool(item.get("has_unseen_story", False)),
        "story_count": int(item.get("story_count", 0)),
        "first_story_timestamp": (
            item["first_story_timestamp"].isoformat() if item.get("first_story_timestamp") else None
        ),
        "latest_story_timestamp": (
            item["latest_story_timestamp"].isoformat() if item.get("latest_story_timestamp") else None
        ),
        "story_ids": item.get("story_ids", []),
    }


def _serialize_story_detail(story, viewer_view_row=None):
    return {
        "story_id": story.id,
        "user_id": story.user_id,
        "media_url": story.media_url,
        "media_type": story.media_type,
        "created_at": story.created_at.isoformat() if story.created_at else None,
        "expires_at": story.expires_at.isoformat() if story.expires_at else None,
        "mention_user_ids": _safe_story_mentions(story.mention_user_ids),
        "view_count": int(story.view_count or 0),
        "like_count": int(story.like_count or 0),
        "viewer_has_viewed": viewer_view_row is not None,
        "viewer_liked": bool(viewer_view_row.liked) if viewer_view_row else False,
    }


def _safe_story_mentions(raw_text):
    if not raw_text:
        return []
    try:
        data = json.loads(raw_text)
    except (TypeError, ValueError):
        return []
    if not isinstance(data, list):
        return []
    safe_values = []
    for item in data:
        try:
            safe_values.append(int(item))
        except (TypeError, ValueError):
            continue
    return safe_values


def _normalize_mention_usernames(raw_mentions):
    if not isinstance(raw_mentions, list):
        return []

    normalized = []
    seen = set()
    for item in raw_mentions:
        if not isinstance(item, str):
            continue
        username = item.strip()
        if not username or username in seen:
            continue
        seen.add(username)
        normalized.append(username)
        if len(normalized) >= 20:
            break
    return normalized


def _resolve_allowed_mentions(*, poster_username: str, mention_usernames: list[str]):
    if not mention_usernames:
        return []

    poster = user_repository.get_by_username(poster_username)
    if not poster:
        return []

    contact_rows = (
        db.session.query(User.username, User.id)
        .join(Follow, Follow.following_id == User.id)
        .filter(
            Follow.follower_id == poster.id,
            User.username.in_(mention_usernames),
            User.is_suspended.is_(False),
        )
        .all()
    )
    allowed_by_username = {row.username: row.id for row in contact_rows}

    allowed_ids = []
    for username in mention_usernames:
        mention_id = allowed_by_username.get(username)
        if mention_id is None or mention_id == poster.id:
            continue
        allowed_ids.append(mention_id)
    return allowed_ids


def get_mention_candidates(
    *,
    username: str,
    query: str = "",
    limit: int = DEFAULT_STORY_MENTION_SUGGESTION_LIMIT,
):
    poster = user_repository.get_by_username(username)
    if not poster:
        raise ValueError("User not found")

    normalized_query = (query or "").strip().lower()
    normalized_limit = _story_mention_suggestion_limit(limit)

    base_query = (
        db.session.query(User.username)
        .join(Follow, Follow.following_id == User.id)
        .filter(
            Follow.follower_id == poster.id,
            User.is_suspended.is_(False),
            User.id != poster.id,
        )
    )

    if normalized_query:
        contains_pattern = f"%{normalized_query}%"
        starts_pattern = f"{normalized_query}%"
        lower_username = func.lower(User.username)
        base_query = (
            base_query
            .filter(lower_username.like(contains_pattern))
            .order_by(
                case(
                    (lower_username == normalized_query, 0),
                    (lower_username.like(starts_pattern), 1),
                    else_=2,
                ),
                func.length(User.username).asc(),
                User.username.asc(),
            )
        )
    else:
        base_query = base_query.order_by(User.username.asc())

    rows = base_query.limit(normalized_limit).all()
    usernames = [row.username for row in rows if row.username]
    return {
        "query": query or "",
        "limit": normalized_limit,
        "users": [{"username": item} for item in usernames],
    }


def _send_story_mention_dm(*, sender: User, recipient: User, story):
    message_text = f"{sender.username} mentioned you in a story"
    attachment = {
        "type": "story_mention",
        "story_id": story.id,
        "story_media_type": story.media_type,
        "story_media_url": story.media_url,
        "story_link": _build_story_link(story.id),
    }
    payload = message_repository.build_message_payload(
        sender=sender.username,
        encrypted_message=message_text,
        encrypted_key="story_mention",
        attachment=attachment,
        message_type="text",
    )
    message_repository.push_message_payload(recipient.username, payload)
    message_repository.store_private_message_metadata(payload, recipient.username)
    message_repository.record_conversation_timestamp(
        sender.username,
        recipient.username,
        payload.get("timestamp"),
    )


def _send_story_reply_dm(*, sender: User, recipient: User, story, reply_text: str):
    normalized_text = reply_text.strip()
    if not normalized_text:
        raise ValueError("reply_text is required")

    attachment = {
        "type": "story_reply",
        "story_id": story.id,
        "story_media_type": story.media_type,
        "story_media_url": story.media_url,
        "story_link": _build_story_link(story.id),
        "story_reference_preview": {
            "story_id": story.id,
            "media_type": story.media_type,
            "media_url": story.media_url,
        },
    }
    payload = message_repository.build_message_payload(
        sender=sender.username,
        encrypted_message=normalized_text,
        encrypted_key="story_reply",
        attachment=attachment,
        message_type="text",
    )
    message_repository.push_message_payload(recipient.username, payload)
    message_repository.store_private_message_metadata(payload, recipient.username)
    message_repository.record_conversation_timestamp(
        sender.username,
        recipient.username,
        payload.get("timestamp"),
    )


def _cache_story_feed(viewer_user_id: int, payload: dict):
    try:
        redis_client.setex(
            _story_feed_cache_key(viewer_user_id),
            _feed_cache_ttl_seconds(),
            json.dumps(payload),
        )
    except Exception:
        return


def _get_cached_story_feed(viewer_user_id: int):
    try:
        raw = redis_client.get(_story_feed_cache_key(viewer_user_id))
    except Exception:
        return None
    if not raw:
        return None

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")

    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _invalidate_story_feed_cache_for_user_ids(user_ids: set[int]):
    if not user_ids:
        return
    try:
        pipe = redis_client.pipeline()
        for user_id in user_ids:
            pipe.delete(_story_feed_cache_key(user_id))
        pipe.execute()
    except Exception:
        return


def _fetch_follower_user_ids(user_id: int):
    rows = (
        db.session.query(Follow.follower_id)
        .filter(Follow.following_id == user_id)
        .all()
    )
    return {row[0] for row in rows}


def _ensure_story_visible_to_viewer(story, viewer_user: User):
    if story.user_id == viewer_user.id:
        return

    is_following = (
        db.session.query(Follow.id)
        .filter(
            Follow.follower_id == viewer_user.id,
            Follow.following_id == story.user_id,
        )
        .first()
    )
    if not is_following:
        raise PermissionError("You are not allowed to view this story")


def cleanup_expired_stories(*, batch_size: int | None = None):
    size = batch_size if batch_size is not None else _cleanup_batch_size()
    return delete_expired_stories(before_dt=_utc_now(), batch_size=size)


def enqueue_story_view(*, story_id: int, viewer_user_id: int):
    payload = {
        "story_id": int(story_id),
        "viewer_user_id": int(viewer_user_id),
        "recorded_at": _utc_now().isoformat(),
    }
    try:
        redis_client.rpush(_story_view_queue_key(), json.dumps(payload))
        redis_client.expire(_story_view_queue_key(), 24 * 60 * 60)
    except Exception:
        return False
    return True


def flush_story_view_queue(*, batch_size: int | None = None):
    size = max(1, int(batch_size or _view_queue_batch_size()))
    processed = 0

    while processed < size:
        try:
            raw = redis_client.lpop(_story_view_queue_key())
        except Exception:
            break
        if raw is None:
            break

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            continue

        story_id = payload.get("story_id")
        viewer_user_id = payload.get("viewer_user_id")
        if not story_id or not viewer_user_id:
            continue

        try:
            record_story_view(story_id=int(story_id), viewer_user_id=int(viewer_user_id))
            processed += 1
        except Exception:
            continue

    return processed


def upload_story(*, username: str, file_storage, mention_usernames=None):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")
    if user.is_suspended:
        raise ValueError("Account suspended")
    if file_storage is None or not getattr(file_storage, "filename", ""):
        raise ValueError("Attachment file is required")

    normalized_mimetype = normalize_mimetype(getattr(file_storage, "mimetype", ""))
    if not normalized_mimetype:
        raise ValueError("Attachment mime type is required")
    if normalized_mimetype.startswith("audio/"):
        raise ValueError("Only photo and video stories are supported")

    if normalized_mimetype.startswith("video/"):
        duration_seconds = _get_mp4_duration_seconds(file_storage)
        if duration_seconds is not None and duration_seconds > MAX_STORY_VIDEO_DURATION_SECONDS:
            raise ValueError("Story videos must be 30 seconds or shorter")

    now = _utc_now()
    daily_limit = _story_daily_upload_limit()
    quota_bucket_start = _daily_quota_bucket_start_utc(now)
    reserved_quota = reserve_daily_story_slot(
        user_id=user.id,
        bucket_start=quota_bucket_start,
        limit=daily_limit,
    )
    if not reserved_quota:
        raise ValueError(
            f"Daily story limit reached (max {daily_limit} stories per day)"
        )
    try:

        attachment_payload = message_service.upload_message_attachment(
            username=username,
            file_storage=file_storage,
            upload_scope="story",
        )
        media_type = (attachment_payload.get("type") or "").strip().lower()
        if media_type not in STORY_MEDIA_TYPES:
            raise ValueError("Only photo and video stories are supported")

        mentions = _normalize_mention_usernames(mention_usernames or [])
        allowed_mention_ids = _resolve_allowed_mentions(
            poster_username=username,
            mention_usernames=mentions,
        )

        story = create_story(
            user_id=user.id,
            media_url=attachment_payload.get("url") or "",
            media_type=media_type,
            expires_at=now + timedelta(hours=_story_ttl_hours()),
            mention_user_ids=allowed_mention_ids,
            auto_commit=False,
        )
        db.session.commit()
    except Exception:
        db.session.rollback()
        try:
            release_daily_story_slot(
                user_id=user.id,
                bucket_start=quota_bucket_start,
            )
            db.session.commit()
        except Exception:
            db.session.rollback()
        raise

    cache_users = _fetch_follower_user_ids(user.id)
    cache_users.add(user.id)
    _invalidate_story_feed_cache_for_user_ids(cache_users)

    if allowed_mention_ids:
        recipients = User.query.filter(User.id.in_(allowed_mention_ids)).all()
        for recipient in recipients:
            try:
                _send_story_mention_dm(sender=user, recipient=recipient, story=story)
                activity_notification_service.notify_story_mention(
                    actor_username=username,
                    target_username=recipient.username,
                    story_id=story.id,
                )
            except Exception:
                current_app.logger.exception(
                    "Failed to deliver story mention side effect story_id=%s recipient=%s",
                    story.id,
                    recipient.username,
                )

    return {
        "story_id": story.id,
        "media_url": story.media_url,
        "media_type": story.media_type,
        "expires_at": story.expires_at.isoformat(),
        "mention_user_ids": allowed_mention_ids,
    }


def get_story_feed(*, username: str):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    cleanup_expired_stories()

    cached = _get_cached_story_feed(user.id)
    if cached is not None:
        return cached

    grouped = get_active_feed_grouped(viewer_user_id=user.id, now=_utc_now())

    payload = {
        "user_stories": [_serialize_story_summary(item) for item in grouped],
        "generated_at": _utc_now().isoformat(),
    }
    _cache_story_feed(user.id, payload)
    return payload


def get_story_bundle(*, username: str, story_id: int):
    viewer = user_repository.get_by_username(username)
    if not viewer:
        raise ValueError("User not found")

    cleanup_expired_stories()

    story = get_active_story(int(story_id), now=_utc_now())
    if story is None:
        raise ValueError("Story not found")

    _ensure_story_visible_to_viewer(story, viewer)

    owner = User.query.filter(User.id == story.user_id).first()
    if owner is None:
        raise ValueError("Story owner not found")
    owner_profile = Profile.query.filter(Profile.user_id == owner.id).first()

    owner_stories = get_active_stories_for_user(owner.id, now=_utc_now())
    story_ids = [item.id for item in owner_stories]
    views_map = get_story_views_map(story_ids=story_ids, viewer_user_id=viewer.id)

    serialized_owner_stories = []
    for owner_story in owner_stories:
        serialized_owner_stories.append(
            _serialize_story_detail(
                owner_story,
                viewer_view_row=views_map.get(owner_story.id),
            )
        )

    current_view_row = views_map.get(story.id)

    return {
        "story": _serialize_story_detail(story, viewer_view_row=current_view_row),
        "owner": {
            "user_id": owner.id,
            "username": owner.username,
            "name": owner_profile.name if owner_profile and owner_profile.name else owner.username,
            "badge": owner.badge,
        },
        "user_stories": serialized_owner_stories,
    }


def delete_story(*, username: str, story_id: int):
    owner = user_repository.get_by_username(username)
    if not owner:
        raise ValueError("User not found")

    story = get_story_by_id(int(story_id))
    if story is None:
        raise ValueError("Story not found")
    if story.user_id != owner.id:
        raise PermissionError("Only story owner can delete this story")

    story_id_value = int(story.id)
    media_url = story.media_url
    deleted = delete_story_with_views(story_id=story_id_value)
    if not deleted:
        raise ValueError("Story not found")

    cache_users = _fetch_follower_user_ids(owner.id)
    cache_users.add(owner.id)
    _invalidate_story_feed_cache_for_user_ids(cache_users)

    try:
        _delete_story_media_object(media_url)
    except Exception:
        current_app.logger.exception(
            "Failed to delete story media for story_id=%s",
            story_id_value,
        )

    return {
        "story_id": story_id_value,
        "deleted": True,
        "message": "Story deleted",
    }


def record_view(*, username: str, story_id: int):
    viewer = user_repository.get_by_username(username)
    if not viewer:
        raise ValueError("User not found")

    story = get_active_story(int(story_id), now=_utc_now())
    if story is None:
        raise ValueError("Story not found")

    _ensure_story_visible_to_viewer(story, viewer)

    if _view_recording_async_enabled() and enqueue_story_view(
        story_id=story.id,
        viewer_user_id=viewer.id,
    ):
        created = False
    else:
        created = record_story_view(story_id=story.id, viewer_user_id=viewer.id)

    cache_users = {viewer.id, story.user_id}
    _invalidate_story_feed_cache_for_user_ids(cache_users)
    refreshed = get_story_by_id(story.id)

    return {
        "story_id": story.id,
        "recorded": True,
        "view_count": int(refreshed.view_count if refreshed else story.view_count or 0),
        "created": bool(created),
        "queued": bool(_view_recording_async_enabled()),
    }


def set_like(*, username: str, story_id: int, liked: bool):
    viewer = user_repository.get_by_username(username)
    if not viewer:
        raise ValueError("User not found")

    story = get_active_story(int(story_id), now=_utc_now())
    if story is None:
        raise ValueError("Story not found")

    _ensure_story_visible_to_viewer(story, viewer)

    liked_flag = set_story_like(story_id=story.id, viewer_user_id=viewer.id, liked=liked)
    # Refresh row for accurate counters after update.
    refreshed = get_story_by_id(story.id)

    cache_users = {viewer.id, story.user_id}
    _invalidate_story_feed_cache_for_user_ids(cache_users)

    return {
        "story_id": story.id,
        "liked": bool(liked_flag),
        "like_count": int(refreshed.like_count if refreshed else 0),
        "view_count": int(refreshed.view_count if refreshed else 0),
    }


def get_viewers(*, username: str, story_id: int, page: int, limit: int):
    owner = user_repository.get_by_username(username)
    if not owner:
        raise ValueError("User not found")

    story = get_story_by_id(int(story_id))
    if story is None:
        raise ValueError("Story not found")
    if story.user_id != owner.id:
        raise PermissionError("Only story owner can access viewers")

    page = page if isinstance(page, int) and page > 0 else 1
    limit = limit if isinstance(limit, int) and limit > 0 else 20
    limit = min(limit, _max_viewers_limit())

    total, rows = get_story_viewers_page(story_id=story.id, page=page, limit=limit)

    viewers = []
    for row in rows:
        viewers.append(
            {
                "viewer_id": row["viewer_id"],
                "username": row["username"],
                "badge": row.get("badge"),
                "avatar": _build_media_url_from_avatar(row.get("avatar_object_name")),
                "profile_image_shape": row.get("profile_image_shape", "circle"),
                "viewed_at": row["viewed_at"].isoformat() if row.get("viewed_at") else None,
                "liked": bool(row.get("liked", False)),
            }
        )

    return {
        "story_id": story.id,
        "page": page,
        "limit": limit,
        "total": total,
        "viewers": viewers,
    }


def reply_to_story(*, username: str, story_id: int, reply_text: str):
    sender = user_repository.get_by_username(username)
    if not sender:
        raise ValueError("User not found")

    story = get_active_story(int(story_id), now=_utc_now())
    if story is None:
        raise ValueError("Story not found")

    _ensure_story_visible_to_viewer(story, sender)

    owner = User.query.filter(User.id == story.user_id).first()
    if owner is None:
        raise ValueError("Story owner not found")
    if owner.id == sender.id:
        raise ValueError("You cannot reply to your own story")

    _send_story_reply_dm(
        sender=sender,
        recipient=owner,
        story=story,
        reply_text=reply_text,
    )
    activity_notification_service.notify_story_reply(
        actor_username=sender.username,
        target_username=owner.username,
        story_id=story.id,
        reply_preview=reply_text.strip()[:160],
    )

    return {
        "story_id": story.id,
        "recipient": owner.username,
        "message": "Story reply sent",
    }


def run_story_cleanup_loop():
    interval_seconds = max(
        10,
        int(current_app.config.get("STORY_CLEANUP_INTERVAL_SECONDS", 300)),
    )

    while True:
        time.sleep(interval_seconds)
        try:
            deleted = cleanup_expired_stories()
            if deleted:
                current_app.logger.info(
                    "Story cleanup removed expired stories: %s",
                    deleted,
                )
            if _view_recording_async_enabled():
                flush_story_view_queue()
        except Exception:
            current_app.logger.exception("Story cleanup loop failed")
