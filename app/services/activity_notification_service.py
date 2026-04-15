import json
import logging
import math
import time
from datetime import datetime, timedelta
from datetime import timezone

from sqlalchemy import func
from flask import current_app, has_request_context, request

from app.db import db
from app.models.comment_model import Comment
from app.models.follow_model import Follow
from app.models.post_model import Post
from app.models.user_model import User
from app.models.profile_model import Profile
from app.models.vote_model import Vote
from app.repositories import user_repository
from app.services import async_task_service
from app.services import report_service
from app.repositories.activity_notification_repository import (
    create_notification,
    get_latest_notification_for_target,
    get_notifications_page,
    count_unread,
    mark_all_read,
    mark_read_by_ids,
)
from app.extensions.extensions import socketio


MAX_LIMIT = 50
DEFAULT_LIMIT = 20
LOGGER = logging.getLogger(__name__)
MILESTONE_KIND_POST_LIKE = "post_like_milestone"
MILESTONE_KIND_POST_COMMENT = "post_comment_milestone"
_ACTIVE_USERS_CACHE = {"value": 0, "expires_at": 0.0}


def _build_profile_image_url(image_object_name):
    if not image_object_name:
        return None
    base_url = current_app.config.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")
    if base_url:
        return f"{base_url}/media/{image_object_name}"
    return f"/media/{image_object_name}"


def _serialize_notification(notif, user_by_id, profile_by_user_id):
    actor = user_by_id.get(notif.actor_id)
    profile = profile_by_user_id.get(notif.actor_id)

    actor_username = actor.username if actor else f"user-{notif.actor_id}"
    actor_name = profile.name if profile else actor_username
    actor_image = None
    if profile and profile.image_object_name:
        actor_image = _build_profile_image_url(profile.image_object_name)

    extra = None
    if notif.extra:
        try:
            extra = json.loads(notif.extra)
        except (json.JSONDecodeError, TypeError):
            extra = None

    return {
        "id": notif.id,
        "kind": notif.kind,
        "actor": {
            "id": notif.actor_id,
            "username": actor_username,
            "name": actor_name,
            "profile_image_url": actor_image,
        },
        "target_type": notif.target_type,
        "target_id": notif.target_id,
        "extra": extra,
        "is_read": notif.is_read,
        "created_at": notif.created_at.replace(tzinfo=timezone.utc).isoformat()
        if notif.created_at
        else None,
    }


def _build_author_maps(author_ids):
    if not author_ids:
        return {}, {}
    users = User.query.filter(User.id.in_(author_ids)).all()
    profiles = Profile.query.filter(Profile.user_id.in_(author_ids)).all()
    return (
        {u.id: u for u in users},
        {p.user_id: p for p in profiles},
    )


def get_activity_notifications(username, page, limit, unread_only=False):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    page = max(1, page)
    limit = min(max(1, limit), MAX_LIMIT)

    total, items = get_notifications_page(user.id, page, limit, unread_only=unread_only)
    actor_ids = {n.actor_id for n in items}
    user_by_id, profile_by_user_id = _build_author_maps(actor_ids)

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "notifications": [
            _serialize_notification(n, user_by_id, profile_by_user_id) for n in items
        ],
    }


def get_unread_count(username):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")
    return count_unread(user.id)


def mark_notifications_read(username, notification_ids=None):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    if notification_ids:
        return mark_read_by_ids(user.id, notification_ids)
    return mark_all_read(user.id)


def _emit_activity_notification(recipient_username, payload):
    socketio.emit("activity_notification", payload, room=recipient_username)


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_extra_dict(extra_text):
    if not extra_text:
        return {}
    try:
        data = json.loads(extra_text)
    except (json.JSONDecodeError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _milestones_enabled():
    return bool(current_app.config.get("ACTIVITY_MILESTONE_ENABLED", True))


def _active_users_cache_ttl_seconds():
    return max(
        0,
        _safe_int(
            current_app.config.get(
                "ACTIVITY_MILESTONE_ACTIVE_USERS_CACHE_TTL_SECONDS",
                300,
            ),
            default=300,
        ),
    )


def _active_users_window_days():
    return max(
        1,
        _safe_int(
            current_app.config.get("ACTIVITY_MILESTONE_ACTIVE_USERS_WINDOW_DAYS", 7),
            default=7,
        ),
    )


def _count_recent_active_users():
    now_ts = time.time()
    ttl = _active_users_cache_ttl_seconds()
    if (
        ttl > 0
        and _ACTIVE_USERS_CACHE["value"] > 0
        and _ACTIVE_USERS_CACHE["expires_at"] > now_ts
    ):
        return _ACTIVE_USERS_CACHE["value"]

    cutoff = datetime.utcnow() - timedelta(days=_active_users_window_days())

    post_authors_q = (
        db.session.query(Post.author_id.label("user_id"))
        .filter(
            Post.created_at >= cutoff,
            Post.is_hidden.is_(False),
        )
    )
    comment_authors_q = (
        db.session.query(Comment.author_id.label("user_id"))
        .filter(
            Comment.created_at >= cutoff,
            Comment.is_deleted.is_(False),
        )
    )
    vote_authors_q = (
        db.session.query(Vote.user_id.label("user_id"))
        .filter(Vote.created_at >= cutoff)
    )
    follow_authors_q = (
        db.session.query(Follow.follower_id.label("user_id"))
        .filter(Follow.created_at >= cutoff)
    )

    active_subquery = post_authors_q.union(
        comment_authors_q,
        vote_authors_q,
        follow_authors_q,
    ).subquery()

    active_count = _safe_int(
        db.session.query(func.count()).select_from(active_subquery).scalar(),
        default=0,
    )
    if active_count <= 0:
        active_count = _safe_int(
            User.query.filter(User.is_suspended.is_(False)).count(),
            default=0,
        )

    active_count = max(1, active_count)
    _ACTIVE_USERS_CACHE["value"] = active_count
    _ACTIVE_USERS_CACHE["expires_at"] = now_ts + ttl if ttl > 0 else 0.0
    return active_count


def _resolve_threshold(active_users, percent_config_key, min_config_key):
    percent = min(
        100,
        max(
            1,
            _safe_int(current_app.config.get(percent_config_key, 10), default=10),
        ),
    )
    min_count = max(
        1,
        _safe_int(current_app.config.get(min_config_key, 1), default=1),
    )
    ratio_count = math.ceil((active_users * percent) / 100.0)
    threshold = max(min_count, ratio_count)
    return threshold, percent


def _count_post_likes(post_id, *, exclude_user_id=None):
    query = db.session.query(func.count(func.distinct(Vote.user_id))).filter(
        Vote.target_type == "post",
        Vote.target_id == post_id,
        Vote.value == 1,
    )
    if exclude_user_id is not None:
        query = query.filter(Vote.user_id != exclude_user_id)
    return max(0, _safe_int(query.scalar(), default=0))


def _count_post_unique_commenters(post_id, *, exclude_user_id=None):
    query = db.session.query(func.count(func.distinct(Comment.author_id))).filter(
        Comment.post_id == post_id,
        Comment.is_deleted.is_(False),
    )
    if exclude_user_id is not None:
        query = query.filter(Comment.author_id != exclude_user_id)
    return max(0, _safe_int(query.scalar(), default=0))


def _last_milestone_count(recipient_id, kind, post_id):
    previous = get_latest_notification_for_target(
        recipient_id=recipient_id,
        kind=kind,
        target_type="post",
        target_id=post_id,
    )
    if not previous:
        return 0
    extra = _parse_extra_dict(previous.extra)
    return max(0, _safe_int(extra.get("engagement_count"), default=0))


def _maybe_emit_post_engagement_milestone(
    *,
    post,
    kind,
    engagement_type,
    engagement_count,
    threshold,
    percent,
    active_users,
):
    if engagement_count < threshold:
        return

    last_count = _last_milestone_count(post.author_id, kind, post.id)
    if engagement_count <= last_count:
        return

    milestone_count = (engagement_count // threshold) * threshold
    previous_milestone = (last_count // threshold) * threshold if last_count else 0
    if milestone_count <= previous_milestone:
        return

    post_owner = User.query.get(post.author_id)
    if not post_owner:
        return

    extra = json.dumps(
        {
            "engagement_type": engagement_type,
            "engagement_count": engagement_count,
            "milestone_count": milestone_count,
            "threshold": threshold,
            "percent_threshold": percent,
            "active_users": active_users,
            "post_id": post.id,
            "post_text_preview": (post.text or "")[:120],
        }
    )

    notif = create_notification(
        recipient_id=post_owner.id,
        actor_id=post_owner.id,
        kind=kind,
        target_type="post",
        target_id=post.id,
        extra=extra,
    )
    db.session.commit()

    user_by_id, profile_by_user_id = _build_author_maps({post_owner.id})
    payload = _serialize_notification(notif, user_by_id, profile_by_user_id)
    _emit_activity_notification(post_owner.username, payload)


def _maybe_emit_post_like_milestone(post):
    if not _milestones_enabled():
        return

    active_users = _count_recent_active_users()
    threshold, percent = _resolve_threshold(
        active_users,
        "ACTIVITY_MILESTONE_LIKE_PERCENT",
        "ACTIVITY_MILESTONE_MIN_LIKES",
    )
    like_count = _count_post_likes(post.id, exclude_user_id=post.author_id)
    _maybe_emit_post_engagement_milestone(
        post=post,
        kind=MILESTONE_KIND_POST_LIKE,
        engagement_type="likes",
        engagement_count=like_count,
        threshold=threshold,
        percent=percent,
        active_users=active_users,
    )


def _maybe_emit_post_comment_milestone(post):
    if not _milestones_enabled():
        return

    active_users = _count_recent_active_users()
    threshold, percent = _resolve_threshold(
        active_users,
        "ACTIVITY_MILESTONE_COMMENT_PERCENT",
        "ACTIVITY_MILESTONE_MIN_COMMENTERS",
    )
    commenters_count = _count_post_unique_commenters(
        post.id,
        exclude_user_id=post.author_id,
    )
    _maybe_emit_post_engagement_milestone(
        post=post,
        kind=MILESTONE_KIND_POST_COMMENT,
        engagement_type="comments",
        engagement_count=commenters_count,
        threshold=threshold,
        percent=percent,
        active_users=active_users,
    )


def _notify_follow_sync(actor_username, target_username):
    actor = user_repository.get_by_username(actor_username)
    target = user_repository.get_by_username(target_username)
    if not actor or not target or actor.id == target.id:
        return

    notif = create_notification(
        recipient_id=target.id,
        actor_id=actor.id,
        kind="follow",
    )
    db.session.commit()

    user_by_id, profile_by_user_id = _build_author_maps({actor.id})
    payload = _serialize_notification(notif, user_by_id, profile_by_user_id)
    _emit_activity_notification(target_username, payload)


def _notify_unfollow_sync(actor_username, target_username):
    actor = user_repository.get_by_username(actor_username)
    target = user_repository.get_by_username(target_username)
    if not actor or not target or actor.id == target.id:
        return

    notif = create_notification(
        recipient_id=target.id,
        actor_id=actor.id,
        kind="unfollow",
    )
    db.session.commit()

    user_by_id, profile_by_user_id = _build_author_maps({actor.id})
    payload = _serialize_notification(notif, user_by_id, profile_by_user_id)
    _emit_activity_notification(target_username, payload)


def _notify_comment_sync(
    actor_username,
    post_id,
    comment_text,
    comment_id=None,
    parent_comment_id=None,
):
    actor = user_repository.get_by_username(actor_username)
    if not actor:
        return

    post = report_service.get_visible_post(post_id)
    if not post:
        return

    base_extra = {
        "comment_preview": comment_text or "",
        "post_text_preview": (post.text or "")[:120],
        "post_id": post.id,
        "comment_id": comment_id,
    }

    notifications_by_recipient = {}

    post_owner = User.query.get(post.author_id)
    if post_owner and post_owner.id != actor.id:
        target_type = "comment" if comment_id is not None else "post"
        target_id = comment_id if comment_id is not None else post.id
        notifications_by_recipient[post_owner.id] = {
            "recipient": post_owner,
            "kind": "comment",
            "target_type": target_type,
            "target_id": target_id,
            "extra": json.dumps(base_extra),
        }

    if parent_comment_id:
        parent_comment = Comment.query.get(parent_comment_id)
        if (
            parent_comment
            and parent_comment.post_id == post.id
            and parent_comment.author_id != actor.id
        ):
            parent_author = User.query.get(parent_comment.author_id)
            if parent_author:
                reply_extra = dict(base_extra)
                reply_extra["parent_comment_id"] = parent_comment.id
                reply_extra["parent_comment_preview"] = parent_comment.text or ""
                notifications_by_recipient[parent_author.id] = {
                    "recipient": parent_author,
                    "kind": "comment_reply",
                    "target_type": "comment",
                    "target_id": comment_id,
                    "extra": json.dumps(reply_extra),
                }

    if notifications_by_recipient:
        created_notifications = []
        for data in notifications_by_recipient.values():
            recipient = data["recipient"]
            notif = create_notification(
                recipient_id=recipient.id,
                actor_id=actor.id,
                kind=data["kind"],
                target_type=data["target_type"],
                target_id=data["target_id"],
                extra=data["extra"],
            )
            created_notifications.append((recipient, notif))
        db.session.commit()

        user_by_id, profile_by_user_id = _build_author_maps({actor.id})
        for recipient, notif in created_notifications:
            payload = _serialize_notification(notif, user_by_id, profile_by_user_id)
            _emit_activity_notification(recipient.username, payload)

    if post_owner and post_owner.id == post.author_id:
        _maybe_emit_post_comment_milestone(post)


def _notify_vote_sync(actor_username, target_type, target_id, value):
    actor = user_repository.get_by_username(actor_username)
    if not actor:
        return

    if target_type == "post":
        post = report_service.get_visible_post(target_id)
        if not post or post.author_id == actor.id:
            return
        recipient = User.query.get(post.author_id)
        post_preview = (post.text or "")[:120]
    else:
        return

    if not recipient:
        return

    vote_label = "upvote" if value == 1 else "downvote"
    extra = json.dumps({
        "vote_value": value,
        "vote_label": vote_label,
        "post_text_preview": post_preview,
    })

    notif = create_notification(
        recipient_id=recipient.id,
        actor_id=actor.id,
        kind="vote",
        target_type=target_type,
        target_id=target_id,
        extra=extra,
    )
    db.session.commit()

    user_by_id, profile_by_user_id = _build_author_maps({actor.id})
    payload = _serialize_notification(notif, user_by_id, profile_by_user_id)
    _emit_activity_notification(recipient.username, payload)

    if target_type == "post" and value == 1:
        _maybe_emit_post_like_milestone(post)


def process_async_notification_event(payload: dict):
    if not isinstance(payload, dict):
        return

    event = (payload.get("event") or "").strip().lower()
    if event == "follow":
        _notify_follow_sync(
            payload.get("actor_username"),
            payload.get("target_username"),
        )
        return

    if event == "unfollow":
        _notify_unfollow_sync(
            payload.get("actor_username"),
            payload.get("target_username"),
        )
        return

    if event == "comment":
        _notify_comment_sync(
            actor_username=payload.get("actor_username"),
            post_id=payload.get("post_id"),
            comment_text=payload.get("comment_text"),
            comment_id=payload.get("comment_id"),
            parent_comment_id=payload.get("parent_comment_id"),
        )
        return

    if event == "vote":
        _notify_vote_sync(
            payload.get("actor_username"),
            payload.get("target_type"),
            payload.get("target_id"),
            payload.get("value"),
        )
        return

    LOGGER.warning("Unknown activity notification async event: %s", event)


def notify_follow(actor_username, target_username):
    task_payload = {
        "event": "follow",
        "actor_username": actor_username,
        "target_username": target_username,
    }
    if async_task_service.enqueue_activity_notification_event(
        task_payload,
        source="activity_notification.notify_follow",
    ):
        return
    if async_task_service.should_fallback_inline():
        _notify_follow_sync(actor_username, target_username)


def notify_unfollow(actor_username, target_username):
    task_payload = {
        "event": "unfollow",
        "actor_username": actor_username,
        "target_username": target_username,
    }
    if async_task_service.enqueue_activity_notification_event(
        task_payload,
        source="activity_notification.notify_unfollow",
    ):
        return
    if async_task_service.should_fallback_inline():
        _notify_unfollow_sync(actor_username, target_username)


def notify_comment(
    actor_username,
    post_id,
    comment_text,
    comment_id=None,
    parent_comment_id=None,
):
    task_payload = {
        "event": "comment",
        "actor_username": actor_username,
        "post_id": post_id,
        "comment_text": comment_text,
        "comment_id": comment_id,
        "parent_comment_id": parent_comment_id,
    }
    if async_task_service.enqueue_activity_notification_event(
        task_payload,
        source="activity_notification.notify_comment",
    ):
        return
    if async_task_service.should_fallback_inline():
        _notify_comment_sync(
            actor_username=actor_username,
            post_id=post_id,
            comment_text=comment_text,
            comment_id=comment_id,
            parent_comment_id=parent_comment_id,
        )


def notify_vote(actor_username, target_type, target_id, value):
    task_payload = {
        "event": "vote",
        "actor_username": actor_username,
        "target_type": target_type,
        "target_id": target_id,
        "value": value,
    }
    if async_task_service.enqueue_activity_notification_event(
        task_payload,
        source="activity_notification.notify_vote",
    ):
        return
    if async_task_service.should_fallback_inline():
        _notify_vote_sync(actor_username, target_type, target_id, value)
