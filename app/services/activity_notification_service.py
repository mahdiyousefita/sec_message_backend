import json
from datetime import timezone

from flask import current_app, has_request_context, request

from app.db import db
from app.models.comment_model import Comment
from app.models.post_model import Post
from app.models.user_model import User
from app.models.profile_model import Profile
from app.repositories import user_repository
from app.repositories.activity_notification_repository import (
    create_notification,
    get_notifications_page,
    count_unread,
    mark_all_read,
    mark_read_by_ids,
)
from app.extensions.extensions import socketio


MAX_LIMIT = 50
DEFAULT_LIMIT = 20


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


def notify_follow(actor_username, target_username):
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


def notify_unfollow(actor_username, target_username):
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


def notify_comment(
    actor_username,
    post_id,
    comment_text,
    comment_id=None,
    parent_comment_id=None,
):
    actor = user_repository.get_by_username(actor_username)
    if not actor:
        return

    post = Post.query.get(post_id)
    if not post:
        return

    base_extra = {
        "comment_preview": (comment_text or "")[:120],
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
                notifications_by_recipient[parent_author.id] = {
                    "recipient": parent_author,
                    "kind": "comment_reply",
                    "target_type": "comment",
                    "target_id": comment_id,
                    "extra": json.dumps(reply_extra),
                }

    if not notifications_by_recipient:
        return

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


def notify_vote(actor_username, target_type, target_id, value):
    actor = user_repository.get_by_username(actor_username)
    if not actor:
        return

    if target_type == "post":
        post = Post.query.get(target_id)
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
