import time
from sqlalchemy import func
from flask import current_app, has_app_context, has_request_context, request

from app.db import db
from app.models.comment_model import Comment
from app.models.profile_model import Profile
from app.models.user_model import User
from app.models.vote_model import Vote
from app.repositories import user_repository
from app.repositories.comment_repository import create_comment, get_comments_by_post
from app.repositories.comment_repository import (
    get_comment_subtree_for_roots,
    get_root_comments_by_post_id,
)


def add_comment(author_id, post_id, text, parent_id=None):
    if not text or not text.strip():
        raise ValueError("Comment text is required")

    comment = create_comment(
        author_id=author_id,
        post_id=post_id,
        text=text,
        parent_id=parent_id
    )

    db.session.commit()
    return comment


def get_post_comments(post_id):
    comments = get_comments_by_post(post_id)
    return build_comment_tree(comments)


def sort_comments(comments: list):
    return sorted(
        comments,
        key=lambda c: (
            c.get("score", 0),
            c.get("created_at")
        ),
        reverse=True,
    )


def build_comment_tree(comments):
    comment_map = {c["id"]: c for c in comments}
    roots = []

    for comment in comments:
        pid = comment["parent_id"]
        if pid:
            parent = comment_map.get(pid)
            if parent:
                parent["replies"].append(comment)
        else:
            roots.append(comment)

    return _sort_comment_tree(roots)


def _sort_comment_tree(comments: list):
    ordered = sort_comments(comments)
    for comment in ordered:
        if comment["replies"]:
            comment["replies"] = _sort_comment_tree(comment["replies"])
    return ordered


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


def _log_comments_timing(
    *,
    post_id: int,
    page: int,
    page_size: int,
    roots_count: int,
    comments_count: int,
    started_at: float,
):
    if not has_app_context():
        return

    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    threshold_ms = int(current_app.config.get("QUERY_TIMING_LOG_SLOW_MS", 150))
    level = "info" if elapsed_ms >= threshold_ms else "debug"
    log_fn = current_app.logger.info if level == "info" else current_app.logger.debug
    log_fn(
        "comments_query post_id=%s page=%s page_size=%s roots=%s nodes=%s duration_ms=%s",
        post_id,
        page,
        page_size,
        roots_count,
        comments_count,
        elapsed_ms,
    )


def get_comments_tree_by_post(post_id: int, page: int, page_size: int):
    from app.services import report_service

    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 10), 50))
    started_at = time.perf_counter()

    if not report_service.get_visible_post(post_id):
        raise ValueError("Post not found")

    root_comments = get_root_comments_by_post_id(post_id, page, page_size)
    root_ids = [comment.id for comment in root_comments]
    if not root_ids:
        _log_comments_timing(
            post_id=post_id,
            page=page,
            page_size=page_size,
            roots_count=0,
            comments_count=0,
            started_at=started_at,
        )
        return []

    raw_comments = get_comment_subtree_for_roots(post_id, root_ids)
    author_ids = {comment.author_id for comment in raw_comments}
    users = User.query.filter(User.id.in_(author_ids)).all() if author_ids else []
    profiles = Profile.query.filter(Profile.user_id.in_(author_ids)).all() if author_ids else []

    user_by_id = {user.id: user for user in users}
    profile_by_user_id = {profile.user_id: profile for profile in profiles}

    all_comments = [serialize_comment(c, user_by_id, profile_by_user_id) for c in raw_comments]

    tree = build_comment_tree(all_comments)
    comments_by_id = {comment["id"]: comment for comment in tree}
    paged_roots = [comments_by_id[root_id] for root_id in root_ids if root_id in comments_by_id]

    _log_comments_timing(
        post_id=post_id,
        page=page,
        page_size=page_size,
        roots_count=len(root_ids),
        comments_count=len(raw_comments),
        started_at=started_at,
    )
    return paged_roots


def serialize_comment(comment, user_by_id=None, profile_by_user_id=None):
    user_by_id = user_by_id or {}
    profile_by_user_id = profile_by_user_id or {}
    author_id = comment.author_id
    user = user_by_id.get(author_id)
    profile = profile_by_user_id.get(author_id)

    username = user.username if user else f"user-{author_id}"
    name = profile.name if profile else username

    profile_image_url = None
    if profile and profile.image_object_name:
        profile_image_url = _build_media_url(profile.image_object_name)

    author_payload = {
        "id": author_id,
        "username": username,
        "name": name,
        "badge": user.badge if user else None,
        "profile_image_url": profile_image_url,
        "profile_image_shape": (
            profile.profile_image_shape
            if profile and profile.profile_image_shape
            else "circle"
        ),
    }

    return {
        "id": comment.id,
        "author": author_payload,
        "text": comment.text,
        "score": comment.score,
        "created_at": comment.created_at,
        "parent_id": comment.parent_id,
        "replies": []
    }


def get_score(target_type: str, target_id: int) -> int:
    score = db.session.query(func.coalesce(func.sum(Vote.value), 0)).filter_by(
        target_type=target_type,
        target_id=target_id
    ).scalar()

    return int(score or 0)


def _delete_comment_tree(comment_id: int):
    children = Comment.query.filter_by(parent_id=comment_id).all()
    for child in children:
        _delete_comment_tree(child.id)

    Vote.query.filter(
        Vote.target_type == "comment",
        Vote.target_id == comment_id,
    ).delete(synchronize_session=False)
    Comment.query.filter_by(id=comment_id).delete(synchronize_session=False)


def delete_comment_by_username(comment_id: int, username: str):
    user = user_repository.get_by_username(username)
    if not user or getattr(user, "is_suspended", False):
        raise ValueError("User not found")

    comment = Comment.query.get(comment_id)
    if not comment:
        raise ValueError("Comment not found")

    from app.services import report_service

    if not report_service.get_visible_post(comment.post_id):
        raise ValueError("Comment not found")

    if comment.author_id != user.id:
        raise PermissionError("You can only delete your own comments")

    _delete_comment_tree(comment.id)
    db.session.commit()
