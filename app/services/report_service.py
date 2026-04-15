import logging
import time
from datetime import datetime, timedelta

from flask import current_app, has_app_context

from app.config import Config
from app.db import db
from app.models.admin_model import AdminUser
from app.models.comment_model import Comment
from app.models.follow_model import Follow
from app.models.media_model import Media
from app.models.post_model import Post
from app.models.profile_model import Profile
from app.models.report_model import PostReport
from app.models.user_model import User
from app.models.vote_model import Vote
from app.repositories import report_repository


REPORT_STATUS_PENDING = "pending"
REPORT_STATUS_HANDLED = "handled"

REPORT_TYPE_SCAM = "scam"
REPORT_TYPE_UNETHICAL_CONTENT = "unethical_content"
REPORT_TYPE_VIOLENCE = "violence"
REPORT_TYPE_SPAM = "spam"
REPORT_TYPE_FALSE_INFORMATION = "false_information"

REPORT_TYPES = (
    REPORT_TYPE_SCAM,
    REPORT_TYPE_UNETHICAL_CONTENT,
    REPORT_TYPE_VIOLENCE,
    REPORT_TYPE_SPAM,
    REPORT_TYPE_FALSE_INFORMATION,
)

ADMIN_DECISION_DISMISS = "dismiss"
ADMIN_DECISION_DELETE_POST = "delete_post"
ADMIN_DECISION_DELETE_ACCOUNT = "delete_account"

ADMIN_DECISIONS = (
    ADMIN_DECISION_DISMISS,
    ADMIN_DECISION_DELETE_POST,
    ADMIN_DECISION_DELETE_ACCOUNT,
)

_last_cleanup_at = None


def _normalize_optional_text(value, max_length: int, field_name: str):
    if value is None:
        return None

    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")

    normalized = value.strip()
    if not normalized:
        return None
    if len(normalized) > max_length:
        raise ValueError(f"{field_name} must be at most {max_length} characters")
    return normalized


def _retention_days():
    if has_app_context():
        return max(int(current_app.config.get("MODERATION_SOFT_DELETE_DAYS", 7)), 1)
    return max(int(Config.MODERATION_SOFT_DELETE_DAYS), 1)


def _decision_retention_days():
    if has_app_context():
        return max(int(current_app.config.get("REPORT_DECISION_RETENTION_DAYS", 7)), 1)
    return max(int(Config.REPORT_DECISION_RETENTION_DAYS), 1)


def _cleanup_interval_seconds():
    if has_app_context():
        return max(int(current_app.config.get("MODERATION_CLEANUP_INTERVAL_SECONDS", 300)), 10)
    return max(int(Config.MODERATION_CLEANUP_INTERVAL_SECONDS), 10)


def _cleanup_batch_size(override: int | None = None):
    if override is not None:
        return max(int(override), 1)
    if has_app_context():
        return max(int(current_app.config.get("MODERATION_CLEANUP_BATCH_SIZE", 100)), 1)
    return max(int(Config.MODERATION_CLEANUP_BATCH_SIZE), 1)


def _cleanup_logger():
    if has_app_context():
        return current_app.logger
    return logging.getLogger(__name__)


def _report_expiry_from(now: datetime):
    return now + timedelta(days=_decision_retention_days())


def _purge_after_from(now: datetime):
    return now + timedelta(days=_retention_days())


def _is_admin(user_id: int) -> bool:
    return AdminUser.query.filter_by(user_id=user_id).first() is not None


def _normalize_report_type(value: str):
    if not isinstance(value, str):
        raise ValueError("report_type is required")

    normalized = value.strip().lower()
    if normalized not in REPORT_TYPES:
        allowed_values = ", ".join(REPORT_TYPES)
        raise ValueError(f"Invalid report_type. Allowed values: {allowed_values}")
    return normalized


def _normalize_decision(value: str):
    if not isinstance(value, str):
        raise ValueError("decision is required")

    normalized = value.strip().lower()
    if normalized not in ADMIN_DECISIONS:
        allowed_values = ", ".join(ADMIN_DECISIONS)
        raise ValueError(f"Invalid decision. Allowed values: {allowed_values}")
    return normalized


def _max_datetime(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return a if a >= b else b


def _visible_post_query():
    return (
        Post.query
        .join(User, User.id == Post.author_id)
        .filter(
            Post.is_hidden.is_(False),
            User.is_suspended.is_(False),
        )
    )


def get_visible_post(post_id: int):
    return _visible_post_query().filter(Post.id == post_id).first()


def is_account_suspended(username: str) -> bool:
    user = User.query.filter_by(username=username).first()
    return bool(user and user.is_suspended)


def create_post_report(
    reporter_username: str,
    post_id: int,
    report_type: str,
    description: str | None = None,
):
    reporter = User.query.filter_by(username=reporter_username).first()
    if not reporter or reporter.is_suspended:
        raise ValueError("User not found")

    post = get_visible_post(post_id)
    if not post:
        raise ValueError("Post not found")

    normalized_type = _normalize_report_type(report_type)
    normalized_description = _normalize_optional_text(
        description,
        max_length=255,
        field_name="description",
    )

    existing_pending = report_repository.get_pending_by_reporter_and_post(
        reporter_id=reporter.id,
        post_id=post.id,
    )
    if existing_pending:
        raise ValueError("You already have a pending report for this post")

    report = report_repository.create_report(
        reporter_id=reporter.id,
        post_id=post.id,
        report_type=normalized_type,
        description=normalized_description,
    )
    db.session.commit()
    return report


def _serialize_user_min(user: User | None):
    if not user:
        return None
    return {
        "id": user.id,
        "username": user.username,
    }


def _serialize_post_min(post: Post | None, author: User | None):
    if not post:
        return None
    return {
        "id": post.id,
        "text": post.text,
        "created_at": post.created_at.isoformat() if post.created_at else None,
        "is_hidden": bool(post.is_hidden),
        "author": _serialize_user_min(author),
    }


def _serialize_report(
    report: PostReport,
    user_by_id: dict[int, User],
    post_by_id: dict[int, Post],
):
    reporter = user_by_id.get(report.reporter_id)
    admin = user_by_id.get(report.handled_by_admin_id)
    post = post_by_id.get(report.post_id)
    post_author = user_by_id.get(post.author_id) if post else None

    return {
        "id": report.id,
        "report_type": report.report_type,
        "description": report.description,
        "status": report.status,
        "reporter": _serialize_user_min(reporter),
        "post": _serialize_post_min(post, post_author),
        "admin_decision": report.admin_decision,
        "admin_note": report.admin_note,
        "handled_by_admin": _serialize_user_min(admin),
        "handled_at": report.handled_at.isoformat() if report.handled_at else None,
        "decision_expires_at": (
            report.decision_expires_at.isoformat()
            if report.decision_expires_at
            else None
        ),
        "created_at": report.created_at.isoformat() if report.created_at else None,
        "updated_at": report.updated_at.isoformat() if report.updated_at else None,
    }


def _build_maps_for_reports(reports: list[PostReport]):
    user_ids = set()
    post_ids = set()

    for report in reports:
        user_ids.add(report.reporter_id)
        if report.handled_by_admin_id:
            user_ids.add(report.handled_by_admin_id)
        post_ids.add(report.post_id)

    posts = Post.query.filter(Post.id.in_(post_ids)).all() if post_ids else []
    for post in posts:
        user_ids.add(post.author_id)

    users = User.query.filter(User.id.in_(user_ids)).all() if user_ids else []

    return (
        {user.id: user for user in users},
        {post.id: post for post in posts},
    )


def list_reports_for_admin(
    page: int,
    limit: int,
    *,
    status: str | None = None,
    report_type: str | None = None,
):
    page = max(1, page)
    limit = min(50, max(1, limit))

    normalized_status = status.strip().lower() if isinstance(status, str) and status.strip() else None
    if normalized_status and normalized_status not in (REPORT_STATUS_PENDING, REPORT_STATUS_HANDLED):
        raise ValueError("Invalid status")

    normalized_type = None
    if isinstance(report_type, str) and report_type.strip():
        normalized_type = _normalize_report_type(report_type)

    total, reports = report_repository.list_reports(
        page=page,
        limit=limit,
        status=normalized_status,
        report_type=normalized_type,
    )

    user_by_id, post_by_id = _build_maps_for_reports(reports)
    return {
        "page": page,
        "limit": limit,
        "total": total,
        "reports": [
            _serialize_report(report, user_by_id, post_by_id) for report in reports
        ],
    }


def get_report_detail_for_admin(report_id: int):
    report = report_repository.get_by_id(report_id)
    if not report:
        raise ValueError("Report not found")

    user_by_id, post_by_id = _build_maps_for_reports([report])
    return _serialize_report(report, user_by_id, post_by_id)


def _soft_delete_post(
    post: Post,
    *,
    reason: str,
    report_id: int,
    now: datetime,
):
    post.is_hidden = True
    if post.hidden_at is None:
        post.hidden_at = now
    post.hidden_reason = reason
    post.hidden_by_report_id = report_id
    post.purge_after = _max_datetime(post.purge_after, _purge_after_from(now))


def _soft_delete_user(
    user: User,
    *,
    reason: str,
    report_id: int,
    now: datetime,
):
    user.is_suspended = True
    if user.suspended_at is None:
        user.suspended_at = now
    user.suspension_reason = reason
    user.suspended_by_report_id = report_id
    user.purge_after = _max_datetime(user.purge_after, _purge_after_from(now))

    posts = Post.query.filter_by(author_id=user.id).all()
    for post in posts:
        _soft_delete_post(
            post,
            reason="reported_user",
            report_id=report_id,
            now=now,
        )


def handle_report_by_admin(
    report_id: int,
    admin_username: str,
    decision: str,
    admin_note: str | None = None,
):
    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not _is_admin(admin_user.id):
        raise ValueError("Admin not found")

    report = report_repository.get_by_id(report_id)
    if not report:
        raise ValueError("Report not found")
    if report.status != REPORT_STATUS_PENDING:
        raise ValueError("Report already handled")

    normalized_decision = _normalize_decision(decision)
    normalized_note = _normalize_optional_text(
        admin_note,
        max_length=255,
        field_name="admin_note",
    )

    now = datetime.utcnow()

    if normalized_decision == ADMIN_DECISION_DELETE_POST:
        post = Post.query.get(report.post_id)
        if not post:
            raise ValueError("Post not found")
        _soft_delete_post(
            post,
            reason="reported_post",
            report_id=report.id,
            now=now,
        )

    elif normalized_decision == ADMIN_DECISION_DELETE_ACCOUNT:
        post = Post.query.get(report.post_id)
        if not post:
            raise ValueError("Post not found")

        target_user = User.query.get(post.author_id)
        if not target_user:
            raise ValueError("User not found")
        if _is_admin(target_user.id):
            raise ValueError("Cannot delete an admin account from report handling")

        _soft_delete_user(
            target_user,
            reason="reported_account",
            report_id=report.id,
            now=now,
        )

    report.status = REPORT_STATUS_HANDLED
    report.admin_decision = normalized_decision
    report.admin_note = normalized_note
    report.handled_by_admin_id = admin_user.id
    report.handled_at = now
    report.decision_expires_at = _report_expiry_from(now)

    db.session.commit()
    return report


def _hard_delete_post(post: Post):
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
    Post.query.filter(
        Post.quoted_post_id == post.id
    ).update(
        {Post.quoted_post_id: None},
        synchronize_session=False,
    )
    Media.query.filter_by(post_id=post.id).delete(synchronize_session=False)
    PostReport.query.filter_by(post_id=post.id).delete(synchronize_session=False)
    db.session.delete(post)


def _hard_delete_user(user: User):
    user_posts = Post.query.filter_by(author_id=user.id).all()
    for post in user_posts:
        _hard_delete_post(post)

    authored_comment_ids = [
        row[0]
        for row in db.session.query(Comment.id).filter(Comment.author_id == user.id).all()
    ]
    if authored_comment_ids:
        Vote.query.filter(
            Vote.target_type == "comment",
            Vote.target_id.in_(authored_comment_ids),
        ).delete(synchronize_session=False)

    Comment.query.filter_by(author_id=user.id).delete(synchronize_session=False)
    Vote.query.filter_by(user_id=user.id).delete(synchronize_session=False)
    Follow.query.filter(
        (Follow.follower_id == user.id) | (Follow.following_id == user.id)
    ).delete(synchronize_session=False)
    Profile.query.filter_by(user_id=user.id).delete(synchronize_session=False)
    PostReport.query.filter(
        PostReport.handled_by_admin_id == user.id
    ).update(
        {PostReport.handled_by_admin_id: None},
        synchronize_session=False,
    )
    PostReport.query.filter(
        PostReport.reporter_id == user.id
    ).delete(synchronize_session=False)
    AdminUser.query.filter_by(user_id=user.id).delete(synchronize_session=False)
    db.session.delete(user)


def run_scheduled_cleanup_with_metrics(
    force: bool = False,
    *,
    batch_size: int | None = None,
):
    global _last_cleanup_at

    logger = _cleanup_logger()
    now = datetime.utcnow()
    if not force and _last_cleanup_at:
        if (now - _last_cleanup_at).total_seconds() < _cleanup_interval_seconds():
            return {
                "skipped": True,
                "force": bool(force),
                "batch_size": _cleanup_batch_size(batch_size),
                "users_deleted": 0,
                "posts_deleted": 0,
                "reports_deleted": 0,
                "rows_processed": 0,
                "duration_ms": 0,
            }

    started_at = time.perf_counter()
    limit = _cleanup_batch_size(batch_size)
    users_deleted = 0
    posts_deleted = 0
    reports_deleted = 0
    try:
        due_users = (
            User.query
            .filter(
                User.is_suspended.is_(True),
                User.purge_after.isnot(None),
                User.purge_after <= now,
            )
            .order_by(User.purge_after.asc(), User.id.asc())
            .limit(limit)
            .all()
        )
        for user in due_users:
            _hard_delete_user(user)
            users_deleted += 1

        due_posts = (
            Post.query
            .filter(
                Post.is_hidden.is_(True),
                Post.purge_after.isnot(None),
                Post.purge_after <= now,
            )
            .order_by(Post.purge_after.asc(), Post.id.asc())
            .limit(limit)
            .all()
        )
        for post in due_posts:
            _hard_delete_post(post)
            posts_deleted += 1

        expired_reports = report_repository.list_expired_handled(now, limit=limit)
        for report in expired_reports:
            db.session.delete(report)
            reports_deleted += 1

        changes = users_deleted + posts_deleted + reports_deleted
        if changes:
            db.session.commit()
        _last_cleanup_at = now
        duration_ms = int((time.perf_counter() - started_at) * 1000)
        logger.info(
            "moderation_cleanup completed force=%s batch_size=%s rows=%s users=%s posts=%s reports=%s duration_ms=%s",
            force,
            limit,
            changes,
            users_deleted,
            posts_deleted,
            reports_deleted,
            duration_ms,
        )
        return {
            "skipped": False,
            "force": bool(force),
            "batch_size": limit,
            "users_deleted": users_deleted,
            "posts_deleted": posts_deleted,
            "reports_deleted": reports_deleted,
            "rows_processed": changes,
            "duration_ms": duration_ms,
        }
    except Exception:
        db.session.rollback()
        duration_ms = int((time.perf_counter() - started_at) * 1000)
        logger.exception(
            "moderation_cleanup failed force=%s batch_size=%s duration_ms=%s",
            force,
            limit,
            duration_ms,
        )
        raise


def run_scheduled_cleanup(force: bool = False):
    stats = run_scheduled_cleanup_with_metrics(force=force)
    return int(stats.get("rows_processed", 0))
