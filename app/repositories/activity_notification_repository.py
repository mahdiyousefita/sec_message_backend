from app.db import db
from app.models.activity_notification_model import ActivityNotification


def create_notification(recipient_id, actor_id, kind, target_type=None, target_id=None, extra=None):
    notif = ActivityNotification(
        recipient_id=recipient_id,
        actor_id=actor_id,
        kind=kind,
        target_type=target_type,
        target_id=target_id,
        extra=extra,
    )
    db.session.add(notif)
    db.session.flush()
    return notif


def get_latest_notification_for_target(recipient_id, kind, target_type, target_id):
    if target_id is None:
        return None
    return (
        ActivityNotification.query
        .filter_by(
            recipient_id=recipient_id,
            kind=kind,
            target_type=target_type,
            target_id=target_id,
        )
        .order_by(ActivityNotification.created_at.desc(), ActivityNotification.id.desc())
        .first()
    )


def get_notifications_page(recipient_id, page, limit, unread_only=False):
    query = (
        ActivityNotification.query
        .filter_by(recipient_id=recipient_id)
    )
    if unread_only:
        query = query.filter_by(is_read=False)
    query = query.order_by(ActivityNotification.created_at.desc())
    total = query.count()
    items = query.offset((page - 1) * limit).limit(limit).all()
    return total, items


def count_unread(recipient_id):
    return (
        ActivityNotification.query
        .filter_by(recipient_id=recipient_id, is_read=False)
        .count()
    )


def mark_all_read(recipient_id):
    updated = (
        ActivityNotification.query
        .filter_by(recipient_id=recipient_id, is_read=False)
        .update({"is_read": True})
    )
    db.session.commit()
    return updated


def mark_read_by_ids(recipient_id, notification_ids):
    updated = (
        ActivityNotification.query
        .filter(
            ActivityNotification.recipient_id == recipient_id,
            ActivityNotification.id.in_(notification_ids),
            ActivityNotification.is_read == False,
        )
        .update({"is_read": True}, synchronize_session="fetch")
    )
    db.session.commit()
    return updated
