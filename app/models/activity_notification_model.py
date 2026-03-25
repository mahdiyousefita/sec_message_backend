from datetime import datetime

from app.db import db


class ActivityNotification(db.Model):
    __tablename__ = "activity_notifications"

    id = db.Column(db.Integer, primary_key=True)

    recipient_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    actor_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )

    # "follow", "unfollow", "comment", "vote"
    kind = db.Column(db.String(32), nullable=False)

    target_type = db.Column(db.String(32), nullable=True)  # "post", "comment", or null
    target_id = db.Column(db.Integer, nullable=True)

    extra = db.Column(db.Text, nullable=True)  # JSON blob for extra context

    is_read = db.Column(db.Boolean, default=False, nullable=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
