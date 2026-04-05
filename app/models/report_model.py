from datetime import datetime

from app.db import db


class PostReport(db.Model):
    __tablename__ = "post_reports"

    id = db.Column(db.Integer, primary_key=True)
    reporter_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    post_id = db.Column(
        db.Integer,
        db.ForeignKey("posts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    report_type = db.Column(db.String(32), nullable=False, index=True)
    description = db.Column(db.String(255), nullable=True)

    status = db.Column(db.String(20), nullable=False, default="pending", index=True)

    admin_decision = db.Column(db.String(32), nullable=True)
    admin_note = db.Column(db.String(255), nullable=True)
    handled_by_admin_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    handled_at = db.Column(db.DateTime, nullable=True, index=True)
    decision_expires_at = db.Column(db.DateTime, nullable=True, index=True)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
