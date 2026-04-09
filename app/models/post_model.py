from app.db import db
from datetime import datetime


class Post(db.Model):
    __tablename__ = "posts"

    id = db.Column(db.Integer, primary_key=True)
    author_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    text = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    followers_only = db.Column(db.Boolean, nullable=False, default=False, index=True)
    is_hidden = db.Column(db.Boolean, nullable=False, default=False, index=True)
    hidden_at = db.Column(db.DateTime, nullable=True, index=True)
    purge_after = db.Column(db.DateTime, nullable=True, index=True)
    hidden_reason = db.Column(db.String(32), nullable=True)
    hidden_by_report_id = db.Column(db.Integer, nullable=True, index=True)

    media = db.relationship(
        "Media",
        backref="post",
        lazy="select",
        cascade="all, delete-orphan"
    )
