from datetime import datetime

from app import db


class Vote(db.Model):
    __tablename__ = "votes"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)

    target_type = db.Column(
        db.String(20), nullable=False
    )  # "post" | "comment"

    target_id = db.Column(db.Integer, nullable=False)

    value = db.Column(db.Integer, nullable=False)  # +1 | -1

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint(
            "user_id", "target_type", "target_id",
            name="unique_user_vote"
        ),
    )
