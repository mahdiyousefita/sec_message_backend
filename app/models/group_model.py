from datetime import datetime

from app.db import db


class Group(db.Model):
    __tablename__ = "groups"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    creator_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    creator = db.relationship("User", backref="created_groups", foreign_keys=[creator_id])
    members = db.relationship(
        "GroupMember",
        backref="group",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )

    MAX_GROUPS_PER_USER = 5

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "creator_id": self.creator_id,
            "created_at": self.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }


class GroupMember(db.Model):
    __tablename__ = "group_members"

    id = db.Column(db.Integer, primary_key=True)
    group_id = db.Column(
        db.Integer,
        db.ForeignKey("groups.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    joined_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    user = db.relationship("User", backref="group_memberships", foreign_keys=[user_id])

    __table_args__ = (
        db.UniqueConstraint(
            "group_id",
            "user_id",
            name="unique_group_member",
        ),
    )
