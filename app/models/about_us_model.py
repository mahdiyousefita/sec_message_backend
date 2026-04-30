from datetime import datetime

from app.db import db


class AboutUsConfig(db.Model):
    __tablename__ = "about_us_configs"

    id = db.Column(db.Integer, primary_key=True)
    description = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class AboutUsLink(db.Model):
    __tablename__ = "about_us_links"

    id = db.Column(db.Integer, primary_key=True)
    config_id = db.Column(
        db.Integer,
        db.ForeignKey("about_us_configs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    link_type = db.Column(db.String(32), nullable=False)
    title = db.Column(db.String(120), nullable=False)
    url = db.Column(db.String(512), nullable=False)
    is_disabled = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        db.UniqueConstraint(
            "config_id",
            "link_type",
            name="uq_about_us_links_config_type",
        ),
    )


class AboutUsTeamMember(db.Model):
    __tablename__ = "about_us_team_members"

    id = db.Column(db.Integer, primary_key=True)
    config_id = db.Column(
        db.Integer,
        db.ForeignKey("about_us_configs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    display_name = db.Column(db.String(120), nullable=True)
    role_description = db.Column(db.String(255), nullable=False)
    sort_order = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        db.UniqueConstraint(
            "config_id",
            "user_id",
            name="uq_about_us_team_member_config_user",
        ),
        db.Index("ix_about_us_team_members_config_sort", "config_id", "sort_order"),
    )
