from datetime import datetime

from app.db import db


class Story(db.Model):
    __tablename__ = "stories"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    media_url = db.Column(db.String(1024), nullable=False)
    media_type = db.Column(db.String(24), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    mention_user_ids = db.Column(db.Text, nullable=True)
    view_count = db.Column(db.Integer, nullable=False, default=0)
    like_count = db.Column(db.Integer, nullable=False, default=0)
    # Future-safe fields for close friends/highlights/archive without breaking migrations.
    audience_type = db.Column(db.String(24), nullable=False, default="followers", index=True)
    metadata_json = db.Column(db.Text, nullable=True)

    __table_args__ = (
        db.Index(
            "ix_stories_active_feed",
            "user_id",
            "expires_at",
            "created_at",
        ),
    )


class StoryView(db.Model):
    __tablename__ = "story_views"

    id = db.Column(db.Integer, primary_key=True)
    story_id = db.Column(
        db.Integer,
        db.ForeignKey("stories.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    viewer_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    viewed_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    liked = db.Column(db.Boolean, nullable=False, default=False, index=True)

    __table_args__ = (
        db.UniqueConstraint(
            "story_id",
            "viewer_id",
            name="uq_story_view_story_viewer",
        ),
        db.Index(
            "ix_story_view_story_viewed",
            "story_id",
            "viewed_at",
        ),
        db.Index(
            "ix_story_view_viewer_story",
            "viewer_id",
            "story_id",
        ),
    )


class StoryDailyQuota(db.Model):
    __tablename__ = "story_daily_quotas"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    bucket_start = db.Column(db.DateTime, nullable=False, index=True)
    story_count = db.Column(db.Integer, nullable=False, default=0)

    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "bucket_start",
            name="uq_story_daily_quota_user_bucket",
        ),
        db.Index(
            "ix_story_daily_quota_user_bucket",
            "user_id",
            "bucket_start",
        ),
    )
