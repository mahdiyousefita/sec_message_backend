from datetime import datetime

from app.db import db


class PlaylistTrack(db.Model):
    __tablename__ = "playlist_tracks"
    __table_args__ = (
        db.UniqueConstraint(
            "user_id",
            "media_id",
            name="uq_playlist_tracks_user_media",
        ),
    )

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id"),
        nullable=False,
        index=True,
    )
    media_id = db.Column(
        db.Integer,
        db.ForeignKey("media.id"),
        nullable=False,
        index=True,
    )
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
