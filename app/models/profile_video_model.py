from app.db import db


class ProfileVideo(db.Model):
    __tablename__ = "profile_videos"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    video_object_name = db.Column(db.String(255), nullable=False)
