from app.db import db


class Profile(db.Model):
    __tablename__ = "profiles"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    name = db.Column(db.String(120), nullable=False)
    bio = db.Column(db.Text, nullable=False, default="")
    image_object_name = db.Column(db.String(255), nullable=True)

