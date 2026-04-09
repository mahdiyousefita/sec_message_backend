from datetime import datetime

from app.db import db


class AppUpdateConfig(db.Model):
    __tablename__ = "app_update_configs"

    id = db.Column(db.Integer, primary_key=True)
    platform = db.Column(
        db.String(32),
        nullable=False,
        unique=True,
        index=True,
        default="android",
    )
    download_url = db.Column(db.String(512), nullable=True)
    latest_version = db.Column(db.String(32), nullable=True)
    force_update_below = db.Column(db.String(32), nullable=True)
    optional_update_below = db.Column(db.String(32), nullable=True)
    force_title = db.Column(db.String(120), nullable=True)
    force_message = db.Column(db.String(255), nullable=True)
    optional_title = db.Column(db.String(120), nullable=True)
    optional_message = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
