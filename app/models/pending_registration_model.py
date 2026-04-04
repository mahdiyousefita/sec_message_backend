from datetime import datetime

from app.db import db


class PendingRegistration(db.Model):
    __tablename__ = "pending_registrations"

    id = db.Column(db.Integer, primary_key=True)
    registration_id = db.Column(db.String(64), unique=True, nullable=False, index=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(128), nullable=False)
    public_key = db.Column(db.Text, nullable=False)
    name = db.Column(db.String(120), nullable=False)
    client_nonce = db.Column(db.String(64), nullable=True)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    def is_expired(self, now=None):
        now = now or datetime.utcnow()
        return self.expires_at <= now
