from datetime import datetime, timezone

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

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def is_expired(self, now=None):
        now = now or datetime.utcnow()
        return self._as_utc(self.expires_at) <= self._as_utc(now)

    def seconds_until_expiry(self, now=None) -> int:
        now = now or datetime.utcnow()
        return int((self._as_utc(self.expires_at) - self._as_utc(now)).total_seconds())
