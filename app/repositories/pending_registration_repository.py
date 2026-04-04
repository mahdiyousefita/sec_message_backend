from datetime import datetime

from app.db import db
from app.models.pending_registration_model import PendingRegistration


def get_by_registration_id(registration_id: str):
    return PendingRegistration.query.filter_by(registration_id=registration_id).first()


def get_by_username(username: str):
    return PendingRegistration.query.filter_by(username=username).first()


def get_by_username_and_client_nonce(username: str, client_nonce: str):
    return PendingRegistration.query.filter_by(
        username=username,
        client_nonce=client_nonce,
    ).first()


def create_pending_registration(
    registration_id: str,
    username: str,
    password_hash: str,
    public_key: str,
    name: str,
    expires_at: datetime,
    client_nonce: str | None = None,
):
    pending = PendingRegistration(
        registration_id=registration_id,
        username=username,
        password_hash=password_hash,
        public_key=public_key,
        name=name,
        expires_at=expires_at,
        client_nonce=client_nonce,
    )
    db.session.add(pending)
    db.session.commit()
    return pending


def update_pending_registration(
    pending: PendingRegistration,
    registration_id: str,
    password_hash: str,
    public_key: str,
    name: str,
    expires_at: datetime,
    client_nonce: str | None = None,
):
    pending.registration_id = registration_id
    pending.password_hash = password_hash
    pending.public_key = public_key
    pending.name = name
    pending.expires_at = expires_at
    pending.client_nonce = client_nonce
    db.session.commit()
    return pending


def delete_pending_registration(pending: PendingRegistration, *, commit: bool = True):
    db.session.delete(pending)
    if commit:
        db.session.commit()


def delete_expired(now: datetime | None = None):
    now = now or datetime.utcnow()
    PendingRegistration.query.filter(PendingRegistration.expires_at <= now).delete()
    db.session.commit()
