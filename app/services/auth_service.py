from datetime import datetime, timedelta
import re
import secrets

from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import create_access_token, create_refresh_token

from app.db import db
from app.repositories import pending_registration_repository, user_repository
from app.repositories import follow_repository, group_repository


PENDING_REGISTRATION_TTL_SECONDS = 30 * 60
USERNAME_ALLOWED_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


class AuthError(ValueError):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def _require_non_empty_string(value):
    return isinstance(value, str) and value.strip()


def _cleanup_expired_pending_registrations():
    # Cleanup is cheap and keeps "username already pending" scenarios predictable.
    pending_registration_repository.delete_expired()


def _normalize_register_fields(username, password, public_key, name):
    if (
        not _require_non_empty_string(username)
        or not _require_non_empty_string(password)
        or not _require_non_empty_string(public_key)
    ):
        raise AuthError("Missing fields", status_code=400)

    username = username.strip()
    if not USERNAME_ALLOWED_PATTERN.fullmatch(username):
        raise AuthError(
            "Username can only contain English letters, numbers, '.', '-', and '_'",
            status_code=400,
        )

    public_key = public_key.strip()

    resolved_name = username
    if name is not None:
        if not _require_non_empty_string(name):
            raise AuthError("Name must be a non-empty string", status_code=400)
        resolved_name = name.strip()

    return username, password, public_key, resolved_name


def register(username, password, public_key, name=None):
    username, password, public_key, resolved_name = _normalize_register_fields(
        username,
        password,
        public_key,
        name,
    )

    if user_repository.get_by_username(username):
        # Keep legacy behavior for old clients that expect 400 from /register.
        raise AuthError("Username already exists", status_code=400)

    password_hash = generate_password_hash(password)
    user_repository.create_user(
        username=username,
        password_hash=password_hash,
        public_key=public_key,
        name=resolved_name,
    )


def start_registration(username, password, public_key, name=None, client_nonce=None):
    username, password, public_key, resolved_name = _normalize_register_fields(
        username,
        password,
        public_key,
        name,
    )

    if client_nonce is not None:
        if not _require_non_empty_string(client_nonce):
            raise AuthError("client_nonce must be a non-empty string", status_code=400)
        client_nonce = client_nonce.strip()

    _cleanup_expired_pending_registrations()

    if user_repository.get_by_username(username):
        raise AuthError("Username already exists", status_code=409)

    now = datetime.utcnow()
    expires_at = now + timedelta(seconds=PENDING_REGISTRATION_TTL_SECONDS)
    registration_id = secrets.token_urlsafe(24)
    password_hash = generate_password_hash(password)

    if client_nonce:
        existing_same_nonce = pending_registration_repository.get_by_username_and_client_nonce(
            username, client_nonce
        )
        if existing_same_nonce and not existing_same_nonce.is_expired(now):
            return {
                "registration_id": existing_same_nonce.registration_id,
                "expires_in_seconds": max(
                    1, existing_same_nonce.seconds_until_expiry(now)
                ),
            }

    pending = pending_registration_repository.get_by_username(username)
    if pending:
        pending_registration_repository.update_pending_registration(
            pending=pending,
            registration_id=registration_id,
            password_hash=password_hash,
            public_key=public_key,
            name=resolved_name,
            expires_at=expires_at,
            client_nonce=client_nonce,
        )
    else:
        pending_registration_repository.create_pending_registration(
            registration_id=registration_id,
            username=username,
            password_hash=password_hash,
            public_key=public_key,
            name=resolved_name,
            expires_at=expires_at,
            client_nonce=client_nonce,
        )

    return {
        "registration_id": registration_id,
        "expires_in_seconds": PENDING_REGISTRATION_TTL_SECONDS,
    }


def confirm_registration(registration_id):
    if not _require_non_empty_string(registration_id):
        raise AuthError("Missing registration_id", status_code=400)

    registration_id = registration_id.strip()
    now = datetime.utcnow()
    pending = pending_registration_repository.get_by_registration_id(registration_id)

    if not pending:
        raise AuthError("Registration session not found", status_code=404)

    if pending.is_expired(now):
        pending_registration_repository.delete_pending_registration(pending)
        raise AuthError("Registration session expired", status_code=410)

    if user_repository.get_by_username(pending.username):
        pending_registration_repository.delete_pending_registration(pending)
        raise AuthError("Username already exists", status_code=409)

    try:
        user_repository.create_user(
            username=pending.username,
            password_hash=pending.password_hash,
            public_key=pending.public_key,
            name=pending.name,
            auto_commit=False,
        )
        pending_registration_repository.delete_pending_registration(pending, commit=False)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise


def login(username, password):
    if not _require_non_empty_string(username) or not _require_non_empty_string(password):
        raise AuthError("Invalid credentials", status_code=401)

    username = username.strip()

    _cleanup_expired_pending_registrations()

    user = user_repository.get_by_username(username)
    if not user or not check_password_hash(user.password_hash, password):
        raise AuthError("Invalid credentials", status_code=401)
    if getattr(user, "is_suspended", False):
        raise AuthError("Account suspended", status_code=403)

    return {
        "access_token": create_access_token(identity=username),
        "refresh_token": create_refresh_token(identity=username)
    }


def refresh_access_token(username):
    user = user_repository.get_by_username(username)
    if not user:
        raise AuthError("Unauthorized", status_code=401)
    if getattr(user, "is_suspended", False):
        raise AuthError("Account suspended", status_code=403)

    return {
        "access_token": create_access_token(identity=username)
    }

def rotate_public_key(username, public_key):
    if not _require_non_empty_string(username):
        raise AuthError("Unauthorized", status_code=401)
    if not _require_non_empty_string(public_key):
        raise AuthError("Missing fields", status_code=400)

    normalized_username = username.strip()
    normalized_public_key = public_key.strip()

    user = user_repository.get_by_username(normalized_username)
    if not user:
        raise AuthError("User not found", status_code=404)
    if getattr(user, "is_suspended", False):
        raise AuthError("Account suspended", status_code=403)

    if user.public_key != normalized_public_key:
        updated = user_repository.update_public_key(
            username=normalized_username,
            public_key=normalized_public_key,
        )
        if not updated:
            raise AuthError("User not found", status_code=404)

    recipients = set(follow_repository.get_follower_usernames(user.id))

    groups = group_repository.get_groups_for_user(user.id)
    group_ids = [group.id for group in groups]
    for group in groups:
        recipients.update(group_repository.get_group_member_usernames(group.id))

    recipients.discard(normalized_username)

    return {
        "username": normalized_username,
        "group_ids": group_ids,
        "notify_usernames": sorted(recipients),
    }


def get_key_status(username):
    if not _require_non_empty_string(username):
        raise AuthError("Unauthorized", status_code=401)

    normalized_username = username.strip()
    user = user_repository.get_by_username(normalized_username)
    if not user:
        raise AuthError("User not found", status_code=404)
    if getattr(user, "is_suspended", False):
        raise AuthError("Account suspended", status_code=403)

    return {
        "has_public_key": bool(getattr(user, "public_key", None)),
    }
