import hmac
from dataclasses import dataclass

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from flask import current_app
from werkzeug.security import check_password_hash

from app.db import db
from app.models.pending_registration_model import PendingRegistration
from app.models.user_model import User


ARGON2_PREFIX = "$argon2id$"
_password_hasher: PasswordHasher | None = None
_password_hasher_signature: tuple[int, int, int, int, int] | None = None


@dataclass
class PasswordVerificationResult:
    is_valid: bool
    needs_upgrade: bool


def _pepper() -> str:
    return current_app.config.get("AUTH_PASSWORD_PEPPER", "")


def _password_hasher_instance() -> PasswordHasher:
    global _password_hasher
    global _password_hasher_signature

    signature = (
        int(current_app.config.get("AUTH_ARGON2_TIME_COST", 3)),
        int(current_app.config.get("AUTH_ARGON2_MEMORY_COST_KIB", 65536)),
        int(current_app.config.get("AUTH_ARGON2_PARALLELISM", 4)),
        int(current_app.config.get("AUTH_ARGON2_HASH_LEN", 32)),
        int(current_app.config.get("AUTH_ARGON2_SALT_LEN", 16)),
    )

    if _password_hasher is None or _password_hasher_signature != signature:
        _password_hasher = PasswordHasher(
            time_cost=signature[0],
            memory_cost=signature[1],
            parallelism=signature[2],
            hash_len=signature[3],
            salt_len=signature[4],
        )
        _password_hasher_signature = signature

    return _password_hasher


def is_argon2_hash(value: str | None) -> bool:
    return bool(value and value.startswith(ARGON2_PREFIX))


def _is_probable_werkzeug_hash(value: str | None) -> bool:
    if not value:
        return False
    normalized = value.strip().lower()
    return normalized.startswith("pbkdf2:") or normalized.startswith("scrypt:")


def hash_password(password: str) -> str:
    return _password_hasher_instance().hash(f"{password}{_pepper()}")


def verify_password(stored_hash: str | None, candidate_password: str) -> PasswordVerificationResult:
    if not stored_hash:
        return PasswordVerificationResult(is_valid=False, needs_upgrade=False)

    if is_argon2_hash(stored_hash):
        hasher = _password_hasher_instance()
        try:
            hasher.verify(stored_hash, f"{candidate_password}{_pepper()}")
        except (VerifyMismatchError, InvalidHashError):
            return PasswordVerificationResult(is_valid=False, needs_upgrade=False)
        return PasswordVerificationResult(
            is_valid=True,
            needs_upgrade=hasher.check_needs_rehash(stored_hash),
        )

    if _is_probable_werkzeug_hash(stored_hash):
        is_valid = check_password_hash(stored_hash, candidate_password)
        return PasswordVerificationResult(is_valid=is_valid, needs_upgrade=is_valid)

    # Legacy plaintext password compatibility path for pre-hash users.
    is_valid = hmac.compare_digest(stored_hash, candidate_password)
    return PasswordVerificationResult(is_valid=is_valid, needs_upgrade=is_valid)


def verify_and_upgrade_user_password(user: User, candidate_password: str) -> bool:
    result = verify_password(user.password_hash, candidate_password)
    if not result.is_valid:
        return False

    if result.needs_upgrade:
        user.password_hash = hash_password(candidate_password)
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
    return True


def migrate_plaintext_passwords() -> int:
    if not current_app.config.get("AUTH_PASSWORD_MIGRATION_ENABLED", True):
        return 0

    batch_size = int(current_app.config.get("AUTH_PASSWORD_MIGRATION_BATCH_SIZE", 1000))

    users = (
        User.query
        .filter(~User.password_hash.like(f"{ARGON2_PREFIX}%"))
        .limit(batch_size)
        .all()
    )
    pending_rows = (
        PendingRegistration.query
        .filter(~PendingRegistration.password_hash.like(f"{ARGON2_PREFIX}%"))
        .limit(batch_size)
        .all()
    )

    migrated_count = 0
    for user in users:
        if not _is_probable_werkzeug_hash(user.password_hash):
            user.password_hash = hash_password(user.password_hash)
            migrated_count += 1

    for row in pending_rows:
        if not _is_probable_werkzeug_hash(row.password_hash):
            row.password_hash = hash_password(row.password_hash)
            migrated_count += 1

    if migrated_count:
        db.session.commit()

    return migrated_count
