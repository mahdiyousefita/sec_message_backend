from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import create_access_token, create_refresh_token

from app.repositories import user_repository


def _require_non_empty_string(value):
    return isinstance(value, str) and value.strip()


def register(username, password, public_key, name=None):
    if not _require_non_empty_string(username) or not _require_non_empty_string(password) or not _require_non_empty_string(public_key):
        raise ValueError("Missing fields")

    username = username.strip()
    public_key = public_key.strip()
    resolved_name = username
    if name is not None:
        if not _require_non_empty_string(name):
            raise ValueError("Name must be a non-empty string")
        resolved_name = name.strip()

    if user_repository.get_by_username(username):
        raise ValueError("Username already exists")

    password_hash = generate_password_hash(password)
    user_repository.create_user(
        username=username,
        password_hash=password_hash,
        public_key=public_key,
        name=resolved_name,
    )


def login(username, password):
    if not _require_non_empty_string(username) or not _require_non_empty_string(password):
        raise ValueError("Invalid credentials")

    username = username.strip()

    user = user_repository.get_by_username(username)
    if not user or not check_password_hash(user.password_hash, password):
        raise ValueError("Invalid credentials")

    return {
        "access_token": create_access_token(identity=username),
        "refresh_token": create_refresh_token(identity=username)
    }


def refresh_access_token(username):
    return {
        "access_token": create_access_token(identity=username)
    }
