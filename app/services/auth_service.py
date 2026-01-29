from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import create_access_token, create_refresh_token

from app.repositories import user_repository

def register(username, password, public_key):
    if not username or not password or not public_key:
        raise ValueError("Missing fields")

    if user_repository.get_by_username(username):
        raise ValueError("Username already exists")

    password_hash = generate_password_hash(password)
    user_repository.create_user(username, password_hash, public_key)

def login(username, password):
    user = user_repository.get_by_username(username)
    if not user or not check_password_hash(user.password_hash, password):
        raise ValueError("Invalid credentials")

    return {
        "access_token": create_access_token(identity=username),
        "refresh_token": create_refresh_token(identity=username)
    }
