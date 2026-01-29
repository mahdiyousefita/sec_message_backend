from app.models.user_model import User
from app.db import db

def get_by_username(username: str):
    return User.query.filter_by(username=username).first()

def create_user(username, password_hash, public_key):
    user = User(
        username=username,
        password_hash=password_hash,
        public_key=public_key
    )
    db.session.add(user)
    db.session.commit()
    return user
