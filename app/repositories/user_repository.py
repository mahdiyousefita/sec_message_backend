from app.models.user_model import User
from app.db import db
from app.repositories.profile_repository import create_profile_for_user

def get_by_username(username: str):
    return User.query.filter_by(username=username).first()


def create_user(username, password_hash, public_key, name=None):
    user = User(
        username=username,
        password_hash=password_hash,
        public_key=public_key
    )
    db.session.add(user)
    db.session.flush()

    create_profile_for_user(
        user_id=user.id,
        name=(name or username).strip(),
    )

    db.session.commit()
    return user
