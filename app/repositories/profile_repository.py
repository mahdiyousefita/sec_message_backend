from app.db import db
from app.models.profile_model import Profile


def create_profile_for_user(user_id: int, name: str):
    profile = Profile(
        user_id=user_id,
        name=name,
        bio="",
    )
    db.session.add(profile)
    return profile


def get_by_user_id(user_id: int):
    return Profile.query.filter_by(user_id=user_id).first()

