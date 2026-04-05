from sqlalchemy.orm import aliased

from app.db import db
from app.models.follow_model import Follow
from app.models.profile_model import Profile
from app.models.user_model import User


def is_following(follower_id: int, following_id: int) -> bool:
    return (
        Follow.query.filter_by(
            follower_id=follower_id,
            following_id=following_id,
        ).first()
        is not None
    )


def create_follow(follower_id: int, following_id: int) -> bool:
    if is_following(follower_id, following_id):
        return False

    db.session.add(
        Follow(
            follower_id=follower_id,
            following_id=following_id,
        )
    )
    db.session.commit()
    return True


def delete_follow(follower_id: int, following_id: int) -> bool:
    follow = Follow.query.filter_by(
        follower_id=follower_id,
        following_id=following_id,
    ).first()
    if not follow:
        return False

    db.session.delete(follow)
    db.session.commit()
    return True


def count_followers(user_id: int) -> int:
    follower_user = aliased(User)
    return (
        db.session.query(Follow.id)
        .join(follower_user, follower_user.id == Follow.follower_id)
        .filter(
            Follow.following_id == user_id,
            follower_user.is_suspended.is_(False),
        )
        .count()
    )


def count_following(user_id: int) -> int:
    following_user = aliased(User)
    return (
        db.session.query(Follow.id)
        .join(following_user, following_user.id == Follow.following_id)
        .filter(
            Follow.follower_id == user_id,
            following_user.is_suspended.is_(False),
        )
        .count()
    )


def get_following_usernames(follower_id: int):
    following_user = aliased(User)
    rows = (
        db.session.query(following_user.username)
        .join(Follow, Follow.following_id == following_user.id)
        .filter(
            Follow.follower_id == follower_id,
            following_user.is_suspended.is_(False),
        )
        .order_by(following_user.username.asc())
        .all()
    )
    return [row[0] for row in rows]


def get_follower_usernames(following_id: int):
    follower_user = aliased(User)
    rows = (
        db.session.query(follower_user.username)
        .join(Follow, Follow.follower_id == follower_user.id)
        .filter(
            Follow.following_id == following_id,
            follower_user.is_suspended.is_(False),
        )
        .order_by(follower_user.username.asc())
        .all()
    )
    return [row[0] for row in rows]


def get_followers_page(following_id: int, page: int, limit: int):
    follower_user = aliased(User)

    total = (
        db.session.query(Follow.id)
        .join(follower_user, follower_user.id == Follow.follower_id)
        .filter(
            Follow.following_id == following_id,
            follower_user.is_suspended.is_(False),
        )
        .count()
    )
    rows = (
        db.session.query(
            follower_user.id,
            follower_user.username,
            Profile.name,
            Profile.image_object_name,
        )
        .join(Follow, Follow.follower_id == follower_user.id)
        .outerjoin(Profile, Profile.user_id == follower_user.id)
        .filter(
            Follow.following_id == following_id,
            follower_user.is_suspended.is_(False),
        )
        .order_by(follower_user.username.asc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    users = [
        {
            "id": row.id,
            "username": row.username,
            "name": row.name or row.username,
            "image_object_name": row.image_object_name,
        }
        for row in rows
    ]
    return total, users


def get_following_page(follower_id: int, page: int, limit: int):
    following_user = aliased(User)

    total = (
        db.session.query(Follow.id)
        .join(following_user, following_user.id == Follow.following_id)
        .filter(
            Follow.follower_id == follower_id,
            following_user.is_suspended.is_(False),
        )
        .count()
    )
    rows = (
        db.session.query(
            following_user.id,
            following_user.username,
            Profile.name,
            Profile.image_object_name,
        )
        .join(Follow, Follow.following_id == following_user.id)
        .outerjoin(Profile, Profile.user_id == following_user.id)
        .filter(
            Follow.follower_id == follower_id,
            following_user.is_suspended.is_(False),
        )
        .order_by(following_user.username.asc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    users = [
        {
            "id": row.id,
            "username": row.username,
            "name": row.name or row.username,
            "image_object_name": row.image_object_name,
        }
        for row in rows
    ]
    return total, users
