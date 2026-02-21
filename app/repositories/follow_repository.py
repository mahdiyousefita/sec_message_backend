from sqlalchemy.orm import aliased

from app.db import db
from app.models.follow_model import Follow
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
    return Follow.query.filter_by(following_id=user_id).count()


def count_following(user_id: int) -> int:
    return Follow.query.filter_by(follower_id=user_id).count()


def get_following_usernames(follower_id: int):
    following_user = aliased(User)
    rows = (
        db.session.query(following_user.username)
        .join(Follow, Follow.following_id == following_user.id)
        .filter(Follow.follower_id == follower_id)
        .order_by(following_user.username.asc())
        .all()
    )
    return [row[0] for row in rows]

