from app.repositories import user_repository
from app.repositories.follow_repository import (
    create_follow,
    delete_follow,
    get_following_usernames,
    is_following,
)


def follow_by_username(follower_username: str, following_username: str) -> bool:
    follower = user_repository.get_by_username(follower_username)
    target = user_repository.get_by_username(following_username)

    if not follower or not target:
        raise ValueError("User not found")
    if follower.id == target.id:
        raise ValueError("You cannot follow yourself")

    return create_follow(follower.id, target.id)


def unfollow_by_username(follower_username: str, following_username: str) -> bool:
    follower = user_repository.get_by_username(follower_username)
    target = user_repository.get_by_username(following_username)

    if not follower or not target:
        raise ValueError("User not found")
    if follower.id == target.id:
        raise ValueError("You cannot unfollow yourself")

    return delete_follow(follower.id, target.id)


def get_following_for_username(username: str):
    user = user_repository.get_by_username(username)
    if not user:
        return []
    return get_following_usernames(user.id)


def is_user_following(follower_username: str, following_username: str) -> bool:
    follower = user_repository.get_by_username(follower_username)
    target = user_repository.get_by_username(following_username)
    if not follower or not target:
        return False
    return is_following(follower.id, target.id)

