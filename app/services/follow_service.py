from flask import current_app, has_request_context, request

from app.repositories import user_repository
from app.repositories.follow_repository import (
    create_follow,
    delete_follow,
    get_followers_page,
    get_following_page,
    get_following_usernames,
    is_following,
)


MAX_FOLLOW_LIST_LIMIT = 100
DEFAULT_FOLLOW_LIST_LIMIT = 20


def _build_profile_image_url(image_object_name: str | None):
    if not image_object_name:
        return None

    base_url = current_app.config.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")

    if base_url:
        return f"{base_url}/media/{image_object_name}"
    return f"/media/{image_object_name}"


def _normalize_page_limit(page: int, limit: int):
    page = page if isinstance(page, int) and page > 0 else 1
    limit = limit if isinstance(limit, int) and limit > 0 else DEFAULT_FOLLOW_LIST_LIMIT
    if limit > MAX_FOLLOW_LIST_LIMIT:
        limit = MAX_FOLLOW_LIST_LIMIT
    return page, limit


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


def get_follow_status_by_username(
    follower_username: str, following_username: str
) -> bool:
    follower = user_repository.get_by_username(follower_username)
    target = user_repository.get_by_username(following_username)

    if not follower or not target:
        raise ValueError("User not found")

    return is_following(follower.id, target.id)


def get_followers_by_username(username: str, page: int, limit: int):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    page, limit = _normalize_page_limit(page, limit)
    total, users = get_followers_page(user.id, page, limit)

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "users": [
            {
                "id": row["id"],
                "username": row["username"],
                "name": row["name"],
                "profile_image_url": _build_profile_image_url(row["image_object_name"]),
            }
            for row in users
        ],
    }


def get_following_page_by_username(username: str, page: int, limit: int):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    page, limit = _normalize_page_limit(page, limit)
    total, users = get_following_page(user.id, page, limit)

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "users": [
            {
                "id": row["id"],
                "username": row["username"],
                "name": row["name"],
                "profile_image_url": _build_profile_image_url(row["image_object_name"]),
            }
            for row in users
        ],
    }
