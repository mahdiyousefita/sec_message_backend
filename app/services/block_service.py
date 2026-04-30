from flask import current_app, has_request_context, request

from app.repositories import block_repository, user_repository


def _build_media_url(object_name: str | None):
    if not object_name:
        return None

    base_url = current_app.config.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")

    if object_name.startswith("static/"):
        if base_url:
            return f"{base_url}/{object_name}"
        return f"/{object_name}"

    if base_url:
        return f"{base_url}/media/{object_name}"
    return f"/media/{object_name}"


def block_by_username(blocker_username: str, blocked_username: str) -> dict:
    blocker = user_repository.get_by_username(blocker_username)
    blocked = user_repository.get_by_username(blocked_username)

    if not blocker or getattr(blocker, "is_suspended", False):
        raise ValueError("User not found")
    if not blocked or getattr(blocked, "is_suspended", False):
        raise ValueError("User not found")
    if blocker.id == blocked.id:
        raise ValueError("You cannot block yourself")

    created = block_repository.create_block(blocker.id, blocked.id)
    return {
        "created": created,
        "blocker_id": blocker.id,
        "blocked_id": blocked.id,
        "blocker_username": blocker.username,
        "blocked_username": blocked.username,
    }


def unblock_by_username(blocker_username: str, blocked_username: str) -> dict:
    blocker = user_repository.get_by_username(blocker_username)
    blocked = user_repository.get_by_username(blocked_username)

    if not blocker or getattr(blocker, "is_suspended", False):
        raise ValueError("User not found")
    if not blocked:
        raise ValueError("User not found")
    if blocker.id == blocked.id:
        raise ValueError("You cannot unblock yourself")

    removed = block_repository.delete_block(blocker.id, blocked.id)
    return {
        "removed": removed,
        "blocker_id": blocker.id,
        "blocked_id": blocked.id,
        "blocker_username": blocker.username,
        "blocked_username": blocked.username,
    }


def users_have_block_relation(username_a: str | None, username_b: str | None) -> bool:
    if not username_a or not username_b or username_a == username_b:
        return False

    user_a = user_repository.get_by_username(username_a)
    user_b = user_repository.get_by_username(username_b)
    if not user_a or not user_b:
        return False

    return block_repository.has_block_relation(user_a.id, user_b.id)


def user_ids_have_block_relation(user_a_id: int | None, user_b_id: int | None) -> bool:
    if not user_a_id or not user_b_id:
        return False
    return block_repository.has_block_relation(user_a_id, user_b_id)


def hidden_user_ids_for_viewer(viewer_username: str | None) -> set[int]:
    if not viewer_username:
        return set()

    viewer = user_repository.get_by_username(viewer_username)
    if not viewer or getattr(viewer, "is_suspended", False):
        return set()

    return block_repository.get_hidden_user_ids_for_viewer(viewer.id)


def get_blocked_users_page(
    blocker_username: str,
    page: int,
    limit: int,
):
    blocker = user_repository.get_by_username(blocker_username)
    if not blocker or getattr(blocker, "is_suspended", False):
        raise ValueError("User not found")

    total, users = block_repository.get_blocked_users_page(
        blocker_id=blocker.id,
        page=page,
        limit=limit,
    )

    serialized_users = []
    for user in users:
        serialized_users.append(
            {
                "id": user["id"],
                "username": user["username"],
                "name": user["name"],
                "badge": user.get("badge"),
                "profile_image_url": _build_media_url(user["image_object_name"]),
                "profile_image_shape": user.get("profile_image_shape", "circle"),
            }
        )

    return {
        "page": page,
        "limit": min(limit, 100),
        "total": total,
        "users": serialized_users,
    }
