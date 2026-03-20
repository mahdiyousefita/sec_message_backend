from sqlalchemy.orm import joinedload

from app.models.user_model import User
from app.models.profile_model import Profile
from app.models.post_model import Post
from app.services.post_service import _build_author_maps, _serialize_post


def _serialize_user(user, profile):
    from app.services.post_service import _build_media_url

    profile_image_url = None
    if profile and profile.image_object_name:
        profile_image_url = _build_media_url(profile.image_object_name)

    return {
        "id": user.id,
        "username": user.username,
        "name": profile.name if profile else user.username,
        "profile_image_url": profile_image_url,
    }


def search_users(query: str, page: int, limit: int):
    if limit > 50:
        limit = 50

    pattern = f"%{query}%"

    base_query = (
        User.query
        .outerjoin(Profile, Profile.user_id == User.id)
        .filter(
            (User.username.ilike(pattern)) | (Profile.name.ilike(pattern))
        )
        .order_by(User.id.asc())
    )

    total = base_query.count()
    users = base_query.offset((page - 1) * limit).limit(limit).all()

    user_ids = {u.id for u in users}
    profiles = Profile.query.filter(Profile.user_id.in_(user_ids)).all() if user_ids else []
    profile_by_user_id = {p.user_id: p for p in profiles}

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "users": [_serialize_user(u, profile_by_user_id.get(u.id)) for u in users],
    }


def search_posts(query: str, page: int, limit: int):
    if limit > 50:
        limit = 50

    pattern = f"%{query}%"

    base_query = (
        Post.query
        .options(joinedload(Post.media))
        .filter(Post.text.ilike(pattern))
        .order_by(Post.created_at.desc())
    )

    total = base_query.count()
    posts = base_query.offset((page - 1) * limit).limit(limit).all()

    author_ids = {p.author_id for p in posts}
    user_by_id, profile_by_user_id = _build_author_maps(author_ids)

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "posts": [_serialize_post(p, user_by_id, profile_by_user_id) for p in posts],
    }


def search_all(query: str, page: int, limit: int):
    users_result = search_users(query, page, limit)
    posts_result = search_posts(query, page, limit)

    return {
        "page": page,
        "limit": limit,
        "users": users_result["users"],
        "users_total": users_result["total"],
        "posts": posts_result["posts"],
        "posts_total": posts_result["total"],
    }
