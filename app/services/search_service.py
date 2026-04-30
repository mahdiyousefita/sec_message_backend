from sqlalchemy.orm import joinedload

from app.models.user_model import User
from app.models.profile_model import Profile
from app.models.post_model import Post
from app.models.vote_model import Vote
from app.services import block_service
from app.services.post_service import (
    _build_author_maps,
    _build_playlist_adders_by_media,
    _build_visible_quoted_posts,
    _post_visibility_filter,
    _serialize_post,
    _viewer_user_id,
)


def _serialize_user(user, profile):
    from app.services.post_service import _build_media_url

    profile_image_url = None
    if profile and profile.image_object_name:
        profile_image_url = _build_media_url(profile.image_object_name)

    return {
        "id": user.id,
        "username": user.username,
        "name": profile.name if profile else user.username,
        "badge": user.badge,
        "profile_image_url": profile_image_url,
        "profile_image_shape": (
            profile.profile_image_shape
            if profile and profile.profile_image_shape
            else "circle"
        ),
    }


def _build_vote_map(posts: list[Post], viewer_user_id: int | None):
    if not posts or not viewer_user_id:
        return {}

    post_ids = {post.id for post in posts}
    votes = (
        Vote.query
        .filter(
            Vote.user_id == viewer_user_id,
            Vote.target_type == "post",
            Vote.target_id.in_(post_ids),
        )
        .all()
    )
    return {vote.target_id: vote.value for vote in votes}


def search_users(
    query: str,
    page: int,
    limit: int,
    viewer_username: str | None = None,
):
    if limit > 50:
        limit = 50

    pattern = f"%{query}%"
    hidden_user_ids = block_service.hidden_user_ids_for_viewer(viewer_username)

    base_query = (
        User.query
        .outerjoin(Profile, Profile.user_id == User.id)
        .filter(User.is_suspended.is_(False))
        .filter(
            (User.username.ilike(pattern)) | (Profile.name.ilike(pattern))
        )
        .order_by(User.id.asc())
    )
    if hidden_user_ids:
        base_query = base_query.filter(~User.id.in_(hidden_user_ids))

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


def search_posts(
    query: str,
    page: int,
    limit: int,
    viewer_username: str | None = None,
):
    if limit > 50:
        limit = 50

    pattern = f"%{query}%"
    viewer_user_id = _viewer_user_id(viewer_username)
    hidden_user_ids = block_service.hidden_user_ids_for_viewer(viewer_username)

    base_query = (
        Post.query
        .join(User, User.id == Post.author_id)
        .options(joinedload(Post.media))
        .filter(
            Post.is_hidden.is_(False),
            User.is_suspended.is_(False),
            _post_visibility_filter(viewer_user_id),
        )
        .filter(Post.text.ilike(pattern))
        .order_by(Post.created_at.desc())
    )
    if hidden_user_ids:
        base_query = base_query.filter(~Post.author_id.in_(hidden_user_ids))

    total = base_query.count()
    posts = base_query.offset((page - 1) * limit).limit(limit).all()

    quoted_posts_by_id = _build_visible_quoted_posts(
        posts,
        viewer_user_id=viewer_user_id,
        hidden_user_ids=hidden_user_ids,
    )
    author_ids = {p.author_id for p in posts} | {
        quoted_post.author_id
        for quoted_post in quoted_posts_by_id.values()
    }
    user_by_id, profile_by_user_id = _build_author_maps(author_ids)
    playlist_adders_by_media_id = _build_playlist_adders_by_media(posts)
    vote_by_post_id = _build_vote_map(posts=posts, viewer_user_id=viewer_user_id)

    serialized_posts = []
    for post in posts:
        payload = _serialize_post(
            post,
            user_by_id,
            profile_by_user_id,
            playlist_adders_by_media_id=playlist_adders_by_media_id,
            quoted_posts_by_id=quoted_posts_by_id,
        )
        payload["viewer_vote"] = int(vote_by_post_id.get(post.id, 0))
        serialized_posts.append(payload)

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "posts": serialized_posts,
    }


def search_all(
    query: str,
    page: int,
    limit: int,
    viewer_username: str | None = None,
):
    users_result = search_users(query, page, limit, viewer_username=viewer_username)
    posts_result = search_posts(
        query=query,
        page=page,
        limit=limit,
        viewer_username=viewer_username,
    )

    return {
        "page": page,
        "limit": limit,
        "users": users_result["users"],
        "users_total": users_result["total"],
        "posts": posts_result["posts"],
        "posts_total": posts_result["total"],
    }
