import uuid

from flask import current_app

from app.db import db
from app.extensions.minio_client import get_minio_client
from app.models.post_model import Post
from app.repositories import user_repository
from app.repositories.follow_repository import count_followers, count_following
from app.repositories.profile_repository import create_profile_for_user, get_by_user_id
from app.services.post_service import get_posts_by_username


ALLOWED_PROFILE_IMAGE_MIME_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
}


def _build_profile_image_url(image_object_name: str | None):
    if not image_object_name:
        return None

    return (
        f"{current_app.config['MINIO_PUBLIC_BASE_URL']}/"
        f"{current_app.config['MINIO_BUCKET']}/"
        f"{image_object_name}"
    )


def _get_or_create_profile(user):
    profile = get_by_user_id(user.id)
    if profile:
        return profile

    profile = create_profile_for_user(user.id, user.username)
    db.session.commit()
    return profile


def _serialize_profile(user, profile):
    return {
        "username": user.username,
        "name": profile.name,
        "bio": profile.bio,
        "profile_image_url": _build_profile_image_url(profile.image_object_name),
        "followers_count": count_followers(user.id),
        "following_count": count_following(user.id),
        "posts_count": Post.query.filter_by(author_id=user.id).count(),
    }


def get_profile_by_username(username: str):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    profile = _get_or_create_profile(user)
    return _serialize_profile(user, profile)


def update_profile(
    username: str,
    name=None,
    bio=None,
    profile_image=None,
):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    profile = _get_or_create_profile(user)

    if name is None and bio is None and profile_image is None:
        raise ValueError("At least one field is required")

    if name is not None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Name must be a non-empty string")
        profile.name = name.strip()

    if bio is not None:
        if not isinstance(bio, str):
            raise ValueError("Bio must be a string")
        profile.bio = bio.strip()

    if profile_image is not None:
        if not getattr(profile_image, "filename", ""):
            raise ValueError("Profile image file is required")
        if profile_image.mimetype not in ALLOWED_PROFILE_IMAGE_MIME_TYPES:
            raise ValueError(
                f"Unsupported media type: {profile_image.mimetype}"
            )

        minio = get_minio_client()
        bucket = current_app.config["MINIO_BUCKET"]
        extension = profile_image.mimetype.split("/")[-1]
        object_name = f"profiles/{user.id}/{uuid.uuid4()}.{extension}"

        minio.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=profile_image,
            length=-1,
            part_size=10 * 1024 * 1024,
            content_type=profile_image.mimetype,
        )
        profile.image_object_name = object_name

    db.session.commit()
    return _serialize_profile(user, profile)


def get_profile_posts(username: str, page: int, limit: int):
    return get_posts_by_username(username, page, limit)

