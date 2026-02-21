import uuid
from flask import current_app
from sqlalchemy.orm import joinedload

from app.extensions.minio_client import get_minio_client
from app.models.post_model import Post
from app.repositories.post_repository import create_post_by_username
from app.repositories.media_repository import add_media
from app.repositories import user_repository
from app.db import db




ALLOWED_MIME_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp"
}


def _serialize_post(post):
    return {
        "id": post.id,
        "text": post.text,
        "author": post.author_id,
        "created_at": post.created_at.isoformat(),
        "media": [
            {
                "id": media.id,
                "url": f"{current_app.config['MINIO_PUBLIC_BASE_URL']}/"
                       f"{current_app.config['MINIO_BUCKET']}/"
                       f"{media.object_name}",
                "mime_type": media.mime_type
            }
            for media in post.media
        ]
    }


def create_post_with_media(username, text, files):
    if not text or not text.strip():
        raise ValueError("Text is required")

    if len(files) > 8:
        raise ValueError("Maximum 8 media files allowed")

    post = create_post_by_username(username, text.strip())

    if files:
        minio = get_minio_client()
        bucket = current_app.config["MINIO_BUCKET"]

        for file in files:
            if file.mimetype not in ALLOWED_MIME_TYPES:
                raise ValueError(f"Unsupported media type: {file.mimetype}")

            object_name = f"posts/{post.id}/{uuid.uuid4()}.{file.mimetype.split('/')[-1]}"

            minio.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=file,
                length=-1,
                part_size=10 * 1024 * 1024,
                content_type=file.mimetype
            )

            add_media(
                post_id=post.id,
                object_name=object_name,
                mime_type=file.mimetype
            )

    db.session.commit()
    return {"post_id": post.id}


def get_posts(page: int, limit: int):
    if limit > 50:
        limit = 50

    query = (
        Post.query
        .options(joinedload(Post.media))
        .order_by(Post.created_at.desc())
    )

    total = query.count()
    posts = query.offset((page - 1) * limit).limit(limit).all()

    result = [_serialize_post(post) for post in posts]

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "posts": result
    }


def get_posts_by_username(username: str, page: int, limit: int):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    if limit > 50:
        limit = 50

    query = (
        Post.query
        .filter(Post.author_id == user.id)
        .options(joinedload(Post.media))
        .order_by(Post.created_at.desc())
    )

    total = query.count()
    posts = query.offset((page - 1) * limit).limit(limit).all()

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "posts": [_serialize_post(post) for post in posts]
    }
