import os
import uuid

from flask import current_app, has_request_context, request
from minio.error import S3Error
from sqlalchemy import or_

from app.db import db
from app.extensions.minio_client import get_minio_client
from app.models.activity_notification_model import ActivityNotification
from app.models.admin_model import AdminUser
from app.models.block_model import Block
from app.models.comment_model import Comment
from app.models.crash_log_model import CrashLog
from app.models.follow_model import Follow
from app.models.group_model import Group, GroupMember
from app.models.media_model import Media
from app.models.pending_registration_model import PendingRegistration
from app.models.playlist_track_model import PlaylistTrack
from app.models.post_model import Post
from app.models.profile_model import Profile
from app.models.profile_video_model import ProfileVideo
from app.models.report_model import PostReport
from app.models.user_model import User
from app.models.vote_model import Vote
from app.repositories import profile_video_repository, user_repository
from app.repositories import message_repository
from app.repositories.follow_repository import count_followers, count_following
from app.repositories.profile_repository import create_profile_for_user, get_by_user_id
from app.services import block_service
from app.services.media_security import (
    is_blocked_declared_mimetype,
    normalize_mimetype,
    validate_upload_content,
)
from app.services.post_service import _get_mp4_duration_seconds, get_posts_by_username

VIDEO_MIME_TYPES_WITH_RELIABLE_DURATION = {
    "video/mp4",
    "video/quicktime",
}
PROFILE_IMAGE_SHAPE_CATALOG = {
    "circle",
    "pill",
    "cookie_12_sided",
    "cookie_9_sided",
}


def _build_profile_media_url(object_name: str | None):
    if not object_name:
        return None

    base_url = current_app.config.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")

    if base_url:
        return f"{base_url}/media/{object_name}"
    return f"/media/{object_name}"


def _profile_video_max_duration_seconds() -> int:
    return max(int(current_app.config.get("PROFILE_VIDEO_MAX_DURATION_SECONDS", 5)), 1)


def _profile_video_max_size_bytes() -> int:
    return max(
        int(current_app.config.get("PROFILE_VIDEO_MAX_SIZE_BYTES", 15 * 1024 * 1024)),
        1,
    )


def _format_size_mb(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.1f}MB"


def _extension_for_mimetype(mimetype: str) -> str:
    mapping = {
        "image/jpeg": "jpeg",
        "image/png": "png",
        "image/webp": "webp",
        "video/mp4": "mp4",
        "video/quicktime": "mov",
    }
    return mapping.get(mimetype, mimetype.split("/")[-1])


def _normalize_mimetype(raw_mimetype: str | None) -> str:
    return normalize_mimetype(raw_mimetype)


def _is_blocked_media_mimetype(mimetype: str) -> bool:
    return is_blocked_declared_mimetype(mimetype)


def _is_profile_image_mimetype_allowed(mimetype: str) -> bool:
    normalized = _normalize_mimetype(mimetype)
    return normalized.startswith("image/") and not _is_blocked_media_mimetype(normalized)


def _is_profile_video_mimetype_allowed(mimetype: str) -> bool:
    normalized = _normalize_mimetype(mimetype)
    return normalized.startswith("video/")


def _get_stream_and_length(file_storage):
    stream = getattr(file_storage, "stream", file_storage)
    try:
        stream.seek(0, 2)
        length = stream.tell()
        stream.seek(0)
        return stream, length
    except Exception:
        try:
            stream.seek(0)
        except Exception:
            pass
        return stream, -1


def _is_media_not_found(error: S3Error) -> bool:
    return error.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}


def _delete_media_object(object_name: str | None):
    if not object_name:
        return

    if object_name.startswith("static/"):
        relative_path = object_name[len("static/"):]
        absolute_path = os.path.join(current_app.static_folder, relative_path)
        if os.path.isfile(absolute_path):
            os.remove(absolute_path)
        return

    bucket = current_app.config["MINIO_BUCKET"]
    minio = get_minio_client()
    try:
        minio.remove_object(
            bucket_name=bucket,
            object_name=object_name,
        )
    except S3Error as e:
        if _is_media_not_found(e):
            return
        raise


def _get_or_create_profile(user):
    profile = get_by_user_id(user.id)
    if profile:
        return profile

    profile = create_profile_for_user(user.id, user.username)
    db.session.commit()
    return profile


def _serialize_profile(user, profile):
    profile_video = profile_video_repository.get_by_user_id(user.id)
    video_object_name = profile_video.video_object_name if profile_video else None

    return {
        "username": user.username,
        "name": profile.name,
        "badge": user.badge,
        "bio": profile.bio,
        "profile_image_url": _build_profile_media_url(profile.image_object_name),
        "profile_image_shape": (
            profile.profile_image_shape
            if profile.profile_image_shape in PROFILE_IMAGE_SHAPE_CATALOG
            else "circle"
        ),
        "profile_video_url": _build_profile_media_url(video_object_name),
        "followers_count": count_followers(user.id),
        "following_count": count_following(user.id),
        "posts_count": Post.query.filter_by(author_id=user.id, is_hidden=False).count(),
    }


def _normalize_profile_image_shape(raw_shape):
    if raw_shape is None:
        return None
    if not isinstance(raw_shape, str):
        raise ValueError("Profile image shape must be a string")

    normalized = raw_shape.strip().lower()
    if not normalized:
        return "circle"
    if normalized not in PROFILE_IMAGE_SHAPE_CATALOG:
        raise ValueError(
            "Unsupported profile image shape. "
            f"Allowed values: {', '.join(sorted(PROFILE_IMAGE_SHAPE_CATALOG))}"
        )
    return normalized


def get_profile_by_username(
    username: str,
    viewer_username: str | None = None,
):
    user = user_repository.get_by_username(username)
    if not user or getattr(user, "is_suspended", False):
        raise ValueError("User not found")
    if viewer_username and viewer_username != username:
        viewer = user_repository.get_by_username(viewer_username)
        if viewer and block_service.user_ids_have_block_relation(viewer.id, user.id):
            raise ValueError("User not found")

    profile = _get_or_create_profile(user)
    return _serialize_profile(user, profile)


def update_profile(
    username: str,
    name=None,
    bio=None,
    profile_image_shape=None,
    profile_image=None,
    profile_video=None,
):
    user = user_repository.get_by_username(username)
    if not user or getattr(user, "is_suspended", False):
        raise ValueError("User not found")

    profile = _get_or_create_profile(user)

    if (
        name is None
        and bio is None
        and profile_image_shape is None
        and profile_image is None
        and profile_video is None
    ):
        raise ValueError("At least one field is required")

    if name is not None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Name must be a non-empty string")
        profile.name = name.strip()

    if bio is not None:
        if not isinstance(bio, str):
            raise ValueError("Bio must be a string")
        profile.bio = bio.strip()

    normalized_shape = _normalize_profile_image_shape(profile_image_shape)
    if normalized_shape is not None:
        if not user.badge:
            raise ValueError(
                "Profile image shape customization is available only for users with a badge"
            )
        profile.profile_image_shape = normalized_shape

    bucket = current_app.config["MINIO_BUCKET"]
    minio = None

    old_image_object_name = None
    old_video_object_name = None

    if profile_image is not None:
        if not getattr(profile_image, "filename", ""):
            raise ValueError("Profile image file is required")
        image_mimetype = _normalize_mimetype(getattr(profile_image, "mimetype", ""))
        if not _is_profile_image_mimetype_allowed(image_mimetype):
            raise ValueError(f"Unsupported media type: {image_mimetype}")
        if bool(current_app.config.get("MEDIA_CONTENT_SNIFFING_ENABLED", True)):
            image_validation_error = validate_upload_content(
                profile_image,
                image_mimetype,
                allowed_categories={"image"},
                reject_active_text_payloads=bool(
                    current_app.config.get("MEDIA_CONTENT_REJECT_ACTIVE_TEXT", True)
                ),
                enforce_declared_category_match=bool(
                    current_app.config.get("MEDIA_CONTENT_ENFORCE_CATEGORY_MATCH", True)
                ),
                sniff_bytes=int(current_app.config.get("MEDIA_CONTENT_SNIFF_BYTES", 2048)),
            )
            if image_validation_error in {"unsupported_declared_type", "unsupported_detected_type"}:
                raise ValueError(f"Unsupported media type: {image_mimetype}")
            if image_validation_error in {"blocked_active_content", "declared_type_mismatch"}:
                raise ValueError("Invalid profile image content")
            if image_validation_error is not None:
                raise ValueError("Could not validate profile image")

        if minio is None:
            minio = get_minio_client()

        extension = _extension_for_mimetype(image_mimetype)
        object_name = f"profiles/{user.id}/images/{uuid.uuid4()}.{extension}"
        stream, length = _get_stream_and_length(profile_image)

        upload_kwargs = {
            "bucket_name": bucket,
            "object_name": object_name,
            "data": stream,
            "length": length,
            "content_type": image_mimetype,
        }
        if length == -1:
            upload_kwargs["part_size"] = 10 * 1024 * 1024

        minio.put_object(**upload_kwargs)

        old_image_object_name = profile.image_object_name
        profile.image_object_name = object_name

    if profile_video is not None:
        if not getattr(profile_video, "filename", ""):
            raise ValueError("Profile video file is required")

        mimetype = _normalize_mimetype(getattr(profile_video, "mimetype", ""))
        if not _is_profile_video_mimetype_allowed(mimetype):
            raise ValueError(f"Unsupported media type: {mimetype}")
        if bool(current_app.config.get("MEDIA_CONTENT_SNIFFING_ENABLED", True)):
            video_validation_error = validate_upload_content(
                profile_video,
                mimetype,
                allowed_categories={"video"},
                reject_active_text_payloads=bool(
                    current_app.config.get("MEDIA_CONTENT_REJECT_ACTIVE_TEXT", True)
                ),
                enforce_declared_category_match=bool(
                    current_app.config.get("MEDIA_CONTENT_ENFORCE_CATEGORY_MATCH", True)
                ),
                sniff_bytes=int(current_app.config.get("MEDIA_CONTENT_SNIFF_BYTES", 2048)),
            )
            if video_validation_error in {"unsupported_declared_type", "unsupported_detected_type"}:
                raise ValueError(f"Unsupported media type: {mimetype}")
            if video_validation_error in {"blocked_active_content", "declared_type_mismatch"}:
                raise ValueError("Invalid profile video content")
            if video_validation_error is not None:
                raise ValueError("Could not validate profile video")

        if minio is None:
            minio = get_minio_client()

        if mimetype in VIDEO_MIME_TYPES_WITH_RELIABLE_DURATION:
            duration_seconds = _get_mp4_duration_seconds(profile_video)
            if duration_seconds is None:
                raise ValueError("Could not determine profile video duration")
            max_duration = _profile_video_max_duration_seconds()
            if duration_seconds > max_duration:
                raise ValueError(
                    f"Profile video must be {max_duration} seconds or shorter"
                )

        extension = _extension_for_mimetype(mimetype)
        object_name = f"profiles/{user.id}/videos/{uuid.uuid4()}.{extension}"
        stream, length = _get_stream_and_length(profile_video)

        if length == -1:
            raise ValueError("Could not determine profile video size")

        max_size = _profile_video_max_size_bytes()
        if length > max_size:
            raise ValueError(
                "Profile video is too large. "
                f"Maximum allowed size is {_format_size_mb(max_size)}."
            )

        minio.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=stream,
            length=length,
            content_type=mimetype,
        )

        current_profile_video = profile_video_repository.get_by_user_id(user.id)
        old_video_object_name = (
            current_profile_video.video_object_name if current_profile_video else None
        )
        profile_video_repository.upsert_for_user(
            user_id=user.id,
            video_object_name=object_name,
        )

    db.session.commit()

    if old_image_object_name and old_image_object_name != profile.image_object_name:
        try:
            _delete_media_object(old_image_object_name)
        except Exception:
            pass

    current_profile_video = profile_video_repository.get_by_user_id(user.id)
    current_video_object_name = (
        current_profile_video.video_object_name if current_profile_video else None
    )
    if old_video_object_name and old_video_object_name != current_video_object_name:
        try:
            _delete_media_object(old_video_object_name)
        except Exception:
            pass

    return _serialize_profile(user, profile)


def get_profile_posts(
    username: str,
    page: int,
    limit: int,
    viewer_username: str | None = None,
):
    return get_posts_by_username(
        username=username,
        page=page,
        limit=limit,
        viewer_username=viewer_username,
    )


def _expand_comment_descendants(seed_comment_ids: list[int]) -> set[int]:
    all_ids = {int(comment_id) for comment_id in seed_comment_ids if comment_id is not None}
    frontier = set(all_ids)

    while frontier:
        child_rows = (
            db.session.query(Comment.id)
            .filter(Comment.parent_id.in_(frontier))
            .all()
        )
        next_frontier = {
            int(row[0])
            for row in child_rows
            if row and row[0] is not None and int(row[0]) not in all_ids
        }
        if not next_frontier:
            break
        all_ids.update(next_frontier)
        frontier = next_frontier

    return all_ids


def _collect_account_media_object_names(user_id: int):
    object_names = set()

    profile = get_by_user_id(user_id)
    if profile and profile.image_object_name:
        object_names.add(profile.image_object_name)

    profile_video = profile_video_repository.get_by_user_id(user_id)
    if profile_video and profile_video.video_object_name:
        object_names.add(profile_video.video_object_name)

    post_media_rows = (
        db.session.query(Media.object_name)
        .join(Post, Post.id == Media.post_id)
        .filter(Post.author_id == user_id)
        .all()
    )
    for row in post_media_rows:
        if row and row[0]:
            object_names.add(row[0])

    return sorted(object_names)


def delete_account(username: str):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    user_id = int(user.id)
    all_usernames = [
        row[0]
        for row in db.session.query(User.username).all()
        if row and row[0]
    ]
    media_object_names = _collect_account_media_object_names(user_id)

    user_post_ids = [
        int(row[0])
        for row in db.session.query(Post.id).filter(Post.author_id == user_id).all()
        if row and row[0] is not None
    ]

    post_media_ids = []
    if user_post_ids:
        post_media_ids = [
            int(row[0])
            for row in (
                db.session.query(Media.id)
                .filter(Media.post_id.in_(user_post_ids))
                .all()
            )
            if row and row[0] is not None
        ]

    comment_ids_on_user_posts = []
    if user_post_ids:
        comment_ids_on_user_posts = [
            int(row[0])
            for row in (
                db.session.query(Comment.id)
                .filter(Comment.post_id.in_(user_post_ids))
                .all()
            )
            if row and row[0] is not None
        ]

    authored_comment_ids = [
        int(row[0])
        for row in db.session.query(Comment.id).filter(Comment.author_id == user_id).all()
        if row and row[0] is not None
    ]

    removable_comment_ids = set(comment_ids_on_user_posts)
    removable_comment_ids.update(
        _expand_comment_descendants(authored_comment_ids)
    )

    for object_name in media_object_names:
        _delete_media_object(object_name)

    message_repository.purge_user_data(
        username=username,
        candidate_usernames=all_usernames,
    )

    try:
        if removable_comment_ids:
            Vote.query.filter(
                Vote.target_type == "comment",
                Vote.target_id.in_(list(removable_comment_ids)),
            ).delete(synchronize_session=False)

        Vote.query.filter_by(user_id=user_id).delete(synchronize_session=False)

        if post_media_ids:
            PlaylistTrack.query.filter(
                PlaylistTrack.media_id.in_(post_media_ids)
            ).delete(synchronize_session=False)

        PlaylistTrack.query.filter_by(user_id=user_id).delete(synchronize_session=False)

        if removable_comment_ids:
            Comment.query.filter(
                Comment.id.in_(list(removable_comment_ids))
            ).delete(synchronize_session=False)
        else:
            Comment.query.filter_by(author_id=user_id).delete(synchronize_session=False)

        if user_post_ids:
            Vote.query.filter(
                Vote.target_type == "post",
                Vote.target_id.in_(user_post_ids),
            ).delete(synchronize_session=False)
            Post.query.filter(
                Post.quoted_post_id.in_(user_post_ids)
            ).update(
                {Post.quoted_post_id: None},
                synchronize_session=False,
            )
            ActivityNotification.query.filter(
                ActivityNotification.target_type == "post",
                ActivityNotification.target_id.in_(user_post_ids),
            ).delete(synchronize_session=False)
            PostReport.query.filter(
                PostReport.post_id.in_(user_post_ids)
            ).delete(synchronize_session=False)
            Media.query.filter(
                Media.post_id.in_(user_post_ids)
            ).delete(synchronize_session=False)
            Post.query.filter(
                Post.id.in_(user_post_ids)
            ).delete(synchronize_session=False)

        if removable_comment_ids:
            ActivityNotification.query.filter(
                ActivityNotification.target_type == "comment",
                ActivityNotification.target_id.in_(list(removable_comment_ids)),
            ).delete(synchronize_session=False)

        ActivityNotification.query.filter(
            or_(
                ActivityNotification.recipient_id == user_id,
                ActivityNotification.actor_id == user_id,
            )
        ).delete(synchronize_session=False)

        Follow.query.filter(
            (Follow.follower_id == user_id) | (Follow.following_id == user_id)
        ).delete(synchronize_session=False)

        Block.query.filter(
            (Block.blocker_id == user_id) | (Block.blocked_id == user_id)
        ).delete(synchronize_session=False)

        created_group_ids = [
            int(row[0])
            for row in db.session.query(Group.id).filter(Group.creator_id == user_id).all()
            if row and row[0] is not None
        ]

        GroupMember.query.filter(
            GroupMember.user_id == user_id
        ).delete(synchronize_session=False)
        if created_group_ids:
            GroupMember.query.filter(
                GroupMember.group_id.in_(created_group_ids)
            ).delete(synchronize_session=False)
        Group.query.filter(
            Group.creator_id == user_id
        ).delete(synchronize_session=False)

        PostReport.query.filter(
            PostReport.handled_by_admin_id == user_id
        ).update(
            {PostReport.handled_by_admin_id: None},
            synchronize_session=False,
        )
        PostReport.query.filter(
            PostReport.reporter_id == user_id
        ).delete(synchronize_session=False)

        ProfileVideo.query.filter_by(user_id=user_id).delete(synchronize_session=False)
        PendingRegistration.query.filter_by(username=username).delete(synchronize_session=False)
        CrashLog.query.filter(
            or_(
                CrashLog.user_id == user_id,
                CrashLog.username_snapshot == username,
            )
        ).delete(synchronize_session=False)
        AdminUser.query.filter_by(user_id=user_id).delete(synchronize_session=False)
        Profile.query.filter_by(user_id=user_id).delete(synchronize_session=False)
        User.query.filter_by(id=user_id).delete(synchronize_session=False)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
