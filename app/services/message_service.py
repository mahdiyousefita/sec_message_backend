import os
import re
import uuid

from flask import current_app, has_request_context, request

from app.extensions.minio_client import get_minio_client
from app.repositories import block_repository, message_repository, user_repository


ALLOWED_IMAGE_MIME_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
}

ALLOWED_VIDEO_MIME_TYPES = {
    "video/mp4",
    "video/quicktime",
}

ALLOWED_AUDIO_MIME_TYPES = {
    "audio/mpeg",
    "audio/mp3",
    "audio/wav",
    "audio/webm",
    "audio/ogg",
    "audio/aac",
    "audio/mp4",
}

ALL_ALLOWED_MIME_TYPES = (
    ALLOWED_IMAGE_MIME_TYPES
    | ALLOWED_VIDEO_MIME_TYPES
    | ALLOWED_AUDIO_MIME_TYPES
)

ALLOWED_MESSAGE_TYPES = {"text", "image", "video", "voice", "mixed"}

MAX_IMAGE_SIZE_BYTES = 10 * 1024 * 1024       # 10 MB
MAX_VIDEO_SIZE_BYTES = 50 * 1024 * 1024       # 50 MB
MAX_AUDIO_SIZE_BYTES = 16 * 1024 * 1024       # 16 MB

_SAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9._-]")


class MessageAttachmentStorageError(Exception):
    pass


def _sanitize_filename(filename: str) -> str:
    name = os.path.basename(filename or "file")
    name = _SAFE_FILENAME_RE.sub("_", name)
    if len(name) > 200:
        name = name[:200]
    return name or "file"


def _build_media_url(object_name: str) -> str:
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


def _extension_for_mimetype(mimetype: str) -> str:
    mapping = {
        "image/jpeg": "jpeg",
        "image/png": "png",
        "image/webp": "webp",
        "video/mp4": "mp4",
        "video/quicktime": "mov",
        "audio/mpeg": "mp3",
        "audio/mp3": "mp3",
        "audio/wav": "wav",
        "audio/webm": "webm",
        "audio/ogg": "ogg",
        "audio/aac": "aac",
        "audio/mp4": "m4a",
    }
    return mapping.get(mimetype, mimetype.split("/")[-1] if "/" in mimetype else "bin")


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


def _store_locally(file_storage, username: str, extension: str) -> str:
    filename = f"{uuid.uuid4()}.{extension}"
    relative_parts = ["uploads", "messages", username, filename]
    relative_path = os.path.join(*relative_parts)
    absolute_path = os.path.join(current_app.static_folder, relative_path)
    os.makedirs(os.path.dirname(absolute_path), exist_ok=True)

    try:
        file_storage.stream.seek(0)
    except Exception:
        pass

    file_storage.save(absolute_path)
    return "static/" + "/".join(relative_parts)


def _attachment_type_from_mimetype(mimetype: str) -> str:
    if mimetype in ALLOWED_IMAGE_MIME_TYPES:
        return "image"
    if mimetype in ALLOWED_VIDEO_MIME_TYPES:
        return "video"
    if mimetype in ALLOWED_AUDIO_MIME_TYPES:
        return "voice"
    raise ValueError(f"Unsupported attachment type: {mimetype}")


def _max_size_for_type(attachment_type: str) -> int:
    return {
        "image": MAX_IMAGE_SIZE_BYTES,
        "video": MAX_VIDEO_SIZE_BYTES,
        "voice": MAX_AUDIO_SIZE_BYTES,
    }.get(attachment_type, MAX_IMAGE_SIZE_BYTES)


def _format_mb(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.0f}MB"


def upload_message_attachment(username: str, file_storage):
    if not user_repository.get_by_username(username):
        raise ValueError("User not found")

    if file_storage is None or not getattr(file_storage, "filename", ""):
        raise ValueError("Attachment file is required")

    mimetype = (getattr(file_storage, "mimetype", "") or "").strip().lower()
    if not mimetype:
        raise ValueError("Attachment mime type is required")

    if mimetype not in ALL_ALLOWED_MIME_TYPES:
        raise ValueError(f"Unsupported attachment type: {mimetype}")

    attachment_type = _attachment_type_from_mimetype(mimetype)
    stream, size_bytes = _get_stream_and_length(file_storage)

    max_size = _max_size_for_type(attachment_type)
    if size_bytes > max_size:
        raise ValueError(
            f"{attachment_type.capitalize()} too large. "
            f"Maximum allowed is {_format_mb(max_size)}."
        )

    safe_filename = _sanitize_filename(getattr(file_storage, "filename", "file"))

    extension = _extension_for_mimetype(mimetype)
    bucket = current_app.config["MINIO_BUCKET"]
    object_name = None

    local_fallback_enabled = bool(current_app.config.get("MEDIA_LOCAL_FALLBACK_ENABLED", True))
    use_local_storage = False

    try:
        minio = get_minio_client()
        if not minio.bucket_exists(bucket):
            minio.make_bucket(bucket)
    except Exception as exc:
        if not local_fallback_enabled:
            raise MessageAttachmentStorageError("Media storage is unavailable") from exc
        use_local_storage = True

    if not use_local_storage:
        try:
            object_name = f"messages/{username}/{uuid.uuid4()}.{extension}"
            upload_kwargs = {
                "bucket_name": bucket,
                "object_name": object_name,
                "data": stream,
                "length": size_bytes,
                "content_type": mimetype,
            }
            if size_bytes == -1:
                upload_kwargs["part_size"] = 10 * 1024 * 1024
            minio.put_object(**upload_kwargs)
        except Exception as exc:
            if not local_fallback_enabled:
                raise MessageAttachmentStorageError("Media storage is unavailable") from exc
            use_local_storage = True

    if use_local_storage:
        try:
            object_name = _store_locally(file_storage, username, extension)
        except Exception as exc:
            raise MessageAttachmentStorageError("Media storage is unavailable") from exc

    return {
        "type": attachment_type,
        "mime_type": mimetype,
        "file_name": safe_filename,
        "size_bytes": max(size_bytes, 0),
        "object_name": object_name,
        "url": _build_media_url(object_name),
    }


def send_message(sender, recipient, message, encrypted_key, attachment=None, message_type=None,
                reply_to_message_id=None, reply_to_sender=None,
                 encrypted_reply_preview=None,
                 encrypted_reply_key=None):
    sender_user = user_repository.get_by_username(sender)
    recipient_user = user_repository.get_by_username(recipient)

    if not sender_user:
        raise ValueError("Sender not found")
    if not recipient_user:
        raise ValueError("Recipient not found")
    if block_repository.has_block_relation(sender_user.id, recipient_user.id):
        raise PermissionError("Messaging is unavailable because one of you has blocked the other")

    if not encrypted_key:
        raise ValueError("Encrypted key is required")

    if not message and not attachment:
        raise ValueError("Message or attachment is required")

    normalized_message_type = message_type
    if normalized_message_type:
        normalized_message_type = normalized_message_type.strip().lower()
        if normalized_message_type not in ALLOWED_MESSAGE_TYPES:
            raise ValueError("Invalid message type")
    elif attachment and message:
        normalized_message_type = "mixed"
    elif attachment:
        normalized_message_type = attachment.get("type", "image")
    else:
        normalized_message_type = "text"

    payload = message_repository.build_message_payload(
        sender,
        message,
        encrypted_key,
        attachment=attachment,
        message_type=normalized_message_type,
        reply_to_message_id=reply_to_message_id,
        reply_to_sender=reply_to_sender,
        encrypted_reply_preview=encrypted_reply_preview,
        encrypted_reply_key=encrypted_reply_key,
    )
    message_repository.push_message_payload(recipient, payload)
    message_repository.store_private_message_metadata(payload, recipient)
    message_repository.record_conversation_timestamp(
        sender, recipient, payload.get("timestamp")
    )

    return payload


def receive_messages(username):
    return message_repository.pop_messages(username)


def peek_messages(username):
    return message_repository.peek_messages(username)


def peek_messages_batch(username, limit=100):
    return message_repository.peek_messages_batch(username, limit=limit)


def get_pending_count(username):
    return message_repository.get_pending_count(username)


def peek_group_messages_for_user(username, group_id, limit=100):
    return message_repository.peek_group_messages_batch_for_user(
        username, group_id, limit=limit
    )


def get_group_pending_count(username, group_id):
    return message_repository.get_group_pending_count(username, group_id)


def ack_messages(username, message_ids):
    return message_repository.ack_messages(username, message_ids)


def get_message_metadata(message_id):
    return message_repository.get_message_metadata(message_id)


def delete_message_metadata(message_id):
    return message_repository.delete_message_metadata(message_id)


def queue_message_deletion_event(username, event_name, payload):
    return message_repository.queue_message_deletion_event(username, event_name, payload)


def pop_message_deletion_events(username):
    return message_repository.pop_message_deletion_events(username)


def store_group_message_metadata(payload, group_id):
    return message_repository.store_group_message_metadata(payload, group_id)


def mark_private_message_seen(sender, recipient, message_id):
    return message_repository.mark_private_message_seen(sender, recipient, message_id)


def get_private_seen_message_ids(sender, recipient, message_ids):
    return message_repository.get_private_seen_message_ids(sender, recipient, message_ids)


def mark_group_message_seen(group_id, message_id):
    return message_repository.mark_group_message_seen(group_id, message_id)


def get_group_seen_message_ids(group_id, message_ids):
    return message_repository.get_group_seen_message_ids(group_id, message_ids)


def mark_private_message_deleted(username, chat_id, message_id):
    return message_repository.mark_private_message_deleted(username, chat_id, message_id)


def get_private_deleted_message_ids(username, chat_id, message_ids):
    return message_repository.get_private_deleted_message_ids(username, chat_id, message_ids)


def mark_group_message_deleted(username, group_id, message_id):
    return message_repository.mark_group_message_deleted(username, group_id, message_id)


def get_group_deleted_message_ids(username, group_id, message_ids):
    return message_repository.get_group_deleted_message_ids(username, group_id, message_ids)
