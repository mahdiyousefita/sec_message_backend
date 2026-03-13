import os
import uuid

from flask import current_app, has_request_context, request

from app.extensions.minio_client import get_minio_client
from app.repositories import user_repository, message_repository


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

ALLOWED_FILE_MIME_TYPES = {
    "application/pdf",
    "text/plain",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/zip",
    "application/x-rar-compressed",
}

ALLOWED_MESSAGE_TYPES = {"text", "image", "video", "voice", "file", "mixed"}


class MessageAttachmentStorageError(Exception):
    pass


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
        "application/pdf": "pdf",
        "text/plain": "txt",
        "application/msword": "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "application/vnd.ms-excel": "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "application/vnd.ms-powerpoint": "ppt",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": "pptx",
        "application/zip": "zip",
        "application/x-rar-compressed": "rar",
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
    if mimetype in ALLOWED_FILE_MIME_TYPES:
        return "file"
    raise ValueError(f"Unsupported attachment type: {mimetype}")


def upload_message_attachment(username: str, file_storage):
    if not user_repository.get_by_username(username):
        raise ValueError("User not found")

    if file_storage is None or not getattr(file_storage, "filename", ""):
        raise ValueError("Attachment file is required")

    mimetype = (getattr(file_storage, "mimetype", "") or "").strip().lower()
    if not mimetype:
        raise ValueError("Attachment mime type is required")

    attachment_type = _attachment_type_from_mimetype(mimetype)
    stream, size_bytes = _get_stream_and_length(file_storage)

    max_size_bytes = int(current_app.config.get("MESSAGE_ATTACHMENT_MAX_SIZE_BYTES", 25 * 1024 * 1024))
    if size_bytes > max_size_bytes:
        raise ValueError("Attachment is too large")

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
        "file_name": getattr(file_storage, "filename", ""),
        "size_bytes": max(size_bytes, 0),
        "object_name": object_name,
        "url": _build_media_url(object_name),
    }


def send_message(sender, recipient, message, encrypted_key, persist=True, attachment=None, message_type=None):
    if not user_repository.get_by_username(recipient):
        raise ValueError("Recipient not found")

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
        normalized_message_type = attachment.get("type", "file")
    else:
        normalized_message_type = "text"

    payload = message_repository.build_message_payload(
        sender,
        message,
        encrypted_key,
        attachment=attachment,
        message_type=normalized_message_type,
    )
    if persist:
        message_repository.push_message_payload(recipient, payload)

    return payload


def receive_messages(username):
    return message_repository.pop_messages(username)
