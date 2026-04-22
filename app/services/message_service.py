import os
import re
import tempfile
import time
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

ALLOWED_MESSAGE_TYPES = {"text", "image", "video", "voice", "mixed", "post"}

MAX_IMAGE_SIZE_BYTES = 10 * 1024 * 1024       # 10 MB
MAX_VIDEO_SIZE_BYTES = 50 * 1024 * 1024       # 50 MB
MAX_AUDIO_SIZE_BYTES = 16 * 1024 * 1024       # 16 MB
STREAM_COPY_CHUNK_SIZE = 1024 * 1024          # 1 MB
DEFAULT_STREAM_SPOOL_MAX_MEMORY_BYTES = 1024 * 1024  # 1 MB

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


def _rewind_stream(stream) -> bool:
    try:
        stream.seek(0)
        return True
    except Exception:
        return False


def _too_large_message(attachment_type: str, max_size: int) -> str:
    return (
        f"{attachment_type.capitalize()} too large. "
        f"Maximum allowed is {_format_mb(max_size)}."
    )


def _ensure_stream_has_known_length(stream, size_bytes: int, max_size: int, attachment_type: str):
    if size_bytes >= 0:
        return stream, size_bytes, False

    spool_max_memory = DEFAULT_STREAM_SPOOL_MAX_MEMORY_BYTES
    if has_request_context():
        configured_spool_max = current_app.config.get(
            "MESSAGE_ATTACHMENT_SPOOL_MAX_MEMORY_BYTES",
            DEFAULT_STREAM_SPOOL_MAX_MEMORY_BYTES,
        )
        try:
            spool_max_memory = max(64 * 1024, int(configured_spool_max))
        except (TypeError, ValueError):
            spool_max_memory = DEFAULT_STREAM_SPOOL_MAX_MEMORY_BYTES

    temp_stream = tempfile.SpooledTemporaryFile(mode="w+b", max_size=spool_max_memory)
    total_size = 0
    while True:
        chunk = stream.read(STREAM_COPY_CHUNK_SIZE)
        if not chunk:
            break
        total_size += len(chunk)
        if total_size > max_size:
            temp_stream.close()
            raise ValueError(_too_large_message(attachment_type, max_size))
        temp_stream.write(chunk)

    temp_stream.seek(0)
    return temp_stream, total_size, True


def _store_locally(stream, username: str, extension: str) -> str:
    filename = f"{uuid.uuid4()}.{extension}"
    relative_parts = ["uploads", "messages", username, filename]
    relative_path = os.path.join(*relative_parts)
    absolute_path = os.path.join(current_app.static_folder, relative_path)
    os.makedirs(os.path.dirname(absolute_path), exist_ok=True)

    if not _rewind_stream(stream):
        raise MessageAttachmentStorageError("Unable to rewind media stream for local storage")

    with open(absolute_path, "wb") as destination:
        while True:
            chunk = stream.read(STREAM_COPY_CHUNK_SIZE)
            if not chunk:
                break
            destination.write(chunk)
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


def _is_likely_timeout_error(error: Exception) -> bool:
    message = str(error).strip().lower()
    return "timed out" in message or "timeout" in message


def _log_attachment_upload(
    *,
    level: str,
    event: str,
    username: str,
    upload_scope: str,
    mimetype: str | None = None,
    size_bytes: int | None = None,
    duration_ms: int | None = None,
    reason: str | None = None,
):
    log_message = (
        "message_attachment_upload event=%s scope=%s user=%s mime=%s size_bytes=%s duration_ms=%s reason=%s"
    )
    logger = current_app.logger
    log_fn = getattr(logger, level, logger.info)
    log_fn(
        log_message,
        event,
        upload_scope,
        username,
        mimetype or "-",
        size_bytes if size_bytes is not None else -1,
        duration_ms if duration_ms is not None else -1,
        reason or "-",
    )


def upload_message_attachment(username: str, file_storage, upload_scope: str = "private"):
    started_at = time.perf_counter()

    def elapsed_ms() -> int:
        return int((time.perf_counter() - started_at) * 1000)

    if not user_repository.get_by_username(username):
        _log_attachment_upload(
            level="warning",
            event="rejected_user_not_found",
            username=username,
            upload_scope=upload_scope,
            duration_ms=elapsed_ms(),
        )
        raise ValueError("User not found")

    if file_storage is None or not getattr(file_storage, "filename", ""):
        _log_attachment_upload(
            level="warning",
            event="rejected_missing_file",
            username=username,
            upload_scope=upload_scope,
            duration_ms=elapsed_ms(),
        )
        raise ValueError("Attachment file is required")

    mimetype = (
        (getattr(file_storage, "mimetype", "") or "")
        .split(";", 1)[0]
        .strip()
        .lower()
    )
    if not mimetype:
        _log_attachment_upload(
            level="warning",
            event="rejected_missing_mime",
            username=username,
            upload_scope=upload_scope,
            duration_ms=elapsed_ms(),
        )
        raise ValueError("Attachment mime type is required")

    if mimetype not in ALL_ALLOWED_MIME_TYPES:
        _log_attachment_upload(
            level="warning",
            event="rejected_unsupported_mime",
            username=username,
            upload_scope=upload_scope,
            mimetype=mimetype,
            duration_ms=elapsed_ms(),
        )
        raise ValueError(f"Unsupported attachment type: {mimetype}")

    attachment_type = _attachment_type_from_mimetype(mimetype)
    max_size = _max_size_for_type(attachment_type)
    stream, size_bytes = _get_stream_and_length(file_storage)
    try:
        stream, size_bytes, close_stream_after = _ensure_stream_has_known_length(
            stream=stream,
            size_bytes=size_bytes,
            max_size=max_size,
            attachment_type=attachment_type,
        )
    except ValueError:
        _log_attachment_upload(
            level="warning",
            event="rejected_stream_too_large",
            username=username,
            upload_scope=upload_scope,
            mimetype=mimetype,
            size_bytes=size_bytes if size_bytes >= 0 else None,
            duration_ms=elapsed_ms(),
            reason=f"max_size_exceeded_{attachment_type}",
        )
        raise
    if size_bytes > max_size:
        _log_attachment_upload(
            level="warning",
            event="rejected_too_large",
            username=username,
            upload_scope=upload_scope,
            mimetype=mimetype,
            size_bytes=size_bytes,
            duration_ms=elapsed_ms(),
            reason=f"max_size_exceeded_{attachment_type}",
        )
        raise ValueError(_too_large_message(attachment_type, max_size))

    safe_filename = _sanitize_filename(getattr(file_storage, "filename", "file"))

    extension = _extension_for_mimetype(mimetype)
    bucket = current_app.config["MINIO_BUCKET"]
    object_name = None

    local_fallback_enabled = bool(current_app.config.get("MEDIA_LOCAL_FALLBACK_ENABLED", True))
    use_local_storage = False

    try:
        try:
            minio = get_minio_client()
            if not minio.bucket_exists(bucket):
                minio.make_bucket(bucket)
        except Exception as exc:
            _log_attachment_upload(
                level="warning",
                event="storage_unavailable",
                username=username,
                upload_scope=upload_scope,
                mimetype=mimetype,
                size_bytes=size_bytes,
                duration_ms=elapsed_ms(),
                reason=f"storage_client_init_failed timeout={_is_likely_timeout_error(exc)}",
            )
            if not local_fallback_enabled:
                raise MessageAttachmentStorageError("Media storage is unavailable") from exc
            use_local_storage = True

        if not use_local_storage:
            try:
                if not _rewind_stream(stream):
                    raise MessageAttachmentStorageError("Unable to rewind media stream for upload")
                object_name = f"messages/{username}/{uuid.uuid4()}.{extension}"
                minio.put_object(
                    bucket_name=bucket,
                    object_name=object_name,
                    data=stream,
                    length=size_bytes,
                    content_type=mimetype,
                )
            except Exception as exc:
                _log_attachment_upload(
                    level="warning",
                    event="storage_put_failed",
                    username=username,
                    upload_scope=upload_scope,
                    mimetype=mimetype,
                    size_bytes=size_bytes,
                    duration_ms=elapsed_ms(),
                    reason=f"minio_put_failed timeout={_is_likely_timeout_error(exc)}",
                )
                if not local_fallback_enabled:
                    raise MessageAttachmentStorageError("Media storage is unavailable") from exc
                use_local_storage = True

        if use_local_storage:
            try:
                object_name = _store_locally(stream, username, extension)
            except Exception as exc:
                _log_attachment_upload(
                    level="error",
                    event="local_fallback_failed",
                    username=username,
                    upload_scope=upload_scope,
                    mimetype=mimetype,
                    size_bytes=size_bytes,
                    duration_ms=elapsed_ms(),
                    reason=f"local_write_failed:{type(exc).__name__}",
                )
                raise MessageAttachmentStorageError("Media storage is unavailable") from exc
    finally:
        if close_stream_after:
            stream.close()

    _log_attachment_upload(
        level="info",
        event="success",
        username=username,
        upload_scope=upload_scope,
        mimetype=mimetype,
        size_bytes=size_bytes,
        duration_ms=elapsed_ms(),
    )

    return {
        "type": attachment_type,
        "mime_type": mimetype,
        "file_name": safe_filename,
        "size_bytes": max(size_bytes, 0),
        "object_name": object_name,
        "url": _build_media_url(object_name),
    }


def send_message_with_status(
    sender,
    recipient,
    message,
    encrypted_key,
    attachment=None,
    message_type=None,
    reply_to_message_id=None,
    reply_to_sender=None,
    encrypted_reply_preview=None,
    encrypted_reply_key=None,
    sender_encrypted_message=None,
    sender_encrypted_key=None,
    client_message_id=None,
):
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
        sender_encrypted_message=sender_encrypted_message,
        sender_encrypted_key=sender_encrypted_key,
        client_message_id=client_message_id,
    )
    canonical_payload, created = message_repository.push_message_payload(
        recipient,
        payload,
    )
    message_repository.store_private_message_metadata(canonical_payload, recipient)
    if created:
        message_repository.record_conversation_timestamp(
            sender,
            recipient,
            canonical_payload.get("timestamp"),
        )

    return canonical_payload, created


def send_message(
    sender,
    recipient,
    message,
    encrypted_key,
    attachment=None,
    message_type=None,
    reply_to_message_id=None,
    reply_to_sender=None,
    encrypted_reply_preview=None,
    encrypted_reply_key=None,
    sender_encrypted_message=None,
    sender_encrypted_key=None,
    client_message_id=None,
):
    payload, _created = send_message_with_status(
        sender=sender,
        recipient=recipient,
        message=message,
        encrypted_key=encrypted_key,
        attachment=attachment,
        message_type=message_type,
        reply_to_message_id=reply_to_message_id,
        reply_to_sender=reply_to_sender,
        encrypted_reply_preview=encrypted_reply_preview,
        encrypted_reply_key=encrypted_reply_key,
        sender_encrypted_message=sender_encrypted_message,
        sender_encrypted_key=sender_encrypted_key,
        client_message_id=client_message_id,
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


def purge_group_delivery_for_user(group_id, username):
    return message_repository.purge_group_delivery_for_user(
        group_id=group_id,
        username=username,
    )


def get_private_message_history(username, chat_id, limit=50, before_timestamp=None):
    return message_repository.get_private_message_history(
        username=username,
        chat_id=chat_id,
        limit=limit,
        before_timestamp=before_timestamp,
    )


def get_group_message_history(username, group_id, limit=50, before_timestamp=None):
    return message_repository.get_group_message_history(
        group_id=group_id,
        username=username,
        limit=limit,
        before_timestamp=before_timestamp,
    )


def ack_group_messages_with_payloads(username, group_id, message_ids):
    return message_repository.ack_group_messages_with_payloads(
        username, group_id, message_ids
    )


def ack_group_transient_messages(username, group_id, message_ids):
    return message_repository.ack_group_transient_messages(
        username, group_id, message_ids
    )


def ack_messages(username, message_ids):
    return message_repository.ack_messages(username, message_ids)


def ack_transient_messages(username, message_ids):
    return message_repository.ack_transient_messages(username, message_ids)


def ack_messages_with_payloads(username, message_ids):
    return message_repository.ack_messages_with_payloads(username, message_ids)


def classify_private_message_ids_for_chat(recipient, sender, message_ids):
    return message_repository.classify_private_message_ids_for_chat(
        recipient=recipient,
        sender=sender,
        message_ids=message_ids,
    )


def sync_private_chat_read_state(recipient, sender):
    return message_repository.sync_private_chat_read_state(
        recipient=recipient,
        sender=sender,
    )


def get_message_metadata(message_id):
    return message_repository.get_message_metadata(message_id)


def get_private_message_by_client_message_id(sender, recipient, client_message_id):
    return message_repository.get_private_message_by_client_message_id(
        sender=sender,
        recipient=recipient,
        client_message_id=client_message_id,
    )


def get_group_message_by_client_message_id(sender, group_id, client_message_id):
    return message_repository.get_group_message_by_client_message_id(
        sender=sender,
        group_id=group_id,
        client_message_id=client_message_id,
    )


def get_message_metadata_bulk(message_ids):
    return message_repository.get_message_metadata_bulk(message_ids)


def delete_message_metadata(message_id):
    return message_repository.delete_message_metadata(message_id)


def queue_message_deletion_event(username, event_name, payload):
    return message_repository.queue_message_deletion_event(username, event_name, payload)


def queue_message_deletion_events_batch(username, events):
    return message_repository.queue_message_deletion_events_batch(username, events)


def pop_message_deletion_events(username):
    return message_repository.pop_message_deletion_events(username)


def store_group_message_metadata(payload, group_id):
    return message_repository.store_group_message_metadata(payload, group_id)


def mark_private_message_seen(sender, recipient, message_id):
    return message_repository.mark_private_message_seen(sender, recipient, message_id)


def mark_private_messages_seen_batch(sender, recipient, message_ids):
    return message_repository.mark_private_messages_seen_batch(sender, recipient, message_ids)


def mark_private_message_delivered(sender, recipient, message_id):
    return message_repository.mark_private_message_delivered(sender, recipient, message_id)


def mark_private_messages_delivered_batch(sender, recipient, message_ids):
    return message_repository.mark_private_messages_delivered_batch(sender, recipient, message_ids)


def get_private_delivered_message_ids(sender, recipient, message_ids):
    return message_repository.get_private_delivered_message_ids(sender, recipient, message_ids)


def get_private_seen_message_ids(sender, recipient, message_ids):
    return message_repository.get_private_seen_message_ids(sender, recipient, message_ids)


def mark_group_message_seen(username, group_id, message_id):
    return message_repository.mark_group_message_seen(username, group_id, message_id)


def mark_group_messages_seen_with_payloads(username, group_id, message_ids):
    return message_repository.mark_group_messages_seen_with_payloads(
        username=username,
        group_id=group_id,
        message_ids=message_ids,
    )


def mark_group_messages_seen_batch(username, group_id, message_ids):
    return message_repository.mark_group_messages_seen_batch(username, group_id, message_ids)


def get_group_delivered_message_ids(group_id, message_ids, sender_username=None):
    return message_repository.get_group_delivered_message_ids(
        group_id=group_id,
        message_ids=message_ids,
        sender_username=sender_username,
    )


def get_group_seen_message_ids(group_id, message_ids, sender_username=None):
    return message_repository.get_group_seen_message_ids(
        group_id=group_id,
        message_ids=message_ids,
        sender_username=sender_username,
    )


def mark_private_message_deleted(username, chat_id, message_id):
    return message_repository.mark_private_message_deleted(username, chat_id, message_id)


def get_private_deleted_message_ids(username, chat_id, message_ids):
    return message_repository.get_private_deleted_message_ids(username, chat_id, message_ids)


def mark_group_message_deleted(username, group_id, message_id):
    return message_repository.mark_group_message_deleted(username, group_id, message_id)


def get_group_deleted_message_ids(username, group_id, message_ids):
    return message_repository.get_group_deleted_message_ids(username, group_id, message_ids)
