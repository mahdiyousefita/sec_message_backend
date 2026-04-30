import json
import uuid
from fnmatch import fnmatch
from datetime import datetime, timezone

from flask import has_app_context
from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError

from app.db import db
from app.extensions.redis_client import redis_client
from app.models.chat_message_model import (
    GroupMessageUserDelete,
    GroupMessage,
    GroupMessageKeyRecipient,
    GroupMessageRecipient,
    PrivateMessageUserDelete,
    PrivateMessage,
)
from app.models.group_model import GroupMember
from app.models.user_model import User

INBOX_TTL_SECONDS = 24 * 60 * 60
MESSAGE_META_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_DELETE_EVENT_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_DELIVERED_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_SEEN_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_DELETED_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_GROUP_KEY_REF_LENGTH = 128


def _db_available():
    return has_app_context()


def _utc_now_naive():
    return datetime.utcnow()


def _parse_iso_datetime(value):
    if not value:
        return _utc_now_naive()
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except (AttributeError, TypeError, ValueError):
        return _utc_now_naive()


def _parse_optional_iso_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except (AttributeError, TypeError, ValueError):
        return None


def _format_iso_datetime(value):
    if not isinstance(value, datetime):
        value = _utc_now_naive()
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond:06d}Z"


def _serialize_json(value):
    if value is None:
        return None
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return None


def _deserialize_json(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _normalize_encrypted_keys_map(encrypted_keys):
    if not isinstance(encrypted_keys, dict):
        return {}
    normalized = {}
    for username, encrypted_key in encrypted_keys.items():
        if not isinstance(username, str) or not username.strip():
            continue
        if not isinstance(encrypted_key, str) or not encrypted_key:
            continue
        normalized[username] = encrypted_key
    return normalized


def _normalize_group_key_ref(group_key_ref):
    if not isinstance(group_key_ref, str):
        return None
    normalized = group_key_ref.strip()
    if not normalized:
        return None
    if len(normalized) > MAX_GROUP_KEY_REF_LENGTH:
        return None
    return normalized


def normalize_group_key_ref(group_key_ref):
    return _normalize_group_key_ref(group_key_ref)


def _normalize_recipient_key_records(recipient_key_records):
    normalized = {}
    if isinstance(recipient_key_records, dict):
        return _normalize_encrypted_keys_map(recipient_key_records)

    if not isinstance(recipient_key_records, list):
        return normalized

    for record in recipient_key_records:
        if not isinstance(record, dict):
            continue
        recipient = (
            record.get("recipient")
            or record.get("username")
            or record.get("user")
        )
        encrypted_key = record.get("encrypted_key")
        if not isinstance(recipient, str) or not recipient.strip():
            continue
        if not isinstance(encrypted_key, str) or not encrypted_key:
            continue
        normalized[recipient.strip()] = encrypted_key
    return normalized


def normalize_recipient_key_records(recipient_key_records):
    return _normalize_recipient_key_records(recipient_key_records)


def normalize_encrypted_keys_map(encrypted_keys):
    return _normalize_encrypted_keys_map(encrypted_keys)


def _encrypted_keys_from_payload(payload):
    if not isinstance(payload, dict):
        return {}

    normalized = _normalize_recipient_key_records(
        payload.get("recipient_key_records"),
    )
    if normalized:
        return normalized
    return _normalize_encrypted_keys_map(payload.get("encrypted_keys"))


def _recipient_encrypted_key_view(encrypted_keys, recipient_username):
    keys_map = _normalize_encrypted_keys_map(encrypted_keys)
    if not recipient_username:
        return keys_map, None

    recipient_key = keys_map.get(recipient_username)
    if not recipient_key:
        return {}, None
    return {recipient_username: recipient_key}, recipient_key


def build_group_message_payload_for_recipient(payload, recipient_username):
    if not isinstance(payload, dict):
        return payload

    scoped_payload = dict(payload)
    scoped_keys, recipient_key = _recipient_encrypted_key_view(
        _encrypted_keys_from_payload(scoped_payload),
        recipient_username,
    )
    if recipient_key is None and isinstance(scoped_payload.get("encrypted_key"), str):
        direct_key = scoped_payload.get("encrypted_key")
        if direct_key:
            recipient_key = direct_key
            if recipient_username:
                scoped_keys = {recipient_username: recipient_key}

    scoped_payload["encrypted_keys"] = scoped_keys
    scoped_payload.pop("recipient_key_records", None)
    if recipient_key is None:
        scoped_payload.pop("encrypted_key", None)
    else:
        scoped_payload["encrypted_key"] = recipient_key
    return scoped_payload


def _private_message_to_payload(row):
    if row is None:
        return None
    return {
        "from": row.sender_username,
        "type": row.message_type or "text",
        "message": row.encrypted_message,
        "encrypted_key": row.encrypted_key,
        "attachment": _deserialize_json(row.attachment_json),
        "message_id": row.message_id,
        "client_message_id": row.client_message_id,
        "timestamp": _format_iso_datetime(row.timestamp),
        "reply_to_message_id": row.reply_to_message_id,
        "reply_to_sender": row.reply_to_sender,
        "encrypted_reply_preview": row.encrypted_reply_preview,
        "encrypted_reply_key": row.encrypted_reply_key,
    }


def _private_message_to_payload_for_viewer(row, viewer_username):
    payload = _private_message_to_payload(row)
    if (
        not isinstance(payload, dict)
        or not viewer_username
        or row is None
        or row.sender_username != viewer_username
    ):
        return payload

    if isinstance(row.sender_encrypted_message, str) and row.sender_encrypted_message:
        payload["message"] = row.sender_encrypted_message
    if isinstance(row.sender_encrypted_key, str) and row.sender_encrypted_key:
        payload["encrypted_key"] = row.sender_encrypted_key
    return payload


def _group_message_to_payload(
    row,
    recipient_username=None,
    recipient_encrypted_key=None,
):
    if row is None:
        return None
    encrypted_keys = _normalize_encrypted_keys_map(
        _deserialize_json(row.encrypted_keys_json)
    )
    if (
        recipient_username
        and not recipient_encrypted_key
        and row.sender_username == recipient_username
        and isinstance(row.sender_encrypted_key, str)
        and row.sender_encrypted_key
    ):
        recipient_encrypted_key = row.sender_encrypted_key
    if (
        recipient_username
        and not recipient_encrypted_key
        and row.group_key_ref
    ):
        recipient_encrypted_key = _fetch_group_key_record_map(
            group_id=row.group_id,
            sender=row.sender_username,
            group_key_ref=row.group_key_ref,
            recipient_usernames=[recipient_username],
        ).get(recipient_username)
    if (
        recipient_username
        and recipient_encrypted_key
        and not encrypted_keys
    ):
        encrypted_keys = {recipient_username: recipient_encrypted_key}

    payload = {
        "from": row.sender_username,
        "group_id": row.group_id,
        "group_key_ref": row.group_key_ref,
        "type": row.message_type or "text",
        "message": row.encrypted_message,
        "encrypted_keys": encrypted_keys,
        "attachment": _deserialize_json(row.attachment_json),
        "message_id": row.message_id,
        "client_message_id": row.client_message_id,
        "timestamp": _format_iso_datetime(row.timestamp),
        "reply_to_message_id": row.reply_to_message_id,
        "reply_to_sender": row.reply_to_sender,
        "encrypted_reply_preview": row.encrypted_reply_preview,
    }
    if recipient_encrypted_key:
        payload["encrypted_key"] = recipient_encrypted_key
    if recipient_username:
        return build_group_message_payload_for_recipient(payload, recipient_username)
    return payload


def _fetch_group_key_record_map(group_id, sender, group_key_ref, recipient_usernames=None):
    if not _db_available():
        return {}

    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return {}
    normalized_sender = sender.strip() if isinstance(sender, str) else ""
    normalized_key_ref = _normalize_group_key_ref(group_key_ref)
    if not normalized_group_id or not normalized_sender or not normalized_key_ref:
        return {}

    query = GroupMessageKeyRecipient.query.filter(
        GroupMessageKeyRecipient.group_id == normalized_group_id,
        GroupMessageKeyRecipient.sender_username == normalized_sender,
        GroupMessageKeyRecipient.group_key_ref == normalized_key_ref,
    )
    if recipient_usernames:
        normalized_usernames = [
            username.strip()
            for username in recipient_usernames
            if isinstance(username, str) and username.strip()
        ]
        if normalized_usernames:
            query = query.filter(
                GroupMessageKeyRecipient.recipient_username.in_(normalized_usernames)
            )

    rows = query.all()
    return {
        row.recipient_username: row.encrypted_key
        for row in rows
        if isinstance(row.encrypted_key, str) and row.encrypted_key
    }


def get_group_key_record_map(group_id, sender, group_key_ref, recipient_usernames=None):
    return _fetch_group_key_record_map(
        group_id=group_id,
        sender=sender,
        group_key_ref=group_key_ref,
        recipient_usernames=recipient_usernames,
    )


def store_group_key_records(group_id, sender, group_key_ref, recipient_keys):
    if not _db_available():
        return 0

    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return 0
    normalized_sender = sender.strip() if isinstance(sender, str) else ""
    normalized_key_ref = _normalize_group_key_ref(group_key_ref)
    normalized_keys = _normalize_encrypted_keys_map(recipient_keys)
    if (
        not normalized_group_id
        or not normalized_sender
        or not normalized_key_ref
        or not normalized_keys
    ):
        return 0

    existing_rows = (
        GroupMessageKeyRecipient.query.filter(
            GroupMessageKeyRecipient.group_id == normalized_group_id,
            GroupMessageKeyRecipient.sender_username == normalized_sender,
            GroupMessageKeyRecipient.group_key_ref == normalized_key_ref,
            GroupMessageKeyRecipient.recipient_username.in_(list(normalized_keys.keys())),
        )
        .all()
    )
    existing_by_recipient = {
        row.recipient_username: row
        for row in existing_rows
    }

    inserted = 0
    for recipient_username, encrypted_key in normalized_keys.items():
        row = existing_by_recipient.get(recipient_username)
        if row is None:
            db.session.add(
                GroupMessageKeyRecipient(
                    group_id=normalized_group_id,
                    sender_username=normalized_sender,
                    group_key_ref=normalized_key_ref,
                    recipient_username=recipient_username,
                    encrypted_key=encrypted_key,
                )
            )
            inserted += 1
            continue
        if row.encrypted_key != encrypted_key:
            row.encrypted_key = encrypted_key

    db.session.commit()
    return inserted


def _resolve_group_recipient_keys(payload, recipient_usernames=None):
    direct_keys = _encrypted_keys_from_payload(payload)
    if not isinstance(payload, dict):
        return direct_keys if direct_keys else {}

    if not recipient_usernames:
        if direct_keys:
            return direct_keys
        return _fetch_group_key_record_map(
            group_id=payload.get("group_id"),
            sender=payload.get("from"),
            group_key_ref=payload.get("group_key_ref"),
            recipient_usernames=None,
        )

    normalized_usernames = [
        username.strip()
        for username in recipient_usernames
        if isinstance(username, str) and username.strip()
    ]
    resolved = {
        username: direct_keys.get(username)
        for username in normalized_usernames
        if isinstance(direct_keys.get(username), str) and direct_keys.get(username)
    }
    missing_usernames = [
        username
        for username in normalized_usernames
        if username not in resolved
    ]
    if missing_usernames:
        fetched = _fetch_group_key_record_map(
            group_id=payload.get("group_id"),
            sender=payload.get("from"),
            group_key_ref=payload.get("group_key_ref"),
            recipient_usernames=missing_usernames,
        )
        for username, encrypted_key in fetched.items():
            if isinstance(encrypted_key, str) and encrypted_key:
                resolved[username] = encrypted_key
    return resolved


def _upsert_private_message(payload, recipient, *, auto_commit=True):
    if not _db_available():
        return

    message_id = (payload or {}).get("message_id")
    if not message_id or not recipient:
        return

    row = PrivateMessage.query.filter_by(message_id=message_id).first()
    if row is None:
        row = PrivateMessage(message_id=message_id)
        db.session.add(row)

    row.sender_username = (payload or {}).get("from")
    row.recipient_username = recipient
    row.client_message_id = (payload or {}).get("client_message_id")
    row.message_type = (payload or {}).get("type") or "text"
    row.encrypted_message = (payload or {}).get("message")
    row.encrypted_key = (payload or {}).get("encrypted_key")
    if "sender_encrypted_message" in (payload or {}):
        row.sender_encrypted_message = (payload or {}).get("sender_encrypted_message")
    if "sender_encrypted_key" in (payload or {}):
        row.sender_encrypted_key = (payload or {}).get("sender_encrypted_key")
    row.attachment_json = _serialize_json((payload or {}).get("attachment"))
    row.reply_to_message_id = (payload or {}).get("reply_to_message_id")
    row.reply_to_sender = (payload or {}).get("reply_to_sender")
    row.encrypted_reply_preview = (payload or {}).get("encrypted_reply_preview")
    row.encrypted_reply_key = (payload or {}).get("encrypted_reply_key")
    row.timestamp = _parse_iso_datetime((payload or {}).get("timestamp"))
    if auto_commit:
        db.session.commit()


def _upsert_group_message(payload, group_id, *, auto_commit=True):
    if not _db_available():
        return

    message_id = (payload or {}).get("message_id")
    if not message_id or not group_id:
        return

    row = GroupMessage.query.filter_by(message_id=message_id).first()
    if row is None:
        row = GroupMessage(message_id=message_id)
        db.session.add(row)

    row.group_id = int(group_id)
    row.sender_username = (payload or {}).get("from")
    row.client_message_id = (payload or {}).get("client_message_id")
    row.group_key_ref = _normalize_group_key_ref((payload or {}).get("group_key_ref"))
    row.message_type = (payload or {}).get("type") or "text"
    row.encrypted_message = (payload or {}).get("message")
    if isinstance((payload or {}).get("sender_encrypted_key"), str):
        row.sender_encrypted_key = (payload or {}).get("sender_encrypted_key")

    incoming_keys = _encrypted_keys_from_payload(payload)
    sender = (payload or {}).get("from")
    if (
        isinstance(sender, str)
        and sender
        and not row.sender_encrypted_key
        and isinstance(incoming_keys.get(sender), str)
    ):
        row.sender_encrypted_key = incoming_keys.get(sender)

    existing_keys = _normalize_encrypted_keys_map(
        _deserialize_json(row.encrypted_keys_json)
    )
    if incoming_keys:
        if row.group_key_ref:
            sender_key = incoming_keys.get(row.sender_username)
            if sender_key:
                existing_keys[row.sender_username] = sender_key
        else:
            existing_keys.update(incoming_keys)
    row.encrypted_keys_json = _serialize_json(existing_keys)
    row.attachment_json = _serialize_json((payload or {}).get("attachment"))
    row.reply_to_message_id = (payload or {}).get("reply_to_message_id")
    row.reply_to_sender = (payload or {}).get("reply_to_sender")
    row.encrypted_reply_preview = (payload or {}).get("encrypted_reply_preview")
    row.timestamp = _parse_iso_datetime((payload or {}).get("timestamp"))
    if auto_commit:
        db.session.commit()


def _find_private_row_by_client_message_id(sender, recipient, client_message_id):
    if not _db_available():
        return None
    if not sender or not recipient or not client_message_id:
        return None
    return (
        PrivateMessage.query.filter_by(
            sender_username=sender,
            recipient_username=recipient,
            client_message_id=client_message_id,
        )
        .order_by(PrivateMessage.id.asc())
        .first()
    )


def _find_group_row_by_client_message_id(sender, group_id, client_message_id):
    if not _db_available():
        return None
    if not sender or not group_id or not client_message_id:
        return None
    return (
        GroupMessage.query.filter_by(
            sender_username=sender,
            group_id=int(group_id),
            client_message_id=client_message_id,
        )
        .order_by(GroupMessage.id.asc())
        .first()
    )


def get_private_message_by_client_message_id(sender, recipient, client_message_id):
    row = _find_private_row_by_client_message_id(sender, recipient, client_message_id)
    if row is None:
        return None
    return _private_message_to_payload_for_viewer(row, sender)


def get_group_message_by_client_message_id(sender, group_id, client_message_id):
    row = _find_group_row_by_client_message_id(sender, group_id, client_message_id)
    if row is None:
        return None
    return _group_message_to_payload(row)

ACK_MESSAGES_LUA = """
local list_key = KEYS[1]
local order_key = KEYS[2]
local payload_key = KEYS[3]
local ids_key = KEYS[4]
local removed = {}

for i = 1, #ARGV do
    local message_id = ARGV[i]
    local raw = redis.call('HGET', payload_key, message_id)
    if raw then
        local zremoved = redis.call('ZREM', order_key, message_id)
        redis.call('SREM', ids_key, message_id)
        redis.call('HDEL', payload_key, message_id)
        redis.call('LREM', list_key, 1, raw)
        if zremoved > 0 then
            table.insert(removed, message_id)
            table.insert(removed, raw)
        end
    end
end

return removed
"""

def _inbox_key(username):
    return f"inbox:{username}"


def _inbox_index_order_key(username):
    return f"inbox_order:{username}"


def _inbox_index_payload_key(username):
    return f"inbox_payloads:{username}"


def _inbox_index_ids_key(username):
    return f"inbox_ids:{username}"


def _group_inbox_key(username, group_id):
    return f"group_user_inbox:{username}:{group_id}"


def _group_inbox_index_order_key(username, group_id):
    return f"group_inbox_order:{username}:{group_id}"


def _group_inbox_index_payload_key(username, group_id):
    return f"group_inbox_payloads:{username}:{group_id}"


def _group_inbox_index_ids_key(username, group_id):
    return f"group_inbox_ids:{username}:{group_id}"


def _chat_unread_count_key(username):
    return f"chat:unread_count:{username}"


def _chat_last_key(username, contact):
    return f"chat:last:{username}:{contact}"


def _decode_raw_message(raw):
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _timestamp_score(iso_timestamp):
    if iso_timestamp:
        try:
            return datetime.fromisoformat(
                iso_timestamp.replace("Z", "+00:00")
            ).timestamp()
        except (AttributeError, ValueError):
            pass
    return datetime.now(timezone.utc).timestamp()


def _refresh_index_ttls(order_key, payload_key, ids_key, ttl_seconds):
    pipe = redis_client.pipeline()
    pipe.expire(order_key, ttl_seconds)
    pipe.expire(payload_key, ttl_seconds)
    pipe.expire(ids_key, ttl_seconds)
    pipe.execute()


def _hydrate_inbox_index_if_needed(
    list_key,
    order_key,
    payload_key,
    ids_key,
    ttl_seconds,
):
    has_index = redis_client.zcard(order_key) > 0 or redis_client.hlen(payload_key) > 0
    if has_index:
        return

    if redis_client.llen(list_key) == 0:
        return

    raw_messages = redis_client.lrange(list_key, 0, -1)
    if not raw_messages:
        return

    pipe = redis_client.pipeline()
    indexed = 0
    for raw in raw_messages:
        message = _decode_raw_message(raw)
        if not message:
            continue
        message_id = message.get("message_id")
        if not message_id:
            continue
        score = _timestamp_score(message.get("timestamp"))
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        pipe.hset(payload_key, message_id, raw)
        pipe.sadd(ids_key, message_id)
        pipe.zadd(order_key, {message_id: score})
        indexed += 1

    if indexed > 0:
        pipe.expire(order_key, ttl_seconds)
        pipe.expire(payload_key, ttl_seconds)
        pipe.expire(ids_key, ttl_seconds)
    pipe.execute()


def _ordered_messages_from_index(
    *,
    list_key,
    order_key,
    payload_key,
    ids_key,
    ttl_seconds,
    start,
    end,
):
    _hydrate_inbox_index_if_needed(
        list_key=list_key,
        order_key=order_key,
        payload_key=payload_key,
        ids_key=ids_key,
        ttl_seconds=ttl_seconds,
    )

    message_ids = redis_client.zrange(order_key, start, end)
    if not message_ids:
        raw_messages = redis_client.lrange(list_key, start, end)
        return [
            message
            for message in (_decode_raw_message(raw) for raw in raw_messages)
            if message is not None
        ]

    raw_values = redis_client.hmget(payload_key, message_ids)
    decoded = []
    missing_ids = []
    for message_id, raw in zip(message_ids, raw_values):
        message = _decode_raw_message(raw)
        if message is None:
            missing_ids.append(message_id)
            continue
        decoded.append(message)

    if missing_ids:
        pipe = redis_client.pipeline()
        pipe.zrem(order_key, *missing_ids)
        pipe.srem(ids_key, *missing_ids)
        pipe.execute()

    return decoded


def _pending_count_from_index(
    *,
    list_key,
    order_key,
    payload_key,
    ids_key,
    ttl_seconds,
):
    _hydrate_inbox_index_if_needed(
        list_key=list_key,
        order_key=order_key,
        payload_key=payload_key,
        ids_key=ids_key,
        ttl_seconds=ttl_seconds,
    )
    count = redis_client.zcard(order_key)
    if count > 0:
        return count
    return redis_client.llen(list_key)


def _pop_all_messages(
    *,
    list_key,
    order_key,
    payload_key,
    ids_key,
    ttl_seconds,
):
    messages = _ordered_messages_from_index(
        list_key=list_key,
        order_key=order_key,
        payload_key=payload_key,
        ids_key=ids_key,
        ttl_seconds=ttl_seconds,
        start=0,
        end=-1,
    )

    pipe = redis_client.pipeline()
    pipe.delete(list_key)
    pipe.delete(order_key)
    pipe.delete(payload_key)
    pipe.delete(ids_key)
    pipe.execute()
    return messages


def _normalize_message_ids(message_ids):
    normalized_ids = []
    seen = set()
    for message_id in message_ids:
        if not isinstance(message_id, str) or not message_id:
            continue
        if message_id in seen:
            continue
        seen.add(message_id)
        normalized_ids.append(message_id)
    return normalized_ids


def _decode_redis_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _ack_messages_from_index_python(
    *,
    list_key,
    order_key,
    payload_key,
    ids_key,
    normalized_ids,
):
    raw_values = redis_client.hmget(payload_key, normalized_ids)
    pipe = redis_client.pipeline()
    indexed_ids = []
    for message_id, raw in zip(normalized_ids, raw_values):
        if raw is None:
            continue
        indexed_ids.append((message_id, raw))
        pipe.zrem(order_key, message_id)
        pipe.srem(ids_key, message_id)
        pipe.hdel(payload_key, message_id)
        pipe.lrem(list_key, 1, raw)

    if not indexed_ids:
        return 0, []

    results = pipe.execute()
    removed_payloads = []
    removed_total = 0
    result_index = 0
    for _message_id, raw in indexed_ids:
        removed = int(results[result_index] or 0)
        result_index += 4
        if removed > 0:
            removed_total += removed
            message = _decode_raw_message(raw)
            if message:
                removed_payloads.append(message)

    return removed_total, removed_payloads


def _ack_messages_from_index_lua(
    *,
    list_key,
    order_key,
    payload_key,
    ids_key,
    normalized_ids,
):
    eval_fn = getattr(redis_client, "eval", None)
    if eval_fn is None:
        return None

    try:
        raw_result = eval_fn(
            ACK_MESSAGES_LUA,
            4,
            list_key,
            order_key,
            payload_key,
            ids_key,
            *normalized_ids,
        )
    except Exception:
        return None

    if not raw_result:
        return 0, []

    removed_total = 0
    removed_payloads = []
    # Script returns flat pairs: [message_id_1, raw_1, message_id_2, raw_2, ...]
    for index in range(1, len(raw_result), 2):
        raw = _decode_redis_text(raw_result[index])
        message = _decode_raw_message(raw)
        if message:
            removed_payloads.append(message)
        removed_total += 1

    return removed_total, removed_payloads


def _ack_messages_from_index(
    *,
    list_key,
    order_key,
    payload_key,
    ids_key,
    ttl_seconds,
    message_ids,
):
    _hydrate_inbox_index_if_needed(
        list_key=list_key,
        order_key=order_key,
        payload_key=payload_key,
        ids_key=ids_key,
        ttl_seconds=ttl_seconds,
    )

    normalized_ids = _normalize_message_ids(message_ids)

    if not normalized_ids:
        return 0, []

    acked = _ack_messages_from_index_lua(
        list_key=list_key,
        order_key=order_key,
        payload_key=payload_key,
        ids_key=ids_key,
        normalized_ids=normalized_ids,
    )
    if acked is None:
        acked = _ack_messages_from_index_python(
            list_key=list_key,
            order_key=order_key,
            payload_key=payload_key,
            ids_key=ids_key,
            normalized_ids=normalized_ids,
        )
    removed_total, removed_payloads = acked

    remaining = redis_client.zcard(order_key)
    if remaining > 0:
        _refresh_index_ttls(order_key, payload_key, ids_key, ttl_seconds)
        redis_client.expire(list_key, ttl_seconds)
    else:
        pipe = redis_client.pipeline()
        pipe.delete(order_key)
        pipe.delete(payload_key)
        pipe.delete(ids_key)
        if redis_client.llen(list_key) == 0:
            pipe.delete(list_key)
        pipe.execute()

    return removed_total, removed_payloads


def _increment_unread_metadata(recipient, payload, pipe=None):
    sender = (payload or {}).get("from")
    message_id = (payload or {}).get("message_id")
    if not sender or not message_id:
        return

    unread_key = _chat_unread_count_key(recipient)
    last_key = _chat_last_key(recipient, sender)

    owns_pipeline = pipe is None
    if pipe is None:
        pipe = redis_client.pipeline()
    pipe.hincrby(unread_key, sender, 1)
    pipe.expire(unread_key, INBOX_TTL_SECONDS)
    pipe.hset(last_key, mapping={
        "sender": sender,
        "type": (payload or {}).get("type", "text"),
        "timestamp": (payload or {}).get("timestamp", ""),
        "message_id": message_id,
    })
    pipe.expire(last_key, INBOX_TTL_SECONDS)
    if owns_pipeline:
        pipe.execute()


def _decrement_unread_metadata(username, removed_payloads):
    if not removed_payloads:
        return

    per_sender = {}
    for payload in removed_payloads:
        sender = payload.get("from")
        if not sender:
            continue
        per_sender[sender] = per_sender.get(sender, 0) + 1

    if not per_sender:
        return

    unread_key = _chat_unread_count_key(username)
    pipe = redis_client.pipeline()
    for sender, amount in per_sender.items():
        next_count = redis_client.hincrby(unread_key, sender, -amount)
        if next_count <= 0:
            pipe.hdel(unread_key, sender)
            pipe.delete(_chat_last_key(username, sender))
    if redis_client.hlen(unread_key) > 0:
        pipe.expire(unread_key, INBOX_TTL_SECONDS)
    else:
        pipe.delete(unread_key)
    pipe.execute()


def add_contact(username, contact):
    redis_client.sadd(f"contacts:{username}", contact)


def get_contacts(username):
    return list(redis_client.smembers(f"contacts:{username}"))


def build_message_payload(sender, encrypted_message, encrypted_key, attachment=None, message_type="text",
                         reply_to_message_id=None, reply_to_sender=None,
                          encrypted_reply_preview=None,
                          encrypted_reply_key=None,
                          sender_encrypted_message=None,
                          sender_encrypted_key=None,
                          client_message_id=None):
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"
    return {
        "from": sender,
        "type": message_type,
        "message": encrypted_message,
        "encrypted_key": encrypted_key,
        "sender_encrypted_message": sender_encrypted_message,
        "sender_encrypted_key": sender_encrypted_key,
        "attachment": attachment,
        "message_id": str(uuid.uuid4()),
        "client_message_id": client_message_id,
        "timestamp": ts,
        "reply_to_message_id": reply_to_message_id,
        "reply_to_sender": reply_to_sender,
        "encrypted_reply_preview": encrypted_reply_preview,
        "encrypted_reply_key": encrypted_reply_key,
    }


def push_message_payload(recipient, payload):
    sender = (payload or {}).get("from")
    client_message_id = (payload or {}).get("client_message_id")

    if client_message_id:
        existing_payload = get_private_message_by_client_message_id(
            sender,
            recipient,
            client_message_id,
        )
        if existing_payload is not None:
            return existing_payload, False

    if _db_available():
        try:
            _upsert_private_message(payload, recipient)
        except IntegrityError:
            db.session.rollback()
            existing_payload = get_private_message_by_client_message_id(
                sender,
                recipient,
                client_message_id,
            )
            if existing_payload is not None:
                return existing_payload, False
            raise
    recipient_payload = dict(payload or {})
    recipient_payload.pop("sender_encrypted_message", None)
    recipient_payload.pop("sender_encrypted_key", None)

    key = _inbox_key(recipient)
    order_key = _inbox_index_order_key(recipient)
    payload_key = _inbox_index_payload_key(recipient)
    ids_key = _inbox_index_ids_key(recipient)

    data = json.dumps(recipient_payload)
    message_id = recipient_payload.get("message_id")
    score = _timestamp_score(recipient_payload.get("timestamp"))

    pipe = redis_client.pipeline()
    pipe.rpush(key, data)
    pipe.expire(key, INBOX_TTL_SECONDS)
    if message_id:
        pipe.hset(payload_key, message_id, data)
        pipe.sadd(ids_key, message_id)
        pipe.zadd(order_key, {message_id: score})
        pipe.expire(payload_key, INBOX_TTL_SECONDS)
        pipe.expire(ids_key, INBOX_TTL_SECONDS)
        pipe.expire(order_key, INBOX_TTL_SECONDS)
    _increment_unread_metadata(recipient, recipient_payload, pipe=pipe)
    pipe.execute()
    return payload, True


def _hydrate_private_pending_from_redis(username):
    if not _db_available():
        return

    existing_count = (
        PrivateMessage.query.filter_by(
            recipient_username=username,
            deleted_for_everyone=False,
        )
        .filter(PrivateMessage.delivered_at.is_(None))
        .count()
    )
    if existing_count > 0:
        return

    redis_pending = _ordered_messages_from_index(
        list_key=_inbox_key(username),
        order_key=_inbox_index_order_key(username),
        payload_key=_inbox_index_payload_key(username),
        ids_key=_inbox_index_ids_key(username),
        ttl_seconds=INBOX_TTL_SECONDS,
        start=0,
        end=-1,
    )
    if not redis_pending:
        return

    for payload in redis_pending:
        _upsert_private_message(payload, username, auto_commit=False)
    db.session.commit()


def _query_private_pending_rows(username, limit=None):
    if not _db_available():
        return []

    _hydrate_private_pending_from_redis(username)

    query = (
        PrivateMessage.query.filter_by(
            recipient_username=username,
            deleted_for_everyone=False,
        )
        .filter(PrivateMessage.delivered_at.is_(None))
        .order_by(PrivateMessage.timestamp.asc(), PrivateMessage.id.asc())
    )
    if limit is not None:
        query = query.limit(max(1, int(limit)))
    return query.all()


def pop_messages(username):
    if not _db_available():
        messages = _pop_all_messages(
            list_key=_inbox_key(username),
            order_key=_inbox_index_order_key(username),
            payload_key=_inbox_index_payload_key(username),
            ids_key=_inbox_index_ids_key(username),
            ttl_seconds=INBOX_TTL_SECONDS,
        )
        _decrement_unread_metadata(username, messages)
        return messages

    rows = _query_private_pending_rows(username)
    if not rows:
        return []

    delivered_at = _utc_now_naive()
    messages = [_private_message_to_payload(row) for row in rows]
    for row in rows:
        row.delivered_at = delivered_at
    db.session.commit()

    # Best-effort cleanup of transient queue state.
    removed_ids = [row.message_id for row in rows if row.message_id]
    if removed_ids:
        _ack_messages_from_index(
            list_key=_inbox_key(username),
            order_key=_inbox_index_order_key(username),
            payload_key=_inbox_index_payload_key(username),
            ids_key=_inbox_index_ids_key(username),
            ttl_seconds=INBOX_TTL_SECONDS,
            message_ids=removed_ids,
        )
    _decrement_unread_metadata(username, messages)
    return messages


def peek_messages(username):
    if not _db_available():
        return _ordered_messages_from_index(
            list_key=_inbox_key(username),
            order_key=_inbox_index_order_key(username),
            payload_key=_inbox_index_payload_key(username),
            ids_key=_inbox_index_ids_key(username),
            ttl_seconds=INBOX_TTL_SECONDS,
            start=0,
            end=-1,
        )

    return [_private_message_to_payload(row) for row in _query_private_pending_rows(username)]


def peek_messages_batch(username, limit=100):
    safe_limit = max(1, int(limit or 1))
    if not _db_available():
        return _ordered_messages_from_index(
            list_key=_inbox_key(username),
            order_key=_inbox_index_order_key(username),
            payload_key=_inbox_index_payload_key(username),
            ids_key=_inbox_index_ids_key(username),
            ttl_seconds=INBOX_TTL_SECONDS,
            start=0,
            end=safe_limit - 1,
        )

    rows = _query_private_pending_rows(username, limit=safe_limit)
    return [_private_message_to_payload(row) for row in rows]


def get_pending_count(username):
    if not _db_available():
        return _pending_count_from_index(
            list_key=_inbox_key(username),
            order_key=_inbox_index_order_key(username),
            payload_key=_inbox_index_payload_key(username),
            ids_key=_inbox_index_ids_key(username),
            ttl_seconds=INBOX_TTL_SECONDS,
        )

    _hydrate_private_pending_from_redis(username)

    return (
        PrivateMessage.query.filter_by(
            recipient_username=username,
            deleted_for_everyone=False,
        )
        .filter(PrivateMessage.delivered_at.is_(None))
        .count()
    )


def get_private_unread_summary(username):
    if not _db_available():
        return {}

    _hydrate_private_pending_from_redis(username)

    rows = (
        PrivateMessage.query.filter_by(
            recipient_username=username,
            deleted_for_everyone=False,
        )
        .filter(PrivateMessage.delivered_at.is_(None))
        .order_by(PrivateMessage.timestamp.asc(), PrivateMessage.id.asc())
        .all()
    )
    per_sender = {}
    for row in rows:
        sender = row.sender_username
        if not sender:
            continue
        sender_summary = per_sender.setdefault(
            sender,
            {
                "sender": sender,
                "count": 0,
                "last_type": "text",
                "last_timestamp": "",
                "message_id": "",
            },
        )
        sender_summary["count"] += 1
        sender_summary["last_type"] = row.message_type or "text"
        sender_summary["last_timestamp"] = _format_iso_datetime(row.timestamp)
        sender_summary["message_id"] = row.message_id or ""
    return per_sender


def get_private_message_history(username, chat_id, limit=50, before_timestamp=None):
    if not _db_available():
        return {
            "messages": [],
            "has_more": False,
            "next_before": None,
        }

    safe_limit = max(1, min(200, int(limit or 50)))
    before_dt = _parse_optional_iso_datetime(before_timestamp)

    user_deleted_subquery = (
        db.session.query(PrivateMessageUserDelete.id)
        .filter(
            PrivateMessageUserDelete.message_id == PrivateMessage.message_id,
            PrivateMessageUserDelete.username == username,
        )
        .exists()
    )

    query = (
        PrivateMessage.query.filter(
            PrivateMessage.deleted_for_everyone.is_(False),
            ~user_deleted_subquery,
            or_(
                and_(
                    PrivateMessage.sender_username == username,
                    PrivateMessage.recipient_username == chat_id,
                ),
                and_(
                    PrivateMessage.sender_username == chat_id,
                    PrivateMessage.recipient_username == username,
                ),
            ),
        )
        .order_by(PrivateMessage.timestamp.desc(), PrivateMessage.id.desc())
    )
    if before_dt is not None:
        query = query.filter(PrivateMessage.timestamp < before_dt)

    rows = query.limit(safe_limit + 1).all()
    has_more = len(rows) > safe_limit
    rows = rows[:safe_limit]
    rows.reverse()

    next_before = None
    if has_more and rows:
        next_before = _format_iso_datetime(rows[0].timestamp)

    return {
        "messages": [
            _private_message_to_payload_for_viewer(row, username)
            for row in rows
        ],
        "has_more": has_more,
        "next_before": next_before,
    }


def ack_messages(username, message_ids):
    removed, _ = ack_messages_with_payloads(username, message_ids)
    return removed


def ack_transient_messages(username, message_ids):
    if not message_ids:
        return 0
    removed, removed_payloads = _ack_messages_from_index(
        list_key=_inbox_key(username),
        order_key=_inbox_index_order_key(username),
        payload_key=_inbox_index_payload_key(username),
        ids_key=_inbox_index_ids_key(username),
        ttl_seconds=INBOX_TTL_SECONDS,
        message_ids=message_ids,
    )
    _decrement_unread_metadata(username, removed_payloads)
    return removed


def ack_messages_with_payloads(username, message_ids):
    if not message_ids:
        return 0, []

    if not _db_available():
        removed, removed_payloads = _ack_messages_from_index(
            list_key=_inbox_key(username),
            order_key=_inbox_index_order_key(username),
            payload_key=_inbox_index_payload_key(username),
            ids_key=_inbox_index_ids_key(username),
            ttl_seconds=INBOX_TTL_SECONDS,
            message_ids=message_ids,
        )
        _decrement_unread_metadata(username, removed_payloads)
        return removed, removed_payloads

    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return 0, []

    rows = (
        PrivateMessage.query.filter(
            PrivateMessage.recipient_username == username,
            PrivateMessage.message_id.in_(normalized_ids),
            PrivateMessage.deleted_for_everyone.is_(False),
            PrivateMessage.delivered_at.is_(None),
        )
        .all()
    )
    row_by_id = {
        row.message_id: row
        for row in rows
    }
    delivered_at = _utc_now_naive()
    removed_payloads = []
    removed = 0
    for message_id in normalized_ids:
        row = row_by_id.get(message_id)
        if row is None:
            continue
        row.delivered_at = delivered_at
        removed_payloads.append(_private_message_to_payload(row))
        removed += 1
    db.session.commit()

    # Best-effort transient queue cleanup.
    _ack_messages_from_index(
        list_key=_inbox_key(username),
        order_key=_inbox_index_order_key(username),
        payload_key=_inbox_index_payload_key(username),
        ids_key=_inbox_index_ids_key(username),
        ttl_seconds=INBOX_TTL_SECONDS,
        message_ids=normalized_ids,
    )
    _decrement_unread_metadata(username, removed_payloads)
    return removed, removed_payloads


def sync_private_chat_read_state(recipient, sender):
    if not recipient or not sender:
        return {
            "message_ids": [],
            "marked_delivered": 0,
            "marked_seen": 0,
        }

    if not _db_available():
        return {
            "message_ids": [],
            "marked_delivered": 0,
            "marked_seen": 0,
        }

    rows = (
        PrivateMessage.query.filter(
            PrivateMessage.sender_username == sender,
            PrivateMessage.recipient_username == recipient,
            PrivateMessage.deleted_for_everyone.is_(False),
            or_(
                PrivateMessage.delivered_at.is_(None),
                PrivateMessage.seen_at.is_(None),
            ),
        )
        .order_by(PrivateMessage.timestamp.asc(), PrivateMessage.id.asc())
        .all()
    )
    if not rows:
        return {
            "message_ids": [],
            "marked_delivered": 0,
            "marked_seen": 0,
        }

    now = _utc_now_naive()
    message_ids = []
    unread_payloads = []
    marked_delivered = 0
    marked_seen = 0
    for row in rows:
        if not row.message_id:
            continue
        message_ids.append(row.message_id)
        if row.delivered_at is None:
            unread_payloads.append(_private_message_to_payload(row))
            row.delivered_at = now
            marked_delivered += 1
        if row.seen_at is None:
            row.seen_at = now
            marked_seen += 1

    if not message_ids:
        return {
            "message_ids": [],
            "marked_delivered": 0,
            "marked_seen": 0,
        }

    db.session.commit()

    _ack_messages_from_index(
        list_key=_inbox_key(recipient),
        order_key=_inbox_index_order_key(recipient),
        payload_key=_inbox_index_payload_key(recipient),
        ids_key=_inbox_index_ids_key(recipient),
        ttl_seconds=INBOX_TTL_SECONDS,
        message_ids=message_ids,
    )
    _decrement_unread_metadata(recipient, unread_payloads)

    return {
        "message_ids": message_ids,
        "marked_delivered": marked_delivered,
        "marked_seen": marked_seen,
    }


def classify_private_message_ids_for_chat(recipient, sender, message_ids):
    if not recipient or not sender or not message_ids:
        return {
            "scoped_ids": [],
            "unknown_ids": [],
            "wrong_chat_ids": [],
            "db_verified": _db_available(),
        }

    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return {
            "scoped_ids": [],
            "unknown_ids": [],
            "wrong_chat_ids": [],
            "db_verified": _db_available(),
        }

    if not _db_available():
        return {
            "scoped_ids": [],
            "unknown_ids": normalized_ids,
            "wrong_chat_ids": [],
            "db_verified": False,
        }

    rows = (
        PrivateMessage.query.with_entities(
            PrivateMessage.message_id,
            PrivateMessage.sender_username,
            PrivateMessage.recipient_username,
        )
        .filter(
            PrivateMessage.message_id.in_(normalized_ids),
            PrivateMessage.deleted_for_everyone.is_(False),
        )
        .all()
    )
    rows_by_id = {row.message_id: row for row in rows}

    scoped_ids = []
    unknown_ids = []
    wrong_chat_ids = []
    for message_id in normalized_ids:
        row = rows_by_id.get(message_id)
        if row is None:
            unknown_ids.append(message_id)
            continue
        if row.recipient_username != recipient:
            # Do not leak cross-user message existence; treat as unknown.
            unknown_ids.append(message_id)
            continue
        if row.sender_username != sender:
            wrong_chat_ids.append(message_id)
            continue
        scoped_ids.append(message_id)

    return {
        "scoped_ids": scoped_ids,
        "unknown_ids": unknown_ids,
        "wrong_chat_ids": wrong_chat_ids,
        "db_verified": True,
    }


def store_private_message_metadata(payload, recipient):
    message_id = (payload or {}).get("message_id")
    if not message_id:
        return

    meta = {
        "type": "private",
        "message_id": message_id,
        "sender": payload.get("from"),
        "recipient": recipient,
        "timestamp": payload.get("timestamp"),
    }
    key = f"message_meta:{message_id}"
    redis_client.setex(key, MESSAGE_META_TTL_SECONDS, json.dumps(meta))


def get_message_metadata(message_id):
    if not message_id:
        return None
    raw = redis_client.get(f"message_meta:{message_id}")
    if raw:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except (TypeError, ValueError):
            pass

    if _db_available():
        private_row = PrivateMessage.query.filter_by(message_id=message_id).first()
        if private_row is not None:
            return {
                "type": "private",
                "message_id": private_row.message_id,
                "sender": private_row.sender_username,
                "recipient": private_row.recipient_username,
                "timestamp": _format_iso_datetime(private_row.timestamp),
            }

        group_row = GroupMessage.query.filter_by(message_id=message_id).first()
        if group_row is not None:
            return {
                "type": "group",
                "message_id": group_row.message_id,
                "sender": group_row.sender_username,
                "group_id": int(group_row.group_id),
                "timestamp": _format_iso_datetime(group_row.timestamp),
            }

    return None


def get_message_metadata_bulk(message_ids):
    if not message_ids:
        return {}

    normalized_ids = [
        message_id for message_id in message_ids
        if isinstance(message_id, str) and message_id
    ]
    if not normalized_ids:
        return {}

    metadata_by_id = {}
    pipe = redis_client.pipeline()
    for message_id in normalized_ids:
        pipe.get(f"message_meta:{message_id}")

    raw_values = pipe.execute()
    unresolved_ids = []
    for message_id, raw in zip(normalized_ids, raw_values):
        if not raw:
            unresolved_ids.append(message_id)
            continue
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            unresolved_ids.append(message_id)
            continue
        if isinstance(parsed, dict):
            metadata_by_id[message_id] = parsed
        else:
            unresolved_ids.append(message_id)

    if unresolved_ids and _db_available():
        private_rows = (
            PrivateMessage.query.filter(PrivateMessage.message_id.in_(unresolved_ids))
            .all()
        )
        for row in private_rows:
            metadata_by_id[row.message_id] = {
                "type": "private",
                "message_id": row.message_id,
                "sender": row.sender_username,
                "recipient": row.recipient_username,
                "timestamp": _format_iso_datetime(row.timestamp),
            }

        unresolved_ids = [
            message_id
            for message_id in unresolved_ids
            if message_id not in metadata_by_id
        ]
    if unresolved_ids and _db_available():
        group_rows = (
            GroupMessage.query.filter(GroupMessage.message_id.in_(unresolved_ids))
            .all()
        )
        for row in group_rows:
            metadata_by_id[row.message_id] = {
                "type": "group",
                "message_id": row.message_id,
                "sender": row.sender_username,
                "group_id": int(row.group_id),
                "timestamp": _format_iso_datetime(row.timestamp),
            }

    return metadata_by_id


def delete_message_metadata(message_id):
    if not message_id:
        return
    redis_client.delete(f"message_meta:{message_id}")


def mark_private_message_seen(sender, recipient, message_id):
    mark_private_messages_seen_batch(sender, recipient, [message_id])


def mark_private_messages_seen_batch(sender, recipient, message_ids):
    if not sender or not recipient or not message_ids:
        return
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return

    if _db_available():
        now = _utc_now_naive()
        rows = (
            PrivateMessage.query.filter(
                PrivateMessage.sender_username == sender,
                PrivateMessage.recipient_username == recipient,
                PrivateMessage.message_id.in_(normalized_ids),
                PrivateMessage.deleted_for_everyone.is_(False),
            )
            .all()
        )
        for row in rows:
            row.seen_at = now
            if row.delivered_at is None:
                row.delivered_at = now
        db.session.commit()

    key = f"private_seen:{sender}:{recipient}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, *normalized_ids)
    pipe.expire(key, MESSAGE_SEEN_TTL_SECONDS)
    pipe.execute()


def _get_set_membership_statuses(key, message_ids):
    if not key or not message_ids:
        return [], []
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return [], []

    try:
        raw_statuses = redis_client.execute_command("SMISMEMBER", key, *normalized_ids)
        return normalized_ids, [bool(status) for status in raw_statuses]
    except Exception:
        pipe = redis_client.pipeline()
        for message_id in normalized_ids:
            pipe.sismember(key, message_id)
        raw_statuses = pipe.execute()
        return normalized_ids, [bool(status) for status in raw_statuses]


def mark_private_message_delivered(sender, recipient, message_id):
    mark_private_messages_delivered_batch(sender, recipient, [message_id])


def mark_private_messages_delivered_batch(sender, recipient, message_ids):
    if not sender or not recipient or not message_ids:
        return
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return

    if _db_available():
        now = _utc_now_naive()
        rows = (
            PrivateMessage.query.filter(
                PrivateMessage.sender_username == sender,
                PrivateMessage.recipient_username == recipient,
                PrivateMessage.message_id.in_(normalized_ids),
                PrivateMessage.deleted_for_everyone.is_(False),
                PrivateMessage.delivered_at.is_(None),
            )
            .all()
        )
        for row in rows:
            row.delivered_at = now
        db.session.commit()

    key = f"private_delivered:{sender}:{recipient}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, *normalized_ids)
    pipe.expire(key, MESSAGE_DELIVERED_TTL_SECONDS)
    pipe.execute()


def get_private_delivered_message_ids(sender, recipient, message_ids):
    if not sender or not recipient or not message_ids:
        return []
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return []

    if not _db_available():
        key = f"private_delivered:{sender}:{recipient}"
        normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
        return [
            message_id
            for message_id, status in zip(normalized_ids, statuses)
            if status
        ]

    rows = (
        PrivateMessage.query.with_entities(PrivateMessage.message_id)
        .filter(
            PrivateMessage.sender_username == sender,
            PrivateMessage.recipient_username == recipient,
            PrivateMessage.message_id.in_(normalized_ids),
            PrivateMessage.delivered_at.isnot(None),
            PrivateMessage.deleted_for_everyone.is_(False),
        )
        .all()
    )
    delivered_set = {row[0] for row in rows}
    return [message_id for message_id in normalized_ids if message_id in delivered_set]


def get_private_seen_message_ids(sender, recipient, message_ids):
    if not sender or not recipient or not message_ids:
        return []
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return []

    if not _db_available():
        key = f"private_seen:{sender}:{recipient}"
        normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
        return [
            message_id
            for message_id, status in zip(normalized_ids, statuses)
            if status
        ]

    rows = (
        PrivateMessage.query.with_entities(PrivateMessage.message_id)
        .filter(
            PrivateMessage.sender_username == sender,
            PrivateMessage.recipient_username == recipient,
            PrivateMessage.message_id.in_(normalized_ids),
            PrivateMessage.seen_at.isnot(None),
            PrivateMessage.deleted_for_everyone.is_(False),
        )
        .all()
    )
    seen_set = {row[0] for row in rows}
    return [message_id for message_id in normalized_ids if message_id in seen_set]


def mark_group_message_seen(username, group_id, message_id):
    mark_group_messages_seen_batch(username, group_id, [message_id])


def mark_group_messages_seen_with_payloads(username, group_id, message_ids):
    if not username or not group_id or not message_ids:
        return 0, []

    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return 0, []

    if not _db_available():
        key = f"group_seen:{group_id}:{username}"
        pipe = redis_client.pipeline()
        pipe.sadd(key, *normalized_ids)
        pipe.expire(key, MESSAGE_SEEN_TTL_SECONDS)
        pipe.sadd(f"group_seen:{group_id}", *normalized_ids)
        pipe.expire(f"group_seen:{group_id}", MESSAGE_SEEN_TTL_SECONDS)
        pipe.execute()
        return 0, []

    now = _utc_now_naive()
    rows = (
        GroupMessageRecipient.query.join(
            GroupMessage,
            GroupMessage.message_id == GroupMessageRecipient.message_id,
        )
        .filter(
            GroupMessageRecipient.recipient_username == username,
            GroupMessageRecipient.group_id == int(group_id),
            GroupMessageRecipient.message_id.in_(normalized_ids),
            GroupMessage.deleted_for_everyone.is_(False),
            GroupMessageRecipient.seen_at.is_(None),
        )
        .all()
    )

    newly_seen_ids = []
    delivered_now_ids = []
    for row in rows:
        row.seen_at = now
        if row.delivered_at is None:
            row.delivered_at = now
            delivered_now_ids.append(row.message_id)
        newly_seen_ids.append(row.message_id)

    if not newly_seen_ids:
        return 0, []

    GroupMessage.query.filter(
        GroupMessage.group_id == int(group_id),
        GroupMessage.message_id.in_(newly_seen_ids),
        GroupMessage.seen_at.is_(None),
        GroupMessage.deleted_for_everyone.is_(False),
    ).update(
        {"seen_at": now},
        synchronize_session=False,
    )
    db.session.commit()

    message_rows = (
        db.session.query(
            GroupMessage,
            GroupMessageRecipient.encrypted_key.label("recipient_encrypted_key"),
        )
        .outerjoin(
            GroupMessageRecipient,
            and_(
                GroupMessageRecipient.message_id == GroupMessage.message_id,
                GroupMessageRecipient.recipient_username == username,
            ),
        )
        .filter(
            GroupMessage.group_id == int(group_id),
            GroupMessage.message_id.in_(newly_seen_ids),
            GroupMessage.deleted_for_everyone.is_(False),
        )
        .all()
    )
    payload_by_id = {
        message_row.message_id: _group_message_to_payload(
            message_row,
            recipient_username=username,
            recipient_encrypted_key=recipient_encrypted_key,
        )
        for message_row, recipient_encrypted_key in message_rows
    }
    delivered_now_set = set(delivered_now_ids)
    seen_payloads = []
    for message_id in normalized_ids:
        payload = payload_by_id.get(message_id)
        if payload is None:
            continue
        payload["delivered_now"] = message_id in delivered_now_set
        seen_payloads.append(payload)

    key = f"group_seen:{group_id}:{username}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, *newly_seen_ids)
    pipe.expire(key, MESSAGE_SEEN_TTL_SECONDS)
    pipe.sadd(f"group_seen:{group_id}", *newly_seen_ids)
    pipe.expire(f"group_seen:{group_id}", MESSAGE_SEEN_TTL_SECONDS)
    pipe.execute()

    return len(seen_payloads), seen_payloads


def mark_group_messages_seen_batch(username, group_id, message_ids):
    marked, _ = mark_group_messages_seen_with_payloads(username, group_id, message_ids)
    return marked


def get_group_delivered_message_ids(group_id, message_ids, sender_username=None):
    if not group_id or not message_ids:
        return []
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return []

    if not _db_available():
        key = (
            f"group_delivered:{group_id}:{sender_username}"
            if sender_username
            else f"group_delivered:{group_id}"
        )
        normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
        return [
            message_id
            for message_id, status in zip(normalized_ids, statuses)
            if status
        ]

    query = (
        db.session.query(GroupMessageRecipient.message_id)
        .join(
            GroupMessage,
            GroupMessage.message_id == GroupMessageRecipient.message_id,
        )
        .filter(
            GroupMessage.group_id == int(group_id),
            GroupMessage.message_id.in_(normalized_ids),
            GroupMessageRecipient.delivered_at.isnot(None),
            GroupMessage.deleted_for_everyone.is_(False),
        )
        .distinct()
    )
    if sender_username:
        query = query.filter(GroupMessage.sender_username == sender_username)

    rows = query.all()
    delivered_set = {row[0] for row in rows}
    return [message_id for message_id in normalized_ids if message_id in delivered_set]


def get_group_seen_message_ids(group_id, message_ids, sender_username=None):
    if not group_id or not message_ids:
        return []
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return []
    if not _db_available():
        key = (
            f"group_seen:{group_id}:{sender_username}"
            if sender_username
            else f"group_seen:{group_id}"
        )
        normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
        return [
            message_id
            for message_id, status in zip(normalized_ids, statuses)
            if status
        ]

    query = (
        db.session.query(GroupMessageRecipient.message_id)
        .join(
            GroupMessage,
            GroupMessage.message_id == GroupMessageRecipient.message_id,
        )
        .filter(
            GroupMessage.group_id == int(group_id),
            GroupMessage.message_id.in_(normalized_ids),
            GroupMessageRecipient.seen_at.isnot(None),
            GroupMessage.deleted_for_everyone.is_(False),
        )
        .distinct()
    )
    if sender_username:
        query = query.filter(GroupMessage.sender_username == sender_username)

    rows = query.all()
    seen_set = {row[0] for row in rows}
    return [message_id for message_id in normalized_ids if message_id in seen_set]


def mark_private_message_deleted(username, chat_id, message_id):
    if not username or not chat_id or not message_id:
        return

    if _db_available():
        row = (
            PrivateMessage.query.filter(
                PrivateMessage.message_id == message_id,
                or_(
                    and_(
                        PrivateMessage.sender_username == chat_id,
                        PrivateMessage.recipient_username == username,
                    ),
                    and_(
                        PrivateMessage.sender_username == username,
                        PrivateMessage.recipient_username == chat_id,
                    ),
                ),
            )
            .first()
        )
        if row is not None:
            row.deleted_for_everyone = True
            row.deleted_at = _utc_now_naive()
            db.session.commit()

    key = f"private_deleted:{username}:{chat_id}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, message_id)
    pipe.expire(key, MESSAGE_DELETED_TTL_SECONDS)
    pipe.execute()


def mark_private_message_deleted_for_user(username, chat_id, message_id):
    if not username or not chat_id or not message_id:
        return False

    deleted = False
    if _db_available():
        row = (
            PrivateMessage.query.filter(
                PrivateMessage.message_id == message_id,
                or_(
                    and_(
                        PrivateMessage.sender_username == chat_id,
                        PrivateMessage.recipient_username == username,
                    ),
                    and_(
                        PrivateMessage.sender_username == username,
                        PrivateMessage.recipient_username == chat_id,
                    ),
                ),
            )
            .first()
        )
        if row is None:
            return False

        existing = (
            PrivateMessageUserDelete.query.filter_by(
                message_id=message_id,
                username=username,
            )
            .first()
        )
        if existing is None:
            db.session.add(
                PrivateMessageUserDelete(
                    message_id=message_id,
                    username=username,
                    deleted_at=_utc_now_naive(),
                )
            )
            db.session.commit()
        deleted = True

    key = f"private_deleted:{username}:{chat_id}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, message_id)
    pipe.expire(key, MESSAGE_DELETED_TTL_SECONDS)
    pipe.execute()
    return deleted or not _db_available()


def get_private_deleted_message_ids(username, chat_id, message_ids):
    if not username or not chat_id or not message_ids:
        return []
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return []

    if not _db_available():
        key = f"private_deleted:{username}:{chat_id}"
        normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
        return [
            message_id
            for message_id, status in zip(normalized_ids, statuses)
            if status
        ]

    user_deleted_subquery = (
        db.session.query(PrivateMessageUserDelete.id)
        .filter(
            PrivateMessageUserDelete.message_id == PrivateMessage.message_id,
            PrivateMessageUserDelete.username == username,
        )
        .exists()
    )

    rows = (
        PrivateMessage.query.with_entities(PrivateMessage.message_id)
        .filter(
            PrivateMessage.message_id.in_(normalized_ids),
            or_(
                PrivateMessage.deleted_for_everyone.is_(True),
                user_deleted_subquery,
            ),
            or_(
                and_(
                    PrivateMessage.sender_username == username,
                    PrivateMessage.recipient_username == chat_id,
                ),
                and_(
                    PrivateMessage.sender_username == chat_id,
                    PrivateMessage.recipient_username == username,
                ),
            ),
        )
        .all()
    )
    deleted_set = {row[0] for row in rows}
    return [message_id for message_id in normalized_ids if message_id in deleted_set]


def mark_group_message_deleted(username, group_id, message_id):
    if not username or not group_id or not message_id:
        return

    if _db_available():
        row = (
            GroupMessage.query.filter_by(
                group_id=int(group_id),
                message_id=message_id,
            )
            .first()
        )
        if row is not None:
            row.deleted_for_everyone = True
            row.deleted_at = _utc_now_naive()
            db.session.commit()

    key = f"group_deleted:{username}:{group_id}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, message_id)
    pipe.expire(key, MESSAGE_DELETED_TTL_SECONDS)
    pipe.execute()


def mark_group_message_deleted_for_user(username, group_id, message_id):
    if not username or not group_id or not message_id:
        return False

    deleted = False
    normalized_group_id = int(group_id)
    if _db_available():
        row = (
            GroupMessage.query.filter_by(
                group_id=normalized_group_id,
                message_id=message_id,
            )
            .first()
        )
        if row is None:
            return False

        existing = (
            GroupMessageUserDelete.query.filter_by(
                message_id=message_id,
                username=username,
            )
            .first()
        )
        if existing is None:
            db.session.add(
                GroupMessageUserDelete(
                    message_id=message_id,
                    group_id=normalized_group_id,
                    username=username,
                    deleted_at=_utc_now_naive(),
                )
            )
            db.session.commit()
        deleted = True

    key = f"group_deleted:{username}:{group_id}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, message_id)
    pipe.expire(key, MESSAGE_DELETED_TTL_SECONDS)
    pipe.execute()
    return deleted or not _db_available()


def get_group_deleted_message_ids(username, group_id, message_ids):
    if not username or not group_id or not message_ids:
        return []
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return []

    if not _db_available():
        key = f"group_deleted:{username}:{group_id}"
        normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
        return [
            message_id
            for message_id, status in zip(normalized_ids, statuses)
            if status
        ]

    user_deleted_subquery = (
        db.session.query(GroupMessageUserDelete.id)
        .filter(
            GroupMessageUserDelete.message_id == GroupMessage.message_id,
            GroupMessageUserDelete.username == username,
            GroupMessageUserDelete.group_id == int(group_id),
        )
        .exists()
    )

    rows = (
        GroupMessage.query.with_entities(GroupMessage.message_id)
        .filter(
            GroupMessage.group_id == int(group_id),
            GroupMessage.message_id.in_(normalized_ids),
            or_(
                GroupMessage.deleted_for_everyone.is_(True),
                user_deleted_subquery,
            ),
        )
        .all()
    )
    deleted_set = {row[0] for row in rows}
    return [message_id for message_id in normalized_ids if message_id in deleted_set]


def queue_message_deletion_event(username, event_name, payload):
    if not username or not event_name or not isinstance(payload, dict):
        return
    key = f"message_delete_events:{username}"
    event = json.dumps({"event": event_name, "payload": payload})
    pipe = redis_client.pipeline()
    pipe.rpush(key, event)
    pipe.expire(key, MESSAGE_DELETE_EVENT_TTL_SECONDS)
    pipe.execute()


def queue_message_deletion_events_batch(username, events):
    if not username or not events:
        return
    key = f"message_delete_events:{username}"
    serialized = []
    for event in events:
        if not isinstance(event, dict):
            continue
        event_name = event.get("event")
        payload = event.get("payload")
        if not event_name or not isinstance(payload, dict):
            continue
        serialized.append(json.dumps({"event": event_name, "payload": payload}))
    if not serialized:
        return

    pipe = redis_client.pipeline()
    pipe.rpush(key, *serialized)
    pipe.expire(key, MESSAGE_DELETE_EVENT_TTL_SECONDS)
    pipe.execute()


def pop_message_deletion_events(username):
    if not username:
        return []
    key = f"message_delete_events:{username}"
    raw_events = redis_client.lrange(key, 0, -1)
    redis_client.delete(key)

    events = []
    for raw in raw_events:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if isinstance(parsed, dict):
            events.append(parsed)
    return events

def record_conversation_timestamp(user_a, user_b, iso_timestamp=None):
    if iso_timestamp is None:
        ts = datetime.now(timezone.utc).timestamp()
    else:
        try:
            dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
            ts = dt.timestamp()
        except (ValueError, AttributeError):
            ts = datetime.now(timezone.utc).timestamp()
    redis_client.zadd(f"contact_ts:{user_a}", {user_b: ts})
    redis_client.zadd(f"contact_ts:{user_b}", {user_a: ts})


def _normalize_contact_usernames(contacts):
    if not isinstance(contacts, (list, tuple, set)):
        return []
    normalized = []
    seen = set()
    for raw_contact in contacts:
        contact = _decode_redis_text(raw_contact)
        if not isinstance(contact, str):
            continue
        contact = contact.strip()
        if not contact or contact in seen:
            continue
        seen.add(contact)
        normalized.append(contact)
    return normalized


def record_conversation_timestamps_batch(username, contact_timestamps):
    if not isinstance(username, str) or not username.strip():
        return 0
    if not isinstance(contact_timestamps, dict) or not contact_timestamps:
        return 0

    owner = username.strip()
    updates = []
    for raw_contact, raw_iso_timestamp in contact_timestamps.items():
        contact = _decode_redis_text(raw_contact)
        if not isinstance(contact, str):
            continue
        contact = contact.strip()
        if not contact or contact == owner:
            continue
        if raw_iso_timestamp is None:
            ts = datetime.now(timezone.utc).timestamp()
        else:
            try:
                dt = datetime.fromisoformat(
                    str(raw_iso_timestamp).replace("Z", "+00:00")
                )
                ts = dt.timestamp()
            except (TypeError, ValueError):
                ts = datetime.now(timezone.utc).timestamp()
        updates.append((contact, ts))

    if not updates:
        return 0

    pipe = redis_client.pipeline()
    owner_key = f"contact_ts:{owner}"
    for contact, ts in updates:
        pipe.zadd(owner_key, {contact: ts})
        pipe.zadd(f"contact_ts:{contact}", {owner: ts})
    pipe.execute()
    return len(updates)


def get_contacts_sorted_by_last_message(username, offset=0, count=20):
    return redis_client.zrevrange(
        f"contact_ts:{username}", offset, offset + count - 1, withscores=True
    )


def get_contact_timestamp_score(username, contact):
    return redis_client.zscore(f"contact_ts:{username}", contact)


def get_contact_timestamp_scores(username, contacts):
    if not isinstance(username, str) or not username.strip():
        return {}
    normalized_contacts = _normalize_contact_usernames(contacts)
    if not normalized_contacts:
        return {}

    key = f"contact_ts:{username.strip()}"
    pipe = redis_client.pipeline()
    for contact in normalized_contacts:
        pipe.zscore(key, contact)
    raw_scores = pipe.execute()

    scores = {}
    for contact, raw_score in zip(normalized_contacts, raw_scores):
        if raw_score is None:
            scores[contact] = None
            continue
        try:
            scores[contact] = float(raw_score)
        except (TypeError, ValueError):
            scores[contact] = None
    return scores


def count_contacts_with_timestamps(username):
    return redis_client.zcard(f"contact_ts:{username}")


GROUP_INBOX_TTL_SECONDS = 24 * 60 * 60


def build_group_message_payload(
    sender, group_id, encrypted_message, attachment=None,
    message_type="text", reply_to_message_id=None,
    reply_to_sender=None, encrypted_reply_preview=None,
    encrypted_keys=None,
    recipient_key_records=None,
    group_key_ref=None,
    client_message_id=None,
):
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"
    normalized_sender = sender.strip() if isinstance(sender, str) else ""
    normalized_group_key_ref = _normalize_group_key_ref(group_key_ref) or str(uuid.uuid4())
    normalized_recipient_keys = _normalize_recipient_key_records(recipient_key_records)
    if not normalized_recipient_keys:
        normalized_recipient_keys = _normalize_encrypted_keys_map(encrypted_keys)
    sender_encrypted_key = normalized_recipient_keys.get(normalized_sender)

    return {
        "from": sender,
        "group_id": group_id,
        "group_key_ref": normalized_group_key_ref,
        "type": message_type,
        "message": encrypted_message,
        "encrypted_keys": normalized_recipient_keys,
        "recipient_key_records": [
            {
                "recipient": recipient,
                "encrypted_key": encrypted_key,
            }
            for recipient, encrypted_key in normalized_recipient_keys.items()
        ],
        "sender_encrypted_key": sender_encrypted_key,
        "attachment": attachment,
        "message_id": str(uuid.uuid4()),
        "client_message_id": client_message_id,
        "timestamp": ts,
        "reply_to_message_id": reply_to_message_id,
        "reply_to_sender": reply_to_sender,
        "encrypted_reply_preview": encrypted_reply_preview,
    }



def _persist_group_message_recipients(group_id, payload, recipients, recipient_keys):
    if not _db_available():
        return
    message_id = (payload or {}).get("message_id")
    if not message_id or not recipients:
        return

    existing_group_row = (
        GroupMessage.query.with_entities(GroupMessage.id)
        .filter_by(message_id=message_id)
        .first()
    )
    if existing_group_row is None:
        _upsert_group_message(payload, group_id, auto_commit=False)

    existing_rows = (
        GroupMessageRecipient.query.filter(
            GroupMessageRecipient.message_id == message_id,
            GroupMessageRecipient.recipient_username.in_(recipients),
        )
        .all()
    )
    existing_by_username = {
        row.recipient_username: row
        for row in existing_rows
    }

    for username in recipients:
        encrypted_key = recipient_keys.get(username)
        if not encrypted_key:
            continue
        row = existing_by_username.get(username)
        if row is None:
            db.session.add(
                GroupMessageRecipient(
                    message_id=message_id,
                    group_id=int(group_id),
                    recipient_username=username,
                    encrypted_key=encrypted_key,
                )
            )
            continue
        if encrypted_key and row.encrypted_key != encrypted_key:
            row.encrypted_key = encrypted_key

    db.session.commit()


def push_group_messages_to_members(group_id, recipients, payload):
    if not recipients:
        return 0

    normalized_recipients = list(
        dict.fromkeys(
            username.strip()
            for username in recipients
            if isinstance(username, str) and username.strip()
        )
    )
    if not normalized_recipients:
        return 0

    recipient_keys = _resolve_group_recipient_keys(
        payload=payload,
        recipient_usernames=normalized_recipients,
    )
    recipients_with_keys = [
        username
        for username in normalized_recipients
        if isinstance(recipient_keys.get(username), str) and recipient_keys.get(username)
    ]
    if not recipients_with_keys:
        return 0

    _persist_group_message_recipients(
        group_id=group_id,
        payload=payload,
        recipients=recipients_with_keys,
        recipient_keys=recipient_keys,
    )

    message_id = (payload or {}).get("message_id")
    score = _timestamp_score((payload or {}).get("timestamp"))
    pipe = redis_client.pipeline()

    for username in recipients_with_keys:
        key = _group_inbox_key(username, group_id)
        order_key = _group_inbox_index_order_key(username, group_id)
        payload_key = _group_inbox_index_payload_key(username, group_id)
        ids_key = _group_inbox_index_ids_key(username, group_id)
        recipient_payload = dict(payload or {})
        recipient_key = recipient_keys.get(username)
        if recipient_key:
            recipient_payload["encrypted_key"] = recipient_key
            recipient_payload["encrypted_keys"] = {username: recipient_key}
        data = json.dumps(
            build_group_message_payload_for_recipient(recipient_payload, username)
        )

        pipe.rpush(key, data)
        pipe.expire(key, GROUP_INBOX_TTL_SECONDS)
        if message_id:
            pipe.hset(payload_key, message_id, data)
            pipe.sadd(ids_key, message_id)
            pipe.zadd(order_key, {message_id: score})
            pipe.expire(payload_key, GROUP_INBOX_TTL_SECONDS)
            pipe.expire(ids_key, GROUP_INBOX_TTL_SECONDS)
            pipe.expire(order_key, GROUP_INBOX_TTL_SECONDS)

    pipe.execute()
    return len(recipients_with_keys)


def build_group_message_payloads_for_recipients(payload, recipients):
    normalized_recipients = list(
        dict.fromkeys(
            username.strip()
            for username in recipients
            if isinstance(username, str) and username.strip()
        )
    )
    if not normalized_recipients:
        return {}

    recipient_keys = _resolve_group_recipient_keys(
        payload=payload,
        recipient_usernames=normalized_recipients,
    )
    payloads = {}
    for username in normalized_recipients:
        recipient_payload = dict(payload or {})
        recipient_key = recipient_keys.get(username)
        if not recipient_key:
            continue
        recipient_payload["encrypted_key"] = recipient_key
        recipient_payload["encrypted_keys"] = {username: recipient_key}
        payloads[username] = build_group_message_payload_for_recipient(recipient_payload, username)
    return payloads


def push_group_message_to_member(group_id, username, payload):
    return push_group_messages_to_members(group_id, [username], payload)


def _hydrate_group_pending_from_redis(username, group_id):
    if not _db_available():
        return

    existing_count = (
        GroupMessageRecipient.query.filter(
            GroupMessageRecipient.recipient_username == username,
            GroupMessageRecipient.group_id == int(group_id),
            GroupMessageRecipient.delivered_at.is_(None),
        )
        .count()
    )
    if existing_count > 0:
        return

    redis_pending = _ordered_messages_from_index(
        list_key=_group_inbox_key(username, group_id),
        order_key=_group_inbox_index_order_key(username, group_id),
        payload_key=_group_inbox_index_payload_key(username, group_id),
        ids_key=_group_inbox_index_ids_key(username, group_id),
        ttl_seconds=GROUP_INBOX_TTL_SECONDS,
        start=0,
        end=-1,
    )
    if not redis_pending:
        return

    for payload in redis_pending:
        message_id = (payload or {}).get("message_id")
        if not message_id:
            continue
        _upsert_group_message(payload, group_id, auto_commit=False)
        recipient = GroupMessageRecipient.query.filter_by(
            message_id=message_id,
            recipient_username=username,
        ).first()
        if recipient is None:
            recipient_key = _resolve_group_recipient_keys(
                payload=payload,
                recipient_usernames=[username],
            ).get(username)
            db.session.add(
                GroupMessageRecipient(
                    message_id=message_id,
                    group_id=int(group_id),
                    recipient_username=username,
                    encrypted_key=recipient_key,
                )
            )
    db.session.commit()


def _query_group_pending_rows(username, group_id, limit=None):
    if not _db_available():
        return []

    _hydrate_group_pending_from_redis(username, group_id)

    query = (
        db.session.query(GroupMessage, GroupMessageRecipient)
        .join(
            GroupMessageRecipient,
            GroupMessageRecipient.message_id == GroupMessage.message_id,
        )
        .filter(
            GroupMessageRecipient.recipient_username == username,
            GroupMessageRecipient.group_id == int(group_id),
            GroupMessageRecipient.delivered_at.is_(None),
            GroupMessage.deleted_for_everyone.is_(False),
        )
        .order_by(GroupMessage.timestamp.asc(), GroupMessage.id.asc())
    )
    if limit is not None:
        query = query.limit(max(1, int(limit)))
    return query.all()


def peek_group_messages_for_user(username, group_id):
    if not _db_available():
        return _ordered_messages_from_index(
            list_key=_group_inbox_key(username, group_id),
            order_key=_group_inbox_index_order_key(username, group_id),
            payload_key=_group_inbox_index_payload_key(username, group_id),
            ids_key=_group_inbox_index_ids_key(username, group_id),
            ttl_seconds=GROUP_INBOX_TTL_SECONDS,
            start=0,
            end=-1,
        )

    return [
        _group_message_to_payload(
            message_row,
            recipient_username=username,
            recipient_encrypted_key=recipient_row.encrypted_key,
        )
        for message_row, recipient_row in _query_group_pending_rows(username, group_id)
    ]


def peek_group_messages_batch_for_user(username, group_id, limit=100):
    safe_limit = max(1, int(limit or 1))
    if not _db_available():
        return _ordered_messages_from_index(
            list_key=_group_inbox_key(username, group_id),
            order_key=_group_inbox_index_order_key(username, group_id),
            payload_key=_group_inbox_index_payload_key(username, group_id),
            ids_key=_group_inbox_index_ids_key(username, group_id),
            ttl_seconds=GROUP_INBOX_TTL_SECONDS,
            start=0,
            end=safe_limit - 1,
        )

    return [
        _group_message_to_payload(
            message_row,
            recipient_username=username,
            recipient_encrypted_key=recipient_row.encrypted_key,
        )
        for message_row, recipient_row in _query_group_pending_rows(
            username,
            group_id,
            limit=safe_limit,
        )
    ]


def get_group_pending_count(username, group_id):
    if not _db_available():
        return _pending_count_from_index(
            list_key=_group_inbox_key(username, group_id),
            order_key=_group_inbox_index_order_key(username, group_id),
            payload_key=_group_inbox_index_payload_key(username, group_id),
            ids_key=_group_inbox_index_ids_key(username, group_id),
            ttl_seconds=GROUP_INBOX_TTL_SECONDS,
        )

    _hydrate_group_pending_from_redis(username, group_id)

    return (
        db.session.query(GroupMessageRecipient.id)
        .join(
            GroupMessage,
            GroupMessage.message_id == GroupMessageRecipient.message_id,
        )
        .filter(
            GroupMessageRecipient.recipient_username == username,
            GroupMessageRecipient.group_id == int(group_id),
            GroupMessageRecipient.delivered_at.is_(None),
            GroupMessage.deleted_for_everyone.is_(False),
        )
        .count()
    )


def purge_group_delivery_for_user(group_id, username):
    if not group_id or not isinstance(username, str) or not username.strip():
        return 0

    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return 0
    normalized_username = username.strip()
    removed_message_ids = set()

    if _db_available():
        pending_rows = (
            GroupMessageRecipient.query.filter(
                GroupMessageRecipient.recipient_username == normalized_username,
                GroupMessageRecipient.group_id == normalized_group_id,
            )
            .all()
        )
        for row in pending_rows:
            if isinstance(row.message_id, str) and row.message_id:
                removed_message_ids.add(row.message_id)
        if pending_rows:
            GroupMessageRecipient.query.filter(
                GroupMessageRecipient.recipient_username == normalized_username,
                GroupMessageRecipient.group_id == normalized_group_id,
            ).delete(synchronize_session=False)
            db.session.commit()

    list_key = _group_inbox_key(normalized_username, normalized_group_id)
    order_key = _group_inbox_index_order_key(normalized_username, normalized_group_id)
    payload_key = _group_inbox_index_payload_key(normalized_username, normalized_group_id)
    ids_key = _group_inbox_index_ids_key(normalized_username, normalized_group_id)

    transient_messages = _ordered_messages_from_index(
        list_key=list_key,
        order_key=order_key,
        payload_key=payload_key,
        ids_key=ids_key,
        ttl_seconds=GROUP_INBOX_TTL_SECONDS,
        start=0,
        end=-1,
    )
    transient_ids = [
        message_id
        for message_id in (
            (message or {}).get("message_id")
            for message in transient_messages
        )
        if isinstance(message_id, str) and message_id
    ]
    if transient_ids:
        _ack_messages_from_index(
            list_key=list_key,
            order_key=order_key,
            payload_key=payload_key,
            ids_key=ids_key,
            ttl_seconds=GROUP_INBOX_TTL_SECONDS,
            message_ids=transient_ids,
        )
        removed_message_ids.update(transient_ids)

    redis_client.delete(list_key)
    redis_client.delete(order_key)
    redis_client.delete(payload_key)
    redis_client.delete(ids_key)
    redis_client.delete(f"group_deleted:{normalized_username}:{normalized_group_id}")

    return len(removed_message_ids)


def get_group_message_history(group_id, username=None, limit=50, before_timestamp=None):
    if not _db_available():
        return {
            "messages": [],
            "has_more": False,
            "next_before": None,
        }

    safe_limit = max(1, min(200, int(limit or 50)))
    before_dt = _parse_optional_iso_datetime(before_timestamp)

    base_filters = (
        GroupMessage.group_id == int(group_id),
        GroupMessage.deleted_for_everyone.is_(False),
    )

    normalized_username = username.strip() if isinstance(username, str) else None

    if normalized_username:
        user_deleted_subquery = (
            db.session.query(GroupMessageUserDelete.id)
            .filter(
                GroupMessageUserDelete.message_id == GroupMessage.message_id,
                GroupMessageUserDelete.group_id == int(group_id),
                GroupMessageUserDelete.username == normalized_username,
            )
            .exists()
        )
        base_filters = base_filters + (~user_deleted_subquery,)

    if normalized_username:
        joined_at_row = (
            db.session.query(GroupMember.joined_at)
            .join(User, User.id == GroupMember.user_id)
            .filter(
                GroupMember.group_id == int(group_id),
                User.username == normalized_username,
            )
            .first()
        )
        joined_at = joined_at_row[0] if joined_at_row else None
        if joined_at is not None:
            base_filters = base_filters + (GroupMessage.timestamp >= joined_at,)

    if normalized_username:
        query = (
            db.session.query(
                GroupMessage,
                GroupMessageRecipient.encrypted_key.label("recipient_encrypted_key"),
            )
            .outerjoin(
                GroupMessageRecipient,
                and_(
                    GroupMessageRecipient.message_id == GroupMessage.message_id,
                    GroupMessageRecipient.recipient_username == normalized_username,
                ),
            )
            .filter(*base_filters)
            .order_by(GroupMessage.timestamp.desc(), GroupMessage.id.desc())
        )
    else:
        query = (
            GroupMessage.query.filter(*base_filters)
            .order_by(GroupMessage.timestamp.desc(), GroupMessage.id.desc())
        )
    if before_dt is not None:
        query = query.filter(GroupMessage.timestamp < before_dt)

    rows = query.limit(safe_limit + 1).all()
    has_more = len(rows) > safe_limit
    rows = rows[:safe_limit]
    rows.reverse()

    next_before = None
    if has_more and rows:
        first_row = rows[0][0] if username else rows[0]
        next_before = _format_iso_datetime(first_row.timestamp)

    payloads = []
    if normalized_username:
        for message_row, recipient_encrypted_key in rows:
            payload = _group_message_to_payload(
                message_row,
                recipient_username=normalized_username,
                recipient_encrypted_key=recipient_encrypted_key,
            )
            if (
                message_row.sender_username != normalized_username
                and not payload.get("encrypted_key")
            ):
                continue
            payloads.append(payload)
    else:
        payloads = [_group_message_to_payload(row) for row in rows]

    return {
        "messages": payloads,
        "has_more": has_more,
        "next_before": next_before,
    }


def ack_group_messages(username, group_id, message_ids):
    removed, _ = ack_group_messages_with_payloads(username, group_id, message_ids)
    return removed


def ack_group_transient_messages(username, group_id, message_ids):
    if not message_ids:
        return 0
    removed, _removed_payloads = _ack_messages_from_index(
        list_key=_group_inbox_key(username, group_id),
        order_key=_group_inbox_index_order_key(username, group_id),
        payload_key=_group_inbox_index_payload_key(username, group_id),
        ids_key=_group_inbox_index_ids_key(username, group_id),
        ttl_seconds=GROUP_INBOX_TTL_SECONDS,
        message_ids=message_ids,
    )
    return removed


def ack_group_messages_with_payloads(username, group_id, message_ids):
    if not message_ids:
        return 0, []

    if not _db_available():
        removed, removed_payloads = _ack_messages_from_index(
            list_key=_group_inbox_key(username, group_id),
            order_key=_group_inbox_index_order_key(username, group_id),
            payload_key=_group_inbox_index_payload_key(username, group_id),
            ids_key=_group_inbox_index_ids_key(username, group_id),
            ttl_seconds=GROUP_INBOX_TTL_SECONDS,
            message_ids=message_ids,
        )
        delivered_ids = [
            (payload or {}).get("message_id")
            for payload in removed_payloads
            if isinstance((payload or {}).get("message_id"), str)
        ]
        if delivered_ids:
            pipe = redis_client.pipeline()
            pipe.sadd(f"group_delivered:{group_id}", *delivered_ids)
            pipe.expire(f"group_delivered:{group_id}", MESSAGE_DELIVERED_TTL_SECONDS)
            pipe.execute()
        return removed, removed_payloads

    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return 0, []

    pending_rows = (
        GroupMessageRecipient.query.filter(
            GroupMessageRecipient.recipient_username == username,
            GroupMessageRecipient.group_id == int(group_id),
            GroupMessageRecipient.message_id.in_(normalized_ids),
            GroupMessageRecipient.delivered_at.is_(None),
        )
        .all()
    )
    pending_by_id = {
        row.message_id: row
        for row in pending_rows
    }
    delivered_at = _utc_now_naive()
    delivered_ids = []
    for message_id in normalized_ids:
        recipient_row = pending_by_id.get(message_id)
        if recipient_row is None:
            continue
        recipient_row.delivered_at = delivered_at
        delivered_ids.append(message_id)
    db.session.commit()

    if not delivered_ids:
        return 0, []

    payload_rows = (
        db.session.query(
            GroupMessage,
            GroupMessageRecipient.encrypted_key.label("recipient_encrypted_key"),
        )
        .outerjoin(
            GroupMessageRecipient,
            and_(
                GroupMessageRecipient.message_id == GroupMessage.message_id,
                GroupMessageRecipient.recipient_username == username,
            ),
        )
        .filter(
            GroupMessage.message_id.in_(delivered_ids),
            GroupMessage.deleted_for_everyone.is_(False),
        )
        .all()
    )
    payload_by_id = {
        message_row.message_id: _group_message_to_payload(
            message_row,
            recipient_username=username,
            recipient_encrypted_key=recipient_encrypted_key,
        )
        for message_row, recipient_encrypted_key in payload_rows
    }
    removed_payloads = [
        payload_by_id[message_id]
        for message_id in delivered_ids
        if message_id in payload_by_id
    ]

    # Best-effort transient queue cleanup.
    _ack_messages_from_index(
        list_key=_group_inbox_key(username, group_id),
        order_key=_group_inbox_index_order_key(username, group_id),
        payload_key=_group_inbox_index_payload_key(username, group_id),
        ids_key=_group_inbox_index_ids_key(username, group_id),
        ttl_seconds=GROUP_INBOX_TTL_SECONDS,
        message_ids=normalized_ids,
    )

    delivered_ids = [
        payload.get("message_id")
        for payload in removed_payloads
        if isinstance(payload, dict) and isinstance(payload.get("message_id"), str)
    ]
    if delivered_ids:
        pipe = redis_client.pipeline()
        pipe.sadd(f"group_delivered:{group_id}", *delivered_ids)
        pipe.expire(f"group_delivered:{group_id}", MESSAGE_DELIVERED_TTL_SECONDS)
        sender_ids = {}
        for payload in removed_payloads:
            sender = (payload or {}).get("from")
            message_id = (payload or {}).get("message_id")
            if not isinstance(sender, str) or not sender:
                continue
            if not isinstance(message_id, str) or not message_id:
                continue
            sender_ids.setdefault(sender, []).append(message_id)
        for sender, ids in sender_ids.items():
            key = f"group_delivered:{group_id}:{sender}"
            pipe.sadd(key, *ids)
            pipe.expire(key, MESSAGE_DELIVERED_TTL_SECONDS)
        pipe.execute()

    return len(removed_payloads), removed_payloads


def store_group_message_metadata(payload, group_id):
    if isinstance(payload, dict):
        payload = dict(payload)
    else:
        payload = {}

    if not payload.get("group_key_ref"):
        payload["group_key_ref"] = str(uuid.uuid4())

    sender = (payload or {}).get("from")
    group_key_ref = _normalize_group_key_ref((payload or {}).get("group_key_ref"))
    recipient_keys = _encrypted_keys_from_payload(payload)
    if (
        _db_available()
        and sender
        and group_key_ref
        and recipient_keys
    ):
        store_group_key_records(
            group_id=group_id,
            sender=sender,
            group_key_ref=group_key_ref,
            recipient_keys=recipient_keys,
        )

    sender = (payload or {}).get("from")
    client_message_id = (payload or {}).get("client_message_id")
    requested_message_id = (payload or {}).get("message_id")

    if client_message_id:
        existing_payload = get_group_message_by_client_message_id(
            sender,
            group_id,
            client_message_id,
        )
        if existing_payload is not None:
            message_id = existing_payload.get("message_id")
            if message_id:
                redis_client.setex(
                    f"message_meta:{message_id}",
                    MESSAGE_META_TTL_SECONDS,
                    json.dumps(
                        {
                            "type": "group",
                            "message_id": message_id,
                            "sender": existing_payload.get("from"),
                            "group_id": int(group_id),
                            "timestamp": existing_payload.get("timestamp"),
                        }
                    ),
                )
            if requested_message_id and message_id == requested_message_id:
                return existing_payload, True
            return existing_payload, False

    if _db_available():
        try:
            _upsert_group_message(payload, group_id)
        except IntegrityError:
            db.session.rollback()
            existing_payload = get_group_message_by_client_message_id(
                sender,
                group_id,
                client_message_id,
            )
            if existing_payload is not None:
                message_id = existing_payload.get("message_id")
                if message_id:
                    redis_client.setex(
                        f"message_meta:{message_id}",
                        MESSAGE_META_TTL_SECONDS,
                        json.dumps(
                            {
                                "type": "group",
                                "message_id": message_id,
                                "sender": existing_payload.get("from"),
                                "group_id": int(group_id),
                                "timestamp": existing_payload.get("timestamp"),
                            }
                        ),
                    )
                if requested_message_id and message_id == requested_message_id:
                    return existing_payload, True
                return existing_payload, False
            raise

    message_id = (payload or {}).get("message_id")
    if not message_id:
        return payload, True
    meta = {
        "type": "group",
        "message_id": message_id,
        "sender": payload.get("from"),
        "group_id": int(group_id),
        "timestamp": payload.get("timestamp"),
    }
    key = f"message_meta:{message_id}"
    redis_client.setex(key, MESSAGE_META_TTL_SECONDS, json.dumps(meta))
    return payload, True



def record_group_conversation_timestamp(group_id, iso_timestamp=None):
    if iso_timestamp is None:
        ts = datetime.now(timezone.utc).timestamp()
    else:
        try:
            dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
            ts = dt.timestamp()
        except (ValueError, AttributeError):
            ts = datetime.now(timezone.utc).timestamp()

    key_prefix = "group_ts"
    redis_client.zadd(f"{key_prefix}:global", {str(group_id): ts})


def _decode_redis_key(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _scan_keys(pattern):
    scan_iter = getattr(redis_client, "scan_iter", None)
    if callable(scan_iter):
        try:
            return [
                _decode_redis_key(key)
                for key in scan_iter(match=pattern, count=200)
            ]
        except TypeError:
            return [
                _decode_redis_key(key)
                for key in scan_iter(pattern)
            ]

    keys_fn = getattr(redis_client, "keys", None)
    if callable(keys_fn):
        try:
            return [_decode_redis_key(key) for key in keys_fn(pattern)]
        except TypeError:
            return [_decode_redis_key(key) for key in keys_fn()]

    all_keys = set()
    all_keys_fn = getattr(redis_client, "_all_keys", None)
    if callable(all_keys_fn):
        all_keys.update(_decode_redis_key(key) for key in all_keys_fn())
    else:
        for attr in ("_sets", "_lists", "_hashes", "_sorted_sets", "_strings"):
            bucket = getattr(redis_client, attr, None)
            if isinstance(bucket, dict):
                all_keys.update(_decode_redis_key(key) for key in bucket.keys())

    return [key for key in all_keys if fnmatch(key, pattern)]


def _safe_load_json(raw_value):
    if raw_value is None:
        return None
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8")
    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def purge_user_data(username, candidate_usernames=None):
    if not username:
        return

    normalized_users = {username}
    if candidate_usernames:
        for candidate in candidate_usernames:
            if isinstance(candidate, bytes):
                candidate = candidate.decode("utf-8")
            if isinstance(candidate, str) and candidate:
                normalized_users.add(candidate)

    contacts = get_contacts(username)
    for contact in contacts:
        contact_text = _decode_redis_text(contact)
        if isinstance(contact_text, str) and contact_text:
            normalized_users.add(contact_text)

    removed_message_ids = set()

    private_rows = (
        PrivateMessage.query.filter(
            or_(
                PrivateMessage.sender_username == username,
                PrivateMessage.recipient_username == username,
            )
        )
        .all()
    )
    for row in private_rows:
        if isinstance(row.message_id, str) and row.message_id:
            removed_message_ids.add(row.message_id)

    group_rows_by_sender = (
        GroupMessage.query.filter(GroupMessage.sender_username == username).all()
    )
    sender_group_message_ids = []
    for row in group_rows_by_sender:
        if isinstance(row.message_id, str) and row.message_id:
            sender_group_message_ids.append(row.message_id)
            removed_message_ids.add(row.message_id)

    if sender_group_message_ids:
        GroupMessageRecipient.query.filter(
            GroupMessageRecipient.message_id.in_(sender_group_message_ids)
        ).delete(synchronize_session=False)
        GroupMessage.query.filter(
            GroupMessage.message_id.in_(sender_group_message_ids)
        ).delete(synchronize_session=False)

    GroupMessageRecipient.query.filter(
        GroupMessageRecipient.recipient_username == username
    ).delete(synchronize_session=False)

    if private_rows:
        private_ids = [row.id for row in private_rows]
        PrivateMessage.query.filter(
            PrivateMessage.id.in_(private_ids)
        ).delete(synchronize_session=False)

    db.session.commit()

    # Remove private pending messages sent by this user from every known inbox.
    for owner in sorted(normalized_users):
        messages = _ordered_messages_from_index(
            list_key=_inbox_key(owner),
            order_key=_inbox_index_order_key(owner),
            payload_key=_inbox_index_payload_key(owner),
            ids_key=_inbox_index_ids_key(owner),
            ttl_seconds=INBOX_TTL_SECONDS,
            start=0,
            end=-1,
        )
        removable_ids = [
            message_id
            for message_id in (
                (message or {}).get("message_id")
                for message in messages
                if (message or {}).get("from") == username
            )
            if isinstance(message_id, str) and message_id
        ]
        if removable_ids:
            _removed_count, removed_payloads = ack_messages_with_payloads(owner, removable_ids)
            for payload in removed_payloads:
                message_id = (payload or {}).get("message_id")
                if isinstance(message_id, str) and message_id:
                    removed_message_ids.add(message_id)

        redis_client.hdel(_chat_unread_count_key(owner), username)
        redis_client.delete(_chat_last_key(owner, username))
        redis_client.srem(f"contacts:{owner}", username)
        redis_client.zrem(f"contact_ts:{owner}", username)
        redis_client.delete(f"private_seen:{owner}:{username}")
        redis_client.delete(f"private_seen:{username}:{owner}")
        redis_client.delete(f"private_deleted:{owner}:{username}")
        redis_client.delete(f"private_deleted:{username}:{owner}")

    redis_client.delete(_inbox_key(username))
    redis_client.delete(_inbox_index_order_key(username))
    redis_client.delete(_inbox_index_payload_key(username))
    redis_client.delete(_inbox_index_ids_key(username))
    redis_client.delete(_chat_unread_count_key(username))
    redis_client.delete(f"contacts:{username}")
    redis_client.delete(f"contact_ts:{username}")
    redis_client.delete(f"message_delete_events:{username}")

    # Remove group pending messages sent by this user from all group inboxes.
    for inbox_key in _scan_keys("group_user_inbox:*:*"):
        parts = inbox_key.split(":")
        if len(parts) != 4:
            continue
        owner = parts[2]
        try:
            group_id = int(parts[3])
        except (TypeError, ValueError):
            continue

        if owner == username:
            redis_client.delete(_group_inbox_key(owner, group_id))
            redis_client.delete(_group_inbox_index_order_key(owner, group_id))
            redis_client.delete(_group_inbox_index_payload_key(owner, group_id))
            redis_client.delete(_group_inbox_index_ids_key(owner, group_id))
            continue

        messages = _ordered_messages_from_index(
            list_key=_group_inbox_key(owner, group_id),
            order_key=_group_inbox_index_order_key(owner, group_id),
            payload_key=_group_inbox_index_payload_key(owner, group_id),
            ids_key=_group_inbox_index_ids_key(owner, group_id),
            ttl_seconds=GROUP_INBOX_TTL_SECONDS,
            start=0,
            end=-1,
        )
        removable_ids = [
            message_id
            for message_id in (
                (message or {}).get("message_id")
                for message in messages
                if (message or {}).get("from") == username
            )
            if isinstance(message_id, str) and message_id
        ]
        if removable_ids:
            _removed_count, removed_payloads = ack_group_messages_with_payloads(
                owner,
                group_id,
                removable_ids,
            )
            for payload in removed_payloads:
                message_id = (payload or {}).get("message_id")
                if isinstance(message_id, str) and message_id:
                    removed_message_ids.add(message_id)

    for key in _scan_keys("chat:last:*"):
        parts = key.split(":")
        if len(parts) != 4:
            continue
        if parts[2] == username or parts[3] == username:
            redis_client.delete(key)

    for key in _scan_keys("chat:unread_count:*"):
        redis_client.hdel(key, username)

    for key in _scan_keys("contacts:*"):
        redis_client.srem(key, username)

    for key in _scan_keys("contact_ts:*"):
        redis_client.zrem(key, username)

    for key in _scan_keys("private_seen:*"):
        parts = key.split(":")
        if len(parts) != 3:
            continue
        if parts[1] == username or parts[2] == username:
            redis_client.delete(key)

    for key in _scan_keys("private_deleted:*"):
        parts = key.split(":")
        if len(parts) != 3:
            continue
        if parts[1] == username or username in parts[2]:
            redis_client.delete(key)

    for key in _scan_keys("group_deleted:*"):
        parts = key.split(":")
        if len(parts) != 3:
            continue
        if parts[1] == username:
            redis_client.delete(key)

    for key in _scan_keys("message_delete_events:*"):
        parts = key.split(":")
        if len(parts) != 2:
            continue
        if parts[1] == username:
            redis_client.delete(key)

    for key in _scan_keys("message_meta:*"):
        metadata = _safe_load_json(redis_client.get(key))
        if not metadata:
            continue
        if metadata.get("sender") == username or metadata.get("recipient") == username:
            message_id = metadata.get("message_id")
            if isinstance(message_id, str) and message_id:
                removed_message_ids.add(message_id)
            redis_client.delete(key)

    if removed_message_ids:
        for key in _scan_keys("private_seen:*"):
            redis_client.srem(key, *removed_message_ids)
        for key in _scan_keys("private_deleted:*"):
            redis_client.srem(key, *removed_message_ids)
        for key in _scan_keys("group_delivered:*"):
            redis_client.srem(key, *removed_message_ids)
        for key in _scan_keys("group_seen:*"):
            redis_client.srem(key, *removed_message_ids)
        for key in _scan_keys("group_deleted:*"):
            redis_client.srem(key, *removed_message_ids)
