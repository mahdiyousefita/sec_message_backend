import json
import uuid
from fnmatch import fnmatch
from datetime import datetime, timezone
from app.extensions.redis_client import redis_client

INBOX_TTL_SECONDS = 24 * 60 * 60
MESSAGE_META_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_DELETE_EVENT_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_SEEN_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_DELETED_TTL_SECONDS = 7 * 24 * 60 * 60

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
                          encrypted_reply_key=None):
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"
    return {
        "from": sender,
        "type": message_type,
        "message": encrypted_message,
        "encrypted_key": encrypted_key,
        "attachment": attachment,
        "message_id": str(uuid.uuid4()),
        "timestamp": ts,
        "reply_to_message_id": reply_to_message_id,
        "reply_to_sender": reply_to_sender,
        "encrypted_reply_preview": encrypted_reply_preview,
        "encrypted_reply_key": encrypted_reply_key,
    }


def push_message_payload(recipient, payload):
    key = _inbox_key(recipient)
    order_key = _inbox_index_order_key(recipient)
    payload_key = _inbox_index_payload_key(recipient)
    ids_key = _inbox_index_ids_key(recipient)

    data = json.dumps(payload)
    message_id = (payload or {}).get("message_id")
    score = _timestamp_score((payload or {}).get("timestamp"))

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
    _increment_unread_metadata(recipient, payload, pipe=pipe)
    pipe.execute()


def pop_messages(username):
    messages = _pop_all_messages(
        list_key=_inbox_key(username),
        order_key=_inbox_index_order_key(username),
        payload_key=_inbox_index_payload_key(username),
        ids_key=_inbox_index_ids_key(username),
        ttl_seconds=INBOX_TTL_SECONDS,
    )
    _decrement_unread_metadata(username, messages)
    return messages


def peek_messages(username):
    return _ordered_messages_from_index(
        list_key=_inbox_key(username),
        order_key=_inbox_index_order_key(username),
        payload_key=_inbox_index_payload_key(username),
        ids_key=_inbox_index_ids_key(username),
        ttl_seconds=INBOX_TTL_SECONDS,
        start=0,
        end=-1,
    )


def peek_messages_batch(username, limit=100):
    safe_limit = max(1, int(limit or 1))
    return _ordered_messages_from_index(
        list_key=_inbox_key(username),
        order_key=_inbox_index_order_key(username),
        payload_key=_inbox_index_payload_key(username),
        ids_key=_inbox_index_ids_key(username),
        ttl_seconds=INBOX_TTL_SECONDS,
        start=0,
        end=safe_limit - 1,
    )


def get_pending_count(username):
    return _pending_count_from_index(
        list_key=_inbox_key(username),
        order_key=_inbox_index_order_key(username),
        payload_key=_inbox_index_payload_key(username),
        ids_key=_inbox_index_ids_key(username),
        ttl_seconds=INBOX_TTL_SECONDS,
    )


def ack_messages(username, message_ids):
    removed, _ = ack_messages_with_payloads(username, message_ids)
    return removed


def ack_messages_with_payloads(username, message_ids):
    if not message_ids:
        return 0, []
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
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
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

    pipe = redis_client.pipeline()
    for message_id in normalized_ids:
        pipe.get(f"message_meta:{message_id}")

    raw_values = pipe.execute()
    metadata_by_id = {}
    for message_id, raw in zip(normalized_ids, raw_values):
        if not raw:
            continue
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if isinstance(parsed, dict):
            metadata_by_id[message_id] = parsed

    return metadata_by_id


def delete_message_metadata(message_id):
    if not message_id:
        return
    redis_client.delete(f"message_meta:{message_id}")


def mark_private_message_seen(sender, recipient, message_id):
    if not sender or not recipient or not message_id:
        return
    key = f"private_seen:{sender}:{recipient}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, message_id)
    pipe.expire(key, MESSAGE_SEEN_TTL_SECONDS)
    pipe.execute()


def mark_private_messages_seen_batch(sender, recipient, message_ids):
    if not sender or not recipient or not message_ids:
        return
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return
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


def get_private_seen_message_ids(sender, recipient, message_ids):
    if not sender or not recipient or not message_ids:
        return []
    key = f"private_seen:{sender}:{recipient}"
    normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
    return [
        message_id
        for message_id, status in zip(normalized_ids, statuses)
        if status
    ]


def mark_group_message_seen(group_id, message_id):
    if not group_id or not message_id:
        return
    key = f"group_seen:{group_id}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, message_id)
    pipe.expire(key, MESSAGE_SEEN_TTL_SECONDS)
    pipe.execute()


def mark_group_messages_seen_batch(group_id, message_ids):
    if not group_id or not message_ids:
        return
    normalized_ids = _normalize_message_ids(message_ids)
    if not normalized_ids:
        return
    key = f"group_seen:{group_id}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, *normalized_ids)
    pipe.expire(key, MESSAGE_SEEN_TTL_SECONDS)
    pipe.execute()


def get_group_seen_message_ids(group_id, message_ids):
    if not group_id or not message_ids:
        return []
    key = f"group_seen:{group_id}"
    normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
    return [
        message_id
        for message_id, status in zip(normalized_ids, statuses)
        if status
    ]


def mark_private_message_deleted(username, chat_id, message_id):
    if not username or not chat_id or not message_id:
        return
    key = f"private_deleted:{username}:{chat_id}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, message_id)
    pipe.expire(key, MESSAGE_DELETED_TTL_SECONDS)
    pipe.execute()


def get_private_deleted_message_ids(username, chat_id, message_ids):
    if not username or not chat_id or not message_ids:
        return []
    key = f"private_deleted:{username}:{chat_id}"
    normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
    return [
        message_id
        for message_id, status in zip(normalized_ids, statuses)
        if status
    ]


def mark_group_message_deleted(username, group_id, message_id):
    if not username or not group_id or not message_id:
        return
    key = f"group_deleted:{username}:{group_id}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, message_id)
    pipe.expire(key, MESSAGE_DELETED_TTL_SECONDS)
    pipe.execute()


def get_group_deleted_message_ids(username, group_id, message_ids):
    if not username or not group_id or not message_ids:
        return []
    key = f"group_deleted:{username}:{group_id}"
    normalized_ids, statuses = _get_set_membership_statuses(key, message_ids)
    return [
        message_id
        for message_id, status in zip(normalized_ids, statuses)
        if status
    ]


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


def get_contacts_sorted_by_last_message(username, offset=0, count=20):
    return redis_client.zrevrange(
        f"contact_ts:{username}", offset, offset + count - 1, withscores=True
    )


def get_contact_timestamp_score(username, contact):
    return redis_client.zscore(f"contact_ts:{username}", contact)


def count_contacts_with_timestamps(username):
    return redis_client.zcard(f"contact_ts:{username}")


GROUP_INBOX_TTL_SECONDS = 24 * 60 * 60


def build_group_message_payload(
    sender, group_id, encrypted_message, attachment=None,
    message_type="text", reply_to_message_id=None,
    reply_to_sender=None, encrypted_reply_preview=None,
    encrypted_keys=None,
):
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"
    return {
        "from": sender,
        "group_id": group_id,
        "type": message_type,
        "message": encrypted_message,
        "encrypted_keys": encrypted_keys,
        "attachment": attachment,
        "message_id": str(uuid.uuid4()),
        "timestamp": ts,
        "reply_to_message_id": reply_to_message_id,
        "reply_to_sender": reply_to_sender,
        "encrypted_reply_preview": encrypted_reply_preview,
    }



def push_group_message_to_member(group_id, username, payload):
    key = _group_inbox_key(username, group_id)
    order_key = _group_inbox_index_order_key(username, group_id)
    payload_key = _group_inbox_index_payload_key(username, group_id)
    ids_key = _group_inbox_index_ids_key(username, group_id)
    data = json.dumps(payload)
    message_id = (payload or {}).get("message_id")
    score = _timestamp_score((payload or {}).get("timestamp"))

    pipe = redis_client.pipeline()
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


def peek_group_messages_for_user(username, group_id):
    return _ordered_messages_from_index(
        list_key=_group_inbox_key(username, group_id),
        order_key=_group_inbox_index_order_key(username, group_id),
        payload_key=_group_inbox_index_payload_key(username, group_id),
        ids_key=_group_inbox_index_ids_key(username, group_id),
        ttl_seconds=GROUP_INBOX_TTL_SECONDS,
        start=0,
        end=-1,
    )


def peek_group_messages_batch_for_user(username, group_id, limit=100):
    safe_limit = max(1, int(limit or 1))
    return _ordered_messages_from_index(
        list_key=_group_inbox_key(username, group_id),
        order_key=_group_inbox_index_order_key(username, group_id),
        payload_key=_group_inbox_index_payload_key(username, group_id),
        ids_key=_group_inbox_index_ids_key(username, group_id),
        ttl_seconds=GROUP_INBOX_TTL_SECONDS,
        start=0,
        end=safe_limit - 1,
    )


def get_group_pending_count(username, group_id):
    return _pending_count_from_index(
        list_key=_group_inbox_key(username, group_id),
        order_key=_group_inbox_index_order_key(username, group_id),
        payload_key=_group_inbox_index_payload_key(username, group_id),
        ids_key=_group_inbox_index_ids_key(username, group_id),
        ttl_seconds=GROUP_INBOX_TTL_SECONDS,
    )


def ack_group_messages(username, group_id, message_ids):
    removed, _ = ack_group_messages_with_payloads(username, group_id, message_ids)
    return removed


def ack_group_messages_with_payloads(username, group_id, message_ids):
    if not message_ids:
        return 0, []
    removed, removed_payloads = _ack_messages_from_index(
        list_key=_group_inbox_key(username, group_id),
        order_key=_group_inbox_index_order_key(username, group_id),
        payload_key=_group_inbox_index_payload_key(username, group_id),
        ids_key=_group_inbox_index_ids_key(username, group_id),
        ttl_seconds=GROUP_INBOX_TTL_SECONDS,
        message_ids=message_ids,
    )
    return removed, removed_payloads


def store_group_message_metadata(payload, group_id):
    message_id = (payload or {}).get("message_id")
    if not message_id:
        return
    meta = {
        "type": "group",
        "message_id": message_id,
        "sender": payload.get("from"),
        "group_id": int(group_id),
        "timestamp": payload.get("timestamp"),
    }
    key = f"message_meta:{message_id}"
    redis_client.setex(key, MESSAGE_META_TTL_SECONDS, json.dumps(meta))



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

    # Remove private pending messages sent by this user from every known inbox.
    for owner in sorted(normalized_users):
        messages = peek_messages(owner)
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

        messages = peek_group_messages_for_user(owner, group_id)
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
        for key in _scan_keys("group_seen:*"):
            redis_client.srem(key, *removed_message_ids)
        for key in _scan_keys("group_deleted:*"):
            redis_client.srem(key, *removed_message_ids)
