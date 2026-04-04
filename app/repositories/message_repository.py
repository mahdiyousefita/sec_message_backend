import json
import uuid
from datetime import datetime, timezone
from app.extensions.redis_client import redis_client

INBOX_TTL_SECONDS = 24 * 60 * 60
MESSAGE_META_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_DELETE_EVENT_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_SEEN_TTL_SECONDS = 7 * 24 * 60 * 60
MESSAGE_DELETED_TTL_SECONDS = 7 * 24 * 60 * 60

_ACK_BY_IDS_LUA = """
local key = KEYS[1]
local ttl  = tonumber(ARGV[1]) or 86400
local id_set = {}
for i = 2, #ARGV do
    id_set[ARGV[i]] = true
end
local msgs = redis.call('lrange', key, 0, -1)
redis.call('del', key)
local kept = {}
for _, raw in ipairs(msgs) do
    local ok, msg = pcall(cjson.decode, raw)
    if not (ok and msg and msg.message_id and id_set[msg.message_id]) then
        kept[#kept + 1] = raw
    end
end
if #kept > 0 then
    redis.call('rpush', key, unpack(kept))
    redis.call('expire', key, ttl)
end
return #msgs - #kept
"""

_POP_ALL_LUA = """
local key = KEYS[1]
local msgs = redis.call('lrange', key, 0, -1)
redis.call('del', key)
return msgs
"""


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
    key = f"inbox:{recipient}"
    data = json.dumps(payload)
    pipe = redis_client.pipeline()
    pipe.rpush(key, data)
    pipe.expire(key, INBOX_TTL_SECONDS)
    pipe.execute()


def pop_messages(username):
    key = f"inbox:{username}"
    raw_messages = redis_client.eval(_POP_ALL_LUA, 1, key)
    if not raw_messages:
        return []
    return [json.loads(msg) for msg in raw_messages]


def peek_messages(username):
    key = f"inbox:{username}"
    messages = redis_client.lrange(key, 0, -1)
    return [json.loads(msg) for msg in messages]


def peek_messages_batch(username, limit=100):
    key = f"inbox:{username}"
    safe_limit = max(1, int(limit or 1))
    messages = redis_client.lrange(key, 0, safe_limit - 1)
    return [json.loads(msg) for msg in messages]


def get_pending_count(username):
    key = f"inbox:{username}"
    return redis_client.llen(key)


def ack_messages(username, message_ids):
    if not message_ids:
        return 0
    key = f"inbox:{username}"
    return redis_client.eval(
        _ACK_BY_IDS_LUA, 1, key, str(INBOX_TTL_SECONDS), *message_ids
    )


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


def get_private_seen_message_ids(sender, recipient, message_ids):
    if not sender or not recipient or not message_ids:
        return []
    key = f"private_seen:{sender}:{recipient}"
    seen_ids = []
    for message_id in message_ids:
        if not message_id:
            continue
        if redis_client.sismember(key, message_id):
            seen_ids.append(message_id)
    return seen_ids


def mark_group_message_seen(group_id, message_id):
    if not group_id or not message_id:
        return
    key = f"group_seen:{group_id}"
    pipe = redis_client.pipeline()
    pipe.sadd(key, message_id)
    pipe.expire(key, MESSAGE_SEEN_TTL_SECONDS)
    pipe.execute()


def get_group_seen_message_ids(group_id, message_ids):
    if not group_id or not message_ids:
        return []
    key = f"group_seen:{group_id}"
    seen_ids = []
    for message_id in message_ids:
        if not message_id:
            continue
        if redis_client.sismember(key, message_id):
            seen_ids.append(message_id)
    return seen_ids


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
    deleted_ids = []
    for message_id in message_ids:
        if not message_id:
            continue
        if redis_client.sismember(key, message_id):
            deleted_ids.append(message_id)
    return deleted_ids


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
    deleted_ids = []
    for message_id in message_ids:
        if not message_id:
            continue
        if redis_client.sismember(key, message_id):
            deleted_ids.append(message_id)
    return deleted_ids


def queue_message_deletion_event(username, event_name, payload):
    if not username or not event_name or not isinstance(payload, dict):
        return
    key = f"message_delete_events:{username}"
    event = json.dumps({"event": event_name, "payload": payload})
    pipe = redis_client.pipeline()
    pipe.rpush(key, event)
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
    key = f"group_user_inbox:{username}:{group_id}"
    data = json.dumps(payload)
    pipe = redis_client.pipeline()
    pipe.rpush(key, data)
    pipe.expire(key, GROUP_INBOX_TTL_SECONDS)
    pipe.execute()


def peek_group_messages_for_user(username, group_id):
    key = f"group_user_inbox:{username}:{group_id}"
    raw = redis_client.lrange(key, 0, -1)
    return [json.loads(msg) for msg in raw]


def peek_group_messages_batch_for_user(username, group_id, limit=100):
    key = f"group_user_inbox:{username}:{group_id}"
    safe_limit = max(1, int(limit or 1))
    raw = redis_client.lrange(key, 0, safe_limit - 1)
    return [json.loads(msg) for msg in raw]


def get_group_pending_count(username, group_id):
    key = f"group_user_inbox:{username}:{group_id}"
    return redis_client.llen(key)


_ACK_GROUP_BY_IDS_LUA = """
local key = KEYS[1]
local ttl  = tonumber(ARGV[1]) or 86400
local id_set = {}
for i = 2, #ARGV do
    id_set[ARGV[i]] = true
end
local msgs = redis.call('lrange', key, 0, -1)
redis.call('del', key)
local kept = {}
for _, raw in ipairs(msgs) do
    local ok, msg = pcall(cjson.decode, raw)
    if not (ok and msg and msg.message_id and id_set[msg.message_id]) then
        kept[#kept + 1] = raw
    end
end
if #kept > 0 then
    redis.call('rpush', key, unpack(kept))
    redis.call('expire', key, ttl)
end
return #msgs - #kept
"""


def ack_group_messages(username, group_id, message_ids):
    if not message_ids:
        return 0
    key = f"group_user_inbox:{username}:{group_id}"
    return redis_client.eval(
        _ACK_GROUP_BY_IDS_LUA, 1, key,
        str(GROUP_INBOX_TTL_SECONDS), *message_ids
    )


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
