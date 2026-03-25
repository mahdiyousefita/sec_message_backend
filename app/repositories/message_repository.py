import json
import uuid
from datetime import datetime, timezone
from app.extensions.redis_client import redis_client

INBOX_TTL_SECONDS = 24 * 60 * 60

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


def ack_messages(username, message_ids):
    if not message_ids:
        return 0
    key = f"inbox:{username}"
    return redis_client.eval(
        _ACK_BY_IDS_LUA, 1, key, str(INBOX_TTL_SECONDS), *message_ids
    )

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
