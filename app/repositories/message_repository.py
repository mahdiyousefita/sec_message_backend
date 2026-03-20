import json
import uuid
from datetime import datetime
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


def build_message_payload(sender, encrypted_message, encrypted_key, attachment=None, message_type="text"):
    return {
        "from": sender,
        "type": message_type,
        "message": encrypted_message,
        "encrypted_key": encrypted_key,
        "attachment": attachment,
        "message_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
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
