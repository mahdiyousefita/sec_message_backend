import json

from app.extensions.redis_client import redis_client


INBOX_TTL_SECONDS = 24 * 60 * 60


def _chat_unread_count_key(username: str) -> str:
    return f"chat:unread_count:{username}"


def _chat_last_key(username: str, contact: str) -> str:
    return f"chat:last:{username}:{contact}"


def _read_inbox_messages(username: str):
    key = f"inbox:{username}"
    messages = redis_client.lrange(key, 0, -1)
    decoded = []
    for raw in messages:
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(msg, dict):
            decoded.append(msg)
    return decoded


def _backfill_unread_metadata_from_inbox(username: str):
    unread_key = _chat_unread_count_key(username)
    if redis_client.hlen(unread_key) > 0:
        return

    messages = _read_inbox_messages(username)
    if not messages:
        return

    per_sender_count = {}
    per_sender_last = {}
    for msg in messages:
        sender = msg.get("from")
        if not sender:
            continue
        per_sender_count[sender] = per_sender_count.get(sender, 0) + 1
        per_sender_last[sender] = {
            "sender": sender,
            "type": msg.get("type", "text"),
            "timestamp": msg.get("timestamp", ""),
            "message_id": msg.get("message_id", ""),
        }

    if not per_sender_count:
        return

    pipe = redis_client.pipeline()
    for sender, count in per_sender_count.items():
        pipe.hset(unread_key, sender, count)
        pipe.expire(unread_key, INBOX_TTL_SECONDS)
        last_key = _chat_last_key(username, sender)
        pipe.hset(last_key, mapping=per_sender_last.get(sender, {}))
        pipe.expire(last_key, INBOX_TTL_SECONDS)
    pipe.execute()


def has_pending_messages(username: str) -> bool:
    return pending_message_count(username) > 0


def pending_message_count(username: str) -> int:
    _backfill_unread_metadata_from_inbox(username)
    values = redis_client.hvals(_chat_unread_count_key(username))
    total = 0
    for value in values:
        try:
            total += int(value)
        except (TypeError, ValueError):
            continue
    return total


def pending_message_senders(username: str) -> set:
    summary = get_unread_summary_map(username)
    return set(summary["per_sender"].keys())


def get_unread_summary_map(username: str) -> dict:
    _backfill_unread_metadata_from_inbox(username)
    unread_key = _chat_unread_count_key(username)
    raw_counts = redis_client.hgetall(unread_key)

    per_sender = {}
    total = 0
    senders = []
    for sender, count in raw_counts.items():
        try:
            normalized_count = int(count)
        except (TypeError, ValueError):
            continue
        if normalized_count <= 0:
            continue
        total += normalized_count
        senders.append(sender)
        per_sender[sender] = {
            "sender": sender,
            "count": normalized_count,
            "last_type": "text",
            "last_timestamp": "",
            "message_id": "",
        }

    if senders:
        pipe = redis_client.pipeline()
        for sender in senders:
            pipe.hgetall(_chat_last_key(username, sender))
        last_values = pipe.execute()
        for sender, last_payload in zip(senders, last_values):
            if not isinstance(last_payload, dict):
                continue
            per_sender[sender]["last_type"] = last_payload.get("type", "text")
            per_sender[sender]["last_timestamp"] = last_payload.get("timestamp", "")
            per_sender[sender]["message_id"] = last_payload.get("message_id", "")

    return {
        "total": total,
        "per_sender": per_sender,
    }


def get_unread_summary(username: str) -> dict:
    summary = get_unread_summary_map(username)

    return {
        "total": summary["total"],
        "senders": list(summary["per_sender"].values()),
    }


def get_last_message_preview(username: str, contact: str) -> dict | None:
    if not contact:
        return None
    _backfill_unread_metadata_from_inbox(username)
    unread_value = redis_client.hget(_chat_unread_count_key(username), contact)
    try:
        unread_count = int(unread_value or 0)
    except (TypeError, ValueError):
        unread_count = 0
    if unread_count <= 0:
        return None

    last_msg = redis_client.hgetall(_chat_last_key(username, contact))
    if not isinstance(last_msg, dict) or not last_msg:
        return {"from": contact, "type": "text", "timestamp": ""}

    return {
        "from": last_msg.get("sender") or contact,
        "type": last_msg.get("type", "text"),
        "timestamp": last_msg.get("timestamp", ""),
    }
