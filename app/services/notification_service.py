import json

from app.extensions.redis_client import redis_client


def has_pending_messages(username: str) -> bool:
    key = f"inbox:{username}"
    return redis_client.llen(key) > 0


def pending_message_count(username: str) -> int:
    key = f"inbox:{username}"
    return redis_client.llen(key)


def pending_message_senders(username: str) -> set:
    key = f"inbox:{username}"
    messages = redis_client.lrange(key, 0, -1)
    senders = set()
    for raw in messages:
        try:
            msg = json.loads(raw)
            sender = msg.get("from")
            if sender:
                senders.add(sender)
        except (json.JSONDecodeError, TypeError):
            continue
    return senders


def get_unread_summary(username: str) -> dict:
    key = f"inbox:{username}"
    messages = redis_client.lrange(key, 0, -1)

    per_sender = {}
    for raw in messages:
        try:
            msg = json.loads(raw)
            sender = msg.get("from")
            if not sender:
                continue
            entry = per_sender.get(sender)
            if entry is None:
                per_sender[sender] = {
                    "sender": sender,
                    "count": 1,
                    "last_type": msg.get("type", "text"),
                    "last_timestamp": msg.get("timestamp", ""),
                }
            else:
                entry["count"] += 1
                entry["last_type"] = msg.get("type", "text")
                entry["last_timestamp"] = msg.get("timestamp", "")
        except (json.JSONDecodeError, TypeError):
            continue

    return {
        "total": len(messages),
        "senders": list(per_sender.values()),
    }


def get_last_message_preview(username: str, contact: str) -> dict | None:
    key = f"inbox:{username}"
    messages = redis_client.lrange(key, 0, -1)

    last_msg = None
    for raw in messages:
        try:
            msg = json.loads(raw)
            if msg.get("from") == contact:
                last_msg = msg
        except (json.JSONDecodeError, TypeError):
            continue

    if not last_msg:
        return None

    return {
        "from": last_msg.get("from"),
        "type": last_msg.get("type", "text"),
        "timestamp": last_msg.get("timestamp", ""),
    }
