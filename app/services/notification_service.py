import json

from app.extensions.redis_client import redis_client


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


def has_pending_messages(username: str) -> bool:
    key = f"inbox:{username}"
    return redis_client.llen(key) > 0


def pending_message_count(username: str) -> int:
    key = f"inbox:{username}"
    return redis_client.llen(key)


def pending_message_senders(username: str) -> set:
    messages = _read_inbox_messages(username)
    senders = set()
    for msg in messages:
        sender = msg.get("from")
        if sender:
            senders.add(sender)
    return senders


def get_unread_summary_map(username: str) -> dict:
    messages = _read_inbox_messages(username)

    per_sender = {}
    for msg in messages:
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

    return {
        "total": len(messages),
        "per_sender": per_sender,
    }


def get_unread_summary(username: str) -> dict:
    summary = get_unread_summary_map(username)

    return {
        "total": summary["total"],
        "senders": list(summary["per_sender"].values()),
    }


def get_last_message_preview(username: str, contact: str) -> dict | None:
    summary = get_unread_summary_map(username)
    last_msg = summary["per_sender"].get(contact)

    if not last_msg:
        return None

    return {
        "from": last_msg.get("sender"),
        "type": last_msg.get("last_type", "text"),
        "timestamp": last_msg.get("last_timestamp", ""),
    }
