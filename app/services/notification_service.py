import json

from app.extensions.redis_client import redis_client


INBOX_TTL_SECONDS = 24 * 60 * 60


def _db_unread_summary(username: str) -> dict:
    from app.repositories import message_repository

    return message_repository.get_private_unread_summary(username)


def _chat_unread_count_key(username: str) -> str:
    return f"chat:unread_count:{username}"


def _chat_last_key(username: str, contact: str) -> str:
    return f"chat:last:{username}:{contact}"


def _read_inbox_messages(username: str):
    key = f"inbox:{username}"
    try:
        messages = redis_client.lrange(key, 0, -1)
    except Exception:
        return []
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
    redis_available = True
    try:
        if redis_client.hlen(unread_key) > 0:
            return
    except Exception:
        redis_available = False

    messages = _read_inbox_messages(username)
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
        db_summary = _db_unread_summary(username)
        for sender, summary in db_summary.items():
            per_sender_count[sender] = int(summary.get("count", 0))
            per_sender_last[sender] = {
                "sender": sender,
                "type": summary.get("last_type", "text"),
                "timestamp": summary.get("last_timestamp", ""),
                "message_id": summary.get("message_id", ""),
            }

    if not per_sender_count:
        return

    if not redis_available:
        return

    try:
        pipe = redis_client.pipeline()
        for sender, count in per_sender_count.items():
            pipe.hset(unread_key, sender, count)
            pipe.expire(unread_key, INBOX_TTL_SECONDS)
            last_key = _chat_last_key(username, sender)
            pipe.hset(last_key, mapping=per_sender_last.get(sender, {}))
            pipe.expire(last_key, INBOX_TTL_SECONDS)
        pipe.execute()
    except Exception:
        # Redis is an optimization layer for unread counters.
        return


def has_pending_messages(username: str) -> bool:
    return pending_message_count(username) > 0


def pending_message_count(username: str) -> int:
    return int(get_unread_summary_map(username).get("total", 0))


def pending_message_senders(username: str) -> set:
    summary = get_unread_summary_map(username)
    return set(summary["per_sender"].keys())


def get_unread_summary_map(username: str) -> dict:
    _backfill_unread_metadata_from_inbox(username)
    unread_key = _chat_unread_count_key(username)
    try:
        raw_counts = redis_client.hgetall(unread_key)
    except Exception:
        raw_counts = {}

    if not raw_counts:
        db_summary = _db_unread_summary(username)
        total = sum(int(item.get("count", 0)) for item in db_summary.values())
        return {
            "total": total,
            "per_sender": db_summary,
        }

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
        try:
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
        except Exception:
            pass

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


def get_sender_unread_summary(username: str, contact: str) -> dict:
    normalized_contact = (contact or "").strip()
    if not normalized_contact:
        return {
            "sender": "",
            "count": 0,
            "last_type": "text",
            "last_timestamp": "",
            "message_id": "",
        }

    _backfill_unread_metadata_from_inbox(username)

    unread_key = _chat_unread_count_key(username)
    try:
        raw_count = redis_client.hget(unread_key, normalized_contact)
    except Exception:
        raw_count = None

    count = 0
    if raw_count is not None:
        try:
            count = max(0, int(raw_count))
        except (TypeError, ValueError):
            count = 0
    else:
        db_summary = _db_unread_summary(username).get(normalized_contact, {})
        try:
            count = max(0, int(db_summary.get("count", 0)))
        except (TypeError, ValueError):
            count = 0

    summary = {
        "sender": normalized_contact,
        "count": count,
        "last_type": "text",
        "last_timestamp": "",
        "message_id": "",
    }
    if count <= 0:
        return summary

    try:
        last_payload = redis_client.hgetall(_chat_last_key(username, normalized_contact))
    except Exception:
        last_payload = {}

    if isinstance(last_payload, dict) and last_payload:
        summary["last_type"] = last_payload.get("type", "text")
        summary["last_timestamp"] = last_payload.get("timestamp", "")
        summary["message_id"] = last_payload.get("message_id", "")
        return summary

    db_sender = _db_unread_summary(username).get(normalized_contact, {})
    if isinstance(db_sender, dict):
        summary["last_type"] = db_sender.get("last_type", "text")
        summary["last_timestamp"] = db_sender.get("last_timestamp", "")
        summary["message_id"] = db_sender.get("message_id", "")
    return summary


def get_last_message_preview(username: str, contact: str) -> dict | None:
    sender_summary = get_sender_unread_summary(username, contact)
    if sender_summary.get("count", 0) <= 0:
        return None

    return {
        "from": contact,
        "type": sender_summary.get("last_type", "text"),
        "timestamp": sender_summary.get("last_timestamp", ""),
    }
