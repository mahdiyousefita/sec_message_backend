#!/usr/bin/env python3
"""
One-time migration helper for durable chat persistence.

This script backfills pending private/group payloads from Redis transient inboxes
into SQL tables introduced for durable message storage.
"""

import json

from app import create_app
from app.db import db
from app.extensions.redis_client import redis_client
from app.models.chat_message_model import GroupMessageRecipient
from app.repositories import message_repository


def _decode(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _safe_json(raw):
    try:
        parsed = json.loads(_decode(raw))
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _scan(match):
    scan_iter = getattr(redis_client, "scan_iter", None)
    if callable(scan_iter):
        try:
            for key in scan_iter(match=match, count=200):
                yield _decode(key)
            return
        except TypeError:
            for key in scan_iter(match):
                yield _decode(key)
            return

    keys = redis_client.keys(match)
    for key in keys:
        yield _decode(key)


def _backfill_private():
    migrated = 0
    for key in _scan("inbox_payloads:*"):
        parts = key.split(":")
        if len(parts) != 2:
            continue
        username = parts[1]
        raw_values = redis_client.hvals(key)
        for raw in raw_values:
            payload = _safe_json(raw)
            if not payload:
                continue
            message_repository._upsert_private_message(  # noqa: SLF001
                payload,
                username,
                auto_commit=False,
            )
            migrated += 1
    db.session.commit()
    return migrated


def _backfill_group():
    migrated = 0
    recipient_links = 0

    for key in _scan("group_inbox_payloads:*:*"):
        parts = key.split(":")
        if len(parts) != 4:
            continue
        username = parts[2]
        try:
            group_id = int(parts[3])
        except (TypeError, ValueError):
            continue

        raw_values = redis_client.hvals(key)
        for raw in raw_values:
            payload = _safe_json(raw)
            if not payload:
                continue
            message_id = payload.get("message_id")
            if not isinstance(message_id, str) or not message_id:
                continue

            message_repository._upsert_group_message(  # noqa: SLF001
                payload,
                group_id,
                auto_commit=False,
            )
            migrated += 1

            existing = GroupMessageRecipient.query.filter_by(
                message_id=message_id,
                recipient_username=username,
            ).first()
            if existing is None:
                db.session.add(
                    GroupMessageRecipient(
                        message_id=message_id,
                        group_id=group_id,
                        recipient_username=username,
                    )
                )
                recipient_links += 1

    db.session.commit()
    return migrated, recipient_links


def main():
    app = create_app()
    with app.app_context():
        db.create_all()
        private_count = _backfill_private()
        group_count, recipient_count = _backfill_group()

        print("Durable chat backfill completed")
        print(f"Private payload rows processed: {private_count}")
        print(f"Group payload rows processed: {group_count}")
        print(f"Group recipient links created: {recipient_count}")


if __name__ == "__main__":
    main()
