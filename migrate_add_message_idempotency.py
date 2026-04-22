#!/usr/bin/env python3
"""
Adds durable client_message_id columns and idempotency unique indexes for chat sends.
"""

from sqlalchemy import inspect, text

from app import create_app
from app.db import db


def _ensure_private_message_idempotency():
    inspector = inspect(db.engine)
    if not inspector.has_table("private_messages"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("private_messages")
    }
    if "client_message_id" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE private_messages "
                "ADD COLUMN client_message_id VARCHAR(128)"
            )
        )

    db.session.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "ux_private_messages_sender_recipient_client_message_id "
            "ON private_messages (sender_username, recipient_username, client_message_id)"
        )
    )


def _ensure_group_message_idempotency():
    inspector = inspect(db.engine)
    if not inspector.has_table("group_messages"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("group_messages")
    }
    if "client_message_id" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE group_messages "
                "ADD COLUMN client_message_id VARCHAR(128)"
            )
        )

    db.session.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "ux_group_messages_sender_group_client_message_id "
            "ON group_messages (sender_username, group_id, client_message_id)"
        )
    )


def main():
    app = create_app()
    with app.app_context():
        _ensure_private_message_idempotency()
        _ensure_group_message_idempotency()
        db.session.commit()
        print("Applied chat send idempotency migration.")


if __name__ == "__main__":
    main()
