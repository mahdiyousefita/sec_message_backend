from datetime import datetime

from app.db import db


class PrivateMessage(db.Model):
    __tablename__ = "private_messages"

    id = db.Column(db.Integer, primary_key=True)
    message_id = db.Column(db.String(64), nullable=False, unique=True, index=True)
    sender_username = db.Column(db.String(80), nullable=False, index=True)
    recipient_username = db.Column(db.String(80), nullable=False, index=True)
    client_message_id = db.Column(db.String(128), nullable=True, index=True)
    message_type = db.Column(db.String(24), nullable=False, default="text")
    encrypted_message = db.Column(db.Text, nullable=True)
    encrypted_key = db.Column(db.Text, nullable=True)
    sender_encrypted_message = db.Column(db.Text, nullable=True)
    sender_encrypted_key = db.Column(db.Text, nullable=True)
    attachment_json = db.Column(db.Text, nullable=True)
    reply_to_message_id = db.Column(db.String(64), nullable=True)
    reply_to_sender = db.Column(db.String(80), nullable=True)
    encrypted_reply_preview = db.Column(db.Text, nullable=True)
    encrypted_reply_key = db.Column(db.Text, nullable=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    delivered_at = db.Column(db.DateTime, nullable=True, index=True)
    seen_at = db.Column(db.DateTime, nullable=True, index=True)
    deleted_for_everyone = db.Column(db.Boolean, nullable=False, default=False, index=True)
    deleted_at = db.Column(db.DateTime, nullable=True)

    __table_args__ = (
        db.Index(
            "ux_private_messages_sender_recipient_client_message_id",
            "sender_username",
            "recipient_username",
            "client_message_id",
            unique=True,
        ),
        db.Index(
            "ix_private_messages_recipient_pending",
            "recipient_username",
            "delivered_at",
            "timestamp",
        ),
        db.Index(
            "ix_private_messages_conversation",
            "sender_username",
            "recipient_username",
            "timestamp",
        ),
    )


class GroupMessage(db.Model):
    __tablename__ = "group_messages"

    id = db.Column(db.Integer, primary_key=True)
    message_id = db.Column(db.String(64), nullable=False, unique=True, index=True)
    group_id = db.Column(db.Integer, nullable=False, index=True)
    sender_username = db.Column(db.String(80), nullable=False, index=True)
    client_message_id = db.Column(db.String(128), nullable=True, index=True)
    group_key_ref = db.Column(db.String(128), nullable=True, index=True)
    message_type = db.Column(db.String(24), nullable=False, default="text")
    encrypted_message = db.Column(db.Text, nullable=True)
    sender_encrypted_key = db.Column(db.Text, nullable=True)
    encrypted_keys_json = db.Column(db.Text, nullable=True)
    attachment_json = db.Column(db.Text, nullable=True)
    reply_to_message_id = db.Column(db.String(64), nullable=True)
    reply_to_sender = db.Column(db.String(80), nullable=True)
    encrypted_reply_preview = db.Column(db.Text, nullable=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    seen_at = db.Column(db.DateTime, nullable=True, index=True)
    deleted_for_everyone = db.Column(db.Boolean, nullable=False, default=False, index=True)
    deleted_at = db.Column(db.DateTime, nullable=True)

    __table_args__ = (
        db.Index(
            "ux_group_messages_sender_group_client_message_id",
            "sender_username",
            "group_id",
            "client_message_id",
            unique=True,
        ),
        db.Index(
            "ix_group_messages_group_timestamp",
            "group_id",
            "timestamp",
        ),
    )


class GroupMessageRecipient(db.Model):
    __tablename__ = "group_message_recipients"

    id = db.Column(db.Integer, primary_key=True)
    message_id = db.Column(db.String(64), nullable=False, index=True)
    group_id = db.Column(db.Integer, nullable=False, index=True)
    recipient_username = db.Column(db.String(80), nullable=False, index=True)
    encrypted_key = db.Column(db.Text, nullable=True)
    delivered_at = db.Column(db.DateTime, nullable=True, index=True)
    seen_at = db.Column(db.DateTime, nullable=True, index=True)

    __table_args__ = (
        db.UniqueConstraint(
            "message_id",
            "recipient_username",
            name="uq_group_message_recipient",
        ),
        db.Index(
            "ix_group_message_recipient_pending",
            "recipient_username",
            "group_id",
            "delivered_at",
        ),
        db.Index(
            "ix_group_message_recipient_seen",
            "recipient_username",
            "group_id",
            "seen_at",
        ),
    )


class GroupMessageKeyRecipient(db.Model):
    __tablename__ = "group_message_key_recipients"

    id = db.Column(db.Integer, primary_key=True)
    group_id = db.Column(db.Integer, nullable=False, index=True)
    sender_username = db.Column(db.String(80), nullable=False, index=True)
    group_key_ref = db.Column(db.String(128), nullable=False, index=True)
    recipient_username = db.Column(db.String(80), nullable=False, index=True)
    encrypted_key = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)

    __table_args__ = (
        db.UniqueConstraint(
            "group_id",
            "sender_username",
            "group_key_ref",
            "recipient_username",
            name="uq_group_message_key_recipient",
        ),
        db.Index(
            "ix_group_message_key_lookup",
            "group_id",
            "sender_username",
            "group_key_ref",
        ),
    )
