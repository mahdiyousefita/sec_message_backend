from flask import request, session
import logging
from datetime import datetime, timezone
from flask_jwt_extended import decode_token
from flask_socketio import emit, join_room

from app.extensions.extensions import socketio 
from app.services import message_service
from app.services import activity_notification_service

logger = logging.getLogger(__name__)

_registered = False
_online_users = {}


def _extract_access_token(auth):
    if isinstance(auth, dict):
        token = auth.get("token") or auth.get("access_token")
        if token:
            return token

    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header.split(" ", 1)[1].strip()

    return request.args.get("token")


def _set_user_online(username):
    _online_users[username] = _online_users.get(username, 0) + 1


def _set_user_offline(username):
    current_count = _online_users.get(username, 0)
    if current_count <= 1:
        _online_users.pop(username, None)
        return
    _online_users[username] = current_count - 1


def is_user_online(username):
    return _online_users.get(username, 0) > 0


def register_socket_events():
    global _registered
    if _registered:
        return

    @socketio.on("connect")
    def handle_connect(auth):
        token = _extract_access_token(auth)
        if not token:
            return False

        try:
            claims = decode_token(token)
        except Exception:
            return False

        username = claims.get("sub")
        if not username:
            return False

        session["username"] = username
        join_room(username)
        _set_user_online(username)

        pending = message_service.peek_messages(username)
        if pending:
            logger.info(
                "Emitting pending_messages to %s: count=%d, types=%s, sample=%s",
                username,
                len(pending),
                [type(m).__name__ for m in pending[:3]],
                str(pending[0])[:300] if pending else "n/a",
            )
            emit("pending_messages", {"messages": pending})

        try:
            from app.repositories import user_repository, group_repository
            from app.repositories import message_repository

            user = user_repository.get_by_username(username)
            if user:
                user_groups = group_repository.get_groups_for_user(user.id)
                for group in user_groups:
                    group_id = group.id
                    pending_group = message_repository.peek_group_messages_for_user(
                        username, group_id
                    )
                    if pending_group:
                        logger.info(
                            "Emitting pending_group_messages to %s for group %s: count=%d",
                            username, group_id, len(pending_group),
                        )
                        emit("pending_group_messages", {
                            "group_id": group_id,
                            "messages": pending_group,
                        })
        except Exception as exc:
            logger.warning("Failed to emit pending group messages for %s: %s", username, exc)

        try:
            unread_count = activity_notification_service.get_unread_count(username)
            if unread_count > 0:
                data = activity_notification_service.get_activity_notifications(
                    username, page=1, limit=10, unread_only=True
                )
                emit("pending_activity_notifications", {
                    "unread_count": unread_count,
                    "notifications": data.get("notifications", []),
                })
                logger.info(
                    "Emitting pending_activity_notifications to %s: count=%d",
                    username, unread_count,
                )
        except Exception as exc:
            logger.warning("Failed to emit pending activity notifications for %s: %s", username, exc)

        emit("connected", {"username": username})
        socketio.emit(
            "user_status",
            {"username": username, "online": True},
            skip_sid=request.sid,
        )

    @socketio.on("disconnect")
    def handle_disconnect():
        username = session.get("username")
        if username:
            _set_user_offline(username)
            socketio.emit("user_status", {"username": username, "online": False})

    @socketio.on("get_user_status")
    def handle_get_user_status(data):
        if not isinstance(data, dict) or not data.get("username"):
            emit("message_error", {"error": "Invalid payload"})
            return

        target_username = data.get("username")
        emit(
            "user_status",
            {"username": target_username, "online": is_user_online(target_username)},
        )

    @socketio.on("send_message")
    def handle_send_message(data):
        sender = session.get("username")
        if not sender:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        recipient = data.get("to")
        encrypted_message = data.get("message")
        encrypted_key = data.get("encrypted_key")
        attachment = data.get("attachment")
        message_type = data.get("type")
        reply_to_message_id = data.get("reply_to_message_id")
        reply_to_sender = data.get("reply_to_sender")
        encrypted_reply_preview = data.get("encrypted_reply_preview")
        encrypted_reply_key = data.get("encrypted_reply_key")

        if attachment is not None and not isinstance(attachment, dict):
            emit("message_error", {"error": "Invalid attachment payload"})
            return

        try:
            payload = message_service.send_message(
                sender,
                recipient,
                encrypted_message,
                encrypted_key,
                attachment=attachment,
                message_type=message_type,
                reply_to_message_id=reply_to_message_id,
                reply_to_sender=reply_to_sender,
                encrypted_reply_preview=encrypted_reply_preview,
                encrypted_reply_key=encrypted_reply_key,
            )
        except ValueError as exc:
            emit("message_error", {"error": str(exc)})
            return

        socketio.emit("new_message", payload, room=recipient)
        logger.debug(
            "Emitting new_message to %s: type=%s, sample=%s",
            recipient,
            type(payload).__name__,
            str(payload)[:300],
        )

        socketio.emit("new_notification", {
            "from": sender,
            "type": payload.get("type", "text"),
            "timestamp": payload.get("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")),
            "message_id": payload.get("message_id", ""),
        }, room=recipient)
        logger.debug(
            "Emitting new_notification to %s: type=%s, sample=%s",
            recipient,
            type(payload).__name__,
            str(payload)[:300],
        )

        contacts_update = {
            "from": sender,
            "to": recipient,
            "timestamp": payload["timestamp"],
            "type": payload["type"],
        }
        socketio.emit("contacts_updated", contacts_update, room=recipient)
        emit("contacts_updated", contacts_update)

        emit(
            "message_sent",
            {
                "to": recipient,
                "message_id": payload["message_id"],
                "timestamp": payload["timestamp"],
                "type": payload["type"],
            },
        )

    @socketio.on("ack_messages")
    def handle_ack_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        message_ids = data.get("message_ids")
        if not isinstance(message_ids, list) or not message_ids:
            emit("message_error", {"error": "message_ids must be a non-empty list"})
            return

        removed = message_service.ack_messages(username, message_ids)
        emit("ack_confirmed", {"removed": removed})

    @socketio.on("join_group")
    def handle_join_group(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict) or not data.get("group_id"):
            emit("message_error", {"error": "group_id is required"})
            return

        group_id = data.get("group_id")

        try:
            from app.repositories import user_repository, group_repository
            user = user_repository.get_by_username(username)
            if not user or not group_repository.is_member(group_id, user.id):
                emit("message_error", {"error": "You are not a member of this group"})
                return
        except Exception as exc:
            logger.warning("join_group membership check failed for %s: %s", username, exc)
            emit("message_error", {"error": "Failed to verify group membership"})
            return

        room_name = f"group_{group_id}"
        join_room(room_name)
        emit("group_joined", {"group_id": group_id})
        logger.debug("User %s joined group room %s", username, room_name)

    @socketio.on("send_group_message")
    def handle_send_group_message(data):
        sender = session.get("username")
        if not sender:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_text = data.get("message")
        attachment = data.get("attachment")
        message_type = data.get("type")
        reply_to_message_id = data.get("reply_to_message_id")
        reply_to_sender = data.get("reply_to_sender")
        encrypted_reply_preview = data.get("encrypted_reply_preview")
        encrypted_keys = data.get("encrypted_keys")

        if not group_id:
            emit("message_error", {"error": "group_id is required"})
            return

        if not message_text and not attachment:
            emit("message_error", {"error": "Message or attachment is required"})
            return

        if not encrypted_keys or not isinstance(encrypted_keys, dict):
            emit("message_error", {"error": "encrypted_keys is required"})
            return

        if attachment is not None and not isinstance(attachment, dict):
            emit("message_error", {"error": "Invalid attachment payload"})
            return

        try:
            from app.repositories import user_repository, group_repository
            from app.repositories import message_repository

            user = user_repository.get_by_username(sender)
            if not user or not group_repository.is_member(group_id, user.id):
                emit("message_error", {"error": "You are not a member of this group"})
                return

            normalized_type = message_type
            if normalized_type:
                normalized_type = normalized_type.strip().lower()
                if normalized_type not in message_service.ALLOWED_MESSAGE_TYPES:
                    emit("message_error", {"error": "Invalid message type"})
                    return
            elif attachment and message_text:
                normalized_type = "mixed"
            elif attachment:
                normalized_type = attachment.get("type", "image")
            else:
                normalized_type = "text"

            payload = message_repository.build_group_message_payload(
                sender=sender,
                group_id=group_id,
                encrypted_message=message_text,
                attachment=attachment,
                message_type=normalized_type,
                reply_to_message_id=reply_to_message_id,
                reply_to_sender=reply_to_sender,
                encrypted_reply_preview=encrypted_reply_preview,
                encrypted_keys=encrypted_keys,
            )

            message_repository.record_group_conversation_timestamp(
                group_id, payload.get("timestamp")
            )

            room_name = f"group_{group_id}"
            socketio.emit("new_group_message", payload, room=room_name)
            logger.debug(
                "Emitting new_group_message to room %s from %s",
                room_name, sender,
            )

            member_usernames = group_repository.get_group_member_usernames(group_id)
            for member_username in member_usernames:
                if member_username != sender:
                    socketio.emit("new_notification", {
                        "from": sender,
                        "group_id": group_id,
                        "type": payload.get("type", "text"),
                        "timestamp": payload.get("timestamp", ""),
                        "message_id": payload.get("message_id", ""),
                    }, room=member_username)
                    message_repository.push_group_message_to_member(
                        group_id, member_username, payload
                    )

            emit("group_message_sent", {
                "group_id": group_id,
                "message_id": payload["message_id"],
                "timestamp": payload["timestamp"],
                "type": payload["type"],
            })

        except Exception as exc:
            logger.error("send_group_message error: %s", exc, exc_info=True)
            emit("message_error", {"error": "Failed to send group message"})

    @socketio.on("get_contacts_status")
    def handle_get_contacts_status(data=None):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        page = 1
        limit = 20
        if isinstance(data, dict):
            try:
                page = int(data.get("page", 1))
            except (TypeError, ValueError):
                page = 1
            try:
                limit = int(data.get("limit", 20))
            except (TypeError, ValueError):
                limit = 20

        from app.services import contact_service
        result = contact_service.get_contacts_with_message_status(
            username, page=page, limit=limit
        )
        emit("contacts_status", result)

    @socketio.on("ack_group_messages")
    def handle_ack_group_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_ids = data.get("message_ids")
        if not group_id:
            emit("message_error", {"error": "group_id is required"})
            return
        if not isinstance(message_ids, list) or not message_ids:
            emit("message_error", {"error": "message_ids must be a non-empty list"})
            return

        from app.repositories import message_repository
        removed = message_repository.ack_group_messages(username, group_id, message_ids)
        emit("ack_group_confirmed", {"group_id": group_id, "removed": removed})

    _registered = True
