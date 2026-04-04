from flask import current_app, request, session
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

    def _private_pending_batch_size():
        value = current_app.config.get("SOCKET_PENDING_PRIVATE_BATCH_SIZE", 100)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 100

    def _group_pending_batch_size():
        value = current_app.config.get("SOCKET_PENDING_GROUP_BATCH_SIZE", 100)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 100

    def _emit_pending_messages_chunk(username):
        batch_size = _private_pending_batch_size()
        pending = message_service.peek_messages_batch(username, limit=batch_size)
        if not pending:
            return 0

        remaining_count = message_service.get_pending_count(username)
        emit("pending_messages", {
            "messages": pending,
            "has_more": remaining_count > len(pending),
            "remaining_count": remaining_count,
        })
        logger.info(
            "Emitting pending_messages to %s: chunk=%d, remaining=%d",
            username,
            len(pending),
            remaining_count,
        )
        return len(pending)

    def _emit_pending_group_messages_chunk(username, group_id, group_name=None):
        batch_size = _group_pending_batch_size()
        pending_group = message_service.peek_group_messages_for_user(
            username, group_id, limit=batch_size
        )
        if not pending_group:
            return 0

        remaining_count = message_service.get_group_pending_count(username, group_id)
        emit("pending_group_messages", {
            "group_id": group_id,
            "group_name": group_name or "Group Chat",
            "messages": pending_group,
            "has_more": remaining_count > len(pending_group),
            "remaining_count": remaining_count,
        })
        logger.info(
            "Emitting pending_group_messages to %s for group %s: chunk=%d, remaining=%d",
            username,
            group_id,
            len(pending_group),
            remaining_count,
        )
        return len(pending_group)

    def _emit_pending_delete_events(username):
        pending_events = message_service.pop_message_deletion_events(username)
        for event in pending_events:
            if not isinstance(event, dict):
                continue
            event_name = event.get("event")
            payload = event.get("payload")
            if not event_name or not isinstance(payload, dict):
                continue
            emit(event_name, payload)

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

        _emit_pending_messages_chunk(username)
        _emit_pending_delete_events(username)

        try:
            from app.repositories import user_repository, group_repository

            user = user_repository.get_by_username(username)
            if user:
                user_groups = group_repository.get_groups_for_user(user.id)
                for group in user_groups:
                    _emit_pending_group_messages_chunk(
                        username=username,
                        group_id=group.id,
                        group_name=group.name,
                    )
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

        normalized_message_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        if not normalized_message_ids:
            emit("message_error", {"error": "message_ids must contain valid ids"})
            return

        pending_messages = message_service.peek_messages(username)
        pending_by_id = {}
        requested_ids = set(normalized_message_ids)
        for payload in pending_messages:
            if not isinstance(payload, dict):
                continue
            message_id = payload.get("message_id")
            if message_id in requested_ids:
                pending_by_id[message_id] = payload

        removed = message_service.ack_messages(username, normalized_message_ids)
        seen_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        for message_id in normalized_message_ids:
            payload = pending_by_id.get(message_id)
            if not payload:
                continue
            sender = payload.get("from")
            if not sender or sender == username:
                continue
            message_service.mark_private_message_seen(sender, username, message_id)
            seen_payload = {
                "chat_id": username,
                "message_id": message_id,
                "seen_by": username,
                "seen_at": seen_at,
            }
            socketio.emit(
                "message_seen",
                seen_payload,
                room=sender,
            )
            if not is_user_online(sender):
                message_service.queue_message_deletion_event(
                    sender,
                    "message_seen",
                    seen_payload,
                )

        emit("ack_confirmed", {"removed": removed})
        _emit_pending_messages_chunk(username)

    @socketio.on("get_seen_messages")
    def handle_get_seen_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        chat_id = (data.get("chat_id") or "").strip()
        message_ids = data.get("message_ids")
        if not chat_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "chat_id and message_ids are required"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        seen_ids = message_service.get_private_seen_message_ids(
            sender=username,
            recipient=chat_id,
            message_ids=normalized_ids,
        )
        emit(
            "seen_messages_status",
            {
                "chat_id": chat_id,
                "message_ids": seen_ids,
            },
        )

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
            message_service.store_group_message_metadata(payload, group_id)

            room_name = f"group_{group_id}"
            socketio.emit("new_group_message", payload, room=room_name)
            logger.debug(
                "Emitting new_group_message to room %s from %s",
                room_name, sender,
            )

            member_usernames = group_repository.get_group_member_usernames(group_id)
            group = group_repository.get_group_by_id(group_id)
            for member_username in member_usernames:
                if member_username != sender:
                    socketio.emit("new_notification", {
                        "from": sender,
                        "group_id": group_id,
                        "group_name": group.name if group else "Group Chat",
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

        from app.repositories import group_repository, message_repository
        normalized_message_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        if not normalized_message_ids:
            emit("message_error", {"error": "message_ids must contain valid ids"})
            return

        pending_messages = message_repository.peek_group_messages_for_user(username, group_id)
        pending_by_id = {}
        requested_ids = set(normalized_message_ids)
        for payload in pending_messages:
            if not isinstance(payload, dict):
                continue
            message_id = payload.get("message_id")
            if message_id in requested_ids:
                pending_by_id[message_id] = payload

        removed = message_repository.ack_group_messages(username, group_id, normalized_message_ids)
        seen_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        for message_id in normalized_message_ids:
            payload = pending_by_id.get(message_id)
            if not payload:
                continue
            sender = payload.get("from")
            if not sender or sender == username:
                continue
            message_service.mark_group_message_seen(group_id, message_id)
            socketio.emit(
                "group_message_seen",
                {
                    "group_id": group_id,
                    "message_id": message_id,
                    "seen_by": username,
                    "seen_at": seen_at,
                },
                room=sender,
            )

        emit("ack_group_confirmed", {"group_id": group_id, "removed": removed})
        group = group_repository.get_group_by_id(group_id)
        _emit_pending_group_messages_chunk(
            username=username,
            group_id=group_id,
            group_name=group.name if group else "Group Chat",
        )

    @socketio.on("get_group_seen_messages")
    def handle_get_group_seen_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_ids = data.get("message_ids")
        if not group_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "group_id and message_ids are required"})
            return

        from app.repositories import user_repository, group_repository

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        seen_ids = message_service.get_group_seen_message_ids(group_id, normalized_ids)
        emit(
            "group_seen_messages_status",
            {
                "group_id": group_id,
                "message_ids": seen_ids,
            },
        )

    @socketio.on("delete_message")
    def handle_delete_message(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        message_id = (data.get("message_id") or "").strip()
        chat_id = (data.get("chat_id") or "").strip()
        if not message_id or not chat_id:
            emit("message_error", {"error": "message_id and chat_id are required"})
            return

        meta = message_service.get_message_metadata(message_id)
        if not meta:
            counterpart = chat_id
        else:
            sender = meta.get("sender")
            recipient = meta.get("recipient")
            if username not in {sender, recipient}:
                emit("message_error", {"error": "Not allowed to delete this message"})
                return
            counterpart = recipient if sender == username else sender

        if not counterpart:
            emit("message_error", {"error": "Unable to resolve message recipient"})
            return

        message_service.ack_messages(counterpart, [message_id])
        message_service.mark_private_message_deleted(
            username=counterpart,
            chat_id=username,
            message_id=message_id,
        )
        payload = {
            "chat_id": username,
            "message_id": message_id,
            "deleted_by": username,
            "deleted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        socketio.emit("message_deleted", payload, room=counterpart)
        message_service.queue_message_deletion_event(counterpart, "message_deleted", payload)
        message_service.delete_message_metadata(message_id)
        emit("message_delete_confirmed", {"message_id": message_id, "chat_id": counterpart})

    @socketio.on("get_deleted_messages")
    def handle_get_deleted_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        chat_id = (data.get("chat_id") or "").strip()
        message_ids = data.get("message_ids")
        if not chat_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "chat_id and message_ids are required"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        deleted_ids = message_service.get_private_deleted_message_ids(
            username=username,
            chat_id=chat_id,
            message_ids=normalized_ids,
        )
        emit(
            "deleted_messages_status",
            {
                "chat_id": chat_id,
                "message_ids": deleted_ids,
            },
        )

    @socketio.on("delete_group_message")
    def handle_delete_group_message(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_id = (data.get("message_id") or "").strip()
        if not group_id or not message_id:
            emit("message_error", {"error": "group_id and message_id are required"})
            return

        from app.repositories import user_repository, group_repository, message_repository

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        meta = message_service.get_message_metadata(message_id)
        if not meta or int(meta.get("group_id") or -1) != int(group_id):
            emit("message_error", {"error": "Message not found in this group"})
            return
        if meta.get("sender") != username:
            emit("message_error", {"error": "Only sender can delete for everyone"})
            return

        member_usernames = group_repository.get_group_member_usernames(group_id)
        for member_username in member_usernames:
            message_repository.ack_group_messages(member_username, group_id, [message_id])

        payload = {
            "group_id": group_id,
            "message_id": message_id,
            "deleted_by": username,
            "deleted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        room_name = f"group_{group_id}"
        socketio.emit("group_message_deleted", payload, room=room_name)

        for member_username in member_usernames:
            if member_username == username:
                continue
            message_service.mark_group_message_deleted(
                username=member_username,
                group_id=group_id,
                message_id=message_id,
            )
            message_service.queue_message_deletion_event(
                member_username,
                "group_message_deleted",
                payload,
            )

        message_service.delete_message_metadata(message_id)
        emit("group_message_delete_confirmed", {"group_id": group_id, "message_id": message_id})

    @socketio.on("get_group_deleted_messages")
    def handle_get_group_deleted_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_ids = data.get("message_ids")
        if not group_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "group_id and message_ids are required"})
            return

        from app.repositories import user_repository, group_repository

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        deleted_ids = message_service.get_group_deleted_message_ids(
            username=username,
            group_id=group_id,
            message_ids=normalized_ids,
        )
        emit(
            "group_deleted_messages_status",
            {
                "group_id": group_id,
                "message_ids": deleted_ids,
            },
        )

    _registered = True
