from flask import request, session
import logging
from flask_jwt_extended import decode_token
from flask_socketio import emit, join_room

from app.extensions.extensions import socketio
from app.services import message_service

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

    @socketio.on("get_contacts_status")
    def handle_get_contacts_status():
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        from app.services import contact_service
        contacts = contact_service.get_contacts_with_message_status(username)
        emit("contacts_status", {"contacts": contacts})

    _registered = True
