from flask import request, session
from flask_jwt_extended import decode_token
from flask_socketio import emit, join_room

from app.extensions.extensions import socketio
from app.services import message_service

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

        pending = message_service.receive_messages(username)
        if pending:
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

        should_persist = not is_user_online(recipient)

        try:
            payload = message_service.send_message(
                sender,
                recipient,
                encrypted_message,
                encrypted_key,
                persist=should_persist,
                attachment=attachment,
                message_type=message_type,
            )
        except ValueError as exc:
            emit("message_error", {"error": str(exc)})
            return

        socketio.emit("new_message", payload, room=recipient)
        emit(
            "message_sent",
            {"to": recipient, "timestamp": payload["timestamp"], "type": payload["type"]}
        )

    _registered = True
