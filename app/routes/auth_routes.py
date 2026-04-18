from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity

from app.services import auth_service
from app.extensions.extensions import socketio



auth_bp = Blueprint("auth", __name__)


def _auth_error_response(error: Exception, fallback_status_code: int = 400):
    status_code = getattr(error, "status_code", fallback_status_code)
    return jsonify({"error": str(error)}), status_code


@auth_bp.route("/register", methods=["POST"])
def register():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    try:
        auth_service.register(
            data.get("username"),
            data.get("password"),
            data.get("public_key"),
            data.get("name"),
        )
        return jsonify({"message": "User registered"}), 201
    except ValueError as e:
        return _auth_error_response(e)


@auth_bp.route("/register/start", methods=["POST"])
def register_start():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    try:
        payload = auth_service.start_registration(
            data.get("username"),
            data.get("password"),
            data.get("public_key"),
            data.get("name"),
            data.get("client_nonce"),
        )
        return jsonify(
            {
                "message": "Registration pending confirmation",
                "registration_id": payload["registration_id"],
                "expires_in_seconds": payload["expires_in_seconds"],
            }
        ), 202
    except ValueError as e:
        return _auth_error_response(e)


@auth_bp.route("/register/confirm", methods=["POST"])
def register_confirm():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    try:
        auth_service.confirm_registration(data.get("registration_id"))
        return jsonify({"message": "User registered"}), 201
    except ValueError as e:
        return _auth_error_response(e)


@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    try:
        tokens = auth_service.login(
            data.get("username"),
            data.get("password")
        )
        return jsonify(tokens), 200
    except ValueError as e:
        return _auth_error_response(e, fallback_status_code=401)

@auth_bp.route("/logout", methods=["POST"])
@jwt_required(optional=True)
def logout():
    # Tokens are stateless JWTs; backend acknowledges logout and client clears local/session state.
    return jsonify({"message": "Logged out"}), 200

@auth_bp.route("/refresh", methods=["POST"])
@auth_bp.route("/token", methods=["POST"])
@jwt_required(refresh=True)
def refresh_token():
    username = get_jwt_identity()
    try:
        return jsonify(auth_service.refresh_access_token(username)), 200
    except ValueError as e:
        return _auth_error_response(e, fallback_status_code=401)


@auth_bp.route("/keys/rotate", methods=["POST"])
@jwt_required()
def rotate_public_key():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    username = get_jwt_identity()

    try:
        result = auth_service.rotate_public_key(
            username=username,
            public_key=data.get("public_key"),
        )
    except ValueError as e:
        return _auth_error_response(e)

    event_payload = {
        "username": result["username"],
        "group_ids": result["group_ids"],
    }

    for recipient in result["notify_usernames"]:
        socketio.emit("user_public_key_updated", event_payload, room=recipient)

    for group_id in result["group_ids"]:
        socketio.emit(
            "group_member_key_updated",
            {
                "username": result["username"],
                "group_id": group_id,
            },
            room=f"group_{group_id}",
        )

    return jsonify({"message": "Public key updated"}), 200


@auth_bp.route("/keys/status", methods=["GET"])
@jwt_required()
def get_key_status():
    username = get_jwt_identity()
    try:
        status = auth_service.get_key_status(username=username)
    except ValueError as e:
        return _auth_error_response(e, fallback_status_code=401)
    return jsonify(status), 200
