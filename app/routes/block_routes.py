from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from app.extensions.extensions import socketio
from app.services import block_service


block_bp = Blueprint("blocks", __name__)


@block_bp.route("/blocks/<username>", methods=["POST"])
@jwt_required()
def block_user(username):
    requester = get_jwt_identity()

    try:
        result = block_service.block_by_username(requester, username)
    except ValueError as e:
        error = str(e)
        if error == "User not found":
            return jsonify({"error": error}), 404
        return jsonify({"error": error}), 400

    if result["created"]:
        socketio.emit(
            "chat_blocked",
            {
                "blocked_by": requester,
                "chat_id": requester,
                "message": f"You have been blocked by {requester}.",
            },
            room=username,
        )

    return (
        jsonify(
            {
                "message": "User blocked",
                "blocked_username": result["blocked_username"],
                "created": result["created"],
            }
        ),
        201 if result["created"] else 200,
    )


@block_bp.route("/blocks/<username>", methods=["DELETE"])
@jwt_required()
def unblock_user(username):
    requester = get_jwt_identity()

    try:
        result = block_service.unblock_by_username(requester, username)
    except ValueError as e:
        error = str(e)
        if error == "User not found":
            return jsonify({"error": error}), 404
        return jsonify({"error": error}), 400

    return jsonify(
        {
            "message": "User unblocked" if result["removed"] else "User was not blocked",
            "unblocked_username": result["blocked_username"],
            "removed": result["removed"],
        }
    ), 200


@block_bp.route("/blocks", methods=["GET"])
@jwt_required()
def list_blocked_users():
    requester = get_jwt_identity()
    page = max(1, int(request.args.get("page", default=1, type=int) or 1))
    limit = max(1, int(request.args.get("limit", default=20, type=int) or 20))

    try:
        data = block_service.get_blocked_users_page(
            blocker_username=requester,
            page=page,
            limit=limit,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    return jsonify(data), 200
