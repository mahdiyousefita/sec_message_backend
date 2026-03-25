from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required, get_jwt_identity

from app.services import activity_notification_service

activity_notification_bp = Blueprint("activity_notifications", __name__)


@activity_notification_bp.route("", methods=["GET"])
@jwt_required()
def list_activity_notifications():
    username = get_jwt_identity()
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=20, type=int)

    try:
        data = activity_notification_service.get_activity_notifications(username, page, limit)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    return jsonify(data), 200


@activity_notification_bp.route("/unread-count", methods=["GET"])
@jwt_required()
def unread_count():
    username = get_jwt_identity()

    try:
        count = activity_notification_service.get_unread_count(username)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    return jsonify({"unread_count": count}), 200


@activity_notification_bp.route("/mark-read", methods=["POST"])
@jwt_required()
def mark_read():
    username = get_jwt_identity()
    data = request.get_json(silent=True) or {}
    notification_ids = data.get("notification_ids")

    try:
        updated = activity_notification_service.mark_notifications_read(
            username, notification_ids
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    return jsonify({"updated": updated}), 200
