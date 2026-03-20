from flask import Blueprint, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services import notification_service

notification_bp = Blueprint("notifications", __name__)


@notification_bp.route("/unread", methods=["GET"])
@jwt_required()
def unread():
    username = get_jwt_identity()
    summary = notification_service.get_unread_summary(username)
    return jsonify(summary)
