from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services import message_service

message_bp = Blueprint("messages", __name__)


@message_bp.route("/send", methods=["POST"])
@jwt_required()
def send():
    return jsonify({
        "error": "HTTP send is deprecated. Use socket event 'send_message'."
    }), 410


@message_bp.route("/attachments", methods=["POST"])
@jwt_required()
def upload_attachment():
    username = get_jwt_identity()
    file = request.files.get("file") or request.files.get("attachment")

    try:
        payload = message_service.upload_message_attachment(username, file)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except message_service.MessageAttachmentStorageError as exc:
        return jsonify({"error": str(exc)}), 503

    return jsonify({"attachment": payload}), 201


@message_bp.route("/inbox", methods=["GET"])
@jwt_required()
def inbox():
    username = get_jwt_identity()
    return jsonify({
        "messages": message_service.receive_messages(username)
    })
