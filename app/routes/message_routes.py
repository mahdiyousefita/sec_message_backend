from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services import message_service

message_bp = Blueprint("messages", __name__)

@message_bp.route("/send", methods=["POST"])
@jwt_required()
def send():
    data = request.json
    sender = get_jwt_identity()

    try:
        message_service.send_message(
            sender,
            data.get("to"),
            data.get("message"),
            data.get("encrypted_key")
        )
        return jsonify({"message": "Message sent"}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

@message_bp.route("/inbox", methods=["GET"])
@jwt_required()
def inbox():
    username = get_jwt_identity()
    return jsonify({
        "messages": message_service.receive_messages(username)
    })
