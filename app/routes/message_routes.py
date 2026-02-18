from flask import Blueprint, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services import message_service

message_bp = Blueprint("messages", __name__)

@message_bp.route("/send", methods=["POST"])
@jwt_required()
def send():
    return jsonify({
        "error": "HTTP send is deprecated. Use socket event 'send_message'."
    }), 410

@message_bp.route("/inbox", methods=["GET"])
@jwt_required()
def inbox():
    username = get_jwt_identity()
    return jsonify({
        "messages": message_service.receive_messages(username)
    })
