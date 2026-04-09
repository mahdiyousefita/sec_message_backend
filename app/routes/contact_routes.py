from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services import contact_service
from app.services import follow_service
from app.services import block_service
from app.repositories import message_repository
from app.models.user_model import User
from app.extensions.redis_client import redis_client as r

contact_bp = Blueprint("contacts", __name__)

@contact_bp.route("", methods=["POST"])
@jwt_required()
def add_contact():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    username = get_jwt_identity()

    try:
        contact_service.add_contact(username, data.get("contact"))
        return jsonify({"message": "Contact added"}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

@contact_bp.route("", methods=["GET"])
@jwt_required()
def get_contacts():
    username = get_jwt_identity()
    detailed = request.args.get("detailed", "").strip().lower()
    if detailed in ("1", "true", "yes"):
        try:
            page = int(request.args.get("page", 1))
        except (TypeError, ValueError):
            page = 1
        try:
            limit = int(request.args.get("limit", 20))
        except (TypeError, ValueError):
            limit = 20

        result = contact_service.get_contacts_with_message_status(
            username, page=page, limit=limit
        )
        return jsonify(result)
    else:
        return jsonify({
            "contacts": contact_service.get_contacts(username)
        })

@contact_bp.route("/<username>/public-key", methods=["GET"])
@jwt_required()
def get_contact_public_key(username):
    requester = get_jwt_identity()

    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({"error": "User not found"}), 404

    if block_service.users_have_block_relation(requester, username):
        return jsonify({"error": "You cannot message this user"}), 403

    is_contact = follow_service.is_user_following(requester, username)
    has_conversation_history = (
        message_repository.get_contact_timestamp_score(requester, username) is not None
        or message_repository.get_contact_timestamp_score(username, requester) is not None
    )
    if not is_contact and not has_conversation_history:
        return jsonify({"error": "Not in your contacts"}), 403

    return jsonify({"public_key": user.public_key})
