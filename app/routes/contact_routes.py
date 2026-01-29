from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services import contact_service
from app.models.user_model import User
from app.extensions.redis_client import redis_client as r

contact_bp = Blueprint("contacts", __name__)

@contact_bp.route("", methods=["POST"])
@jwt_required()
def add_contact():
    data = request.json
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
    return jsonify({
        "contacts": contact_service.get_contacts(username)
    })

@contact_bp.route("/<username>/public-key", methods=["GET"])
@jwt_required()
def get_contact_public_key(username):
    requester = get_jwt_identity()

    # user وجود داشته باشه
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({"error": "User not found"}), 404

    # آیا تو contact list هست؟
    is_contact = r.sismember(f"contacts:{requester}", username)
    if not is_contact:
        return jsonify({"error": "Not in your contacts"}), 403

    return jsonify({"public_key": user.public_key})