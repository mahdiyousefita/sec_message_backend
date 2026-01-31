from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity

from app.services.vote_service import vote

vote_bp = Blueprint("votes", __name__)

@vote_bp.route("/votes", methods=["POST"])
@jwt_required()
def vote_route():
    username = get_jwt_identity()
    data = request.get_json()

    target_type = data.get("target_type")
    target_id = data.get("target_id")
    value = data.get("value")

    try:
        vote(
            username=username,
            target_type=target_type,
            target_id=target_id,
            value=value
        )
        return jsonify({"message": "Vote recorded"}), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
