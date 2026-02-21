from flask import Blueprint, jsonify
from flask_jwt_extended import get_jwt_identity, jwt_required

from app.services import follow_service


follow_bp = Blueprint("follows", __name__)


@follow_bp.route("/follows/<username>", methods=["POST"])
@jwt_required()
def follow_user(username):
    requester = get_jwt_identity()

    try:
        created = follow_service.follow_by_username(requester, username)
    except ValueError as e:
        error = str(e)
        if error == "User not found":
            return jsonify({"error": error}), 404
        return jsonify({"error": error}), 400

    return jsonify(
        {"message": "Followed"} if created else {"message": "Already following"}
    ), 200


@follow_bp.route("/follows/<username>", methods=["DELETE"])
@jwt_required()
def unfollow_user(username):
    requester = get_jwt_identity()

    try:
        removed = follow_service.unfollow_by_username(requester, username)
    except ValueError as e:
        error = str(e)
        if error == "User not found":
            return jsonify({"error": error}), 404
        return jsonify({"error": error}), 400

    return jsonify(
        {"message": "Unfollowed"} if removed else {"message": "Not following"}
    ), 200

