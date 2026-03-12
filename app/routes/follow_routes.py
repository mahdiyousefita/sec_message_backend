from flask import Blueprint, jsonify, request
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


@follow_bp.route("/follows/<username>/status", methods=["GET"])
@jwt_required()
def follow_status(username):
    requester = get_jwt_identity()

    try:
        is_following = follow_service.get_follow_status_by_username(requester, username)
    except ValueError as e:
        error = str(e)
        if error == "User not found":
            return jsonify({"error": error}), 404
        return jsonify({"error": error}), 400

    return jsonify({"is_following": is_following}), 200


@follow_bp.route("/follows/<username>/followers", methods=["GET"])
def get_followers(username):
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=20, type=int)

    try:
        data = follow_service.get_followers_by_username(username, page, limit)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    return jsonify(data), 200


@follow_bp.route("/follows/<username>/following", methods=["GET"])
def get_following(username):
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=20, type=int)

    try:
        data = follow_service.get_following_page_by_username(username, page, limit)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    return jsonify(data), 200
