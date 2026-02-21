from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from app.services import profile_service


profile_bp = Blueprint("profiles", __name__)


@profile_bp.route("/profiles/me", methods=["GET"])
@jwt_required()
def get_my_profile():
    username = get_jwt_identity()
    try:
        return jsonify(profile_service.get_profile_by_username(username)), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404


@profile_bp.route("/profiles/me", methods=["PUT"])
@jwt_required()
def update_my_profile():
    username = get_jwt_identity()

    content_type = (request.content_type or "").lower()
    name = None
    bio = None
    profile_image = None

    if "multipart/form-data" in content_type:
        name = request.form.get("name")
        bio = request.form.get("bio")
        profile_image = request.files.get("profile_image") or request.files.get(
            "avatar"
        )
    else:
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"error": "Invalid JSON body"}), 400
        name = data.get("name")
        bio = data.get("bio")

    try:
        profile = profile_service.update_profile(
            username=username,
            name=name,
            bio=bio,
            profile_image=profile_image,
        )
        return jsonify(profile), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@profile_bp.route("/profiles/<username>", methods=["GET"])
def get_profile(username):
    try:
        return jsonify(profile_service.get_profile_by_username(username)), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404


@profile_bp.route("/profiles/<username>/posts", methods=["GET"])
def get_profile_posts(username):
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=10, type=int)

    try:
        data = profile_service.get_profile_posts(username, page, limit)
        return jsonify(data), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

