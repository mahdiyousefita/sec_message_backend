from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from app.services import profile_service


profile_bp = Blueprint("profiles", __name__)


@profile_bp.route("/profiles/me", methods=["GET"])
@jwt_required()
def get_my_profile():
    username = get_jwt_identity()
    try:
        return jsonify(
            profile_service.get_profile_by_username(
                username=username,
                viewer_username=username,
            )
        ), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404


@profile_bp.route("/profiles/me", methods=["PUT"])
@jwt_required()
def update_my_profile():
    username = get_jwt_identity()

    content_type = (request.content_type or "").lower()
    name = None
    bio = None
    profile_image_shape = None
    profile_image = None
    profile_video = None

    if "multipart/form-data" in content_type:
        name = request.form.get("name")
        bio = request.form.get("bio")
        profile_image_shape = request.form.get("profile_image_shape")
        profile_image = request.files.get("profile_image") or request.files.get(
            "avatar"
        )
        profile_video = request.files.get("profile_video") or request.files.get(
            "video"
        )
    else:
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"error": "Invalid JSON body"}), 400
        name = data.get("name")
        bio = data.get("bio")
        profile_image_shape = data.get("profile_image_shape")

    try:
        profile = profile_service.update_profile(
            username=username,
            name=name,
            bio=bio,
            profile_image_shape=profile_image_shape,
            profile_image=profile_image,
            profile_video=profile_video,
        )
        return jsonify(profile), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@profile_bp.route("/profiles/me", methods=["DELETE"])
@jwt_required()
def delete_my_account():
    username = get_jwt_identity()
    try:
        profile_service.delete_account(username=username)
        return jsonify({"message": "Account deleted permanently"}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404


@profile_bp.route("/profiles/<username>", methods=["GET"])
@jwt_required(optional=True)
def get_profile(username):
    viewer_username = get_jwt_identity()
    try:
        return jsonify(
            profile_service.get_profile_by_username(
                username=username,
                viewer_username=viewer_username,
            )
        ), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404


@profile_bp.route("/profiles/<username>/posts", methods=["GET"])
@jwt_required(optional=True)
def get_profile_posts(username):
    viewer_username = get_jwt_identity()
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=10, type=int)

    try:
        data = profile_service.get_profile_posts(
            username=username,
            page=page,
            limit=limit,
            viewer_username=viewer_username,
        )
        return jsonify(data), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
