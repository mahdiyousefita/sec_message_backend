from flask import Blueprint, request, jsonify
from flask_jwt_extended import get_jwt_identity, jwt_required
from app.services.post_service import (
    MediaStorageError,
    create_post_with_media,
    get_post,
    delete_post_by_username,
    get_posts,
)

post_bp = Blueprint("posts", __name__)


def _parse_followers_only(value):
    if value is None:
        return False

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off", ""}:
            return False

    raise ValueError("followers_only must be a boolean")


def _parse_include_total(value):
    if value is None:
        return True

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False

    raise ValueError("include_total must be a boolean")


@post_bp.route("/posts", methods=["POST"])
@jwt_required()
def create_post():
    username = get_jwt_identity()

    content_type = (request.content_type or "").lower()
    text = None
    files = []
    followers_only = False

    if "multipart/form-data" in content_type:
        text = request.form.get("text")
        try:
            followers_only = _parse_followers_only(request.form.get("followers_only"))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        files = (
            request.files.getlist("media")
            or request.files.getlist("media[]")
            or request.files.getlist("files")
        )
        single = request.files.get("file")
        if single:
            files.append(single)
    else:
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"error": "Invalid JSON body"}), 400
        text = data.get("text")
        try:
            followers_only = _parse_followers_only(data.get("followers_only"))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    try:
        result = create_post_with_media(username, text, files, followers_only=followers_only)
        return jsonify({
            "message": "Post created successfully",
            "post_id": result["post_id"]
        }), 201
    except MediaStorageError as e:
        return jsonify({"error": str(e)}), 503
    except ValueError as e:
        if str(e) == "Account suspended":
            return jsonify({"error": str(e)}), 403
        return jsonify({"error": str(e)}), 400



@post_bp.route("/posts", methods=["GET"])
@jwt_required(optional=True)
def list_posts():
    viewer_username = get_jwt_identity()
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=10, type=int)
    try:
        include_total = _parse_include_total(request.args.get("include_total"))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    data = get_posts(
        page,
        limit,
        viewer_username=viewer_username,
        include_total=include_total,
    )
    return jsonify(data), 200


@post_bp.route("/posts/<int:post_id>", methods=["GET"])
@jwt_required(optional=True)
def get_post_detail(post_id):
    viewer_username = get_jwt_identity()

    try:
        post = get_post(post_id, viewer_username=viewer_username)
        return jsonify({"post": post}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404


@post_bp.route("/posts/<int:post_id>", methods=["DELETE"])
@jwt_required()
def delete_post(post_id):
    username = get_jwt_identity()

    try:
        delete_post_by_username(post_id=post_id, username=username)
        return jsonify({"message": "Post deleted", "post_id": post_id}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 403
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
