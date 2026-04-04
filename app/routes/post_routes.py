from flask import Blueprint, request, jsonify
from flask_jwt_extended import get_jwt_identity, jwt_required
from app.services.post_service import (
    MediaStorageError,
    create_post_with_media,
    get_posts,
)

post_bp = Blueprint("posts", __name__)

@post_bp.route("/posts", methods=["POST"])
@jwt_required()
def create_post():
    username = get_jwt_identity()

    content_type = (request.content_type or "").lower()
    text = None
    files = []

    if "multipart/form-data" in content_type:
        text = request.form.get("text")
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
        result = create_post_with_media(username, text, files)
        return jsonify({
            "message": "Post created successfully",
            "post_id": result["post_id"]
        }), 201
    except MediaStorageError as e:
        return jsonify({"error": str(e)}), 503
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@post_bp.route("/posts", methods=["GET"])
@jwt_required(optional=True)
def list_posts():
    viewer_username = get_jwt_identity()
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=10, type=int)

    data = get_posts(page, limit, viewer_username=viewer_username)
    return jsonify(data), 200
