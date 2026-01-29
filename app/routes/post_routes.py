from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services.post_service import create_post_with_media, get_posts

post_bp = Blueprint("posts", __name__)

@post_bp.route("/posts", methods=["POST"])
@jwt_required()
def create_post():
    username = get_jwt_identity()
    text = request.form.get("text")
    files = request.files.getlist("media")

    try:
        result = create_post_with_media(username, text, files)
        return jsonify({
            "message": "Post created successfully",
            "post_id": result["post_id"]
        }), 201

    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    except Exception:
        return jsonify({"error": "Internal server error"}), 500


@post_bp.route("/posts", methods=["GET"])
@jwt_required()
def list_posts():
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=10, type=int)

    data = get_posts(page, limit)
    return jsonify(data), 200