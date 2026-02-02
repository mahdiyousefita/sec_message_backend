from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity

from app.repositories.vote_repository import upsert_vote
from app.schemas.comment_schema import CommentResponseSchema
from app.services import comment_service
from app.services.comment_service import add_comment, get_post_comments
from app.models.user_model import User


comment_bp = Blueprint("comments", __name__)

@comment_bp.route("/posts/<int:post_id>/comments", methods=["POST"])
@jwt_required()
def create_comment(post_id):
    username = get_jwt_identity()
    text = request.json.get("text")
    parent_id = request.json.get("parent_id")

    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({"error": "User not found"}), 404

    try:
        comment = add_comment(
            author_id=user.id,
            post_id=post_id,
            text=text,
            parent_id=parent_id
        )
        return jsonify({
            "id": comment.id,
            "message": "Comment created"
        }), 201

    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@comment_bp.route("/posts/<int:post_id>/comments", methods=["GET"])
def list_comments(post_id):
    comments = get_post_comments(post_id)
    return jsonify(comments), 200


@comment_bp.route("/posts/<int:post_id>/comments", methods=["GET"])
def get_post_comments(post_id):
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 10))

    comments = comment_service.get_comments_tree_by_post(
        post_id, page, page_size)

    return CommentResponseSchema(many=True).dump(comments), 200
