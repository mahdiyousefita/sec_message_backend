from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity

from app.services import comment_service
from app.services.comment_service import add_comment
from app.services import activity_notification_service
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
    if getattr(user, "is_suspended", False):
        return jsonify({"error": "Account suspended"}), 403

    try:
        comment = add_comment(
            author_id=user.id,
            post_id=post_id,
            text=text,
            parent_id=parent_id
        )

        try:
            activity_notification_service.notify_comment(
                actor_username=username,
                post_id=post_id,
                comment_text=text,
                comment_id=comment.id,
                parent_comment_id=parent_id
            )
        except Exception:
            pass

        return jsonify({
            "id": comment.id,
            "message": "Comment created"
        }), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@comment_bp.route("/posts/<int:post_id>/comments", methods=["GET"])
def list_comments(post_id):
    page = request.args.get("page", default=1, type=int)
    page_size = request.args.get("page_size", default=10, type=int)

    try:
        comments = comment_service.get_comments_tree_by_post(post_id, page, page_size)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    return jsonify(comments), 200


@comment_bp.route("/comments/<int:comment_id>", methods=["DELETE"])
@jwt_required()
def delete_comment(comment_id):
    username = get_jwt_identity()

    try:
        comment_service.delete_comment_by_username(comment_id=comment_id, username=username)
        return jsonify({"message": "Comment deleted", "comment_id": comment_id}), 200
    except PermissionError as e:
        return jsonify({"error": str(e)}), 403
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
