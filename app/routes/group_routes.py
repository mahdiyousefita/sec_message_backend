from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity

from app.services import group_service
from app.services.group_service import (
    GroupLimitReachedError,
    NotMutualFollowError,
    NotGroupMemberError,
    NotGroupCreatorError,
)
from app.services import message_service

group_bp = Blueprint("groups", __name__)


@group_bp.route("", methods=["POST"])
@jwt_required()
def create_group():
    username = get_jwt_identity()
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    group_name = data.get("name", "").strip()
    member_usernames = data.get("members", [])

    if not isinstance(member_usernames, list):
        return jsonify({"error": "members must be a list of usernames"}), 400

    try:
        group = group_service.create_group(username, group_name, member_usernames)
        return jsonify({"group": group}), 201
    except GroupLimitReachedError as exc:
        return jsonify({"error": str(exc)}), 403
    except NotMutualFollowError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@group_bp.route("", methods=["GET"])
@jwt_required()
def list_groups():
    username = get_jwt_identity()
    try:
        groups = group_service.get_user_groups(username)
        return jsonify({"groups": groups})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@group_bp.route("/<int:group_id>", methods=["GET"])
@jwt_required()
def get_group(group_id):
    username = get_jwt_identity()
    try:
        group = group_service.get_group_detail(username, group_id)
        return jsonify({"group": group})
    except NotGroupMemberError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404


@group_bp.route("/<int:group_id>/members", methods=["POST"])
@jwt_required()
def add_member(group_id):
    username = get_jwt_identity()
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    target_username = data.get("username", "").strip()
    if not target_username:
        return jsonify({"error": "username is required"}), 400

    try:
        group_service.add_member_to_group(username, group_id, target_username)
        return jsonify({"message": "Member added"}), 200
    except NotGroupCreatorError as exc:
        return jsonify({"error": str(exc)}), 403
    except NotMutualFollowError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@group_bp.route("/<int:group_id>/members/<target_username>", methods=["DELETE"])
@jwt_required()
def remove_member(group_id, target_username):
    username = get_jwt_identity()
    try:
        group_service.remove_member_from_group(username, group_id, target_username)
        return jsonify({"message": "Member removed"}), 200
    except NotGroupCreatorError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@group_bp.route("/<int:group_id>/leave", methods=["POST"])
@jwt_required()
def leave_group(group_id):
    username = get_jwt_identity()
    try:
        group_service.remove_member_from_group(username, group_id, username)
        return jsonify({"message": "You have left the group"}), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@group_bp.route("/<int:group_id>", methods=["DELETE"])
@jwt_required()
def delete_group(group_id):
    username = get_jwt_identity()
    try:
        group_service.delete_group(username, group_id)
        return jsonify({"message": "Group deleted"}), 200
    except NotGroupCreatorError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@group_bp.route("/<int:group_id>/attachments", methods=["POST"])
@jwt_required()
def upload_group_attachment(group_id):
    username = get_jwt_identity()

    from app.repositories import user_repository
    user = user_repository.get_by_username(username)
    if not user:
        return jsonify({"error": "User not found"}), 404

    from app.repositories import group_repository as grp_repo
    if not grp_repo.is_member(group_id, user.id):
        return jsonify({"error": "You are not a member of this group"}), 403

    file = request.files.get("file") or request.files.get("attachment")

    try:
        payload = message_service.upload_message_attachment(username, file)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except message_service.MessageAttachmentStorageError as exc:
        return jsonify({"error": str(exc)}), 503

    return jsonify({"attachment": payload}), 201


@group_bp.route("/mutual-followers", methods=["GET"])
@jwt_required()
def mutual_followers():
    username = get_jwt_identity()
    try:
        page = int(request.args.get("page", 1))
    except (TypeError, ValueError):
        page = 1
    try:
        limit = int(request.args.get("limit", 50))
    except (TypeError, ValueError):
        limit = 50

    try:
        result = group_service.get_mutual_followers(username, page=page, limit=limit)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
