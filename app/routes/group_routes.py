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
from app.socket_events import (
    emit_group_event_to_members,
    evict_user_from_group_room,
    get_group_online_users_payload,
)

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


@group_bp.route("/unread", methods=["GET"])
@jwt_required()
def unread_group_messages():
    username = get_jwt_identity()
    try:
        summary = group_service.get_group_unread_summary(username)
        return jsonify(summary), 200
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


@group_bp.route("/<int:group_id>/members", methods=["GET"])
@jwt_required()
def list_group_members(group_id):
    username = get_jwt_identity()
    try:
        members_payload = group_service.get_group_members(username, group_id)
        return jsonify(members_payload), 200
    except NotGroupMemberError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404


@group_bp.route("/<int:group_id>/online-users", methods=["GET"])
@jwt_required()
def get_group_online_users(group_id):
    username = get_jwt_identity()
    try:
        # Reuse group detail auth checks to ensure the requester belongs to this group.
        group_service.get_group_detail(username, group_id)
    except NotGroupMemberError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404

    payload = get_group_online_users_payload(group_id)
    if payload is None:
        return jsonify({"error": "Failed to load group online users"}), 500
    return jsonify(payload), 200


@group_bp.route("/<int:group_id>/members", methods=["POST"])
@jwt_required()
def add_member(group_id):
    username = get_jwt_identity()
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    usernames = data.get("usernames")
    single_target_username = None
    if isinstance(usernames, list):
        target_usernames = usernames
    else:
        target_username = data.get("username", "").strip()
        if not target_username:
            return jsonify({"error": "username is required"}), 400
        single_target_username = target_username
        target_usernames = [target_username]

    try:
        result = group_service.add_members_to_group(
            username,
            group_id,
            target_usernames,
        )

        for added_username in result["added"]:
            emit_group_event_to_members(
                group_id=group_id,
                event_name="group_member_key_updated",
                payload={
                    "username": added_username,
                    "group_id": group_id,
                },
            )

        if single_target_username and single_target_username in result["already_members"]:
            return jsonify({"error": "User is already a member of this group"}), 400

        if single_target_username:
            return jsonify(
                {
                    "message": "Member added",
                    "added": result["added"],
                    "already_members": result["already_members"],
                }
            ), 200

        return jsonify(
            {
                "added": result["added"],
                "already_members": result["already_members"],
            }
        ), 200
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
    normalized_target = (target_username or "").strip()
    try:
        group_service.remove_member_from_group(username, group_id, normalized_target)

        removal_reason = "left" if username == normalized_target else "removed"
        message_service.purge_group_delivery_for_user(group_id, normalized_target)
        evict_user_from_group_room(
            normalized_target,
            group_id,
            reason=removal_reason,
            notify=True,
        )
        emit_group_event_to_members(
            group_id=group_id,
            event_name="group_member_removed",
            payload={
                "group_id": group_id,
                "username": normalized_target,
                "removed_by": username,
                "reason": removal_reason,
            },
        )
        emit_group_event_to_members(
            group_id=group_id,
            event_name="group_member_key_updated",
            payload={
                "username": normalized_target,
                "group_id": group_id,
            },
        )
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
        message_service.purge_group_delivery_for_user(group_id, username)
        evict_user_from_group_room(
            username,
            group_id,
            reason="left",
            notify=True,
        )
        emit_group_event_to_members(
            group_id=group_id,
            event_name="group_member_removed",
            payload={
                "group_id": group_id,
                "username": username,
                "removed_by": username,
                "reason": "left",
            },
        )
        emit_group_event_to_members(
            group_id=group_id,
            event_name="group_member_key_updated",
            payload={
                "username": username,
                "group_id": group_id,
            },
        )
        return jsonify({"message": "You have left the group"}), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@group_bp.route("/<int:group_id>", methods=["DELETE"])
@jwt_required()
def delete_group(group_id):
    username = get_jwt_identity()
    try:
        from app.repositories import group_repository

        member_usernames = group_repository.get_group_member_usernames(group_id)
        group_service.delete_group(username, group_id)
        for member_username in member_usernames:
            message_service.purge_group_delivery_for_user(group_id, member_username)
            evict_user_from_group_room(
                member_username,
                group_id,
                reason="group_deleted",
                notify=True,
            )
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
        payload = message_service.upload_message_attachment(
            username=username,
            file_storage=file,
            upload_scope="group",
        )
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
