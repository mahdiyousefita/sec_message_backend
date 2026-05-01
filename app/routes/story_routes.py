from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from app.services import message_service
from app.services.story_service import (
    delete_story,
    get_story_bundle,
    get_story_feed,
    get_mention_candidates,
    get_viewers,
    record_view,
    reply_to_story,
    set_like,
    upload_story,
)

story_bp = Blueprint("story", __name__)


def _parse_story_id_from_payload(payload):
    if not isinstance(payload, dict):
        raise ValueError("Invalid JSON body")

    story_id = payload.get("story_id")
    if isinstance(story_id, bool):
        raise ValueError("story_id is required")
    try:
        normalized = int(story_id)
    except (TypeError, ValueError):
        raise ValueError("story_id is required")

    if normalized <= 0:
        raise ValueError("story_id is required")
    return normalized


def _parse_liked(payload):
    liked = payload.get("liked", True)
    if isinstance(liked, bool):
        return liked
    if isinstance(liked, (int, float)):
        return bool(liked)
    if isinstance(liked, str):
        normalized = liked.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError("liked must be a boolean")


@story_bp.route("/story/upload", methods=["POST"])
@jwt_required()
def upload_story_route():
    username = get_jwt_identity()

    content_type = (request.content_type or "").lower()
    mention_usernames = []

    if "multipart/form-data" in content_type:
        file = request.files.get("file") or request.files.get("media")
        mention_usernames = request.form.getlist("mentions") or request.form.getlist("mentions[]")
    else:
        return jsonify({"error": "Use multipart/form-data with file"}), 400

    try:
        payload = upload_story(
            username=username,
            file_storage=file,
            mention_usernames=mention_usernames,
        )
        return jsonify(payload), 201
    except ValueError as exc:
        if str(exc) == "Account suspended":
            return jsonify({"error": str(exc)}), 403
        return jsonify({"error": str(exc)}), 400
    except message_service.MessageAttachmentStorageError as exc:
        return jsonify({"error": str(exc)}), 503


@story_bp.route("/story/feed", methods=["GET"])
@jwt_required()
def get_feed_route():
    username = get_jwt_identity()
    try:
        payload = get_story_feed(username=username)
        return jsonify(payload), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@story_bp.route("/story/mentions", methods=["GET"])
@jwt_required()
def get_story_mentions_route():
    username = get_jwt_identity()
    query = request.args.get("q", default="", type=str)
    limit = request.args.get("limit", default=6, type=int)
    try:
        payload = get_mention_candidates(
            username=username,
            query=query,
            limit=limit,
        )
        return jsonify(payload), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400


@story_bp.route("/story/<int:story_id>", methods=["GET"])
@jwt_required()
def get_story_route(story_id):
    username = get_jwt_identity()
    try:
        payload = get_story_bundle(username=username, story_id=story_id)
        return jsonify(payload), 200
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        if "not found" in str(exc).lower():
            return jsonify({"error": str(exc)}), 404
        return jsonify({"error": str(exc)}), 400


@story_bp.route("/story/<int:story_id>", methods=["DELETE"])
@jwt_required()
def delete_story_route(story_id):
    username = get_jwt_identity()
    try:
        payload = delete_story(username=username, story_id=story_id)
        return jsonify(payload), 200
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        if "not found" in str(exc).lower():
            return jsonify({"error": str(exc)}), 404
        return jsonify({"error": str(exc)}), 400


@story_bp.route("/story/view", methods=["POST"])
@jwt_required()
def view_story_route():
    username = get_jwt_identity()
    payload = request.get_json(silent=True)

    try:
        story_id = _parse_story_id_from_payload(payload)
        result = record_view(username=username, story_id=story_id)
        return jsonify(result), 200
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        if "not found" in str(exc).lower():
            return jsonify({"error": str(exc)}), 404
        return jsonify({"error": str(exc)}), 400


@story_bp.route("/story/like", methods=["POST"])
@jwt_required()
def like_story_route():
    username = get_jwt_identity()
    payload = request.get_json(silent=True)

    try:
        story_id = _parse_story_id_from_payload(payload)
        liked = _parse_liked(payload)
        result = set_like(username=username, story_id=story_id, liked=liked)
        return jsonify(result), 200
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        if "not found" in str(exc).lower():
            return jsonify({"error": str(exc)}), 404
        return jsonify({"error": str(exc)}), 400


@story_bp.route("/story/viewers", methods=["GET"])
@jwt_required()
def story_viewers_query_route():
    username = get_jwt_identity()
    story_id = request.args.get("story_id", type=int)
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=20, type=int)

    if not story_id:
        return jsonify({"error": "story_id is required"}), 400

    try:
        payload = get_viewers(
            username=username,
            story_id=story_id,
            page=page,
            limit=limit,
        )
        return jsonify(payload), 200
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        if "not found" in str(exc).lower():
            return jsonify({"error": str(exc)}), 404
        return jsonify({"error": str(exc)}), 400


@story_bp.route("/story/<int:story_id>/viewers", methods=["GET"])
@jwt_required()
def story_viewers_param_route(story_id):
    username = get_jwt_identity()
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=20, type=int)

    try:
        payload = get_viewers(
            username=username,
            story_id=story_id,
            page=page,
            limit=limit,
        )
        return jsonify(payload), 200
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        if "not found" in str(exc).lower():
            return jsonify({"error": str(exc)}), 404
        return jsonify({"error": str(exc)}), 400


@story_bp.route("/story/reply", methods=["POST"])
@jwt_required()
def story_reply_route():
    username = get_jwt_identity()
    payload = request.get_json(silent=True)

    if not isinstance(payload, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    try:
        story_id = _parse_story_id_from_payload(payload)
        reply_text = (payload.get("reply_text") or "").strip()
        result = reply_to_story(
            username=username,
            story_id=story_id,
            reply_text=reply_text,
        )
        return jsonify(result), 200
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 403
    except ValueError as exc:
        if "not found" in str(exc).lower():
            return jsonify({"error": str(exc)}), 404
        return jsonify({"error": str(exc)}), 400
