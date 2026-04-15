from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from app.services import playlist_service


playlist_bp = Blueprint("playlists", __name__)


@playlist_bp.route("/playlists/tracks", methods=["POST"])
@jwt_required()
def add_playlist_track():
    requester = get_jwt_identity()
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    media_id = data.get("media_id")
    if not isinstance(media_id, int) or media_id <= 0:
        return jsonify({"error": "media_id must be a positive integer"}), 400

    try:
        result = playlist_service.add_track_by_username(
            username=requester,
            media_id=media_id,
        )
    except ValueError as e:
        message = str(e)
        if message in {"User not found", "Music track not found"}:
            return jsonify({"error": message}), 404
        return jsonify({"error": message}), 400

    return jsonify(
        {
            "message": "Track added to playlist" if result["created"] else "Track already exists in playlist",
            "created": result["created"],
            "track": result["track"],
        }
    ), 201 if result["created"] else 200


@playlist_bp.route("/playlists/tracks", methods=["GET"])
@jwt_required()
def list_playlist_tracks():
    requester = get_jwt_identity()
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=50, type=int)

    try:
        payload = playlist_service.get_tracks_by_username(
            username=requester,
            page=page,
            limit=limit,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    return jsonify(payload), 200


@playlist_bp.route("/playlists/tracks/exists", methods=["GET"])
@jwt_required()
def check_playlist_track_exists():
    requester = get_jwt_identity()
    track_url = request.args.get("track_url", default="", type=str).strip()
    if not track_url:
        return jsonify({"error": "track_url is required"}), 400

    try:
        exists = playlist_service.track_exists_in_user_playlist(
            username=requester,
            track_url=track_url,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    return jsonify({"exists": exists}), 200


@playlist_bp.route("/playlists/tracks/<int:track_id>", methods=["DELETE"])
@jwt_required()
def delete_playlist_track(track_id: int):
    requester = get_jwt_identity()
    try:
        payload = playlist_service.remove_track_by_username(
            username=requester,
            track_id=track_id,
        )
    except ValueError as e:
        message = str(e)
        if message == "Playlist track not found":
            return jsonify({"error": message}), 404
        if message == "track_id must be a positive integer":
            return jsonify({"error": message}), 400
        return jsonify({"error": message}), 404

    return jsonify(payload), 200
