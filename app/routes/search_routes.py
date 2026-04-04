from flask import Blueprint, request, jsonify
from flask_jwt_extended import get_jwt_identity, jwt_required
from app.services.search_service import search_users, search_posts, search_all

search_bp = Blueprint("search", __name__)


@search_bp.route("/search/users", methods=["GET"])
def api_search_users():
    query = request.args.get("q", default="", type=str).strip()
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=10, type=int)

    if not query:
        return jsonify({"error": "Query parameter 'q' is required"}), 400

    data = search_users(query, page, limit)
    return jsonify(data), 200


@search_bp.route("/search/posts", methods=["GET"])
@jwt_required(optional=True)
def api_search_posts():
    viewer_username = get_jwt_identity()
    query = request.args.get("q", default="", type=str).strip()
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=10, type=int)

    if not query:
        return jsonify({"error": "Query parameter 'q' is required"}), 400

    data = search_posts(query, page, limit, viewer_username=viewer_username)
    return jsonify(data), 200


@search_bp.route("/search", methods=["GET"])
@jwt_required(optional=True)
def api_search_all():
    viewer_username = get_jwt_identity()
    query = request.args.get("q", default="", type=str).strip()
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=10, type=int)

    if not query:
        return jsonify({"error": "Query parameter 'q' is required"}), 400

    data = search_all(query, page, limit, viewer_username=viewer_username)
    return jsonify(data), 200
