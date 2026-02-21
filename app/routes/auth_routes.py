from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity

from app.services import auth_service



auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/register", methods=["POST"])
def register():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    try:
        auth_service.register(
            data.get("username"),
            data.get("password"),
            data.get("public_key"),
            data.get("name"),
        )
        return jsonify({"message": "User registered"}), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    try:
        tokens = auth_service.login(
            data.get("username"),
            data.get("password")
        )
        return jsonify(tokens), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 401


@auth_bp.route("/refresh", methods=["POST"])
@auth_bp.route("/token", methods=["POST"])
@jwt_required(refresh=True)
def refresh_token():
    username = get_jwt_identity()
    return jsonify(auth_service.refresh_access_token(username)), 200
