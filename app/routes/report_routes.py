from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, jwt_required

from app.services import report_service


report_bp = Blueprint("reports", __name__)


@report_bp.route("/report-types", methods=["GET"])
def list_report_types():
    return jsonify({"report_types": list(report_service.REPORT_TYPES)}), 200


@report_bp.route("/posts/<int:post_id>/reports", methods=["POST"])
@jwt_required()
def create_post_report(post_id):
    username = get_jwt_identity()
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    try:
        report = report_service.create_post_report(
            reporter_username=username,
            post_id=post_id,
            report_type=data.get("report_type"),
            description=data.get("description"),
        )
    except ValueError as e:
        message = str(e)
        if message == "Post not found":
            return jsonify({"error": message}), 404
        if message == "You already have a pending report for this post":
            return jsonify({"error": message}), 409
        return jsonify({"error": message}), 400

    return jsonify(
        {
            "message": "Report submitted",
            "report_id": report.id,
        }
    ), 201
