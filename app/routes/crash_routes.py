from flask import Blueprint, jsonify, request
from flask_jwt_extended import get_jwt_identity, verify_jwt_in_request

from app.services import crash_log_service

crash_bp = Blueprint("crash", __name__)


def _get_optional_username_from_jwt():
    try:
        verify_jwt_in_request(optional=True)
        return get_jwt_identity()
    except Exception:
        return None


@crash_bp.route("/crash-logs", methods=["POST"])
def ingest_crash_log():
    data = request.get_json(silent=True)

    try:
        crash_log, created = crash_log_service.ingest_crash_log(
            data,
            auth_username=_get_optional_username_from_jwt(),
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    status_code = 201 if created else 200
    return (
        jsonify(
            {
                "message": "Crash log accepted",
                "created": created,
                "crash_log_id": crash_log.id,
                "event_id": crash_log.event_id,
            }
        ),
        status_code,
    )
