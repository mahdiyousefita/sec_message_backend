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
        crash_log, created, ignored_resolved = crash_log_service.ingest_crash_log(
            data,
            auth_username=_get_optional_username_from_jwt(),
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    status_code = 201 if created else 200
    if ignored_resolved:
        return (
            jsonify(
                {
                    "message": "Crash signature already resolved",
                    "created": False,
                    "ignored_resolved": True,
                    "crash_log_id": None,
                    "event_id": data.get("event_id") if isinstance(data, dict) else None,
                }
            ),
            200,
        )

    return (
        jsonify(
            {
                "message": "Crash log accepted",
                "created": created,
                "ignored_resolved": False,
                "crash_log_id": crash_log.id,
                "event_id": crash_log.event_id,
                "occurrence_count": int(crash_log.occurrence_count or 1),
            }
        ),
        status_code,
    )
