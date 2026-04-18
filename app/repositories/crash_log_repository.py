from sqlalchemy import or_

from app.db import db
from app.models.crash_log_model import CrashLog, CrashMappingFile


def get_crash_log_by_event_id(event_id: str):
    return CrashLog.query.filter_by(event_id=event_id).first()


def create_crash_log(**kwargs):
    crash_log = CrashLog(**kwargs)
    db.session.add(crash_log)
    db.session.flush()
    return crash_log


def list_crash_logs(page: int, limit: int, *, app_version: str | None = None, query: str | None = None):
    q = CrashLog.query

    if app_version:
        q = q.filter(CrashLog.app_version == app_version)

    if query:
        pattern = f"%{query}%"
        q = q.filter(
            or_(
                CrashLog.exception_type.ilike(pattern),
                CrashLog.exception_message.ilike(pattern),
                CrashLog.username_snapshot.ilike(pattern),
            )
        )

    q = q.order_by(CrashLog.received_at.desc(), CrashLog.id.desc())

    total = q.count()
    items = q.offset((page - 1) * limit).limit(limit).all()
    return total, items


def get_crash_log_by_id(crash_log_id: int):
    return CrashLog.query.get(crash_log_id)


def get_mapping_for_version(app_version: str, platform: str = "android"):
    return (
        CrashMappingFile.query
        .filter_by(platform=platform, app_version=app_version)
        .order_by(CrashMappingFile.uploaded_at.desc(), CrashMappingFile.id.desc())
        .first()
    )


def upsert_mapping_file(
    *,
    app_version: str,
    mapping_text: str,
    original_filename: str | None,
    uploaded_by_admin_id: int | None,
    app_version_code: int | None,
    platform: str = "android",
):
    mapping = CrashMappingFile.query.filter_by(
        platform=platform,
        app_version=app_version,
    ).first()

    if mapping is None:
        mapping = CrashMappingFile(
            platform=platform,
            app_version=app_version,
        )
        db.session.add(mapping)

    mapping.mapping_text = mapping_text
    mapping.original_filename = original_filename
    mapping.uploaded_by_admin_id = uploaded_by_admin_id
    mapping.app_version_code = app_version_code
    db.session.flush()
    return mapping


def list_mapping_files(limit: int = 50, platform: str = "android"):
    bounded_limit = min(max(int(limit), 1), 200)
    return (
        CrashMappingFile.query
        .filter_by(platform=platform)
        .order_by(CrashMappingFile.uploaded_at.desc(), CrashMappingFile.id.desc())
        .limit(bounded_limit)
        .all()
    )
