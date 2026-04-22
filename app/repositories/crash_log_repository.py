from sqlalchemy import or_
from sqlalchemy.orm import load_only

from app.db import db
from app.models.crash_log_model import (
    CrashLog,
    CrashEventId,
    CrashMappingFile,
    ResolvedCrashSignature,
)


def get_crash_log_by_event_id(event_id: str):
    event_row = CrashEventId.query.filter_by(event_id=event_id).first()
    if event_row is not None:
        return db.session.get(CrashLog, event_row.crash_log_id)
    return CrashLog.query.filter_by(event_id=event_id).first()


def get_crash_log_by_signature(signature: str):
    return CrashLog.query.filter_by(crash_signature=signature).first()


def create_crash_log(**kwargs):
    crash_log = CrashLog(**kwargs)
    db.session.add(crash_log)
    db.session.flush()
    return crash_log


def create_crash_event_id(*, event_id: str, crash_log_id: int):
    row = CrashEventId(event_id=event_id, crash_log_id=crash_log_id)
    db.session.add(row)
    db.session.flush()
    return row


def get_resolved_signature(signature: str):
    return ResolvedCrashSignature.query.filter_by(signature=signature).first()


def upsert_resolved_signature(*, signature: str, resolved_by_admin_id: int | None):
    row = ResolvedCrashSignature.query.filter_by(signature=signature).first()
    if row is None:
        row = ResolvedCrashSignature(signature=signature)
        db.session.add(row)

    row.resolved_by_admin_id = resolved_by_admin_id
    return row


def list_crash_logs(
    page: int,
    limit: int,
    *,
    app_version: str | None = None,
    query: str | None = None,
    exception_prefix: str | None = None,
    exclude_resolved: bool = True,
):
    q = CrashLog.query.options(
        load_only(
            CrashLog.id,
            CrashLog.event_id,
            CrashLog.platform,
            CrashLog.app_version,
            CrashLog.app_version_code,
            CrashLog.thread_name,
            CrashLog.exception_type,
            CrashLog.exception_message,
            CrashLog.occurred_at,
            CrashLog.received_at,
            CrashLog.user_id,
            CrashLog.username_snapshot,
            CrashLog.device_model,
            CrashLog.device_manufacturer,
            CrashLog.os_version,
            CrashLog.sdk_int,
            CrashLog.build_type,
            CrashLog.crash_signature,
            CrashLog.occurrence_count,
            CrashLog.affected_users_json,
        )
    )
    if exclude_resolved:
        q = q.outerjoin(
            ResolvedCrashSignature,
            CrashLog.crash_signature == ResolvedCrashSignature.signature,
        ).filter(ResolvedCrashSignature.id.is_(None))

    if app_version:
        q = q.filter(CrashLog.app_version == app_version)

    if exception_prefix:
        q = q.filter(CrashLog.exception_type.ilike(f"{exception_prefix}%"))

    if query:
        pattern = f"%{query}%"
        q = q.filter(
            or_(
                CrashLog.exception_type.ilike(pattern),
                CrashLog.exception_message.ilike(pattern),
                CrashLog.username_snapshot.ilike(pattern),
                CrashLog.affected_users_json.ilike(pattern),
            )
        )

    q = q.order_by(CrashLog.occurrence_count.desc(), CrashLog.received_at.desc(), CrashLog.id.desc())

    total = q.count()
    items = q.offset((page - 1) * limit).limit(limit).all()
    return total, items


def get_crash_log_by_id(crash_log_id: int):
    return db.session.get(CrashLog, crash_log_id)


def get_mapping_for_version(app_version: str, platform: str = "android"):
    return (
        CrashMappingFile.query
        .filter_by(platform=platform, app_version=app_version)
        .order_by(CrashMappingFile.uploaded_at.desc(), CrashMappingFile.id.desc())
        .first()
    )


def list_mappings_for_versions(app_versions: list[str], platform: str = "android"):
    if not app_versions:
        return {}

    rows = (
        CrashMappingFile.query
        .filter(
            CrashMappingFile.platform == platform,
            CrashMappingFile.app_version.in_(app_versions),
        )
        .order_by(
            CrashMappingFile.app_version.asc(),
            CrashMappingFile.uploaded_at.desc(),
            CrashMappingFile.id.desc(),
        )
        .all()
    )

    by_version = {}
    for row in rows:
        by_version.setdefault(row.app_version, row)
    return by_version


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


def list_crash_logs_by_exception_type(exception_type: str):
    return CrashLog.query.filter(CrashLog.exception_type == exception_type).all()


def delete_crash_logs_by_signature(signature: str):
    rows = (
        CrashLog.query
        .with_entities(CrashLog.id)
        .filter(CrashLog.crash_signature == signature)
        .all()
    )
    crash_log_ids = [row[0] for row in rows if row and row[0] is not None]
    if crash_log_ids:
        CrashEventId.query.filter(CrashEventId.crash_log_id.in_(crash_log_ids)).delete(
            synchronize_session=False
        )

    return (
        CrashLog.query
        .filter(CrashLog.crash_signature == signature)
        .delete(synchronize_session=False)
    )


def delete_crash_event_ids_by_crash_log_ids(crash_log_ids: list[int]):
    if not crash_log_ids:
        return 0
    return (
        CrashEventId.query
        .filter(CrashEventId.crash_log_id.in_(crash_log_ids))
        .delete(synchronize_session=False)
    )
