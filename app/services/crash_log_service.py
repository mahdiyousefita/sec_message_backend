import hashlib
import json
import re
from datetime import datetime, timezone

from app.db import db
from app.models.user_model import User
from app.repositories import crash_log_repository

MAX_EVENT_ID_LENGTH = 64
MAX_APP_VERSION_LENGTH = 64
MAX_THREAD_NAME_LENGTH = 120
MAX_EXCEPTION_TYPE_LENGTH = 255
MAX_EXCEPTION_MESSAGE_LENGTH = 2048
MAX_STACK_TRACE_LENGTH = 64000
MAX_DEVICE_FIELD_LENGTH = 120
MAX_BUILD_TYPE_LENGTH = 40
MAX_USERNAME_LENGTH = 80
MAX_AFFECTED_USERS = 100
MAX_MAPPING_FILE_SIZE_BYTES = 100 * 1024 * 1024


_CLASS_MAPPING_RE = re.compile(r"^(.+?)\s+->\s+(.+?):$")
_STACK_LINE_RE = re.compile(r"^(\s*at\s+)([\w.$]+)\.([\w$<>]+)(\(.*\))$")
_CAUSED_BY_RE = re.compile(r"^(Caused by:\s+)([\w.$]+)(:.*)?$")
_FIRST_EXCEPTION_RE = re.compile(r"^([\w.$]+)(:.*)$")
_STACK_LINE_NUMBER_RE = re.compile(r":\d+")
_MULTI_SPACE_RE = re.compile(r"\s+")

_mapping_cache: dict[int, tuple[dict[str, str], dict[str, dict[str, set[str]]]]] = {}


def _clean_optional_text(value, *, max_length: int):
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized[:max_length]


def _clean_required_text(value, *, field_name: str, max_length: int):
    if not isinstance(value, str):
        raise ValueError(f"{field_name} is required")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized[:max_length]


def _parse_optional_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_occurred_at(value):
    if not value:
        return datetime.utcnow()
    if not isinstance(value, str):
        return datetime.utcnow()

    normalized = value.strip()
    if not normalized:
        return datetime.utcnow()

    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return datetime.utcnow()

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _normalize_stack_trace_for_signature(stack_trace: str):
    normalized_lines = []
    for raw_line in (stack_trace or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = _STACK_LINE_NUMBER_RE.sub(":#", line)
        line = _MULTI_SPACE_RE.sub(" ", line)
        normalized_lines.append(line)
    return "\n".join(normalized_lines)[:MAX_STACK_TRACE_LENGTH]


def build_crash_signature(exception_type: str, stack_trace: str, *, app_version: str | None = None):
    normalized_exception = (exception_type or "UnhandledException").strip()
    normalized_stack = _normalize_stack_trace_for_signature(stack_trace)
    normalized_version = (app_version or "").strip()
    fingerprint = f"{normalized_version}\n{normalized_exception}\n{normalized_stack}".encode(
        "utf-8",
        errors="ignore",
    )
    return hashlib.sha256(fingerprint).hexdigest()


def _resolve_user(auth_username: str | None):
    if not auth_username:
        return None
    return User.query.filter_by(username=auth_username).first()


def _parse_affected_users(raw_json):
    if not raw_json:
        return []
    try:
        parsed = json.loads(raw_json)
    except (TypeError, ValueError):
        return []
    if not isinstance(parsed, list):
        return []
    users = []
    for item in parsed:
        cleaned = _clean_optional_text(item, max_length=MAX_USERNAME_LENGTH)
        if cleaned and cleaned not in users:
            users.append(cleaned)
    return users[:MAX_AFFECTED_USERS]


def _merge_affected_users(existing_json, *, username: str | None):
    users = _parse_affected_users(existing_json)
    normalized_username = _clean_optional_text(username, max_length=MAX_USERNAME_LENGTH)
    if normalized_username and normalized_username not in users:
        users.append(normalized_username)
    if len(users) > MAX_AFFECTED_USERS:
        users = users[-MAX_AFFECTED_USERS:]
    return users


def ingest_crash_log(payload: dict, *, auth_username: str | None = None):
    if not isinstance(payload, dict):
        raise ValueError("Invalid JSON body")

    event_id = _clean_required_text(
        payload.get("event_id"),
        field_name="event_id",
        max_length=MAX_EVENT_ID_LENGTH,
    )

    existing = crash_log_repository.get_crash_log_by_event_id(event_id)
    if existing is not None:
        return existing, False, False

    app_version = _clean_required_text(
        payload.get("app_version"),
        field_name="app_version",
        max_length=MAX_APP_VERSION_LENGTH,
    )

    stack_trace = _clean_required_text(
        payload.get("stack_trace"),
        field_name="stack_trace",
        max_length=MAX_STACK_TRACE_LENGTH,
    )

    exception_type = _clean_required_text(
        payload.get("exception_type") or "UnhandledException",
        field_name="exception_type",
        max_length=MAX_EXCEPTION_TYPE_LENGTH,
    )
    crash_signature = build_crash_signature(exception_type, stack_trace, app_version=app_version)
    if crash_log_repository.get_resolved_signature(crash_signature) is not None:
        return None, False, True

    thread_name = _clean_optional_text(
        payload.get("thread_name"),
        max_length=MAX_THREAD_NAME_LENGTH,
    ) or "unknown"

    auth_user = _resolve_user(auth_username)
    fallback_username = _clean_optional_text(
        payload.get("username"),
        max_length=MAX_USERNAME_LENGTH,
    )
    reported_username = auth_user.username if auth_user else fallback_username
    occurred_at = _parse_occurred_at(payload.get("occurred_at"))
    exception_message = _clean_optional_text(
        payload.get("exception_message"),
        max_length=MAX_EXCEPTION_MESSAGE_LENGTH,
    )

    existing_by_signature = crash_log_repository.get_crash_log_by_signature(crash_signature)
    if existing_by_signature is not None:
        existing_by_signature.occurrence_count = int(existing_by_signature.occurrence_count or 1) + 1
        existing_by_signature.affected_users_json = json.dumps(
            _merge_affected_users(
                existing_by_signature.affected_users_json,
                username=reported_username,
            )
        )
        existing_by_signature.received_at = datetime.utcnow()
        if existing_by_signature.occurred_at is None or occurred_at > existing_by_signature.occurred_at:
            existing_by_signature.occurred_at = occurred_at
        if exception_message and not existing_by_signature.exception_message:
            existing_by_signature.exception_message = exception_message
        if auth_user and existing_by_signature.user_id is None:
            existing_by_signature.user_id = auth_user.id
        if reported_username and not existing_by_signature.username_snapshot:
            existing_by_signature.username_snapshot = reported_username

        crash_log_repository.create_crash_event_id(
            event_id=event_id,
            crash_log_id=existing_by_signature.id,
        )
        db.session.commit()
        return existing_by_signature, False, False

    crash_log = crash_log_repository.create_crash_log(
        event_id=event_id,
        platform="android",
        app_version=app_version,
        app_version_code=_parse_optional_int(payload.get("app_version_code")),
        thread_name=thread_name,
        exception_type=exception_type,
        exception_message=exception_message,
        stack_trace=stack_trace,
        crash_signature=crash_signature,
        occurrence_count=1,
        affected_users_json=json.dumps(
            _merge_affected_users(None, username=reported_username)
        ),
        occurred_at=occurred_at,
        user_id=auth_user.id if auth_user else None,
        username_snapshot=reported_username,
        device_model=_clean_optional_text(payload.get("device_model"), max_length=MAX_DEVICE_FIELD_LENGTH),
        device_manufacturer=_clean_optional_text(
            payload.get("device_manufacturer"),
            max_length=MAX_DEVICE_FIELD_LENGTH,
        ),
        os_version=_clean_optional_text(payload.get("os_version"), max_length=MAX_DEVICE_FIELD_LENGTH),
        sdk_int=_parse_optional_int(payload.get("sdk_int")),
        build_type=_clean_optional_text(payload.get("build_type"), max_length=MAX_BUILD_TYPE_LENGTH),
    )
    crash_log_repository.create_crash_event_id(event_id=event_id, crash_log_id=crash_log.id)

    db.session.commit()
    return crash_log, True, False


def _parse_mapping(mapping_text: str):
    class_map: dict[str, str] = {}
    method_map: dict[str, dict[str, set[str]]] = {}
    current_obfuscated_class: str | None = None

    for raw_line in mapping_text.splitlines():
        line = raw_line.rstrip("\n")
        class_match = _CLASS_MAPPING_RE.match(line.strip())
        if class_match:
            original_class = class_match.group(1).strip()
            obfuscated_class = class_match.group(2).strip()
            class_map[obfuscated_class] = original_class
            current_obfuscated_class = obfuscated_class
            continue

        if current_obfuscated_class is None:
            continue

        stripped = line.strip()
        if not stripped or "->" not in stripped or "(" not in stripped:
            continue

        left, right = stripped.rsplit("->", 1)
        obfuscated_method = right.strip()
        if not obfuscated_method:
            continue

        left = left.strip()
        left = re.sub(r"^\d+:\d+:", "", left)
        left = re.sub(r"^\d+:\d+:", "", left)
        before_params = left.split("(", 1)[0].strip()
        if not before_params:
            continue

        parts = before_params.split()
        if not parts:
            continue
        original_method = parts[-1].strip()
        if not original_method:
            continue

        methods_for_class = method_map.setdefault(current_obfuscated_class, {})
        methods_for_class.setdefault(obfuscated_method, set()).add(original_method)

    return class_map, method_map


def _get_parsed_mapping(mapping_row):
    if mapping_row is None:
        return None

    cached = _mapping_cache.get(mapping_row.id)
    if cached is not None:
        return cached

    parsed = _parse_mapping(mapping_row.mapping_text or "")
    _mapping_cache[mapping_row.id] = parsed
    return parsed


def _map_exception_type(exception_type: str, class_map: dict[str, str]):
    if not exception_type:
        return exception_type
    return class_map.get(exception_type, exception_type)


def _deobfuscate_stack_trace(stack_trace: str, parsed_mapping):
    if not stack_trace or parsed_mapping is None:
        return None

    class_map, method_map = parsed_mapping
    if not class_map:
        return None

    translated_lines = []
    changed = False

    for raw_line in stack_trace.splitlines():
        line = raw_line

        stack_match = _STACK_LINE_RE.match(line)
        if stack_match:
            prefix, obfuscated_class, obfuscated_method, suffix = stack_match.groups()
            mapped_class = class_map.get(obfuscated_class, obfuscated_class)
            mapped_method = obfuscated_method
            class_methods = method_map.get(obfuscated_class, {})
            method_candidates = class_methods.get(obfuscated_method)
            if method_candidates:
                mapped_method = sorted(method_candidates)[0]
            translated_line = f"{prefix}{mapped_class}.{mapped_method}{suffix}"
            translated_lines.append(translated_line)
            changed = changed or (translated_line != line)
            continue

        caused_by_match = _CAUSED_BY_RE.match(line)
        if caused_by_match:
            prefix, obfuscated_class, suffix = caused_by_match.groups()
            mapped_class = class_map.get(obfuscated_class, obfuscated_class)
            translated_line = f"{prefix}{mapped_class}{suffix or ''}"
            translated_lines.append(translated_line)
            changed = changed or (translated_line != line)
            continue

        first_exception_match = _FIRST_EXCEPTION_RE.match(line)
        if first_exception_match:
            exception_name, suffix = first_exception_match.groups()
            mapped_exception = class_map.get(exception_name, exception_name)
            translated_line = f"{mapped_exception}{suffix}"
            translated_lines.append(translated_line)
            changed = changed or (translated_line != line)
            continue

        translated_lines.append(line)

    if not changed:
        return None

    return "\n".join(translated_lines)


def _serialize_crash_log_min(crash_log, *, include_stack: bool = False, mapping_row=None):
    mapping = mapping_row
    if mapping is None:
        mapping = crash_log_repository.get_mapping_for_version(crash_log.app_version)
    parsed_mapping = _get_parsed_mapping(mapping)
    deobfuscated_stack = None
    if include_stack:
        deobfuscated_stack = _deobfuscate_stack_trace(crash_log.stack_trace, parsed_mapping)

    exception_type = crash_log.exception_type
    if parsed_mapping is not None:
        exception_type = _map_exception_type(exception_type, parsed_mapping[0])

    is_deobfuscated = mapping is not None
    deobfuscated_preview = None
    if include_stack:
        is_deobfuscated = deobfuscated_stack is not None
        deobfuscated_preview = (
            deobfuscated_stack.splitlines()[0]
            if deobfuscated_stack
            else None
        )
    affected_users = _parse_affected_users(crash_log.affected_users_json)
    username_snapshot = crash_log.username_snapshot
    if not username_snapshot and affected_users:
        username_snapshot = affected_users[0]

    payload = {
        "id": crash_log.id,
        "event_id": crash_log.event_id,
        "platform": crash_log.platform,
        "app_version": crash_log.app_version,
        "app_version_code": crash_log.app_version_code,
        "thread_name": crash_log.thread_name,
        "exception_type": exception_type,
        "exception_message": crash_log.exception_message,
        "username": username_snapshot,
        "affected_users": affected_users,
        "occurrence_count": int(crash_log.occurrence_count or 1),
        "user_id": crash_log.user_id,
        "device_model": crash_log.device_model,
        "device_manufacturer": crash_log.device_manufacturer,
        "os_version": crash_log.os_version,
        "sdk_int": crash_log.sdk_int,
        "build_type": crash_log.build_type,
        "occurred_at": crash_log.occurred_at.isoformat() if crash_log.occurred_at else None,
        "received_at": crash_log.received_at.isoformat() if crash_log.received_at else None,
        "mapping_available": mapping is not None,
        "is_deobfuscated": is_deobfuscated,
        "deobfuscated_preview": deobfuscated_preview,
    }

    if include_stack:
        payload["raw_stack_trace"] = crash_log.stack_trace
        payload["deobfuscated_stack_trace"] = deobfuscated_stack

    return payload


def list_crash_logs_for_admin(
    *,
    page: int,
    limit: int,
    app_version: str | None = None,
    query: str | None = None,
    exception_prefix: str | None = None,
):
    safe_page = max(int(page or 1), 1)
    safe_limit = min(max(int(limit or 20), 1), 50)

    normalized_version = _clean_optional_text(app_version, max_length=MAX_APP_VERSION_LENGTH)
    normalized_query = _clean_optional_text(query, max_length=120)
    normalized_exception_prefix = _clean_optional_text(
        exception_prefix,
        max_length=MAX_EXCEPTION_TYPE_LENGTH,
    )

    total, crash_logs = crash_log_repository.list_crash_logs(
        page=safe_page,
        limit=safe_limit,
        app_version=normalized_version,
        query=normalized_query,
        exception_prefix=normalized_exception_prefix,
    )
    versions = sorted({
        crash_log.app_version
        for crash_log in crash_logs
        if crash_log.app_version
    })
    mappings_by_version = crash_log_repository.list_mappings_for_versions(versions)

    return {
        "page": safe_page,
        "limit": safe_limit,
        "total": total,
        "crash_logs": [
            _serialize_crash_log_min(
                crash_log,
                include_stack=False,
                mapping_row=mappings_by_version.get(crash_log.app_version),
            )
            for crash_log in crash_logs
        ],
    }


def get_crash_log_detail_for_admin(crash_log_id: int):
    crash_log = crash_log_repository.get_crash_log_by_id(crash_log_id)
    if crash_log is None:
        raise ValueError("Crash log not found")
    return _serialize_crash_log_min(crash_log, include_stack=True)


def _get_or_build_signature(crash_log):
    if crash_log.crash_signature:
        return crash_log.crash_signature
    return build_crash_signature(
        crash_log.exception_type,
        crash_log.stack_trace,
        app_version=crash_log.app_version,
    )


def resolve_crash_group_for_admin(*, crash_log_id: int, admin_username: str):
    crash_log = crash_log_repository.get_crash_log_by_id(crash_log_id)
    if crash_log is None:
        raise ValueError("Crash log not found")

    signature = _get_or_build_signature(crash_log)
    admin_user = User.query.filter_by(username=admin_username).first()

    crash_log_repository.upsert_resolved_signature(
        signature=signature,
        resolved_by_admin_id=admin_user.id if admin_user else None,
    )

    deleted_count = crash_log_repository.delete_crash_logs_by_signature(signature)

    legacy_rows = crash_log_repository.list_crash_logs_by_exception_type(crash_log.exception_type)
    legacy_deleted_ids = []
    for row in legacy_rows:
        if row.crash_signature:
            continue
        if build_crash_signature(
            row.exception_type,
            row.stack_trace,
            app_version=row.app_version,
        ) == signature:
            legacy_deleted_ids.append(row.id)
            db.session.delete(row)
            deleted_count += 1
    crash_log_repository.delete_crash_event_ids_by_crash_log_ids(legacy_deleted_ids)

    db.session.commit()
    return {
        "resolved_signature": signature,
        "deleted_count": deleted_count,
    }


def _serialize_mapping_row(mapping_row):
    return {
        "id": mapping_row.id,
        "platform": mapping_row.platform,
        "app_version": mapping_row.app_version,
        "app_version_code": mapping_row.app_version_code,
        "original_filename": mapping_row.original_filename,
        "uploaded_by_admin_id": mapping_row.uploaded_by_admin_id,
        "uploaded_at": mapping_row.uploaded_at.isoformat() if mapping_row.uploaded_at else None,
    }


def list_mapping_files_for_admin(limit: int = 50):
    rows = crash_log_repository.list_mapping_files(limit=limit)
    return [_serialize_mapping_row(row) for row in rows]


def upload_mapping_file_for_admin(
    *,
    app_version: str,
    mapping_bytes: bytes,
    original_filename: str | None,
    admin_username: str,
    app_version_code,
):
    normalized_version = _clean_required_text(
        app_version,
        field_name="app_version",
        max_length=MAX_APP_VERSION_LENGTH,
    )

    if not isinstance(mapping_bytes, (bytes, bytearray)):
        raise ValueError("mapping_file is required")
    if len(mapping_bytes) == 0:
        raise ValueError("mapping_file is empty")
    if len(mapping_bytes) > MAX_MAPPING_FILE_SIZE_BYTES:
        raise ValueError("mapping_file is too large")

    mapping_text = mapping_bytes.decode("utf-8", errors="replace").strip()
    if not mapping_text:
        raise ValueError("mapping_file is empty")

    admin_user = User.query.filter_by(username=admin_username).first()

    mapping_row = crash_log_repository.upsert_mapping_file(
        app_version=normalized_version,
        mapping_text=mapping_text,
        original_filename=_clean_optional_text(original_filename, max_length=255),
        uploaded_by_admin_id=admin_user.id if admin_user else None,
        app_version_code=_parse_optional_int(app_version_code),
    )
    db.session.commit()

    _mapping_cache.pop(mapping_row.id, None)

    return _serialize_mapping_row(mapping_row)
