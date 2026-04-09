import re
from urllib.parse import urljoin

from flask import Request

from app.db import db
from app.models.app_update_model import AppUpdateConfig

ALLOWED_PLATFORMS = {"android"}
DEFAULT_DOWNLOAD_URL = "/download/app"

DEFAULT_FORCE_TITLE = "Update required"
DEFAULT_FORCE_MESSAGE = "Please update to continue using the app."
DEFAULT_OPTIONAL_TITLE = "Update available"
DEFAULT_OPTIONAL_MESSAGE = "A newer version is available. You can update now or later."

_VERSION_RE = re.compile(r"(\d+(?:\.\d+){0,2})")


def normalize_version(raw_version: str) -> tuple[str, tuple[int, int, int]]:
    if not isinstance(raw_version, str):
        raise ValueError("Version must be a string")

    match = _VERSION_RE.search(raw_version.strip())
    if not match:
        raise ValueError("Invalid app version")

    parts = [int(part) for part in match.group(1).split(".") if part != ""]
    if not parts:
        raise ValueError("Invalid app version")

    while len(parts) < 3:
        parts.append(0)

    normalized_parts = tuple(parts[:3])
    normalized = ".".join(str(part) for part in normalized_parts)
    return normalized, normalized_parts


def _normalize_platform(platform: str | None) -> str:
    value = (platform or "android").strip().lower()
    if value not in ALLOWED_PLATFORMS:
        raise ValueError("Unsupported platform")
    return value


def _parse_optional_threshold(raw_value: str | None, field_name: str) -> str | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    try:
        normalized, _ = normalize_version(value)
    except ValueError:
        raise ValueError(f"{field_name} must be a valid version")
    return normalized


def get_or_create_config(platform: str = "android") -> AppUpdateConfig:
    normalized_platform = _normalize_platform(platform)
    config = AppUpdateConfig.query.filter_by(platform=normalized_platform).first()
    if config:
        return config

    config = AppUpdateConfig(
        platform=normalized_platform,
        download_url=DEFAULT_DOWNLOAD_URL,
    )
    db.session.add(config)
    db.session.commit()
    return config


def serialize_settings(config: AppUpdateConfig) -> dict:
    return {
        "id": config.id,
        "platform": config.platform,
        "force_update_below": config.force_update_below,
        "optional_update_below": config.optional_update_below,
        "latest_version": config.latest_version,
        "force_title": config.force_title,
        "force_message": config.force_message,
        "optional_title": config.optional_title,
        "optional_message": config.optional_message,
        "download_url": (config.download_url or "").strip() or DEFAULT_DOWNLOAD_URL,
        "created_at": config.created_at.isoformat() if config.created_at else None,
        "updated_at": config.updated_at.isoformat() if config.updated_at else None,
    }


def update_settings(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Invalid JSON body")

    platform = _normalize_platform(payload.get("platform") or "android")
    config = get_or_create_config(platform)

    if "force_update_below" in payload:
        config.force_update_below = _parse_optional_threshold(
            payload.get("force_update_below"),
            field_name="force_update_below",
        )

    if "optional_update_below" in payload:
        config.optional_update_below = _parse_optional_threshold(
            payload.get("optional_update_below"),
            field_name="optional_update_below",
        )

    if "latest_version" in payload:
        config.latest_version = _parse_optional_threshold(
            payload.get("latest_version"),
            field_name="latest_version",
        )

    if "download_url" in payload:
        config.download_url = (payload.get("download_url") or "").strip() or DEFAULT_DOWNLOAD_URL

    if "force_title" in payload:
        config.force_title = (payload.get("force_title") or "").strip() or None

    if "force_message" in payload:
        config.force_message = (payload.get("force_message") or "").strip() or None

    if "optional_title" in payload:
        config.optional_title = (payload.get("optional_title") or "").strip() or None

    if "optional_message" in payload:
        config.optional_message = (payload.get("optional_message") or "").strip() or None

    db.session.commit()
    return serialize_settings(config)


def evaluate_version(version: str, platform: str = "android") -> dict:
    normalized_platform = _normalize_platform(platform)
    normalized_version, client_tuple = normalize_version(version)

    config = get_or_create_config(normalized_platform)

    force_threshold = None
    if config.force_update_below:
        _, force_threshold = normalize_version(config.force_update_below)

    optional_threshold = None
    if config.optional_update_below:
        _, optional_threshold = normalize_version(config.optional_update_below)

    # Logic requested by product:
    # - version < force_update_below => force
    # - else version <= optional_update_below => optional
    # - else => none
    if force_threshold is not None and client_tuple < force_threshold:
        return {
            "action": "force",
            "is_blocking": True,
            "title": config.force_title or DEFAULT_FORCE_TITLE,
            "message": config.force_message or DEFAULT_FORCE_MESSAGE,
            "normalized_version": normalized_version,
            "latest_version": config.latest_version,
            "download_url": (config.download_url or "").strip() or DEFAULT_DOWNLOAD_URL,
        }

    if optional_threshold is not None and client_tuple <= optional_threshold:
        return {
            "action": "optional",
            "is_blocking": False,
            "title": config.optional_title or DEFAULT_OPTIONAL_TITLE,
            "message": config.optional_message or DEFAULT_OPTIONAL_MESSAGE,
            "normalized_version": normalized_version,
            "latest_version": config.latest_version,
            "download_url": (config.download_url or "").strip() or DEFAULT_DOWNLOAD_URL,
        }

    return {
        "action": "none",
        "is_blocking": False,
        "title": None,
        "message": None,
        "normalized_version": normalized_version,
        "latest_version": config.latest_version,
        "download_url": (config.download_url or "").strip() or DEFAULT_DOWNLOAD_URL,
    }


def resolve_download_url(download_url: str | None, request: Request, public_base_url: str | None = None) -> str:
    raw_url = (download_url or "").strip() or DEFAULT_DOWNLOAD_URL
    if raw_url.startswith("http://") or raw_url.startswith("https://"):
        return raw_url

    configured_base = (public_base_url or "").strip().rstrip("/")
    if configured_base:
        return urljoin(f"{configured_base}/", raw_url.lstrip("/"))

    request_base = request.host_url.rstrip("/")
    return urljoin(f"{request_base}/", raw_url.lstrip("/"))
