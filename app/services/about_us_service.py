import re
from urllib.parse import urlparse

from flask import current_app, has_request_context, request
from sqlalchemy import func

from app.db import db
from app.models.about_us_model import AboutUsConfig, AboutUsLink, AboutUsTeamMember
from app.models.profile_model import Profile
from app.models.user_model import User

LINK_TYPES = ("website", "source_code", "email")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _build_media_url(object_name: str | None):
    if not object_name:
        return None

    base_url = current_app.config.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")

    if object_name.startswith("static/"):
        if base_url:
            return f"{base_url}/{object_name}"
        return f"/{object_name}"

    if base_url:
        return f"{base_url}/media/{object_name}"
    return f"/media/{object_name}"


def _normalize_description(value) -> str | None:
    text = (value or "").strip()
    return text or None


def _validate_http_url(raw_value: str, field_name: str) -> str:
    parsed = urlparse(raw_value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{field_name} must be a valid http/https URL")
    return raw_value


def _validate_email(raw_value: str) -> str:
    if raw_value.startswith("mailto:"):
        address = raw_value[len("mailto:"):].strip()
    else:
        address = raw_value

    if not _EMAIL_RE.match(address):
        raise ValueError("email link must be a valid email address or mailto URL")
    return raw_value


def _validate_link_value(link_type: str, value: str) -> str:
    if link_type == "email":
        return _validate_email(value)
    return _validate_http_url(value, field_name=f"{link_type} link")


def _get_or_create_about_config() -> AboutUsConfig:
    config = AboutUsConfig.query.order_by(AboutUsConfig.id.asc()).first()
    if config:
        return config

    config = AboutUsConfig(description=None)
    db.session.add(config)
    db.session.commit()
    return config


def _get_link_map(config_id: int) -> dict[str, AboutUsLink]:
    rows = AboutUsLink.query.filter_by(config_id=config_id).all()
    return {row.link_type: row for row in rows}


def _get_team_member_payload(config_id: int) -> list[dict]:
    rows = (
        db.session.query(AboutUsTeamMember, User, Profile)
        .join(User, User.id == AboutUsTeamMember.user_id)
        .outerjoin(Profile, Profile.user_id == User.id)
        .filter(AboutUsTeamMember.config_id == config_id)
        .order_by(AboutUsTeamMember.sort_order.asc(), AboutUsTeamMember.id.asc())
        .all()
    )

    payload = []
    for membership, user, profile in rows:
        default_name = profile.name if profile and profile.name else user.username
        display_name = (membership.display_name or "").strip() or default_name
        payload.append(
            {
                "user_id": user.id,
                "username": user.username,
                "name": display_name,
                "custom_name": (membership.display_name or "").strip(),
                "profile_image_url": _build_media_url(
                    profile.image_object_name if profile else None
                ),
                "profile_image_shape": (
                    profile.profile_image_shape
                    if profile and profile.profile_image_shape
                    else "circle"
                ),
                "role": membership.role_description,
                "sort_order": membership.sort_order,
            }
        )
    return payload


def _serialize_public_payload(config: AboutUsConfig | None) -> dict:
    if not config:
        return {
            "description": "",
            "links": [],
            "team_members": [],
            "updated_at": None,
        }

    link_map = _get_link_map(config.id)
    links = []
    for link_type in LINK_TYPES:
        link = link_map.get(link_type)
        if not link:
            continue
        links.append(
            {
                "type": link.link_type,
                "title": link.title,
                "url": link.url,
                "is_disabled": bool(link.is_disabled),
            }
        )

    return {
        "description": config.description or "",
        "links": links,
        "team_members": _get_team_member_payload(config.id),
        "updated_at": config.updated_at.isoformat() if config.updated_at else None,
    }


def _serialize_admin_payload(config: AboutUsConfig) -> dict:
    link_map = _get_link_map(config.id)
    serialized_links = {
        link_type: {
            "title": "",
            "url": "",
            "is_disabled": False,
        }
        for link_type in LINK_TYPES
    }

    for link_type, link in link_map.items():
        if link_type not in serialized_links:
            continue
        serialized_links[link_type] = {
            "title": link.title,
            "url": link.url,
            "is_disabled": bool(link.is_disabled),
        }

    return {
        "id": config.id,
        "description": config.description or "",
        "links": serialized_links,
        "team_members": _get_team_member_payload(config.id),
        "created_at": config.created_at.isoformat() if config.created_at else None,
        "updated_at": config.updated_at.isoformat() if config.updated_at else None,
    }


def get_public_about_us() -> dict:
    config = AboutUsConfig.query.order_by(AboutUsConfig.id.asc()).first()
    return _serialize_public_payload(config)


def get_admin_about_us() -> dict:
    config = _get_or_create_about_config()
    return _serialize_admin_payload(config)


def _validate_and_replace_team_members(config_id: int, payload_members) -> None:
    if not isinstance(payload_members, list):
        raise ValueError("team_members must be a list")

    normalized_members = []
    seen_usernames = set()

    for index, item in enumerate(payload_members):
        if not isinstance(item, dict):
            raise ValueError("each team member must be an object")

        username = (item.get("username") or "").strip()
        role = (item.get("role") or item.get("role_description") or "").strip()
        custom_name = (
            item.get("custom_name")
            or item.get("display_name")
            or item.get("name")
            or ""
        ).strip()

        if not username and not role:
            continue
        if not username:
            raise ValueError("team member username is required")
        if not role:
            raise ValueError(f"team member role is required for @{username}")

        normalized_username = username.lower()
        if normalized_username in seen_usernames:
            raise ValueError(f"duplicate team member username: {username}")
        seen_usernames.add(normalized_username)

        normalized_members.append(
            {
                "username": username,
                "normalized_username": normalized_username,
                "role": role,
                "custom_name": custom_name or None,
                "sort_order": index,
            }
        )

    user_lookup = {}
    if normalized_members:
        normalized_usernames = [row["normalized_username"] for row in normalized_members]
        users = (
            User.query
            .filter(func.lower(User.username).in_(normalized_usernames))
            .all()
        )
        user_lookup = {user.username.lower(): user for user in users}

    missing = [
        row["username"]
        for row in normalized_members
        if row["normalized_username"] not in user_lookup
    ]
    if missing:
        raise ValueError(
            "Unknown usernames: " + ", ".join(sorted(missing))
        )

    AboutUsTeamMember.query.filter_by(config_id=config_id).delete(
        synchronize_session=False
    )
    for row in normalized_members:
        user = user_lookup[row["normalized_username"]]
        db.session.add(
            AboutUsTeamMember(
                config_id=config_id,
                user_id=user.id,
                display_name=row["custom_name"],
                role_description=row["role"],
                sort_order=row["sort_order"],
            )
        )


def _validate_and_replace_links(config_id: int, payload_links) -> None:
    if not isinstance(payload_links, dict):
        raise ValueError("links must be an object")

    unsupported = sorted(
        key for key in payload_links.keys()
        if key not in LINK_TYPES
    )
    if unsupported:
        raise ValueError(f"Unsupported link type(s): {', '.join(unsupported)}")

    existing_map = _get_link_map(config_id)

    for link_type in LINK_TYPES:
        item = payload_links.get(link_type)
        existing = existing_map.get(link_type)

        if item is None:
            if existing:
                db.session.delete(existing)
            continue

        if not isinstance(item, dict):
            raise ValueError(f"{link_type} link must be an object")

        title = (item.get("title") or "").strip()
        url = (item.get("url") or "").strip()
        is_disabled = bool(item.get("is_disabled", False))

        if not title and not url:
            if existing:
                db.session.delete(existing)
            continue

        if not title or not url:
            raise ValueError(f"{link_type} link requires both title and url")

        normalized_url = _validate_link_value(link_type, url)

        if existing:
            existing.title = title
            existing.url = normalized_url
            existing.is_disabled = is_disabled
            continue

        db.session.add(
            AboutUsLink(
                config_id=config_id,
                link_type=link_type,
                title=title,
                url=normalized_url,
                is_disabled=is_disabled,
            )
        )


def update_about_us(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Invalid JSON body")

    config = _get_or_create_about_config()

    if "description" in payload:
        config.description = _normalize_description(payload.get("description"))

    if "links" in payload:
        _validate_and_replace_links(config.id, payload.get("links"))

    if "team_members" in payload:
        _validate_and_replace_team_members(config.id, payload.get("team_members"))

    db.session.commit()
    return _serialize_admin_payload(config)
