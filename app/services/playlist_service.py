from flask import current_app, has_request_context, request
from urllib.parse import unquote, urlparse

from app.models.media_model import Media
from app.repositories import playlist_repository, user_repository


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


def _normalize_limit(limit: int) -> int:
    return max(1, min(int(limit or 20), 200))


def _extract_object_name_from_track_url(track_url: str | None) -> str | None:
    normalized = (track_url or "").strip()
    if not normalized:
        return None

    parsed = urlparse(normalized)
    path = parsed.path or normalized
    decoded_path = unquote(path).lstrip("/")

    if decoded_path.startswith("media/"):
        object_name = decoded_path[len("media/"):]
        return object_name or None

    if decoded_path.startswith("static/"):
        return decoded_path

    return decoded_path or None


def _serialize_track(entry, media: Media):
    title = (media.title or "").strip()
    display_name = (media.display_name or "").strip()

    if not title:
        title = display_name.rsplit(".", 1)[0] if "." in display_name else display_name
    if not title:
        title = "Music track"

    artist = (media.artist or "").strip() or None
    if artist and artist.lower() == title.lower():
        artist = None

    return {
        "id": entry.id,
        "media_id": media.id,
        "track_url": _build_media_url(media.object_name),
        "title": title,
        "artist": artist,
        "display_name": media.display_name,
        "mime_type": media.mime_type,
        "added_at": entry.created_at.isoformat(),
    }


def add_track_by_username(username: str, media_id: int):
    user = user_repository.get_by_username(username)
    if not user or getattr(user, "is_suspended", False):
        raise ValueError("User not found")

    media = Media.query.get(media_id)
    if not media:
        raise ValueError("Music track not found")
    if not (media.mime_type or "").lower().startswith("audio/"):
        raise ValueError("Only audio tracks can be added to playlists")

    entry, created = playlist_repository.add_track(
        user_id=user.id,
        media_id=media.id,
    )

    return {
        "created": created,
        "track": _serialize_track(entry, media),
    }


def get_tracks_by_username(
    username: str,
    page: int,
    limit: int,
):
    user = user_repository.get_by_username(username)
    if not user or getattr(user, "is_suspended", False):
        raise ValueError("User not found")

    normalized_page = max(1, int(page or 1))
    normalized_limit = _normalize_limit(limit)

    total, rows = playlist_repository.get_user_tracks_page(
        user_id=user.id,
        page=normalized_page,
        limit=normalized_limit,
    )
    tracks = [_serialize_track(entry, media) for entry, media in rows]

    return {
        "page": normalized_page,
        "limit": normalized_limit,
        "total": total,
        "tracks": tracks,
    }


def track_exists_in_user_playlist(username: str, track_url: str) -> bool:
    user = user_repository.get_by_username(username)
    if not user or getattr(user, "is_suspended", False):
        raise ValueError("User not found")

    object_name = _extract_object_name_from_track_url(track_url)
    if object_name is None:
        return False

    return playlist_repository.user_track_exists_by_object_name(
        user_id=user.id,
        object_name=object_name,
    )


def remove_track_by_username(username: str, track_id: int):
    user = user_repository.get_by_username(username)
    if not user or getattr(user, "is_suspended", False):
        raise ValueError("User not found")

    normalized_track_id = int(track_id or 0)
    if normalized_track_id <= 0:
        raise ValueError("track_id must be a positive integer")

    removed = playlist_repository.remove_user_track_by_id(
        user_id=user.id,
        track_id=normalized_track_id,
    )
    if not removed:
        raise ValueError("Playlist track not found")

    return {
        "message": "Track removed from playlist",
    }
