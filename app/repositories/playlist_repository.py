from app.db import db
from app.models.media_model import Media
from app.models.playlist_track_model import PlaylistTrack


def add_track(user_id: int, media_id: int):
    existing = PlaylistTrack.query.filter_by(
        user_id=user_id,
        media_id=media_id,
    ).first()
    if existing:
        return existing, False

    entry = PlaylistTrack(
        user_id=user_id,
        media_id=media_id,
    )
    db.session.add(entry)
    db.session.commit()
    return entry, True


def get_user_tracks_page(user_id: int, page: int, limit: int):
    base_query = PlaylistTrack.query.filter_by(user_id=user_id)
    total = (
        base_query.with_entities(db.func.count(PlaylistTrack.id))
        .order_by(None)
        .scalar()
        or 0
    )

    rows = (
        db.session.query(PlaylistTrack, Media)
        .join(Media, Media.id == PlaylistTrack.media_id)
        .filter(PlaylistTrack.user_id == user_id)
        .order_by(PlaylistTrack.created_at.asc(), PlaylistTrack.id.asc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )
    return total, rows


def user_track_exists_by_object_name(user_id: int, object_name: str) -> bool:
    if not object_name:
        return False

    exists_row = (
        db.session.query(PlaylistTrack.id)
        .join(Media, Media.id == PlaylistTrack.media_id)
        .filter(PlaylistTrack.user_id == user_id, Media.object_name == object_name)
        .limit(1)
        .first()
    )
    return exists_row is not None
