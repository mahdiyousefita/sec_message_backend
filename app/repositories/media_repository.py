from app.models.media_model import Media
from app.db import db


def add_media(
    post_id,
    object_name,
    mime_type,
    display_name=None,
    title=None,
    artist=None,
):
    media = Media(
        post_id=post_id,
        object_name=object_name,
        mime_type=mime_type,
        display_name=display_name,
        title=title,
        artist=artist,
    )
    db.session.add(media)
    # db.session.commit()
    return media
