from app.models.media_model import Media
from app.db import db

def add_media(post_id, object_name, mime_type):
    media = Media(
        post_id=post_id,
        object_name=object_name,
        mime_type=mime_type
    )
    db.session.add(media)
    # db.session.commit()
    return media
