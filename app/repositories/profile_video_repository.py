from app.db import db
from app.models.profile_video_model import ProfileVideo


def get_by_user_id(user_id: int):
    return ProfileVideo.query.filter_by(user_id=user_id).first()


def upsert_for_user(user_id: int, video_object_name: str):
    row = get_by_user_id(user_id)
    if not row:
        row = ProfileVideo(
            user_id=user_id,
            video_object_name=video_object_name,
        )
        db.session.add(row)
        return row

    row.video_object_name = video_object_name
    return row
