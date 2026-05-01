import json
from collections import defaultdict
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from app.db import db
from app.models.follow_model import Follow
from app.models.profile_model import Profile
from app.models.story_model import Story, StoryDailyQuota, StoryView
from app.models.user_model import User


def _utc_now():
    return datetime.utcnow()


def _loads_json_list(raw_value):
    if not raw_value:
        return []
    try:
        value = json.loads(raw_value)
    except (TypeError, ValueError):
        return []
    if not isinstance(value, list):
        return []
    return value


def create_story(
    *,
    user_id: int,
    media_url: str,
    media_type: str,
    expires_at: datetime,
    mention_user_ids: list[int] | None = None,
    auto_commit: bool = True,
):
    story = Story(
        user_id=user_id,
        media_url=media_url,
        media_type=media_type,
        expires_at=expires_at,
        mention_user_ids=json.dumps(mention_user_ids or []),
    )
    db.session.add(story)
    if auto_commit:
        db.session.commit()
    return story


def delete_expired_stories(*, before_dt: datetime, batch_size: int = 200):
    ids = (
        db.session.query(Story.id)
        .filter(Story.expires_at <= before_dt)
        .order_by(Story.expires_at.asc(), Story.id.asc())
        .limit(max(1, int(batch_size)))
        .all()
    )
    story_ids = [row[0] for row in ids]
    if not story_ids:
        return 0

    (
        StoryView.query
        .filter(StoryView.story_id.in_(story_ids))
        .delete(synchronize_session=False)
    )
    (
        Story.query
        .filter(Story.id.in_(story_ids))
        .delete(synchronize_session=False)
    )
    db.session.commit()
    return len(story_ids)


def get_active_story(story_id: int, *, now: datetime | None = None):
    now = now or _utc_now()
    return (
        Story.query
        .filter(
            Story.id == story_id,
            Story.expires_at > now,
        )
        .first()
    )


def get_story_by_id(story_id: int):
    return Story.query.filter(Story.id == story_id).first()


def delete_story_with_views(*, story_id: int):
    (
        StoryView.query
        .filter(StoryView.story_id == story_id)
        .delete(synchronize_session=False)
    )
    deleted = (
        Story.query
        .filter(Story.id == story_id)
        .delete(synchronize_session=False)
    )
    db.session.commit()
    return bool(deleted)


def get_active_stories_for_user(user_id: int, *, now: datetime | None = None):
    now = now or _utc_now()
    return (
        Story.query
        .filter(
            Story.user_id == user_id,
            Story.expires_at > now,
        )
        .order_by(Story.created_at.asc(), Story.id.asc())
        .all()
    )


def _fetch_following_ids(viewer_user_id: int):
    rows = (
        db.session.query(Follow.following_id)
        .filter(Follow.follower_id == viewer_user_id)
        .all()
    )
    return [row[0] for row in rows]


def get_active_feed_grouped(*, viewer_user_id: int, now: datetime | None = None):
    now = now or _utc_now()
    following_ids = _fetch_following_ids(viewer_user_id)
    candidate_user_ids = set(following_ids)
    candidate_user_ids.add(viewer_user_id)

    if not candidate_user_ids:
        return []

    stories = (
        Story.query
        .filter(
            Story.user_id.in_(list(candidate_user_ids)),
            Story.expires_at > now,
        )
        .order_by(Story.user_id.asc(), Story.created_at.asc(), Story.id.asc())
        .all()
    )
    if not stories:
        return []

    story_ids = [story.id for story in stories]
    viewed_story_ids = {
        row[0]
        for row in (
            db.session.query(StoryView.story_id)
            .filter(
                StoryView.viewer_id == viewer_user_id,
                StoryView.story_id.in_(story_ids),
            )
            .all()
        )
    }

    user_ids = {story.user_id for story in stories}
    users = User.query.filter(User.id.in_(list(user_ids))).all()
    profiles = Profile.query.filter(Profile.user_id.in_(list(user_ids))).all()
    user_by_id = {user.id: user for user in users}
    profile_by_user_id = {profile.user_id: profile for profile in profiles}

    grouped = defaultdict(list)
    for story in stories:
        grouped[story.user_id].append(story)

    response = []
    for user_id, user_stories in grouped.items():
        user = user_by_id.get(user_id)
        if not user:
            continue
        profile = profile_by_user_id.get(user_id)

        unseen = any(story.id not in viewed_story_ids for story in user_stories)
        first_story = user_stories[0]
        latest_story = user_stories[-1]

        response.append(
            {
                "user_id": user.id,
                "username": user.username,
                "name": profile.name if profile and profile.name else user.username,
                "badge": user.badge,
                "avatar_object_name": profile.image_object_name if profile else None,
                "profile_image_shape": (
                    profile.profile_image_shape if profile and profile.profile_image_shape else "circle"
                ),
                "has_unseen_story": unseen,
                "story_count": len(user_stories),
                "first_story_timestamp": first_story.created_at,
                "latest_story_timestamp": latest_story.created_at,
                "story_ids": [story.id for story in user_stories],
            }
        )

    response.sort(
        key=lambda item: (
            item["user_id"] != viewer_user_id,
            not item["has_unseen_story"],
            -(item["latest_story_timestamp"].timestamp() if item["latest_story_timestamp"] else 0),
        )
    )
    return response


def get_story_views_map(*, story_ids: list[int], viewer_user_id: int):
    if not story_ids:
        return {}
    rows = (
        StoryView.query
        .filter(
            StoryView.viewer_id == viewer_user_id,
            StoryView.story_id.in_(story_ids),
        )
        .all()
    )
    return {row.story_id: row for row in rows}


def record_story_view(*, story_id: int, viewer_user_id: int, viewed_at: datetime | None = None):
    viewed_at = viewed_at or _utc_now()
    row = (
        StoryView.query
        .filter(
            StoryView.story_id == story_id,
            StoryView.viewer_id == viewer_user_id,
        )
        .first()
    )
    if row is None:
        row = StoryView(
            story_id=story_id,
            viewer_id=viewer_user_id,
            viewed_at=viewed_at,
            liked=False,
        )
        db.session.add(row)
        try:
            db.session.flush()
        except IntegrityError:
            db.session.rollback()
            row = (
                StoryView.query
                .filter(
                    StoryView.story_id == story_id,
                    StoryView.viewer_id == viewer_user_id,
                )
                .first()
            )
            if row is None:
                raise
            created = False
        else:
            created = True
    else:
        created = False

    if created:
        (
            Story.query
            .filter(Story.id == story_id)
            .update({Story.view_count: Story.view_count + 1}, synchronize_session=False)
        )
    db.session.commit()
    return created


def set_story_like(*, story_id: int, viewer_user_id: int, liked: bool):
    now = _utc_now()
    row = (
        StoryView.query
        .filter(
            StoryView.story_id == story_id,
            StoryView.viewer_id == viewer_user_id,
        )
        .first()
    )
    if row is None:
        row = StoryView(
            story_id=story_id,
            viewer_id=viewer_user_id,
            viewed_at=now,
            liked=bool(liked),
        )
        db.session.add(row)
        db.session.flush()
        (
            Story.query
            .filter(Story.id == story_id)
            .update({Story.view_count: Story.view_count + 1}, synchronize_session=False)
        )
        if liked:
            (
                Story.query
                .filter(Story.id == story_id)
                .update({Story.like_count: Story.like_count + 1}, synchronize_session=False)
            )
        db.session.commit()
        return True

    was_liked = bool(row.liked)
    if was_liked == bool(liked):
        db.session.commit()
        return bool(liked)

    row.liked = bool(liked)
    if liked:
        (
            Story.query
            .filter(Story.id == story_id)
            .update({Story.like_count: Story.like_count + 1}, synchronize_session=False)
        )
    else:
        story = Story.query.filter(Story.id == story_id).first()
        if story is not None:
            story.like_count = max(0, int(story.like_count or 0) - 1)
    db.session.commit()
    return bool(liked)


def get_story_viewers_page(*, story_id: int, page: int, limit: int):
    query = (
        db.session.query(
            StoryView.viewer_id,
            StoryView.viewed_at,
            StoryView.liked,
            User.username,
            User.badge,
            Profile.image_object_name,
            Profile.profile_image_shape,
        )
        .join(User, User.id == StoryView.viewer_id)
        .outerjoin(Profile, Profile.user_id == User.id)
        .filter(StoryView.story_id == story_id)
        .order_by(StoryView.viewed_at.desc(), StoryView.id.desc())
    )
    total = query.count()
    rows = query.offset((page - 1) * limit).limit(limit).all()

    viewers = []
    for row in rows:
        viewers.append(
            {
                "viewer_id": row.viewer_id,
                "username": row.username,
                "badge": row.badge,
                "avatar_object_name": row.image_object_name,
                "profile_image_shape": row.profile_image_shape or "circle",
                "viewed_at": row.viewed_at,
                "liked": bool(row.liked),
            }
        )

    return total, viewers


def get_story_view_row(*, story_id: int, viewer_user_id: int):
    return (
        StoryView.query
        .filter(
            StoryView.story_id == story_id,
            StoryView.viewer_id == viewer_user_id,
        )
        .first()
    )


def count_user_stories_since(*, user_id: int, since_dt: datetime):
    return (
        db.session.query(Story.id)
        .filter(
            Story.user_id == user_id,
            Story.created_at >= since_dt,
        )
        .count()
    )


def reserve_daily_story_slot(*, user_id: int, bucket_start: datetime, limit: int, max_retries: int = 3):
    normalized_limit = max(1, int(limit))
    for _ in range(max(1, int(max_retries))):
        try:
            with db.session.begin_nested():
                quota = (
                    StoryDailyQuota.query
                    .filter(
                        StoryDailyQuota.user_id == user_id,
                        StoryDailyQuota.bucket_start == bucket_start,
                    )
                    .with_for_update()
                    .first()
                )
                if quota is None:
                    db.session.add(
                        StoryDailyQuota(
                            user_id=user_id,
                            bucket_start=bucket_start,
                            story_count=1,
                        )
                    )
                    db.session.flush()
                    return True

                if int(quota.story_count or 0) >= normalized_limit:
                    return False

                quota.story_count = int(quota.story_count or 0) + 1
                db.session.flush()
                return True
        except IntegrityError:
            continue
    return False


def release_daily_story_slot(*, user_id: int, bucket_start: datetime):
    quota = (
        StoryDailyQuota.query
        .filter(
            StoryDailyQuota.user_id == user_id,
            StoryDailyQuota.bucket_start == bucket_start,
        )
        .with_for_update()
        .first()
    )
    if quota is None:
        return False

    current = int(quota.story_count or 0)
    if current <= 1:
        db.session.delete(quota)
    else:
        quota.story_count = current - 1
    db.session.flush()
    return True
