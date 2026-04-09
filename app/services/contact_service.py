from app.services import follow_service
from app.services import notification_service
from app.socket_events import is_user_online
from app.repositories import group_repository

from app.models.user_model import User
from app.models.profile_model import Profile
from app.models.profile_video_model import ProfileVideo
from app.repositories import message_repository
from flask import current_app, has_request_context, request


def add_contact(username, contact):
    if not isinstance(contact, str) or not contact.strip():
        raise ValueError("Contact not found")

    try:
        follow_service.follow_by_username(username, contact.strip())
    except ValueError as e:
        if str(e) == "User not found":
            raise ValueError("Contact not found")
        raise


def get_contacts(username):
    contacts = follow_service.get_following_for_username(username)
    return contacts


MAX_CONTACTS_LIMIT = 100
DEFAULT_CONTACTS_LIMIT = 20


def get_contacts_with_message_status(username, page=1, limit=DEFAULT_CONTACTS_LIMIT):
    if not isinstance(page, int) or page < 1:
        page = 1
    if not isinstance(limit, int) or limit < 1:
        limit = DEFAULT_CONTACTS_LIMIT
    if limit > MAX_CONTACTS_LIMIT:
        limit = MAX_CONTACTS_LIMIT

    followed_contacts = follow_service.get_following_for_username(username)
    unread_summary = notification_service.get_unread_summary_map(username)
    pending_senders = set(unread_summary["per_sender"].keys())
    sender_last_timestamp = {
        sender: summary.get("last_timestamp")
        for sender, summary in unread_summary["per_sender"].items()
    }

    all_contact_set = set(followed_contacts) | pending_senders

    for contact in all_contact_set:
        if message_repository.get_contact_timestamp_score(username, contact) is None:
            last_ts = sender_last_timestamp.get(contact)
            if last_ts:
                message_repository.record_conversation_timestamp(
                    username,
                    contact,
                    last_ts,
                )

    total_with_ts = message_repository.count_contacts_with_timestamps(username)
    ts_contacts_with_no_ts = all_contact_set.copy()

    offset = (page - 1) * limit
    sorted_pairs = message_repository.get_contacts_sorted_by_last_message(
        username, offset=offset, count=limit
    )
    sorted_usernames = [pair[0] for pair in sorted_pairs]

    for name in sorted_usernames:
        ts_contacts_with_no_ts.discard(name)

    remaining_without_ts = sorted(ts_contacts_with_no_ts - {
        pair[0] for pair in message_repository.get_contacts_sorted_by_last_message(
            username, offset=0, count=total_with_ts or 9999
        )
    })

    total = total_with_ts + len(remaining_without_ts)

    page_usernames = list(sorted_usernames)
    if len(page_usernames) < limit:
        remaining_offset = max(0, offset - total_with_ts)
        remaining_slice = remaining_without_ts[remaining_offset:remaining_offset + (limit - len(page_usernames))]
        page_usernames.extend(remaining_slice)

    contact_users = User.query.filter(User.username.in_(page_usernames)).all() if page_usernames else []
    user_by_username = {u.username: u for u in contact_users}
    user_ids = {u.id for u in contact_users}

    profiles = Profile.query.filter(Profile.user_id.in_(user_ids)).all() if user_ids else []
    profile_by_user_id = {p.user_id: p for p in profiles}

    profile_videos = ProfileVideo.query.filter(ProfileVideo.user_id.in_(user_ids)).all() if user_ids else []
    video_by_user_id = {pv.user_id: pv for pv in profile_videos}

    result = []
    for contact_username in page_usernames:
        sender_summary = unread_summary["per_sender"].get(contact_username)
        has_message = sender_summary is not None
        last_message = None
        if sender_summary:
            last_message = {
                "from": sender_summary.get("sender"),
                "type": sender_summary.get("last_type", "text"),
                "timestamp": sender_summary.get("last_timestamp", ""),
            }

        user = user_by_username.get(contact_username)
        profile = profile_by_user_id.get(user.id) if user else None
        profile_video = video_by_user_id.get(user.id) if user else None

        result.append({
            "username": contact_username,
            "name": profile.name if profile else None,
            "profile_image_url": _build_media_url(profile.image_object_name) if profile and profile.image_object_name else None,
            "profile_video_url": _build_media_url(profile_video.video_object_name) if profile_video and profile_video.video_object_name else None,
            "has_message": has_message,
            "online": is_user_online(contact_username),
            "last_message": last_message,
        })

    current_user = User.query.filter_by(username=username).first()
    groups = group_repository.get_groups_for_user(current_user.id) if current_user else []
    group_ids = [group.id for group in groups]
    creator_ids = {
        group.creator_id
        for group in groups
        if group.creator_id is not None
    }
    creator_profiles = (
        Profile.query
        .with_entities(Profile.user_id, Profile.name)
        .filter(Profile.user_id.in_(creator_ids))
        .all()
    ) if creator_ids else []
    creator_name_by_user_id = {
        row[0]: row[1]
        for row in creator_profiles
    }
    member_count_by_group_id = group_repository.get_group_member_counts(group_ids)

    group_list = []
    for grp in groups:
        creator_username = grp.creator.username if grp.creator else f"user-{grp.creator_id}"
        creator_name = creator_name_by_user_id.get(grp.creator_id) or creator_username
        group_list.append({
            "type": "group",
            "id": grp.id,
            "name": grp.name,
            "creator": {
                "id": grp.creator_id,
                "username": creator_username,
                "name": creator_name,
            },
            "member_count": int(member_count_by_group_id.get(grp.id, 0)),
            "created_at": grp.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        })

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "contacts": result,
        "groups": group_list,
    }


def _build_media_url(object_name):
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
