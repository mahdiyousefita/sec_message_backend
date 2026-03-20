from app.services import follow_service
from app.services import notification_service
from app.socket_events import is_user_online

from app.models.user_model import User
from app.models.profile_model import Profile
from app.models.profile_video_model import ProfileVideo
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


def get_contacts_with_message_status(username):
    followed_contacts = follow_service.get_following_for_username(username)
    pending_senders = notification_service.pending_message_senders(username)

    all_contact_usernames = list(dict.fromkeys(
        followed_contacts + sorted(pending_senders - set(followed_contacts))
    ))

    contact_users = User.query.filter(User.username.in_(all_contact_usernames)).all() if all_contact_usernames else []
    user_by_username = {u.username: u for u in contact_users}
    user_ids = {u.id for u in contact_users}

    profiles = Profile.query.filter(Profile.user_id.in_(user_ids)).all() if user_ids else []
    profile_by_user_id = {p.user_id: p for p in profiles}

    profile_videos = ProfileVideo.query.filter(ProfileVideo.user_id.in_(user_ids)).all() if user_ids else []
    video_by_user_id = {pv.user_id: pv for pv in profile_videos}

    result = []
    for contact_username in all_contact_usernames:
        has_message = contact_username in pending_senders
        last_message = None
        if has_message:
            last_message = notification_service.get_last_message_preview(
                username, contact_username
            )

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
    return result


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
