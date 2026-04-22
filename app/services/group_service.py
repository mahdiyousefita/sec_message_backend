from flask import current_app, has_request_context, request

from app.models.group_model import Group
from app.models.profile_model import Profile
from app.repositories import user_repository, group_repository, message_repository
from app.repositories.follow_repository import is_following


MAX_GROUP_NAME_LENGTH = 120
MIN_GROUP_NAME_LENGTH = 1


class GroupLimitReachedError(Exception):
    pass


class NotMutualFollowError(Exception):
    pass


class NotGroupMemberError(Exception):
    pass


class NotGroupCreatorError(Exception):
    pass


def _build_profile_image_url(image_object_name: str | None) -> str | None:
    if not image_object_name:
        return None
    base_url = current_app.config.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")
    if image_object_name.startswith("static/"):
        return f"{base_url}/{image_object_name}" if base_url else f"/{image_object_name}"
    return f"{base_url}/media/{image_object_name}" if base_url else f"/media/{image_object_name}"


def _build_group_format_maps(groups: list[Group]):
    if not groups:
        return {}, {}

    creator_ids = {
        group.creator_id
        for group in groups
        if group.creator_id is not None
    }
    profile_rows = (
        Profile.query
        .with_entities(Profile.user_id, Profile.name, Profile.image_object_name)
        .filter(Profile.user_id.in_(creator_ids))
        .all()
    ) if creator_ids else []
    creator_profile_by_user_id = {
        row[0]: {
            "name": row[1],
            "image_object_name": row[2],
        }
        for row in profile_rows
    }

    member_count_by_group_id = group_repository.get_group_member_counts(
        [group.id for group in groups]
    )
    return creator_profile_by_user_id, member_count_by_group_id


def get_mutual_followers(username: str, page: int = 1, limit: int = 50):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    if not isinstance(page, int) or page < 1:
        page = 1
    if not isinstance(limit, int) or limit < 1:
        limit = 50
    if limit > 100:
        limit = 100

    from sqlalchemy.orm import aliased
    from app.db import db
    from app.models.follow_model import Follow
    from app.models.user_model import User
    from app.models.profile_model import Profile

    f1 = aliased(Follow)
    f2 = aliased(Follow)
    mutual_user = aliased(User)

    query = (
        db.session.query(
            mutual_user.id,
            mutual_user.username,
            Profile.name,
            Profile.image_object_name,
        )
        .join(f1, f1.following_id == mutual_user.id)
        .join(f2, (f2.follower_id == mutual_user.id) & (f2.following_id == user.id))
        .outerjoin(Profile, Profile.user_id == mutual_user.id)
        .filter(f1.follower_id == user.id)
        .order_by(mutual_user.username.asc())
    )

    total = query.count()
    rows = query.offset((page - 1) * limit).limit(limit).all()

    users = [
        {
            "id": row.id,
            "username": row.username,
            "name": row.name or row.username,
            "profile_image_url": _build_profile_image_url(row.image_object_name),
        }
        for row in rows
    ]

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "users": users,
    }


def create_group(creator_username: str, group_name: str, member_usernames: list[str]):
    creator = user_repository.get_by_username(creator_username)
    if not creator:
        raise ValueError("User not found")

    if not group_name or not group_name.strip():
        raise ValueError("Group name is required")

    group_name = group_name.strip()
    if len(group_name) < MIN_GROUP_NAME_LENGTH:
        raise ValueError("Group name is too short")
    if len(group_name) > MAX_GROUP_NAME_LENGTH:
        raise ValueError(f"Group name must be at most {MAX_GROUP_NAME_LENGTH} characters")

    current_count = group_repository.count_groups_created_by(creator.id)
    if current_count >= Group.MAX_GROUPS_PER_USER:
        raise GroupLimitReachedError(
            f"You can create at most {Group.MAX_GROUPS_PER_USER} groups"
        )

    valid_member_ids = []
    invalid_usernames = []
    not_mutual = []

    seen = set()
    for uname in member_usernames:
        uname = uname.strip()
        if not uname or uname == creator_username or uname in seen:
            continue
        seen.add(uname)

        target = user_repository.get_by_username(uname)
        if not target:
            invalid_usernames.append(uname)
            continue

        creator_follows_target = is_following(creator.id, target.id)
        target_follows_creator = is_following(target.id, creator.id)

        if not (creator_follows_target and target_follows_creator):
            not_mutual.append(uname)
            continue

        valid_member_ids.append(target.id)

    if invalid_usernames:
        raise ValueError(f"Users not found: {', '.join(invalid_usernames)}")

    if not_mutual:
        raise NotMutualFollowError(
            f"Mutual follow required for: {', '.join(not_mutual)}"
        )

    group = group_repository.create_group(group_name, creator.id)

    for member_id in valid_member_ids:
        group_repository.add_member(group.id, member_id)

    group_repository.bump_membership_version(group.id)
    return _format_group(group)


def get_user_groups(username: str):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    groups = group_repository.get_groups_for_user(user.id)
    creator_profile_by_user_id, member_count_by_group_id = _build_group_format_maps(groups)
    result = []
    for group in groups:
        formatted = _format_group(
            group,
            creator_profile_by_user_id=creator_profile_by_user_id,
            member_count_by_group_id=member_count_by_group_id,
        )
        unread_count = message_repository.get_group_pending_count(username, group.id)
        formatted["has_unread"] = unread_count > 0
        formatted["unread_count"] = unread_count
        result.append(formatted)
    return result


def get_group_unread_summary(username: str):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    groups = group_repository.get_groups_for_user(user.id)
    unread_groups = []
    total = 0

    for group in groups:
        pending = message_repository.peek_group_messages_for_user(username, group.id)
        if not pending:
            continue

        total += len(pending)
        latest = pending[-1]
        unread_groups.append(
            {
                "group_id": group.id,
                "group_name": group.name,
                "count": len(pending),
                "last_type": latest.get("type", "text"),
                "last_timestamp": latest.get("timestamp", ""),
                "last_sender": latest.get("from", "Someone"),
            }
        )

    return {
        "total": total,
        "groups": unread_groups,
    }


def get_group_detail(username: str, group_id: int):
    user = user_repository.get_by_username(username)
    if not user:
        raise ValueError("User not found")

    group = group_repository.get_group_by_id(group_id)
    if not group:
        raise ValueError("Group not found")

    if not group_repository.is_member(group_id, user.id):
        raise NotGroupMemberError("You are not a member of this group")

    members = group_repository.get_group_members(group_id)
    formatted = _format_group(group)
    formatted["members"] = [
        {
            "id": m["id"],
            "username": m["username"],
            "name": m["name"],
            "profile_image_url": _build_profile_image_url(m["image_object_name"]),
            "public_key": m["public_key"],
        }
        for m in members
    ]
    formatted["member_count"] = len(members)
    return formatted


def get_group_members(username: str, group_id: int):
    group = get_group_detail(username, group_id)
    return {
        "group_id": group["id"],
        "group_name": group["name"],
        "member_count": group["member_count"],
        "can_manage_members": group["creator"]["username"] == username,
        "members": group["members"],
    }


def add_members_to_group(requester_username: str, group_id: int, target_usernames: list[str]):
    requester = user_repository.get_by_username(requester_username)
    if not requester:
        raise ValueError("User not found")

    group = group_repository.get_group_by_id(group_id)
    if not group:
        raise ValueError("Group not found")

    if group.creator_id != requester.id:
        raise NotGroupCreatorError("Only the group creator can add members")

    if not isinstance(target_usernames, list):
        raise ValueError("usernames must be a list")

    normalized_usernames: list[str] = []
    seen = set()
    for raw_username in target_usernames:
        if not isinstance(raw_username, str):
            continue
        normalized = raw_username.strip()
        if (
            not normalized
            or normalized == requester_username
            or normalized in seen
        ):
            continue
        seen.add(normalized)
        normalized_usernames.append(normalized)

    if not normalized_usernames:
        raise ValueError("At least one valid username is required")

    valid_targets = []
    invalid_usernames = []
    not_mutual = []

    for username in normalized_usernames:
        target = user_repository.get_by_username(username)
        if not target:
            invalid_usernames.append(username)
            continue

        creator_follows = is_following(requester.id, target.id)
        target_follows = is_following(target.id, requester.id)
        if not (creator_follows and target_follows):
            not_mutual.append(username)
            continue

        valid_targets.append(target)

    if invalid_usernames:
        raise ValueError(f"Users not found: {', '.join(invalid_usernames)}")

    if not_mutual:
        raise NotMutualFollowError(
            f"Mutual follow required for: {', '.join(not_mutual)}"
        )

    added_usernames = []
    already_member_usernames = []

    for target in valid_targets:
        added = group_repository.add_member(group_id, target.id)
        if added:
            added_usernames.append(target.username)
        else:
            already_member_usernames.append(target.username)

    if added_usernames:
        group_repository.bump_membership_version(group_id)

    return {
        "added": added_usernames,
        "already_members": already_member_usernames,
    }


def add_member_to_group(requester_username: str, group_id: int, target_username: str):
    result = add_members_to_group(requester_username, group_id, [target_username])
    if not result["added"]:
        raise ValueError("User is already a member of this group")

    return True


def remove_member_from_group(requester_username: str, group_id: int, target_username: str):
    requester = user_repository.get_by_username(requester_username)
    target = user_repository.get_by_username(target_username)
    if not requester:
        raise ValueError("User not found")
    if not target:
        raise ValueError("Target user not found")

    group = group_repository.get_group_by_id(group_id)
    if not group:
        raise ValueError("Group not found")

    if requester.id == target.id:
        if group.creator_id == requester.id:
            raise ValueError("Group creator cannot leave the group. Delete the group instead.")
        removed = group_repository.remove_member(group_id, requester.id)
        if not removed:
            raise ValueError("You are not a member of this group")
        group_repository.bump_membership_version(group_id)
        return True

    if group.creator_id != requester.id:
        raise NotGroupCreatorError("Only the group creator can remove members")

    removed = group_repository.remove_member(group_id, target.id)
    if not removed:
        raise ValueError("User is not a member of this group")
    group_repository.bump_membership_version(group_id)
    return True


def delete_group(requester_username: str, group_id: int):
    requester = user_repository.get_by_username(requester_username)
    if not requester:
        raise ValueError("User not found")

    group = group_repository.get_group_by_id(group_id)
    if not group:
        raise ValueError("Group not found")

    if group.creator_id != requester.id:
        raise NotGroupCreatorError("Only the group creator can delete the group")

    group_repository.delete_group(group_id)
    group_repository.clear_membership_version(group_id)
    return True


def _format_group(
    group: Group,
    *,
    creator_profile_by_user_id: dict[int, dict] | None = None,
    member_count_by_group_id: dict[int, int] | None = None,
) -> dict:
    creator_profile_by_user_id = creator_profile_by_user_id or {}
    member_count_by_group_id = member_count_by_group_id or {}

    creator_profile = creator_profile_by_user_id.get(group.creator_id)
    if creator_profile is None:
        profile = Profile.query.filter_by(user_id=group.creator_id).first()
        creator_profile = (
            {
                "name": profile.name,
                "image_object_name": profile.image_object_name,
            }
            if profile else None
        )

    creator_username = group.creator.username if group.creator else f"user-{group.creator_id}"
    member_count = member_count_by_group_id.get(group.id)
    if member_count is None:
        member_count = group_repository.get_group_member_count(group.id)

    return {
        "id": group.id,
        "name": group.name,
        "creator": {
            "id": group.creator_id,
            "username": creator_username,
            "name": (
                creator_profile.get("name")
                if creator_profile and creator_profile.get("name")
                else creator_username
            ),
            "profile_image_url": _build_profile_image_url(
                creator_profile.get("image_object_name")
                if creator_profile else None
            ),
        },
        "member_count": int(member_count),
        "created_at": group.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }
