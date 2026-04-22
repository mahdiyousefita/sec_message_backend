from sqlalchemy import func
from sqlalchemy.orm import aliased, joinedload

from app.db import db
from app.extensions.redis_client import redis_client
from app.models.group_model import Group, GroupMember
from app.models.user_model import User
from app.models.profile_model import Profile

GROUP_MEMBERSHIP_VERSION_HASH = "group:membership_versions"


def _normalize_group_id(group_id):
    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return None
    return normalized_group_id if normalized_group_id > 0 else None


def _decode_redis_int(value, default=0):
    if value is None:
        return default
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def count_groups_created_by(user_id: int) -> int:
    return Group.query.filter_by(creator_id=user_id).count()


def create_group(name: str, creator_id: int) -> Group:
    group = Group(name=name, creator_id=creator_id)
    db.session.add(group)
    db.session.flush()

    member = GroupMember(group_id=group.id, user_id=creator_id)
    db.session.add(member)
    db.session.commit()
    return group


def add_member(group_id: int, user_id: int) -> bool:
    existing = GroupMember.query.filter_by(
        group_id=group_id, user_id=user_id
    ).first()
    if existing:
        return False
    db.session.add(GroupMember(group_id=group_id, user_id=user_id))
    db.session.commit()
    return True


def remove_member(group_id: int, user_id: int) -> bool:
    member = GroupMember.query.filter_by(
        group_id=group_id, user_id=user_id
    ).first()
    if not member:
        return False
    db.session.delete(member)
    db.session.commit()
    return True


def is_member(group_id: int, user_id: int) -> bool:
    return (
        GroupMember.query.filter_by(
            group_id=group_id, user_id=user_id
        ).first()
        is not None
    )


def is_username_member(group_id: int, username: str) -> bool:
    normalized_group_id = _normalize_group_id(group_id)
    normalized_username = (username or "").strip()
    if not normalized_group_id or not normalized_username:
        return False

    return (
        db.session.query(GroupMember.id)
        .join(User, User.id == GroupMember.user_id)
        .filter(
            GroupMember.group_id == normalized_group_id,
            User.username == normalized_username,
        )
        .first()
        is not None
    )


def get_membership_version(group_id: int) -> int:
    normalized_group_id = _normalize_group_id(group_id)
    if not normalized_group_id:
        return 0

    try:
        raw_value = redis_client.hget(
            GROUP_MEMBERSHIP_VERSION_HASH,
            str(normalized_group_id),
        )
    except Exception:
        return 0

    return _decode_redis_int(raw_value, default=0)


def bump_membership_version(group_id: int) -> int:
    normalized_group_id = _normalize_group_id(group_id)
    if not normalized_group_id:
        return 0

    try:
        return int(
            redis_client.hincrby(
                GROUP_MEMBERSHIP_VERSION_HASH,
                str(normalized_group_id),
                1,
            )
        )
    except Exception:
        return get_membership_version(normalized_group_id)


def clear_membership_version(group_id: int) -> None:
    normalized_group_id = _normalize_group_id(group_id)
    if not normalized_group_id:
        return

    try:
        redis_client.hdel(
            GROUP_MEMBERSHIP_VERSION_HASH,
            str(normalized_group_id),
        )
    except Exception:
        return


def get_group_by_id(group_id: int) -> Group | None:
    return Group.query.get(group_id)


def get_groups_for_user(user_id: int):
    return (
        Group.query.join(GroupMember, GroupMember.group_id == Group.id)
        .options(joinedload(Group.creator))
        .filter(GroupMember.user_id == user_id)
        .order_by(Group.created_at.desc())
        .all()
    )


def get_group_members(group_id: int):
    rows = (
        db.session.query(
            User.id,
            User.username,
            Profile.name,
            Profile.image_object_name,
            User.public_key,
        )
        .join(GroupMember, GroupMember.user_id == User.id)
        .outerjoin(Profile, Profile.user_id == User.id)
        .filter(GroupMember.group_id == group_id)
        .order_by(User.username.asc())
        .all()
    )
    return [
        {
            "id": row.id,
            "username": row.username,
            "name": row.name or row.username,
            "image_object_name": row.image_object_name,
            "public_key": row.public_key,
        }
        for row in rows
    ]


def get_group_member_usernames(group_id: int) -> list[str]:
    rows = (
        db.session.query(User.username)
        .join(GroupMember, GroupMember.user_id == User.id)
        .filter(GroupMember.group_id == group_id)
        .all()
    )
    return [row[0] for row in rows]


def get_group_member_count(group_id: int) -> int:
    return GroupMember.query.filter_by(group_id=group_id).count()


def get_group_member_counts(group_ids: list[int]) -> dict[int, int]:
    if not group_ids:
        return {}

    normalized_ids = sorted({
        int(group_id)
        for group_id in group_ids
        if group_id is not None
    })
    if not normalized_ids:
        return {}

    rows = (
        db.session.query(
            GroupMember.group_id,
            func.count(GroupMember.user_id),
        )
        .filter(GroupMember.group_id.in_(normalized_ids))
        .group_by(GroupMember.group_id)
        .all()
    )
    return {
        int(row[0]): int(row[1])
        for row in rows
    }


def delete_group(group_id: int) -> bool:
    group = Group.query.get(group_id)
    if not group:
        return False
    db.session.delete(group)
    db.session.commit()
    return True
