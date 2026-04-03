from sqlalchemy.orm import aliased

from app.db import db
from app.models.group_model import Group, GroupMember
from app.models.user_model import User
from app.models.profile_model import Profile


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


def get_group_by_id(group_id: int) -> Group | None:
    return Group.query.get(group_id)


def get_groups_for_user(user_id: int):
    return (
        Group.query.join(GroupMember, GroupMember.group_id == Group.id)
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


def delete_group(group_id: int) -> bool:
    group = Group.query.get(group_id)
    if not group:
        return False
    db.session.delete(group)
    db.session.commit()
    return True
