from sqlalchemy import or_

from app.db import db
from app.models.block_model import Block
from app.models.profile_model import Profile
from app.models.user_model import User


def is_blocking(blocker_id: int, blocked_id: int) -> bool:
    return (
        Block.query.filter_by(
            blocker_id=blocker_id,
            blocked_id=blocked_id,
        ).first()
        is not None
    )


def create_block(blocker_id: int, blocked_id: int) -> bool:
    if is_blocking(blocker_id, blocked_id):
        return False

    db.session.add(
        Block(
            blocker_id=blocker_id,
            blocked_id=blocked_id,
        )
    )
    db.session.commit()
    return True


def delete_block(blocker_id: int, blocked_id: int) -> bool:
    block = Block.query.filter_by(
        blocker_id=blocker_id,
        blocked_id=blocked_id,
    ).first()
    if not block:
        return False

    db.session.delete(block)
    db.session.commit()
    return True


def has_block_relation(user_a_id: int, user_b_id: int) -> bool:
    if user_a_id == user_b_id:
        return False

    return (
        Block.query.filter(
            or_(
                (Block.blocker_id == user_a_id) & (Block.blocked_id == user_b_id),
                (Block.blocker_id == user_b_id) & (Block.blocked_id == user_a_id),
            )
        ).first()
        is not None
    )


def get_hidden_user_ids_for_viewer(viewer_user_id: int) -> set[int]:
    if not viewer_user_id:
        return set()

    blocked_rows = (
        db.session.query(Block.blocked_id)
        .filter(Block.blocker_id == viewer_user_id)
        .all()
    )
    blocker_rows = (
        db.session.query(Block.blocker_id)
        .filter(Block.blocked_id == viewer_user_id)
        .all()
    )

    hidden_ids = {row[0] for row in blocked_rows}
    hidden_ids.update(row[0] for row in blocker_rows)
    return hidden_ids


def get_blocked_users_page(blocker_id: int, page: int, limit: int):
    if limit > 100:
        limit = 100

    total = (
        db.session.query(Block.id)
        .join(User, User.id == Block.blocked_id)
        .filter(
            Block.blocker_id == blocker_id,
            User.is_suspended.is_(False),
        )
        .count()
    )

    rows = (
        db.session.query(
            User.id,
            User.username,
            Profile.name,
            Profile.image_object_name,
        )
        .join(Block, Block.blocked_id == User.id)
        .outerjoin(Profile, Profile.user_id == User.id)
        .filter(
            Block.blocker_id == blocker_id,
            User.is_suspended.is_(False),
        )
        .order_by(User.username.asc())
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    users = [
        {
            "id": row.id,
            "username": row.username,
            "name": row.name or row.username,
            "image_object_name": row.image_object_name,
        }
        for row in rows
    ]
    return total, users
