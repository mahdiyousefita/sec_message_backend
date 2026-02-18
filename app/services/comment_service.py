from sqlalchemy import func

from app.db import db
from app.models.user_model import User
from app.models.vote_model import Vote
from app.repositories.comment_repository import create_comment, get_comments_by_post
from app.repositories.comment_repository import get_root_comments_by_post_id


def add_comment(author_id, post_id, text, parent_id=None):
    if not text or not text.strip():
        raise ValueError("Comment text is required")

    comment = create_comment(
        author_id=author_id,
        post_id=post_id,
        text=text,
        parent_id=parent_id
    )

    db.session.commit()
    return comment


def get_post_comments(post_id):
    comments = get_comments_by_post(post_id)
    return build_comment_tree(comments)


def sort_comments(comments: list):
    return sorted(
        comments,
        key=lambda c: (-c.score, -c.created_at.timestamp())
    )


def build_comment_tree(comments):
    comment_map = {c["id"]: c for c in comments}
    roots = []

    for comment in comments:
        pid = comment["parent_id"]
        if pid:
            parent = comment_map.get(pid)
            if parent:
                parent["replies"].append(comment)
        else:
            roots.append(comment)

    return roots


def get_comments_tree_by_post(post_id: int, page: int, page_size: int):
    raw_comments = get_comments_by_post(post_id)

    author_ids = {comment.author_id for comment in raw_comments}
    users = User.query.filter(User.id.in_(author_ids)).all() if author_ids else []
    username_by_id = {user.id: user.username for user in users}

    all_comments = [serialize_comment(c, username_by_id) for c in raw_comments]

    tree = build_comment_tree(all_comments)

    root_comments = get_root_comments_by_post_id(post_id, page, page_size)
    root_ids = {c.id for c in root_comments}

    paged_roots = [c for c in tree if c["id"] in root_ids]

    return paged_roots


def serialize_comment(comment, username_by_id=None):
    username_by_id = username_by_id or {}
    author_id = comment.author_id

    return {
        "id": comment.id,
        "author_id": author_id,
        "author": username_by_id.get(author_id, f"user-{author_id}"),
        "text": comment.text,
        "score": comment.score,
        "created_at": comment.created_at,
        "parent_id": comment.parent_id,
        "replies": []
    }


def get_score(target_type: str, target_id: int) -> int:
    score = db.session.query(func.coalesce(func.sum(Vote.value), 0)).filter_by(
        target_type=target_type,
        target_id=target_id
    ).scalar()

    return int(score or 0)
