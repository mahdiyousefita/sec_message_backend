from sqlalchemy import func

from app.db import db
from app.models.vote_model import Vote
from app.repositories.comment_repository import create_comment, get_comments_by_post
from app.repositories.vote_repository import get_score


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


def build_comment_tree(comments):
    comment_map = {}
    roots = []

    for c in comments:
        comment_map[c.id] = {
            "id": c.id,
            "author_id": c.author_id,
            "text": None if c.is_deleted else c.text,
            "score": get_score("comment", c.id),
            "created_at": c.created_at.isoformat(),
            "replies": []
        }

    for c in comments:
        node = comment_map[c.id]
        if c.parent_id:
            parent = comment_map.get(c.parent_id)
            if parent:
                parent["replies"].append(node)
        else:
            roots.append(node)

    return roots


def get_post_comments(post_id):
    comments = get_comments_by_post(post_id)
    return build_comment_tree(comments)