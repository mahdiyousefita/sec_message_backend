from sqlalchemy import func, select

from app.db import db
from app.models.comment_model import Comment
from app.models.vote_model import Vote


def create_comment(author_id, post_id, text, parent_id=None):
    from app.services import report_service

    post = report_service.get_visible_post(post_id)
    if not post:
        raise ValueError("Post not found")

    if parent_id:
        parent = Comment.query.get(parent_id)
        if not parent or parent.post_id != post_id:
            raise ValueError("Invalid parent comment")

    comment = Comment(
        author_id=author_id,
        post_id=post_id,
        parent_id=parent_id,
        text=text.strip()
    )

    db.session.add(comment)
    return comment

def get_comments_by_post(post_id):
    score = func.coalesce(func.sum(Vote.value), 0)

    return (
        db.session.query(Comment)
        .outerjoin(
            Vote,
            (Vote.target_type == "comment") &
            (Vote.target_id == Comment.id)
        )
        .filter(Comment.post_id == post_id)
        .group_by(Comment.id)
        .order_by(
            score.desc(),
            Comment.created_at.desc()
        )
        .all()
    )
def get_all_comments_by_post_id(post_id: int):
    return (
        db.session.query(Comment)
        .filter(Comment.post_id == post_id)
        .all()
    )

def get_root_comments_by_post_id(
    post_id: int,
    page: int,
    page_size: int
):
    offset = (page - 1) * page_size

    return (
        db.session.query(Comment)
        .filter(
            Comment.post_id == post_id,
            Comment.parent_id.is_(None)
        )
        .order_by(
            Comment.score.desc(),
            Comment.created_at.desc()
        )
        .limit(page_size)
        .offset(offset)
        .all()
    )


def count_root_comments_by_post_id(post_id: int) -> int:
    return (
        db.session.query(func.count(Comment.id))
        .filter(
            Comment.post_id == post_id,
            Comment.parent_id.is_(None),
        )
        .scalar()
        or 0
    )


def get_comment_subtree_for_roots(post_id: int, root_ids: list[int]):
    if not root_ids:
        return []

    root_cte = (
        select(Comment.id)
        .where(
            Comment.post_id == post_id,
            Comment.id.in_(root_ids),
        )
        .cte(name="comment_tree", recursive=True)
    )
    children = (
        select(Comment.id)
        .where(
            Comment.post_id == post_id,
            Comment.parent_id == root_cte.c.id,
        )
    )
    comment_tree = root_cte.union_all(children)

    return (
        db.session.query(Comment)
        .join(comment_tree, Comment.id == comment_tree.c.id)
        .all()
    )
