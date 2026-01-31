from sqlalchemy import func

from app.db import db
from app.models.comment_model import Comment
from app.models.post_model import Post
from app.models.vote_model import Vote


def create_comment(author_id, post_id, text, parent_id=None):
    post = Post.query.get(post_id)
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
