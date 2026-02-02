from app import db
from app.models.comment_model import Comment
from app.models.post_model import Post
from app.repositories.vote_repository import upsert_vote
from app.models.user_model import User


def vote(username: str, target_type: str, target_id: int, value: int):
    if target_type not in ("post", "comment"):
        raise ValueError("Invalid target type")

    if value not in (1, -1):
        raise ValueError("Invalid vote value")

    user = User.query.filter_by(username=username).first()
    if not user:
        raise ValueError("User not found")

    delta = upsert_vote(
        user_id=user.id,
        target_type=target_type,
        target_id=target_id,
        value=value
    )

    if delta == 0:
        return  # رأی تکراری، هیچ تغییری

    if target_type == "comment":
        comment = Comment.query.get(target_id)
        if comment:
            comment.score += delta

    elif target_type == "post":
        post = Post.query.get(target_id)
        if post:
            post.score += delta

    db.session.commit()