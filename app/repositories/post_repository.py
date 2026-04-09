from app.models.post_model import Post
from app.db import db
from app.models.user_model import User


def create_post_by_username(username, text, followers_only=False):
    user = User.query.filter_by(username=username).first()
    if not user:
        raise ValueError("User not found")
    if getattr(user, "is_suspended", False):
        raise ValueError("Account suspended")

    post = Post(
        author_id=user.id,
        text=text,
        followers_only=bool(followers_only),
    )
    db.session.add(post)
    db.session.flush()

    return post

def get_posts(page, size, sort):
    raise NotImplementedError
