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

    upsert_vote(
        user_id=user.id,
        target_type=target_type,
        target_id=target_id,
        value=value
    )
