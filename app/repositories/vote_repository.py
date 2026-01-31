from app import db
from app.models.vote_model import Vote
from sqlalchemy import func

def upsert_vote(user_id, target_type, target_id, value):
    vote = Vote.query.filter_by(
        user_id=user_id,
        target_type=target_type,
        target_id=target_id
    ).first()

    if vote:
        if vote.value == value:
            return
        vote.value = value
    else:
        vote = Vote(
            user_id=user_id,
            target_type=target_type,
            target_id=target_id,
            value=value
        )
        db.session.add(vote)

    db.session.commit()


def get_score(target_type: str, target_id: int) -> int:
    return (
        db.session.query(func.coalesce(func.sum(Vote.value), 0))
        .filter(
            Vote.target_type == target_type,
            Vote.target_id == target_id
        )
        .scalar()
    )
