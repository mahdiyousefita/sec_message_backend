from app import db
from app.models.vote_model import Vote
from sqlalchemy import func

def upsert_vote(user_id, target_type, target_id, value):
    vote = Vote.query.filter_by(
        user_id=user_id,
        target_type=target_type,
        target_id=target_id
    ).first()

    old_value = 0

    if vote:
        if vote.value == value:
            return 0  # هیچ تغییری
        old_value = vote.value
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
    return value - old_value
