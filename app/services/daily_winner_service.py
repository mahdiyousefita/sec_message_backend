from __future__ import annotations

import random
from datetime import datetime, timedelta

from sqlalchemy import text

from app.config import Config
from app.constants.badges import DAILY_WINNER_BADGE
from app.db import db
from app.models.post_model import Post
from app.models.user_model import User
from app.repositories.daily_winner_repository import list_recent_post_scores

_DAILY_WINNER_LOCK_KEY = 90421001


def resolve_cycle_end(run_at: datetime | None = None) -> datetime:
    """
    Return the latest scheduled 21:00 boundary at-or-before run_at.
    All windows are [cycle_end - 24h, cycle_end).
    """
    current = run_at or datetime.now()
    cycle_end = current.replace(hour=21, minute=0, second=0, microsecond=0)
    if current < cycle_end:
        cycle_end = cycle_end - timedelta(days=1)
    return cycle_end


def run_daily_winner_selection(
    *,
    run_at: datetime | None = None,
    cycle_end_override: datetime | None = None,
    source: str = "scheduler",
) -> dict:
    cycle_end = cycle_end_override or resolve_cycle_end(run_at=run_at)
    window_start = cycle_end - timedelta(hours=24)

    if not _try_acquire_daily_winner_lock():
        return {
            "status": "skipped_lock_not_acquired",
            "source": source,
            "cycle_end": cycle_end.isoformat(),
            "window_start": window_start.isoformat(),
        }

    existing_winner = Post.query.filter_by(daily_winner_at=cycle_end).first()
    if existing_winner:
        _set_current_winner(existing_winner)
        badge_result = _apply_daily_winner_badge_assignment(existing_winner.author_id)
        db.session.commit()
        return {
            "status": "already_selected",
            "source": source,
            "cycle_end": cycle_end.isoformat(),
            "window_start": window_start.isoformat(),
            "winner_post_id": existing_winner.id,
            "winner_author_id": existing_winner.author_id,
            "badge_action": badge_result["action"],
        }

    scored_posts = list_recent_post_scores(
        window_start=window_start,
        window_end=cycle_end,
        upvote_score=int(Config.UPVOTE_SCORE),
        downvote_score=int(Config.DOWNVOTE_SCORE),
        comment_score=int(Config.COMMENT_SCORE),
    )
    if not scored_posts:
        _clear_current_winner_state()
        _clear_daily_winner_badges()
        db.session.commit()
        return {
            "status": "no_candidates",
            "source": source,
            "cycle_end": cycle_end.isoformat(),
            "window_start": window_start.isoformat(),
        }

    selected = _select_winner(scored_posts)
    winner_post = Post.query.filter_by(id=selected["post_id"]).first()
    if winner_post is None:
        db.session.rollback()
        return {
            "status": "winner_not_found",
            "source": source,
            "cycle_end": cycle_end.isoformat(),
            "window_start": window_start.isoformat(),
        }

    _set_current_winner(winner_post)
    winner_post.daily_winner_at = cycle_end
    badge_result = _apply_daily_winner_badge_assignment(winner_post.author_id)
    db.session.commit()

    return {
        "status": "selected",
        "source": source,
        "cycle_end": cycle_end.isoformat(),
        "window_start": window_start.isoformat(),
        "winner_post_id": winner_post.id,
        "winner_author_id": winner_post.author_id,
        "winner_score": selected["total_score"],
        "badge_action": badge_result["action"],
    }


def _try_acquire_daily_winner_lock() -> bool:
    bind = db.session.get_bind()
    if bind is None:
        return True
    dialect_name = bind.dialect.name
    if dialect_name != "postgresql":
        return True

    got_lock = db.session.execute(
        text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
        {"lock_key": _DAILY_WINNER_LOCK_KEY},
    ).scalar()
    return bool(got_lock)


def _set_current_winner(winner_post: Post):
    (
        Post.query
        .filter(
            Post.is_daily_winner.is_(True),
            Post.id != winner_post.id,
        )
        .update(
            {Post.is_daily_winner: False},
            synchronize_session=False,
        )
    )
    winner_post.is_daily_winner = True


def _clear_current_winner_state():
    (
        Post.query
        .filter(Post.is_daily_winner.is_(True))
        .update(
            {Post.is_daily_winner: False},
            synchronize_session=False,
        )
    )


def _clear_daily_winner_badges():
    (
        User.query
        .filter(User.badge == DAILY_WINNER_BADGE)
        .update({User.badge: None}, synchronize_session=False)
    )


def _apply_daily_winner_badge_assignment(winner_author_id: int) -> dict:
    winner_user = User.query.filter_by(id=winner_author_id).first()
    if winner_user is None:
        raise ValueError("Winner user not found")

    (
        User.query
        .filter(
            User.badge == DAILY_WINNER_BADGE,
            User.id != winner_author_id,
        )
        .update({User.badge: None}, synchronize_session=False)
    )

    current_badge = (winner_user.badge or "").strip() if winner_user.badge else None
    if current_badge and current_badge != DAILY_WINNER_BADGE:
        # Preserve pre-existing badges like staff/moderator/verified.
        return {"action": "kept_existing_badge"}

    winner_user.badge = DAILY_WINNER_BADGE
    return {"action": "assigned_daily_winner_badge"}


def _select_winner(scored_posts: list[dict]) -> dict:
    top_score = scored_posts[0]["total_score"]
    top_candidates = [
        row
        for row in scored_posts
        if row["total_score"] == top_score
    ]

    newest_created_at = max(row["created_at"] for row in top_candidates)
    newest_candidates = [
        row
        for row in top_candidates
        if row["created_at"] == newest_created_at
    ]

    if len(newest_candidates) == 1:
        return newest_candidates[0]

    chooser = random.SystemRandom()
    return chooser.choice(newest_candidates)


def get_daily_winner_status(*, now: datetime | None = None) -> dict:
    current_time = now or datetime.now()
    cycle_end = resolve_cycle_end(run_at=current_time)
    next_cycle_end = cycle_end + timedelta(days=1)
    seconds_until_next_cycle_end = max(
        0,
        int((next_cycle_end - current_time).total_seconds()),
    )
    current_winner = Post.query.filter(Post.is_daily_winner.is_(True)).first()

    winner_payload = None
    if current_winner is not None:
        winner_user = User.query.filter_by(id=current_winner.author_id).first()
        winner_payload = {
            "post_id": current_winner.id,
            "author_id": current_winner.author_id,
            "author_username": winner_user.username if winner_user else None,
            "author_badge": winner_user.badge if winner_user else None,
            "daily_winner_at": (
                current_winner.daily_winner_at.isoformat()
                if current_winner.daily_winner_at is not None
                else None
            ),
            "created_at": (
                current_winner.created_at.isoformat()
                if current_winner.created_at is not None
                else None
            ),
            "text_preview": (current_winner.text or "")[:160],
        }

    return {
        "server_time": current_time.isoformat(),
        "scheduled_cycle_end": cycle_end.isoformat(),
        "next_scheduled_cycle_end": next_cycle_end.isoformat(),
        "seconds_until_next_cycle_end": seconds_until_next_cycle_end,
        "current_winner": winner_payload,
        "scheduler_enabled": bool(Config.POST_OF_DAY_SCHEDULER_ENABLED),
        "scoring": {
            "upvote_score": int(Config.UPVOTE_SCORE),
            "downvote_score": int(Config.DOWNVOTE_SCORE),
            "comment_score": int(Config.COMMENT_SCORE),
        },
    }
