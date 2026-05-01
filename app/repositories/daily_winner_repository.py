from datetime import datetime

from sqlalchemy import text

from app.db import db


_RECENT_POST_SCORES_SQL = text(
    """
    WITH recent_posts AS (
        SELECT
            p.id,
            p.author_id,
            p.created_at
        FROM posts AS p
        WHERE p.created_at >= :window_start
          AND p.created_at < :window_end
          AND p.is_hidden IS FALSE
    ),
    vote_scores AS (
        SELECT
            v.target_id AS post_id,
            SUM(
                CASE
                    WHEN v.value > 0 THEN :upvote_score
                    WHEN v.value < 0 THEN -:downvote_score
                    ELSE 0
                END
            ) AS vote_points
        FROM votes AS v
        INNER JOIN recent_posts AS rp
            ON rp.id = v.target_id
        WHERE v.target_type = 'post'
        GROUP BY v.target_id
    ),
    comment_scores AS (
        SELECT
            c.post_id,
            COUNT(c.id) * :comment_score AS comment_points
        FROM comments AS c
        INNER JOIN recent_posts AS rp
            ON rp.id = c.post_id
        WHERE c.is_deleted IS FALSE
        GROUP BY c.post_id
    )
    SELECT
        rp.id AS post_id,
        rp.author_id AS author_id,
        rp.created_at AS created_at,
        (
            COALESCE(vs.vote_points, 0) +
            COALESCE(cs.comment_points, 0)
        ) AS total_score
    FROM recent_posts AS rp
    LEFT JOIN vote_scores AS vs
        ON vs.post_id = rp.id
    LEFT JOIN comment_scores AS cs
        ON cs.post_id = rp.id
    ORDER BY total_score DESC, rp.created_at DESC, rp.id DESC
    """
)


def list_recent_post_scores(
    *,
    window_start: datetime,
    window_end: datetime,
    upvote_score: int,
    downvote_score: int,
    comment_score: int,
):
    """Return scored posts created in the given window, highest score first."""
    rows = db.session.execute(
        _RECENT_POST_SCORES_SQL,
        {
            "window_start": window_start,
            "window_end": window_end,
            "upvote_score": max(0, int(upvote_score)),
            "downvote_score": max(0, int(downvote_score)),
            "comment_score": max(0, int(comment_score)),
        },
    )

    return [
        {
            "post_id": row.post_id,
            "author_id": row.author_id,
            "created_at": row.created_at,
            "total_score": int(row.total_score or 0),
        }
        for row in rows
    ]
