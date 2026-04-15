from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import inspect, text


@dataclass(frozen=True)
class IndexSpec:
    table_name: str
    index_name: str
    ddl_target: str
    rollback_safe: bool = False


@dataclass(frozen=True)
class LookupRequirement:
    table_name: str
    columns: tuple[str, ...]


MANAGED_INDEX_SPECS: tuple[IndexSpec, ...] = (
    IndexSpec(
        table_name="posts",
        index_name="ix_posts_feed_visible_created",
        ddl_target="posts (is_hidden, followers_only, created_at DESC)",
    ),
    IndexSpec(
        table_name="posts",
        index_name="ix_posts_author_visible_created",
        ddl_target="posts (author_id, is_hidden, created_at DESC)",
    ),
    IndexSpec(
        table_name="posts",
        index_name="ix_posts_author_created",
        ddl_target="posts (author_id, created_at DESC)",
        rollback_safe=True,
    ),
    IndexSpec(
        table_name="posts",
        index_name="ix_posts_hidden_created",
        ddl_target="posts (is_hidden, created_at DESC)",
        rollback_safe=True,
    ),
    IndexSpec(
        table_name="posts",
        index_name="ix_posts_quoted_post_id",
        ddl_target="posts (quoted_post_id)",
        rollback_safe=True,
    ),
    IndexSpec(
        table_name="comments",
        index_name="ix_comments_post_parent_score_created",
        ddl_target="comments (post_id, parent_id, score DESC, created_at DESC)",
    ),
    IndexSpec(
        table_name="comments",
        index_name="ix_comments_post_parent",
        ddl_target="comments (post_id, parent_id)",
    ),
    IndexSpec(
        table_name="comments",
        index_name="ix_comments_post_parent_created",
        ddl_target="comments (post_id, parent_id, created_at DESC)",
        rollback_safe=True,
    ),
    IndexSpec(
        table_name="votes",
        index_name="ix_votes_target_lookup",
        ddl_target="votes (target_type, target_id)",
    ),
    IndexSpec(
        table_name="follows",
        index_name="ix_follows_following_follower",
        ddl_target="follows (following_id, follower_id)",
    ),
    IndexSpec(
        table_name="blocks",
        index_name="ix_blocks_blocked_blocker",
        ddl_target="blocks (blocked_id, blocker_id)",
        rollback_safe=True,
    ),
    IndexSpec(
        table_name="group_members",
        index_name="ix_group_members_user_group",
        ddl_target="group_members (user_id, group_id)",
        rollback_safe=True,
    ),
    IndexSpec(
        table_name="media",
        index_name="ix_media_post_id",
        ddl_target="media (post_id)",
    ),
    IndexSpec(
        table_name="playlist_tracks",
        index_name="ix_playlist_tracks_user_created",
        ddl_target="playlist_tracks (user_id, created_at, id)",
    ),
    IndexSpec(
        table_name="activity_notifications",
        index_name="ix_activity_notifications_recipient_unread_created",
        ddl_target="activity_notifications (recipient_id, is_read, created_at DESC)",
    ),
    IndexSpec(
        table_name="activity_notifications",
        index_name="ix_activity_notifications_recipient_kind_target_created",
        ddl_target=(
            "activity_notifications "
            "(recipient_id, kind, target_type, target_id, created_at DESC)"
        ),
    ),
    IndexSpec(
        table_name="post_reports",
        index_name="ix_post_reports_status_created",
        ddl_target="post_reports (status, created_at DESC, id DESC)",
    ),
    IndexSpec(
        table_name="post_reports",
        index_name="ix_post_reports_reporter_post_status",
        ddl_target="post_reports (reporter_id, post_id, status)",
    ),
)

LOOKUP_REQUIREMENTS: tuple[LookupRequirement, ...] = (
    LookupRequirement(
        table_name="follows",
        columns=("follower_id", "following_id"),
    ),
    LookupRequirement(
        table_name="follows",
        columns=("following_id", "follower_id"),
    ),
    LookupRequirement(
        table_name="blocks",
        columns=("blocker_id", "blocked_id"),
    ),
    LookupRequirement(
        table_name="blocks",
        columns=("blocked_id", "blocker_id"),
    ),
    LookupRequirement(
        table_name="group_members",
        columns=("group_id", "user_id"),
    ),
    LookupRequirement(
        table_name="group_members",
        columns=("user_id", "group_id"),
    ),
)

ROLLBACK_SAFE_INDEX_NAMES: tuple[str, ...] = tuple(
    spec.index_name
    for spec in MANAGED_INDEX_SPECS
    if spec.rollback_safe
)


def ensure_performance_indexes(session, engine) -> tuple[int, int]:
    inspector = inspect(engine)
    created = 0
    ensured = 0

    for spec in MANAGED_INDEX_SPECS:
        if not inspector.has_table(spec.table_name):
            continue
        ensured += 1

        existing_names = {
            item["name"]
            for item in inspector.get_indexes(spec.table_name)
        }
        if spec.index_name in existing_names:
            continue

        session.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS {spec.index_name} "
                f"ON {spec.ddl_target}"
            )
        )
        created += 1

    session.commit()
    return ensured, created


def drop_indexes(session, engine, index_names: Iterable[str]) -> int:
    index_by_name = {
        spec.index_name: spec
        for spec in MANAGED_INDEX_SPECS
    }
    inspector = inspect(engine)
    dropped = 0

    for index_name in index_names:
        spec = index_by_name.get(index_name)
        if not spec or not inspector.has_table(spec.table_name):
            continue

        existing_names = {
            item["name"]
            for item in inspector.get_indexes(spec.table_name)
        }
        if index_name not in existing_names:
            continue

        session.execute(text(f"DROP INDEX IF EXISTS {index_name}"))
        dropped += 1

    session.commit()
    return dropped


def collect_missing_managed_indexes(engine) -> list[IndexSpec]:
    inspector = inspect(engine)
    missing = []

    for spec in MANAGED_INDEX_SPECS:
        if not inspector.has_table(spec.table_name):
            continue

        existing_names = {
            item["name"]
            for item in inspector.get_indexes(spec.table_name)
        }
        if spec.index_name not in existing_names:
            missing.append(spec)
    return missing


def collect_lookup_coverage_gaps(engine) -> list[LookupRequirement]:
    inspector = inspect(engine)
    gaps = []
    by_table = _lookup_column_sets_by_table(inspector)

    for requirement in LOOKUP_REQUIREMENTS:
        if not inspector.has_table(requirement.table_name):
            continue

        column_sets = by_table.get(requirement.table_name, ())
        if _has_prefix_lookup(column_sets, requirement.columns):
            continue
        gaps.append(requirement)

    return gaps


def _lookup_column_sets_by_table(inspector) -> dict[str, tuple[tuple[str, ...], ...]]:
    table_names = {item.table_name for item in LOOKUP_REQUIREMENTS}
    by_table = {}

    for table_name in table_names:
        if not inspector.has_table(table_name):
            continue
        values = []

        for item in inspector.get_indexes(table_name):
            columns = tuple(item.get("column_names") or ())
            if columns:
                values.append(columns)

        for item in inspector.get_unique_constraints(table_name):
            columns = tuple(item.get("column_names") or ())
            if columns:
                values.append(columns)

        by_table[table_name] = tuple(values)

    return by_table


def _has_prefix_lookup(
    available_column_sets: tuple[tuple[str, ...], ...],
    required_columns: tuple[str, ...],
) -> bool:
    required_len = len(required_columns)
    for column_set in available_column_sets:
        if len(column_set) < required_len:
            continue
        if column_set[:required_len] == required_columns:
            return True
    return False
