import unittest
from unittest.mock import patch

from app.performance_indexes import (
    LOOKUP_REQUIREMENTS,
    MANAGED_INDEX_SPECS,
    ROLLBACK_SAFE_INDEX_NAMES,
    collect_lookup_coverage_gaps,
    collect_missing_managed_indexes,
)


class _InspectorStub:
    def __init__(self, *, tables, indexes, unique_constraints):
        self._tables = set(tables)
        self._indexes = indexes
        self._unique_constraints = unique_constraints

    def has_table(self, table_name):
        return table_name in self._tables

    def get_indexes(self, table_name):
        return list(self._indexes.get(table_name, []))

    def get_unique_constraints(self, table_name):
        return list(self._unique_constraints.get(table_name, []))


class TestPerformanceIndexes(unittest.TestCase):
    def test_manifest_contains_required_task5_indexes(self):
        names = {spec.index_name for spec in MANAGED_INDEX_SPECS}
        self.assertIn("ix_posts_author_created", names)
        self.assertIn("ix_posts_hidden_created", names)
        self.assertIn("ix_comments_post_parent_created", names)
        self.assertIn(
            "ix_activity_notifications_recipient_unread_created",
            names,
        )
        self.assertIn("ix_follows_following_follower", names)
        self.assertIn("ix_blocks_blocked_blocker", names)
        self.assertIn("ix_group_members_user_group", names)

    def test_lookup_requirements_cover_both_directions(self):
        requirement_set = {
            (item.table_name, item.columns)
            for item in LOOKUP_REQUIREMENTS
        }
        self.assertIn(("follows", ("follower_id", "following_id")), requirement_set)
        self.assertIn(("follows", ("following_id", "follower_id")), requirement_set)
        self.assertIn(("blocks", ("blocker_id", "blocked_id")), requirement_set)
        self.assertIn(("blocks", ("blocked_id", "blocker_id")), requirement_set)
        self.assertIn(("group_members", ("group_id", "user_id")), requirement_set)
        self.assertIn(("group_members", ("user_id", "group_id")), requirement_set)

    def test_lookup_gap_check_accepts_unique_and_secondary_indexes(self):
        inspector = _InspectorStub(
            tables={"follows", "blocks", "group_members"},
            indexes={
                "follows": [
                    {
                        "name": "ix_follows_following_follower",
                        "column_names": ["following_id", "follower_id"],
                    }
                ],
                "blocks": [
                    {
                        "name": "ix_blocks_blocked_blocker",
                        "column_names": ["blocked_id", "blocker_id"],
                    }
                ],
                "group_members": [
                    {
                        "name": "ix_group_members_user_group",
                        "column_names": ["user_id", "group_id"],
                    }
                ],
            },
            unique_constraints={
                "follows": [
                    {
                        "name": "unique_follow_pair",
                        "column_names": ["follower_id", "following_id"],
                    }
                ],
                "blocks": [
                    {
                        "name": "unique_block_pair",
                        "column_names": ["blocker_id", "blocked_id"],
                    }
                ],
                "group_members": [
                    {
                        "name": "unique_group_member",
                        "column_names": ["group_id", "user_id"],
                    }
                ],
            },
        )

        with patch("app.performance_indexes.inspect", return_value=inspector):
            gaps = collect_lookup_coverage_gaps(engine=object())
        self.assertEqual(gaps, [])

    def test_lookup_gap_check_reports_missing_reverse_index(self):
        inspector = _InspectorStub(
            tables={"group_members"},
            indexes={},
            unique_constraints={
                "group_members": [
                    {
                        "name": "unique_group_member",
                        "column_names": ["group_id", "user_id"],
                    }
                ]
            },
        )

        with patch("app.performance_indexes.inspect", return_value=inspector):
            gaps = collect_lookup_coverage_gaps(engine=object())

        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].table_name, "group_members")
        self.assertEqual(gaps[0].columns, ("user_id", "group_id"))

    def test_missing_managed_indexes_reports_absent_indexes(self):
        inspector = _InspectorStub(
            tables={"posts"},
            indexes={
                "posts": [
                    {
                        "name": "ix_posts_feed_visible_created",
                        "column_names": ["is_hidden", "followers_only", "created_at"],
                    }
                ]
            },
            unique_constraints={},
        )

        with patch("app.performance_indexes.inspect", return_value=inspector):
            missing = collect_missing_managed_indexes(engine=object())

        missing_names = {item.index_name for item in missing}
        self.assertIn("ix_posts_author_created", missing_names)
        self.assertIn("ix_posts_hidden_created", missing_names)

    def test_rollback_index_list_is_expected(self):
        self.assertEqual(
            set(ROLLBACK_SAFE_INDEX_NAMES),
            {
                "ix_posts_author_created",
                "ix_posts_hidden_created",
                "ix_comments_post_parent_created",
                "ix_blocks_blocked_blocker",
                "ix_group_members_user_group",
            },
        )


if __name__ == "__main__":
    unittest.main()
