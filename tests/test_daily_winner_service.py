import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch


class TestDailyWinnerService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_fd, cls.db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["JWT_SECRET_KEY"] = "test-secret"
        os.environ["POST_OF_DAY_SCHEDULER_ENABLED"] = "false"

        from app import create_app
        from app.db import db

        cls.app = create_app()
        cls.db = db

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)
        os.environ.pop("POST_OF_DAY_SCHEDULER_ENABLED", None)

    def setUp(self):
        with self.app.app_context():
            self.db.drop_all()
            self.db.create_all()

    def test_run_daily_winner_selection_sets_single_current_winner(self):
        from app.models.comment_model import Comment
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.models.vote_model import Vote
        from app.services.daily_winner_service import run_daily_winner_selection

        run_at = datetime(2026, 4, 30, 21, 0, 0)
        cycle_start = run_at - timedelta(hours=24)

        with self.app.app_context():
            alice = User(username="alice", password_hash="x", public_key="pk1")
            bob = User(username="bob", password_hash="x", public_key="pk2")
            self.db.session.add_all([alice, bob])
            self.db.session.flush()

            p1 = Post(author_id=alice.id, text="p1", created_at=cycle_start + timedelta(hours=1))
            p2 = Post(author_id=bob.id, text="p2", created_at=cycle_start + timedelta(hours=2))
            self.db.session.add_all([p1, p2])
            self.db.session.flush()

            self.db.session.add_all(
                [
                    Vote(user_id=alice.id, target_type="post", target_id=p1.id, value=1),
                    Vote(user_id=bob.id, target_type="post", target_id=p1.id, value=1),
                    Vote(user_id=alice.id, target_type="post", target_id=p2.id, value=1),
                ]
            )
            self.db.session.add(
                Comment(post_id=p2.id, author_id=alice.id, text="c1", is_deleted=False)
            )
            self.db.session.commit()

            result = run_daily_winner_selection(run_at=run_at, source="test")
            self.assertEqual(result["status"], "selected")
            winner_id = result["winner_post_id"]

            winners = Post.query.filter_by(is_daily_winner=True).all()
            self.assertEqual(len(winners), 1)
            self.assertEqual(winners[0].id, winner_id)
            self.assertEqual(winners[0].daily_winner_at, run_at)

    def test_run_daily_winner_selection_is_idempotent_for_same_cycle(self):
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.services.daily_winner_service import run_daily_winner_selection

        run_at = datetime(2026, 4, 30, 21, 0, 0)
        cycle_start = run_at - timedelta(hours=24)

        with self.app.app_context():
            user = User(username="alice", password_hash="x", public_key="pk1")
            self.db.session.add(user)
            self.db.session.flush()
            post = Post(
                author_id=user.id,
                text="winner",
                created_at=cycle_start + timedelta(minutes=30),
            )
            self.db.session.add(post)
            self.db.session.commit()

            first = run_daily_winner_selection(run_at=run_at, source="test")
            second = run_daily_winner_selection(run_at=run_at, source="test")

            self.assertEqual(first["status"], "selected")
            self.assertEqual(second["status"], "already_selected")
            self.assertEqual(first["winner_post_id"], second["winner_post_id"])

            winners = Post.query.filter_by(is_daily_winner=True).all()
            self.assertEqual(len(winners), 1)
            self.assertEqual(winners[0].daily_winner_at, run_at)

    def test_run_daily_winner_selection_tie_prefers_newest_post(self):
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.services.daily_winner_service import run_daily_winner_selection

        run_at = datetime(2026, 4, 30, 21, 0, 0)
        cycle_start = run_at - timedelta(hours=24)

        with self.app.app_context():
            user = User(username="alice", password_hash="x", public_key="pk1")
            self.db.session.add(user)
            self.db.session.flush()

            older = Post(
                author_id=user.id,
                text="older",
                created_at=cycle_start + timedelta(hours=1),
            )
            newer = Post(
                author_id=user.id,
                text="newer",
                created_at=cycle_start + timedelta(hours=5),
            )
            self.db.session.add_all([older, newer])
            self.db.session.commit()
            newer_id = newer.id

            result = run_daily_winner_selection(run_at=run_at, source="test")
            self.assertEqual(result["status"], "selected")
            self.assertEqual(result["winner_post_id"], newer_id)

    def test_winner_without_badge_gets_daily_badge(self):
        from app.constants.badges import DAILY_WINNER_BADGE
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.services.daily_winner_service import run_daily_winner_selection

        run_at = datetime(2026, 4, 30, 21, 0, 0)
        cycle_start = run_at - timedelta(hours=24)

        with self.app.app_context():
            user = User(username="alice", password_hash="x", public_key="pk1")
            self.db.session.add(user)
            self.db.session.flush()
            post = Post(
                author_id=user.id,
                text="winner",
                created_at=cycle_start + timedelta(hours=1),
            )
            self.db.session.add(post)
            self.db.session.commit()

            result = run_daily_winner_selection(run_at=run_at, source="test")
            self.assertEqual(result["badge_action"], "assigned_daily_winner_badge")

            refreshed_user = User.query.filter_by(id=user.id).first()
            self.assertEqual(refreshed_user.badge, DAILY_WINNER_BADGE)

    def test_winner_with_existing_badge_keeps_badge_and_previous_daily_cleared(self):
        from app.constants.badges import DAILY_WINNER_BADGE
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.services.daily_winner_service import run_daily_winner_selection

        run_at = datetime(2026, 4, 30, 21, 0, 0)
        cycle_start = run_at - timedelta(hours=24)

        with self.app.app_context():
            previous = User(
                username="prev",
                password_hash="x",
                public_key="pk_prev",
                badge=DAILY_WINNER_BADGE,
            )
            winner = User(
                username="winner",
                password_hash="x",
                public_key="pk_winner",
                badge="verified",
            )
            self.db.session.add_all([previous, winner])
            self.db.session.flush()
            post = Post(
                author_id=winner.id,
                text="winner-post",
                created_at=cycle_start + timedelta(hours=2),
            )
            self.db.session.add(post)
            self.db.session.commit()

            result = run_daily_winner_selection(run_at=run_at, source="test")
            self.assertEqual(result["badge_action"], "kept_existing_badge")

            refreshed_previous = User.query.filter_by(id=previous.id).first()
            refreshed_winner = User.query.filter_by(id=winner.id).first()
            self.assertIsNone(refreshed_previous.badge)
            self.assertEqual(refreshed_winner.badge, "verified")

    def test_no_candidates_clears_previous_daily_winner_state(self):
        from app.constants.badges import DAILY_WINNER_BADGE
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.services.daily_winner_service import run_daily_winner_selection

        run_at = datetime(2026, 5, 1, 21, 0, 0)
        old_cycle = run_at - timedelta(days=1)
        stale_created_at = run_at - timedelta(days=3)

        with self.app.app_context():
            user = User(
                username="yesterday_winner",
                password_hash="x",
                public_key="pk_prev",
                badge=DAILY_WINNER_BADGE,
            )
            self.db.session.add(user)
            self.db.session.flush()
            stale_winner_post = Post(
                author_id=user.id,
                text="stale-winner",
                created_at=stale_created_at,
                is_daily_winner=True,
                daily_winner_at=old_cycle,
            )
            self.db.session.add(stale_winner_post)
            self.db.session.commit()

            result = run_daily_winner_selection(run_at=run_at, source="test")
            self.assertEqual(result["status"], "no_candidates")

            refreshed_user = User.query.filter_by(id=user.id).first()
            refreshed_post = Post.query.filter_by(id=stale_winner_post.id).first()
            self.assertIsNone(refreshed_user.badge)
            self.assertFalse(bool(refreshed_post.is_daily_winner))

    def test_zero_score_posts_still_pick_newest(self):
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.services.daily_winner_service import run_daily_winner_selection

        run_at = datetime(2026, 4, 30, 21, 0, 0)
        cycle_start = run_at - timedelta(hours=24)

        with self.app.app_context():
            user = User(username="alice", password_hash="x", public_key="pk1")
            self.db.session.add(user)
            self.db.session.flush()

            older = Post(
                author_id=user.id,
                text="older-zero",
                created_at=cycle_start + timedelta(hours=1),
            )
            newer = Post(
                author_id=user.id,
                text="newer-zero",
                created_at=cycle_start + timedelta(hours=2),
            )
            self.db.session.add_all([older, newer])
            self.db.session.commit()
            newer_id = newer.id

            result = run_daily_winner_selection(run_at=run_at, source="test")
            self.assertEqual(result["status"], "selected")
            self.assertEqual(result["winner_post_id"], newer_id)

    def test_same_score_and_timestamp_uses_random_tie_breaker(self):
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.services.daily_winner_service import run_daily_winner_selection

        run_at = datetime(2026, 4, 30, 21, 0, 0)
        cycle_start = run_at - timedelta(hours=24)
        created_at = cycle_start + timedelta(hours=4)

        with self.app.app_context():
            user = User(username="alice", password_hash="x", public_key="pk1")
            self.db.session.add(user)
            self.db.session.flush()
            p1 = Post(author_id=user.id, text="same-1", created_at=created_at)
            p2 = Post(author_id=user.id, text="same-2", created_at=created_at)
            self.db.session.add_all([p1, p2])
            self.db.session.commit()
            p2_id = p2.id

            with patch(
                "app.services.daily_winner_service.random.SystemRandom.choice",
                return_value={"post_id": p2_id, "author_id": user.id, "created_at": created_at, "total_score": 0},
            ):
                result = run_daily_winner_selection(run_at=run_at, source="test")

            self.assertEqual(result["status"], "selected")
            self.assertEqual(result["winner_post_id"], p2_id)


if __name__ == "__main__":
    unittest.main()
