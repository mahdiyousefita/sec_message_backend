import os
import tempfile
import unittest
from datetime import datetime, timedelta


class TestDailyWinnerRepository(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_fd, cls.db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["JWT_SECRET_KEY"] = "test-secret"

        from app import create_app
        from app.db import db

        cls.app = create_app()
        cls.db = db

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    def setUp(self):
        with self.app.app_context():
            self.db.drop_all()
            self.db.create_all()

    def test_list_recent_post_scores_uses_weights_and_window(self):
        from app.models.comment_model import Comment
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.models.vote_model import Vote
        from app.repositories.daily_winner_repository import list_recent_post_scores

        now = datetime.utcnow()
        window_start = now - timedelta(hours=24)

        with self.app.app_context():
            alice = User(username="alice", password_hash="x", public_key="pk1")
            bob = User(username="bob", password_hash="x", public_key="pk2")
            self.db.session.add_all([alice, bob])
            self.db.session.flush()

            p1 = Post(author_id=alice.id, text="p1", created_at=now - timedelta(hours=3))
            p2 = Post(author_id=bob.id, text="p2", created_at=now - timedelta(hours=1))
            p3 = Post(author_id=alice.id, text="p3", created_at=now - timedelta(hours=30))
            self.db.session.add_all([p1, p2, p3])
            self.db.session.flush()
            p1_id = p1.id
            p2_id = p2.id

            self.db.session.add_all(
                [
                    Vote(user_id=alice.id, target_type="post", target_id=p1.id, value=1),
                    Vote(user_id=bob.id, target_type="post", target_id=p1.id, value=1),
                    Vote(user_id=alice.id, target_type="post", target_id=p2.id, value=1),
                    Vote(user_id=bob.id, target_type="post", target_id=p2.id, value=-1),
                    Vote(user_id=bob.id, target_type="comment", target_id=999, value=1),
                ]
            )
            self.db.session.add_all(
                [
                    Comment(post_id=p1.id, author_id=alice.id, text="c1", is_deleted=False),
                    Comment(post_id=p1.id, author_id=bob.id, text="c2", is_deleted=False),
                    Comment(post_id=p2.id, author_id=alice.id, text="c3", is_deleted=True),
                ]
            )
            self.db.session.commit()

            rows = list_recent_post_scores(
                window_start=window_start,
                window_end=now,
                upvote_score=3,
                downvote_score=1,
                comment_score=2,
            )

        self.assertEqual([row["post_id"] for row in rows], [p1_id, p2_id])
        self.assertEqual(rows[0]["total_score"], 10)  # 2*3 + 2*2
        self.assertEqual(rows[1]["total_score"], 2)   # 3 - 1

    def test_list_recent_post_scores_breaks_score_ties_by_newer_post(self):
        from app.models.post_model import Post
        from app.models.user_model import User
        from app.repositories.daily_winner_repository import list_recent_post_scores

        now = datetime.utcnow()
        window_start = now - timedelta(hours=24)

        with self.app.app_context():
            user = User(username="alice", password_hash="x", public_key="pk1")
            self.db.session.add(user)
            self.db.session.flush()

            older = Post(author_id=user.id, text="older", created_at=now - timedelta(hours=6))
            newer = Post(author_id=user.id, text="newer", created_at=now - timedelta(hours=2))
            self.db.session.add_all([older, newer])
            self.db.session.commit()
            older_id = older.id
            newer_id = newer.id

            rows = list_recent_post_scores(
                window_start=window_start,
                window_end=now,
                upvote_score=3,
                downvote_score=1,
                comment_score=2,
            )

        self.assertEqual([row["post_id"] for row in rows], [newer_id, older_id])


if __name__ == "__main__":
    unittest.main()
