"""
One-time migration: add followers-only visibility flag to posts.

Run from project root:
    source venv/bin/activate
    python migrate_add_post_visibility.py
"""

from sqlalchemy import inspect, text

from app import create_app
from app.db import db


def migrate():
    app = create_app()
    with app.app_context():
        inspector = inspect(db.engine)
        if not inspector.has_table("posts"):
            print("[!] posts table not found. Nothing to migrate.")
            return

        columns = {
            column["name"]
            for column in inspector.get_columns("posts")
        }
        changed = False

        if "followers_only" not in columns:
            db.session.execute(
                text(
                    "ALTER TABLE posts "
                    "ADD COLUMN followers_only BOOLEAN NOT NULL DEFAULT 0"
                )
            )
            changed = True

        db.session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_posts_followers_only "
                "ON posts (followers_only)"
            )
        )
        db.session.commit()

        if changed:
            print("[✓] Added posts.followers_only column and index.")
        else:
            print("[✓] followers_only column already exists. Index ensured.")


if __name__ == "__main__":
    migrate()
