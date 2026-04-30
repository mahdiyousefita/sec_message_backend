"""
One-time migration: add badge column to users.

Run from project root:
    source venv/bin/activate
    python migrate_add_user_badges.py
"""

from sqlalchemy import inspect, text

from app import create_app
from app.db import db


def migrate():
    app = create_app()
    with app.app_context():
        inspector = inspect(db.engine)
        if not inspector.has_table("users"):
            print("[!] users table not found. Nothing to migrate.")
            return

        columns = {
            column["name"]
            for column in inspector.get_columns("users")
        }
        changed = False

        if "badge" not in columns:
            db.session.execute(
                text(
                    "ALTER TABLE users "
                    "ADD COLUMN badge VARCHAR(64)"
                )
            )
            changed = True

        db.session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_users_badge "
                "ON users (badge)"
            )
        )
        db.session.commit()

        if changed:
            print("[✓] Added users.badge column and index.")
        else:
            print("[✓] users.badge already exists. Index ensured.")


if __name__ == "__main__":
    migrate()
