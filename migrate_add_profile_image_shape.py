"""
One-time migration: add profile_image_shape column to profiles.

Run from project root:
    source venv/bin/activate
    python migrate_add_profile_image_shape.py
"""

from sqlalchemy import inspect, text

from app import create_app
from app.db import db


def migrate():
    app = create_app()
    with app.app_context():
        inspector = inspect(db.engine)
        if not inspector.has_table("profiles"):
            print("[!] profiles table not found. Nothing to migrate.")
            return

        columns = {column["name"] for column in inspector.get_columns("profiles")}
        if "profile_image_shape" not in columns:
            db.session.execute(
                text(
                    "ALTER TABLE profiles "
                    "ADD COLUMN profile_image_shape VARCHAR(32) NOT NULL DEFAULT 'circle'"
                )
            )
            db.session.commit()
            print("[✓] Added profiles.profile_image_shape column.")
        else:
            db.session.execute(
                text(
                    "UPDATE profiles "
                    "SET profile_image_shape = 'circle' "
                    "WHERE profile_image_shape IS NULL OR TRIM(profile_image_shape) = ''"
                )
            )
            db.session.commit()
            print("[✓] profiles.profile_image_shape already exists. Null/blank values normalized.")


if __name__ == "__main__":
    migrate()
