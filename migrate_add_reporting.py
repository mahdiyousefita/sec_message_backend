"""
One-time migration: add reporting + moderation lifecycle schema.

Run from project root:
    source venv/bin/activate
    python migrate_add_reporting.py
"""

from sqlalchemy import inspect, text

from app import create_app
from app.db import db


POST_COLUMNS = {
    "is_hidden": "BOOLEAN NOT NULL DEFAULT 0",
    "hidden_at": "DATETIME",
    "purge_after": "DATETIME",
    "hidden_reason": "VARCHAR(32)",
    "hidden_by_report_id": "INTEGER",
}

USER_COLUMNS = {
    "is_suspended": "BOOLEAN NOT NULL DEFAULT 0",
    "suspended_at": "DATETIME",
    "purge_after": "DATETIME",
    "suspension_reason": "VARCHAR(32)",
    "suspended_by_report_id": "INTEGER",
}


def _ensure_column(table_name: str, column_name: str, ddl_suffix: str):
    inspector = inspect(db.engine)
    existing = {column["name"] for column in inspector.get_columns(table_name)}
    if column_name in existing:
        return False

    db.session.execute(
        text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_suffix}")
    )
    return True


def _ensure_index(index_name: str, table_name: str, columns_csv: str):
    db.session.execute(
        text(
            f"CREATE INDEX IF NOT EXISTS {index_name} "
            f"ON {table_name} ({columns_csv})"
        )
    )


def migrate():
    app = create_app()
    with app.app_context():
        changed = 0

        for name, ddl in POST_COLUMNS.items():
            if _ensure_column("posts", name, ddl):
                changed += 1

        for name, ddl in USER_COLUMNS.items():
            if _ensure_column("users", name, ddl):
                changed += 1

        # Ensure new table exists.
        db.create_all()

        _ensure_index("ix_posts_is_hidden", "posts", "is_hidden")
        _ensure_index("ix_posts_hidden_at", "posts", "hidden_at")
        _ensure_index("ix_posts_purge_after", "posts", "purge_after")
        _ensure_index("ix_posts_hidden_by_report_id", "posts", "hidden_by_report_id")
        _ensure_index("ix_users_is_suspended", "users", "is_suspended")
        _ensure_index("ix_users_suspended_at", "users", "suspended_at")
        _ensure_index("ix_users_purge_after", "users", "purge_after")
        _ensure_index(
            "ix_users_suspended_by_report_id",
            "users",
            "suspended_by_report_id",
        )

        db.session.commit()
        print(f"[✓] Migration complete. Added/updated {changed} columns.")


if __name__ == "__main__":
    migrate()
