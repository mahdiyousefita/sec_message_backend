"""
One-time migration: ensure performance indexes for hot feed/comment/report queries.

Run from project root:
    source venv/bin/activate
    python migrate_add_performance_indexes.py

Optional:
    python migrate_add_performance_indexes.py --check
    python migrate_add_performance_indexes.py --rollback
"""

import argparse
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in some environments
    load_dotenv = None

from app.performance_indexes import (
    ROLLBACK_SAFE_INDEX_NAMES,
    collect_lookup_coverage_gaps,
    collect_missing_managed_indexes,
    drop_indexes,
    ensure_performance_indexes,
)


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if load_dotenv:
    load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env"))


def _database_url() -> str:
    explicit_url = os.getenv("DATABASE_URL", "").strip()
    if explicit_url:
        return explicit_url

    sqlite_path = os.path.join(PROJECT_ROOT, "instance", "messenger.db")
    return f"sqlite:///{sqlite_path}"


def _build_session():
    engine = create_engine(_database_url())
    session = sessionmaker(bind=engine)()
    return engine, session


def migrate():
    engine, session = _build_session()
    try:
        ensured, created = ensure_performance_indexes(session, engine)
    finally:
        session.close()
        engine.dispose()

    print(
        "[✓] Performance index migration complete. "
        f"Ensured {ensured} indexes, created {created}."
    )


def rollback():
    engine, session = _build_session()
    try:
        dropped = drop_indexes(session, engine, ROLLBACK_SAFE_INDEX_NAMES)
    finally:
        session.close()
        engine.dispose()

    print(
        "[✓] Rollback complete. Dropped "
        f"{dropped} task-5 indexes."
    )


def check():
    engine = create_engine(_database_url())
    missing_indexes = collect_missing_managed_indexes(engine)
    lookup_gaps = collect_lookup_coverage_gaps(engine)

    if missing_indexes:
        print("[!] Missing managed indexes:")
        for item in missing_indexes:
            print(
                f"  - {item.index_name} "
                f"({item.table_name}: {item.ddl_target})"
            )
    else:
        print("[✓] All managed indexes are present.")

    if lookup_gaps:
        print("[!] Missing bidirectional lookup coverage:")
        for gap in lookup_gaps:
            print(f"  - {gap.table_name} ({', '.join(gap.columns)})")
    else:
        print("[✓] Follows/blocks/group_members lookup coverage is complete.")

    return 0 if not missing_indexes and not lookup_gaps else 1


def parse_args():
    parser = argparse.ArgumentParser(description="Manage performance DB indexes.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate required indexes and lookup coverage.",
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Drop task-5 rollback-safe indexes.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.check:
        raise SystemExit(check())
    if args.rollback:
        rollback()
        raise SystemExit(0)
    migrate()
    raise SystemExit(0)
