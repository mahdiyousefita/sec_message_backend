"""
Runtime check for required backend performance indexes.

Run from project root:
    source venv/bin/activate
    python check_performance_indexes.py
"""

import os

from sqlalchemy import create_engine

from app.performance_indexes import (
    collect_lookup_coverage_gaps,
    collect_missing_managed_indexes,
)


def _database_url() -> str:
    explicit_url = os.getenv("DATABASE_URL", "").strip()
    if explicit_url:
        return explicit_url

    project_root = os.path.dirname(os.path.abspath(__file__))
    sqlite_path = os.path.join(project_root, "instance", "messenger.db")
    return f"sqlite:///{sqlite_path}"


def main() -> int:
    engine = create_engine(_database_url())
    missing_indexes = collect_missing_managed_indexes(engine)
    lookup_gaps = collect_lookup_coverage_gaps(engine)

    if missing_indexes:
        print("[!] Missing managed indexes:")
        for spec in missing_indexes:
            print(
                f"  - {spec.index_name} "
                f"({spec.table_name}: {spec.ddl_target})"
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


if __name__ == "__main__":
    raise SystemExit(main())
