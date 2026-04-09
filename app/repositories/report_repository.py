from datetime import datetime

from app.db import db
from app.models.report_model import PostReport


def create_report(
    reporter_id: int,
    post_id: int,
    report_type: str,
    description: str | None = None,
) -> PostReport:
    report = PostReport(
        reporter_id=reporter_id,
        post_id=post_id,
        report_type=report_type,
        description=description,
    )
    db.session.add(report)
    db.session.flush()
    return report


def get_by_id(report_id: int) -> PostReport | None:
    return PostReport.query.get(report_id)


def get_pending_by_reporter_and_post(
    reporter_id: int,
    post_id: int,
) -> PostReport | None:
    return (
        PostReport.query
        .filter_by(
            reporter_id=reporter_id,
            post_id=post_id,
            status="pending",
        )
        .first()
    )


def list_reports(
    page: int,
    limit: int,
    *,
    status: str | None = None,
    report_type: str | None = None,
):
    query = PostReport.query

    if status:
        query = query.filter(PostReport.status == status)
    if report_type:
        query = query.filter(PostReport.report_type == report_type)

    query = query.order_by(PostReport.created_at.desc(), PostReport.id.desc())
    total = query.count()
    items = query.offset((page - 1) * limit).limit(limit).all()
    return total, items


def list_expired_handled(now: datetime, limit: int | None = None):
    query = (
        PostReport.query
        .filter(
            PostReport.status == "handled",
            PostReport.decision_expires_at.isnot(None),
            PostReport.decision_expires_at <= now,
        )
        .order_by(PostReport.decision_expires_at.asc(), PostReport.id.asc())
    )
    if limit is not None:
        query = query.limit(max(int(limit), 1))
    return query.all()
