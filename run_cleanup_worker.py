import argparse
import os
import time


# Prevent nested in-process cleanup threads when this dedicated worker starts the app.
os.environ.setdefault("MODERATION_CLEANUP_BACKGROUND_ENABLED", "false")

from app import create_app  # noqa: E402
from app.services import async_task_service, report_service  # noqa: E402


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Run moderation cleanup in one-shot (cron) or loop mode.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one cleanup cycle and exit (recommended for cron).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help="Loop interval in seconds. Defaults to MODERATION_CLEANUP_INTERVAL_SECONDS.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Max users/posts/reports processed per cleanup cycle.",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    app = create_app()
    default_interval = max(
        int(app.config.get("MODERATION_CLEANUP_INTERVAL_SECONDS", 300)),
        10,
    )
    interval_seconds = max(int(args.interval_seconds or default_interval), 10)

    app.logger.info(
        "Starting external moderation cleanup worker once=%s interval=%ss batch_size=%s",
        args.once,
        interval_seconds,
        args.batch_size or app.config.get("MODERATION_CLEANUP_BATCH_SIZE", 100),
    )

    while True:
        with app.app_context():
            enqueued = async_task_service.enqueue_cleanup_task(
                force=True,
                batch_size=args.batch_size,
                source="cleanup_worker",
            )
            if not enqueued:
                report_service.run_scheduled_cleanup_with_metrics(
                    force=True,
                    batch_size=args.batch_size,
                )
        if args.once:
            break
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
