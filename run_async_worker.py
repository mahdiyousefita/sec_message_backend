import argparse
import os


# Avoid nested in-process cleanup scheduler when using dedicated workers.
os.environ.setdefault("MODERATION_CLEANUP_BACKGROUND_ENABLED", "false")

from app import create_app  # noqa: E402
from app.services import async_task_service  # noqa: E402


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Run async task worker for notifications/cleanup side effects.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process one task (if present) and exit.",
    )
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="Process at most N tasks and then exit.",
    )
    parser.add_argument(
        "--block-timeout-seconds",
        type=int,
        default=None,
        help="Worker poll timeout. Defaults to ASYNC_TASK_WORKER_BLOCK_TIMEOUT_SECONDS.",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    app = create_app()
    processed = 0

    app.logger.info(
        "Starting async task worker once=%s max_tasks=%s block_timeout=%s queue_enabled=%s",
        args.once,
        args.max_tasks,
        args.block_timeout_seconds,
        app.config.get("ASYNC_TASKS_ENABLED", False),
    )

    while True:
        with app.app_context():
            did_process = async_task_service.process_one_pending_task(
                block_timeout_seconds=args.block_timeout_seconds,
            )

        if did_process:
            processed += 1

        if args.once:
            break

        if args.max_tasks is not None and processed >= max(1, args.max_tasks):
            break


if __name__ == "__main__":
    main()
