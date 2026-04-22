import argparse
import os
from uuid import uuid4


# Avoid nested in-process cleanup scheduler when using dedicated workers.
os.environ.setdefault("MODERATION_CLEANUP_BACKGROUND_ENABLED", "false")
# Worker process should not fail its own startup because no heartbeat exists yet.
os.environ.setdefault("ASYNC_TASK_SKIP_STARTUP_WORKER_CHECK", "true")

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
    parser.add_argument(
        "--worker-id",
        default=None,
        help="Stable worker identifier used for startup health checks.",
    )
    parser.add_argument(
        "--health-log-every",
        type=int,
        default=50,
        help="Log queue depth and failure rate every N processed tasks.",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    app = create_app()
    processed = 0
    startup_snapshot_logged = False
    worker_id = (args.worker_id or "").strip() or f"worker-{uuid4().hex[:12]}"
    health_log_every = max(1, int(args.health_log_every or 50))

    app.logger.info(
        "Starting async task worker id=%s once=%s max_tasks=%s block_timeout=%s queue_enabled=%s",
        worker_id,
        args.once,
        args.max_tasks,
        args.block_timeout_seconds,
        app.config.get("ASYNC_TASKS_ENABLED", False),
    )

    while True:
        with app.app_context():
            active_workers = async_task_service.record_worker_heartbeat(
                worker_id=worker_id,
                source="run_async_worker",
            )
            did_process = async_task_service.process_one_pending_task(
                block_timeout_seconds=args.block_timeout_seconds,
            )
            if not startup_snapshot_logged:
                snapshot = async_task_service.get_operational_snapshot()
                app.logger.info(
                    "Async worker health active_workers=%s queue_depth=%s retry_depth=%s failed_depth=%s failure_rate=%.3f",
                    active_workers,
                    snapshot["queue_depth"],
                    snapshot["retry_queue_depth"],
                    snapshot["failed_queue_depth"],
                    snapshot["process_failure_rate"],
                )
                startup_snapshot_logged = True

        if did_process:
            processed += 1
            if processed % health_log_every == 0:
                with app.app_context():
                    snapshot = async_task_service.get_operational_snapshot()
                app.logger.info(
                    "Async worker progress id=%s processed=%s queue_depth=%s retry_depth=%s failed_depth=%s failure_rate=%.3f",
                    worker_id,
                    processed,
                    snapshot["queue_depth"],
                    snapshot["retry_queue_depth"],
                    snapshot["failed_queue_depth"],
                    snapshot["process_failure_rate"],
                )

        if args.once:
            break

        if args.max_tasks is not None and processed >= max(1, args.max_tasks):
            break


if __name__ == "__main__":
    main()
