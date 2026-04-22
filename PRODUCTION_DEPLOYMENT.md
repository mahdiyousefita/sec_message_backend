# Production Deployment Notes

## Runtime

- Do not run `python run.py` in production.
- Use Gunicorn with Eventlet workers for Socket.IO:

```bash
gunicorn -k eventlet -w 1 -b 0.0.0.0:5000 'run:app'
```

- If you scale to multiple workers/instances, configure `SOCKETIO_MESSAGE_QUEUE` (Redis).

## Async task worker (task #6)

- Enable worker-backed side effects (activity fan-out, group side effects, cleanup tasks, media post-process hooks):

```bash
APP_ENV=production
ASYNC_TASKS_ENABLED=true
ASYNC_TASK_QUEUE_NAME=sec_message:async_tasks
ASYNC_TASK_RETRY_QUEUE_NAME=sec_message:async_tasks:retry
ASYNC_TASK_FAILED_QUEUE_NAME=sec_message:async_tasks:failed
ASYNC_TASK_WORKER_BLOCK_TIMEOUT_SECONDS=5
ASYNC_TASK_MAX_RETRIES=2
ASYNC_TASK_RETRY_BACKOFF_BASE_SECONDS=1.0
ASYNC_TASK_RETRY_BACKOFF_MAX_SECONDS=30.0
ASYNC_TASK_MIN_WORKER_COUNT=1
ASYNC_TASK_WORKER_HEARTBEAT_STALE_SECONDS=30
ASYNC_TASK_WORKER_STARTUP_STRICT=true
ASYNC_TASK_INLINE_FALLBACK=false
```

- Run at least one dedicated worker process:

```bash
python run_async_worker.py
```

- Optional worker ID for clearer health logs:

```bash
python run_async_worker.py --worker-id worker-a
```

- Optional: process a single pending task (debug):

```bash
python run_async_worker.py --once
```

## Database

- `DATABASE_URL` is required in production.
- If it is missing, the app falls back to `sqlite:///messenger.db` for local development only.
- Recommended production URL format:

```bash
DATABASE_URL=postgresql+psycopg2://user:password@host:5432/dbname
```

## Performance index migration

- Apply/ensure performance indexes (idempotent):

```bash
python migrate_add_performance_indexes.py
```

- Verify index presence on the runtime DB:

```bash
python check_performance_indexes.py
```

- Roll back only task-5 indexes if needed:

```bash
python migrate_add_performance_indexes.py --rollback
```

- Rollback-safe indexes removed by `--rollback`:
  - `ix_posts_author_created`
  - `ix_posts_hidden_created`
  - `ix_comments_post_parent_created`
  - `ix_blocks_blocked_blocker`
  - `ix_group_members_user_group`

## Android release checklist (APK + mapping)

- When publishing a new Android APK on the website, upload the matching R8/ProGuard `mapping.txt` in the admin panel:
  - `Admin Panel -> App Updates -> Release Mapping Upload (R8)`
- Android mapping file path after release build:
  - `android/sec_message/app/build/outputs/mapping/release/mapping.txt`
- Use the same app version (and version code if available) as the released APK.
- Crash logs in `Admin Panel -> Crash Logs` are deobfuscated only when the matching mapping file is uploaded.

## Background moderation cleanup

- Moderation retention cleanup now runs in a background worker.
- Control it with:

```bash
MODERATION_CLEANUP_BACKGROUND_ENABLED=true
MODERATION_CLEANUP_INTERVAL_SECONDS=300
```

- If `ASYNC_TASKS_ENABLED=true`, cleanup scheduler enqueues cleanup jobs and `run_async_worker.py` executes them.
- If the queue is unavailable/disabled, cleanup falls back to inline execution in the scheduler.

## Group side-effects behavior in production

- With `APP_ENV=production`, `send_group_message` requires worker-backed side-effect enqueue.
- If enqueue fails, the socket handler returns `message_error` and does **not** run inline side-effects.
- This keeps production latency predictable and makes worker failure/retry visibility explicit in logs.
- Exhausted async tasks are moved to `ASYNC_TASK_FAILED_QUEUE_NAME` (dead-letter list).
- Worker and API startup logs now include queue depth, retry depth, failed depth, and processing failure rate counters.
- `run_async_worker.py` sets `ASYNC_TASK_SKIP_STARTUP_WORKER_CHECK=true` automatically so the first worker can boot before API strict checks pass.
