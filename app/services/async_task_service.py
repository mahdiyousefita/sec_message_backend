import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import redis
from flask import current_app, has_app_context

from app.extensions.redis_client import redis_client


TASK_TYPE_ACTIVITY_NOTIFICATION = "activity_notification_event"
TASK_TYPE_GROUP_MESSAGE_SIDE_EFFECTS = "group_message_side_effects"
TASK_TYPE_MODERATION_CLEANUP = "moderation_cleanup"
TASK_TYPE_MEDIA_POST_PROCESS = "media_post_process"


_enqueue_client = None


def _logger():
    if has_app_context():
        return current_app.logger
    return logging.getLogger(__name__)


def _config(name: str, fallback):
    if has_app_context():
        return current_app.config.get(name, fallback)
    return os.getenv(name, fallback)


def _is_enabled() -> bool:
    value = _config("ASYNC_TASKS_ENABLED", False)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _inline_fallback_enabled() -> bool:
    value = _config("ASYNC_TASK_INLINE_FALLBACK", True)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _queue_name() -> str:
    value = _config("ASYNC_TASK_QUEUE_NAME", "sec_message:async_tasks")
    return str(value).strip() or "sec_message:async_tasks"


def _block_timeout_seconds() -> int:
    value = _config("ASYNC_TASK_WORKER_BLOCK_TIMEOUT_SECONDS", 5)
    try:
        timeout = int(value)
    except (TypeError, ValueError):
        timeout = 5
    return max(1, timeout)


def _max_retries() -> int:
    value = _config("ASYNC_TASK_MAX_RETRIES", 2)
    try:
        retries = int(value)
    except (TypeError, ValueError):
        retries = 2
    return max(0, retries)


def _enqueue_socket_timeout_seconds() -> float:
    value = _config("ASYNC_TASK_ENQUEUE_SOCKET_TIMEOUT_SECONDS", 0.75)
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        timeout = 0.75
    return max(0.1, timeout)


def _enqueue_connect_timeout_seconds() -> float:
    value = _config("ASYNC_TASK_ENQUEUE_CONNECT_TIMEOUT_SECONDS", 0.75)
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        timeout = 0.75
    return max(0.1, timeout)


def _enqueue_redis_client():
    global _enqueue_client

    if _enqueue_client is not None:
        return _enqueue_client

    pool = getattr(redis_client, "connection_pool", None)
    if pool is None:
        return redis_client

    kwargs = dict(pool.connection_kwargs)
    kwargs["socket_timeout"] = _enqueue_socket_timeout_seconds()
    kwargs["socket_connect_timeout"] = _enqueue_connect_timeout_seconds()
    _enqueue_client = redis.Redis(**kwargs)
    return _enqueue_client


def enqueue_activity_notification_event(event_payload: dict, *, source: str) -> bool:
    return enqueue_task(
        task_type=TASK_TYPE_ACTIVITY_NOTIFICATION,
        payload=event_payload,
        source=source,
    )


def enqueue_group_message_side_effects(
    *,
    sender: str,
    group_id: int,
    message_payload: dict,
    source: str,
) -> bool:
    return enqueue_task(
        task_type=TASK_TYPE_GROUP_MESSAGE_SIDE_EFFECTS,
        payload={
            "sender": sender,
            "group_id": int(group_id),
            "message_payload": message_payload,
        },
        source=source,
    )


def enqueue_cleanup_task(
    *,
    force: bool,
    batch_size: int | None,
    source: str,
) -> bool:
    payload = {"force": bool(force)}
    if batch_size is not None:
        payload["batch_size"] = max(1, int(batch_size))

    return enqueue_task(
        task_type=TASK_TYPE_MODERATION_CLEANUP,
        payload=payload,
        source=source,
    )


def enqueue_media_post_process_task(
    *,
    post_id: int,
    media_items: list[dict],
    source: str,
) -> bool:
    return enqueue_task(
        task_type=TASK_TYPE_MEDIA_POST_PROCESS,
        payload={
            "post_id": int(post_id),
            "media_items": media_items or [],
        },
        source=source,
    )


def enqueue_task(*, task_type: str, payload: dict, source: str) -> bool:
    if not _is_enabled():
        return False

    task_envelope = {
        "task_id": str(uuid.uuid4()),
        "task_type": task_type,
        "payload": payload if isinstance(payload, dict) else {},
        "source": source,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
        "attempt": 0,
    }
    queue_name = _queue_name()

    try:
        _enqueue_redis_client().rpush(queue_name, json.dumps(task_envelope))
        return True
    except Exception as exc:
        _logger().warning(
            "async_task_enqueue_failed type=%s source=%s queue=%s error=%s",
            task_type,
            source,
            queue_name,
            exc,
        )
        return False


def process_one_pending_task(*, block_timeout_seconds: int | None = None) -> bool:
    task = _dequeue_task(block_timeout_seconds=block_timeout_seconds)
    if task is None:
        return False

    try:
        _process_task(task)
    except Exception as exc:
        _logger().exception(
            "async_task_failed id=%s type=%s attempt=%s error=%s",
            task.get("task_id"),
            task.get("task_type"),
            task.get("attempt", 0),
            exc,
        )
        _requeue_if_needed(task, exc)
    return True


def _dequeue_task(*, block_timeout_seconds: int | None):
    queue_name = _queue_name()
    timeout = (
        _block_timeout_seconds()
        if block_timeout_seconds is None
        else max(0, int(block_timeout_seconds))
    )

    try:
        raw_item = None
        if timeout > 0 and hasattr(redis_client, "blpop"):
            raw = redis_client.blpop(queue_name, timeout=timeout)
            if raw:
                raw_item = raw[1]
        elif timeout > 0:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                raw_item = redis_client.lpop(queue_name)
                if raw_item is not None:
                    break
                time.sleep(0.1)
        else:
            raw_item = redis_client.lpop(queue_name)
    except Exception as exc:
        _logger().warning(
            "async_task_dequeue_failed queue=%s error=%s",
            queue_name,
            exc,
        )
        return None

    if raw_item is None:
        return None

    if isinstance(raw_item, bytes):
        raw_item = raw_item.decode("utf-8")

    try:
        parsed = json.loads(raw_item)
    except Exception:
        _logger().warning("async_task_invalid_json payload=%r", raw_item)
        return None

    if not isinstance(parsed, dict):
        _logger().warning("async_task_invalid_envelope payload=%r", parsed)
        return None
    return parsed


def _process_task(task: dict):
    task_type = task.get("task_type")
    payload = task.get("payload")
    if not isinstance(payload, dict):
        payload = {}

    if task_type == TASK_TYPE_ACTIVITY_NOTIFICATION:
        _handle_activity_notification(payload)
        return
    if task_type == TASK_TYPE_GROUP_MESSAGE_SIDE_EFFECTS:
        _handle_group_message_side_effects(payload)
        return
    if task_type == TASK_TYPE_MODERATION_CLEANUP:
        _handle_cleanup(payload)
        return
    if task_type == TASK_TYPE_MEDIA_POST_PROCESS:
        _handle_media_post_process(payload)
        return

    _logger().warning(
        "async_task_unknown_type id=%s type=%s",
        task.get("task_id"),
        task_type,
    )


def _handle_activity_notification(payload: dict):
    from app.services import activity_notification_service

    activity_notification_service.process_async_notification_event(payload)


def _handle_group_message_side_effects(payload: dict):
    from app.services import group_notification_service

    group_notification_service.process_group_message_side_effects_task(payload)


def _handle_cleanup(payload: dict):
    from app.services import report_service

    force = bool(payload.get("force", True))
    batch_size = payload.get("batch_size")
    if batch_size is not None:
        try:
            batch_size = max(1, int(batch_size))
        except (TypeError, ValueError):
            batch_size = None

    report_service.run_scheduled_cleanup_with_metrics(
        force=force,
        batch_size=batch_size,
    )


def _handle_media_post_process(payload: dict):
    post_id = payload.get("post_id")
    media_items = payload.get("media_items")
    if not isinstance(media_items, list):
        media_items = []
    _logger().info(
        "media_post_process_task post_id=%s media_items=%s",
        post_id,
        len(media_items),
    )


def _requeue_if_needed(task: dict, error: Exception):
    attempt = int(task.get("attempt", 0))
    if attempt >= _max_retries():
        _logger().error(
            "async_task_dropped id=%s type=%s attempts=%s error=%s",
            task.get("task_id"),
            task.get("task_type"),
            attempt,
            error,
        )
        return

    next_task = dict(task)
    next_task["attempt"] = attempt + 1
    next_task["last_error"] = str(error)
    next_task["last_error_at"] = datetime.now(timezone.utc).isoformat()

    queue_name = _queue_name()
    try:
        redis_client.rpush(queue_name, json.dumps(next_task))
    except Exception as exc:
        _logger().error(
            "async_task_requeue_failed id=%s type=%s error=%s",
            task.get("task_id"),
            task.get("task_type"),
            exc,
        )


def should_fallback_inline() -> bool:
    return _inline_fallback_enabled()
