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


def _runtime_environment() -> str:
    if has_app_context():
        env_value = (
            current_app.config.get("APP_ENV")
            or current_app.config.get("ENV")
            or ""
        )
    else:
        env_value = (
            os.getenv("APP_ENV")
            or os.getenv("FLASK_ENV")
            or ""
        )
    normalized = str(env_value).strip().lower()
    return normalized or "development"


def _is_production_environment() -> bool:
    return _runtime_environment() in {"prod", "production"}


def _queue_name() -> str:
    value = _config("ASYNC_TASK_QUEUE_NAME", "sec_message:async_tasks")
    return str(value).strip() or "sec_message:async_tasks"


def _retry_queue_name() -> str:
    value = _config("ASYNC_TASK_RETRY_QUEUE_NAME", "")
    normalized = str(value).strip()
    if normalized:
        return normalized
    return f"{_queue_name()}:retry"


def _failed_queue_name() -> str:
    value = _config("ASYNC_TASK_FAILED_QUEUE_NAME", "")
    normalized = str(value).strip()
    if normalized:
        return normalized
    return f"{_queue_name()}:failed"


def _metrics_key() -> str:
    value = _config("ASYNC_TASK_METRICS_KEY", "")
    normalized = str(value).strip()
    if normalized:
        return normalized
    return f"{_queue_name()}:metrics"


def _worker_registry_key() -> str:
    value = _config("ASYNC_TASK_WORKER_REGISTRY_KEY", "")
    normalized = str(value).strip()
    if normalized:
        return normalized
    return f"{_queue_name()}:workers"


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


def _retry_backoff_base_seconds() -> float:
    value = _config("ASYNC_TASK_RETRY_BACKOFF_BASE_SECONDS", 1.0)
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        seconds = 1.0
    return max(0.0, seconds)


def _retry_backoff_max_seconds() -> float:
    value = _config("ASYNC_TASK_RETRY_BACKOFF_MAX_SECONDS", 30.0)
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        seconds = 30.0
    return max(0.1, seconds)


def _min_worker_count() -> int:
    value = _config("ASYNC_TASK_MIN_WORKER_COUNT", 1)
    try:
        count = int(value)
    except (TypeError, ValueError):
        count = 1
    return max(1, count)


def _worker_heartbeat_stale_seconds() -> int:
    value = _config("ASYNC_TASK_WORKER_HEARTBEAT_STALE_SECONDS", 30)
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        seconds = 30
    return max(5, seconds)


def _startup_worker_check_strict() -> bool:
    value = _config("ASYNC_TASK_WORKER_STARTUP_STRICT", False)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


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


def _increment_metric(field: str, amount: int = 1):
    if not field:
        return
    try:
        redis_client.hincrby(_metrics_key(), field, int(amount))
    except Exception:
        # Metrics writes are best-effort and must never block task handling.
        return


def _set_metric(field: str, value):
    if not field:
        return
    try:
        redis_client.hset(_metrics_key(), field, value)
    except Exception:
        return


def _queue_depth_safe(queue_name: str) -> int:
    try:
        return int(redis_client.llen(queue_name) or 0)
    except Exception:
        return 0


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
    expected_membership_version: int | None = None,
    source: str,
) -> bool:
    normalized_expected_version = None
    if expected_membership_version is not None:
        try:
            normalized_expected_version = int(expected_membership_version)
        except (TypeError, ValueError):
            normalized_expected_version = None

    return enqueue_task(
        task_type=TASK_TYPE_GROUP_MESSAGE_SIDE_EFFECTS,
        payload={
            "sender": sender,
            "group_id": int(group_id),
            "message_payload": message_payload,
            "expected_membership_version": normalized_expected_version,
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

    started_at = time.perf_counter()
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
        duration_ms = int((time.perf_counter() - started_at) * 1000)
        queue_depth = _queue_depth_safe(queue_name)
        _increment_metric("enqueue_success_total")
        _increment_metric("enqueue_latency_ms_total", max(0, duration_ms))
        _set_metric("queue_depth_last", queue_depth)
        _set_metric("last_enqueue_at", datetime.now(timezone.utc).isoformat())
        _set_metric("last_enqueue_task_type", task_type)
        return True
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started_at) * 1000)
        _increment_metric("enqueue_failed_total")
        _increment_metric("enqueue_latency_ms_total", max(0, duration_ms))
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
        _increment_metric("process_success_total")
        _set_metric("last_processed_task_type", task.get("task_type"))
        _set_metric("last_processed_at", datetime.now(timezone.utc).isoformat())
    except Exception as exc:
        _increment_metric("process_failed_total")
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
    _promote_due_retry_tasks(max_items=100)
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
        _increment_metric("dequeue_failed_total")
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
        _increment_metric("dropped_total")
        _record_failed_task(task, error)
        return

    next_task = dict(task)
    next_attempt = attempt + 1
    next_task["attempt"] = next_attempt
    next_task["last_error"] = str(error)
    next_task["last_error_at"] = datetime.now(timezone.utc).isoformat()
    retry_delay_seconds = _retry_delay_seconds(next_attempt)
    next_task["retry_delay_seconds"] = retry_delay_seconds
    next_task["retry_due_at"] = datetime.fromtimestamp(
        time.time() + retry_delay_seconds,
        tz=timezone.utc,
    ).isoformat()

    retry_queue_name = _retry_queue_name()
    retry_due_epoch = time.time() + retry_delay_seconds
    serialized_task = json.dumps(next_task)
    try:
        redis_client.zadd(retry_queue_name, {serialized_task: retry_due_epoch})
        _increment_metric("requeued_total")
        _set_metric("retry_queue_depth_last", _retry_queue_depth_safe())
        _logger().warning(
            "async_task_retry_scheduled id=%s type=%s next_attempt=%s delay_seconds=%.2f",
            task.get("task_id"),
            task.get("task_type"),
            next_attempt,
            retry_delay_seconds,
        )
    except Exception as exc:
        queue_name = _queue_name()
        _increment_metric("retry_schedule_failed_total")
        _logger().error(
            "async_task_requeue_failed id=%s type=%s retry_queue=%s error=%s",
            task.get("task_id"),
            task.get("task_type"),
            retry_queue_name,
            exc,
        )
        try:
            redis_client.rpush(queue_name, serialized_task)
            _increment_metric("requeued_fallback_total")
        except Exception as push_exc:
            _logger().error(
                "async_task_requeue_fallback_failed id=%s type=%s queue=%s error=%s",
                task.get("task_id"),
                task.get("task_type"),
                queue_name,
                push_exc,
            )
            _increment_metric("dropped_total")
            _record_failed_task(next_task, push_exc)


def _retry_queue_depth_safe() -> int:
    queue_name = _retry_queue_name()
    try:
        return int(redis_client.zcard(queue_name) or 0)
    except Exception:
        return 0


def _retry_delay_seconds(next_attempt: int) -> float:
    base_seconds = _retry_backoff_base_seconds()
    if base_seconds <= 0:
        return 0.0
    exponent = max(0, int(next_attempt) - 1)
    delay = base_seconds * (2 ** exponent)
    return min(delay, _retry_backoff_max_seconds())


def _promote_due_retry_tasks(*, max_items: int = 100):
    max_items = max(1, int(max_items))
    retry_queue_name = _retry_queue_name()
    primary_queue_name = _queue_name()
    now_epoch = time.time()

    due_members: list[str] = []
    try:
        if hasattr(redis_client, "zrangebyscore"):
            due_members = redis_client.zrangebyscore(  # type: ignore[attr-defined]
                retry_queue_name,
                min="-inf",
                max=now_epoch,
                start=0,
                num=max_items,
            )
        else:
            scored_members = redis_client.zrange(
                retry_queue_name,
                0,
                max_items - 1,
                withscores=True,
            )
            for member, score in scored_members:
                try:
                    score_value = float(score)
                except (TypeError, ValueError):
                    score_value = now_epoch + 1
                if score_value <= now_epoch:
                    due_members.append(member)
    except Exception as exc:
        _logger().warning(
            "async_task_retry_dequeue_failed queue=%s error=%s",
            retry_queue_name,
            exc,
        )
        _increment_metric("retry_dequeue_failed_total")
        return

    promoted_count = 0
    for member in due_members:
        normalized_member = (
            member.decode("utf-8")
            if isinstance(member, bytes)
            else member
        )
        try:
            removed = redis_client.zrem(retry_queue_name, member)
            if int(removed or 0) <= 0:
                continue
            redis_client.rpush(primary_queue_name, normalized_member)
            promoted_count += 1
        except Exception as exc:
            _logger().warning(
                "async_task_retry_promote_failed queue=%s error=%s",
                retry_queue_name,
                exc,
            )
            _increment_metric("retry_promote_failed_total")

    if promoted_count > 0:
        _increment_metric("retry_promoted_total", promoted_count)
        _set_metric("retry_queue_depth_last", _retry_queue_depth_safe())


def _record_failed_task(task: dict, error: Exception):
    failed_queue_name = _failed_queue_name()
    failed_envelope = dict(task)
    failed_envelope["failed_at"] = datetime.now(timezone.utc).isoformat()
    failed_envelope["failure_error"] = str(error)
    try:
        redis_client.rpush(failed_queue_name, json.dumps(failed_envelope))
        _increment_metric("failed_task_recorded_total")
        _set_metric("failed_queue_depth_last", _queue_depth_safe(failed_queue_name))
    except Exception as exc:
        _logger().error(
            "async_task_failed_record_write_failed queue=%s id=%s type=%s error=%s",
            failed_queue_name,
            task.get("task_id"),
            task.get("task_type"),
            exc,
        )


def should_fallback_inline(*, task_type: str | None = None) -> bool:
    if not _inline_fallback_enabled():
        return False

    # In production, group side effects must stay worker-backed to avoid
    # socket-path blocking and to keep failure behavior explicit.
    if _is_production_environment() and task_type == TASK_TYPE_GROUP_MESSAGE_SIDE_EFFECTS:
        return False

    return True


def get_operational_snapshot() -> dict:
    metrics = {}
    try:
        raw_metrics = redis_client.hgetall(_metrics_key()) or {}
    except Exception:
        raw_metrics = {}

    for key, value in raw_metrics.items():
        normalized_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        normalized_value = value.decode("utf-8") if isinstance(value, bytes) else value
        metrics[normalized_key] = normalized_value

    process_success = int(metrics.get("process_success_total", 0) or 0)
    process_failed = int(metrics.get("process_failed_total", 0) or 0)
    processed_total = process_success + process_failed
    failure_rate = 0.0
    if processed_total > 0:
        failure_rate = process_failed / processed_total

    return {
        "queue_depth": _queue_depth_safe(_queue_name()),
        "retry_queue_depth": _retry_queue_depth_safe(),
        "failed_queue_depth": _queue_depth_safe(_failed_queue_name()),
        "active_worker_count": get_active_worker_count(),
        "process_failure_rate": failure_rate,
        "metrics": metrics,
    }


def _cleanup_stale_workers(*, now_epoch: float | None = None):
    now_epoch = time.time() if now_epoch is None else now_epoch
    stale_before = now_epoch - _worker_heartbeat_stale_seconds()
    registry_key = _worker_registry_key()
    try:
        if hasattr(redis_client, "zremrangebyscore"):
            redis_client.zremrangebyscore(registry_key, "-inf", stale_before)  # type: ignore[attr-defined]
            return

        entries = redis_client.zrange(registry_key, 0, -1, withscores=True)
        stale_members = [
            member
            for member, score in entries
            if float(score) < stale_before
        ]
        if stale_members:
            redis_client.zrem(registry_key, *stale_members)
    except Exception:
        return


def record_worker_heartbeat(*, worker_id: str, source: str) -> int:
    normalized_worker_id = str(worker_id or "").strip()
    if not normalized_worker_id:
        return 0

    now_epoch = time.time()
    registry_key = _worker_registry_key()
    try:
        redis_client.zadd(registry_key, {normalized_worker_id: now_epoch})
        _cleanup_stale_workers(now_epoch=now_epoch)
        active_count = int(redis_client.zcard(registry_key) or 0)
        _set_metric("last_worker_heartbeat_at", datetime.now(timezone.utc).isoformat())
        _set_metric("last_worker_heartbeat_source", source)
        _set_metric("active_worker_count_last", active_count)
        return active_count
    except Exception as exc:
        _logger().warning(
            "async_task_worker_heartbeat_failed source=%s worker_id=%s error=%s",
            source,
            normalized_worker_id,
            exc,
        )
        return 0


def get_active_worker_count() -> int:
    _cleanup_stale_workers()
    try:
        return int(redis_client.zcard(_worker_registry_key()) or 0)
    except Exception:
        return 0


def verify_worker_capacity_for_startup(*, source: str = "app_startup") -> bool:
    if not (_is_enabled() and _is_production_environment()):
        return True

    required_workers = _min_worker_count()
    active_workers = get_active_worker_count()
    snapshot = get_operational_snapshot()
    if active_workers >= required_workers:
        _logger().info(
            "async_task_worker_capacity_ok source=%s active_workers=%s required_workers=%s "
            "queue_depth=%s retry_depth=%s failed_depth=%s failure_rate=%.3f",
            source,
            active_workers,
            required_workers,
            snapshot["queue_depth"],
            snapshot["retry_queue_depth"],
            snapshot["failed_queue_depth"],
            snapshot["process_failure_rate"],
        )
        return True

    log_message = (
        "async_task_worker_capacity_low source=%s active_workers=%s required_workers=%s "
        "queue_depth=%s retry_depth=%s failed_depth=%s failure_rate=%.3f"
    )
    log_args = (
        source,
        active_workers,
        required_workers,
        snapshot["queue_depth"],
        snapshot["retry_queue_depth"],
        snapshot["failed_queue_depth"],
        snapshot["process_failure_rate"],
    )
    if _startup_worker_check_strict():
        _logger().error(log_message, *log_args)
        return False

    _logger().warning(log_message, *log_args)
    return True
